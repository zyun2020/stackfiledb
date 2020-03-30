
#include "db/stack_db.h"

#include "db/store_writer.h"
#include "db/filename.h"

#include "util/coding.h"
#include "util/crc32c.h"
#include "util/mutexlock.h"

#include "db/log_reader.h"
#include "db/log_writer.h"

namespace stackfiledb {
 
    DB::~DB() = default;

    const int kMax_Bucket = 256;
    const int kMax_ReaderCache = 32;
    const int kLog_Batch_Reserve = 100;
    const int kDB_Meta_Size = 24;

    // Information kept for every waiting writer
    struct StackDB::Log_Writer {
        explicit Log_Writer(port::Mutex* mu)
            : batch(nullptr), sync(false), done(false), cv(mu) {}

        Status status;
        LogBatch* batch;
        bool sync;
        bool done;
        port::CondVar cv;
    };

    // Fix user-supplied options to be reasonable
    template <class T, class V>
    static void ClipToRange(T* ptr, V minvalue, V maxvalue) {
        if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
        if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
    }
    static Options SanitizeOptions(const std::string& dbname, const Options& src) {
        Options result = src;
        ClipToRange(&result.max_open_files, 64, 50000);
        ClipToRange(&result.max_file_size, 1 << 28, 1 << 31);       
        ClipToRange(&result.bucket_count, 1, 8);

        ClipToRange(&result.log_buffer_count, 1000, 50000);
        ClipToRange(&result.max_reader_count, 10, 600);
        ClipToRange(&result.max_writer_count, 1, 16);

        Status s;
        if (result.info_log == nullptr) {
            // Open a log file in the same directory as the db
            s = src.env->CreateDir(dbname);  // In case it does not exist
            s = src.env->RenameFile(InfoLogFileName(dbname), OldInfoLogFileName(dbname));
            s = src.env->NewLogger(InfoLogFileName(dbname), &result.info_log);
            if (!s.ok()) {
                // No place suitable for logging
                result.info_log = nullptr;
            }
        }
        /*if (result.block_cache == nullptr) {
            result.block_cache = NewLRUCache(8 << 20);
        }*/
        return result;
    }

    static int StoreFileCacheSize(const Options& sanitized_options) {
        // Reserve ten files or so for other uses and give the rest to TableCache.
        return sanitized_options.max_open_files - (1 >> sanitized_options.bucket_count);
    }

    StackDB::StackDB(const Options& raw_options, const std::string& dbname)
        : env_(raw_options.env),
        options_(SanitizeOptions(dbname, raw_options)),
        owns_info_log_(options_.info_log != raw_options.info_log),
        dbname_(dbname),
        store_index_(new StoreIndex()),
        store_cache_(new StoreFileCache(dbname_, options_, StoreFileCacheSize(options_))),
        db_lock_(nullptr),
        shutting_down_(false),
        background_work_finished_signal_(&mutex_),
        mem_(nullptr),
        imm_(nullptr),
        has_imm_(false),
        logfile_(nullptr),
        log_(nullptr),
        logfile_number_(0),
        prev_logfile_number_(0),        
        owns_cache_(false),
        reader_count_(0),
        max_writer_sem_(options_.max_writer_count),
        max_reader_sem_(options_.max_reader_count),
        tmp_batch_(new LogBatch(kLog_Batch_Reserve)),
        background_compaction_scheduled_(false) {

        db_meta_.create_ts = env_->NowMicros();
        db_meta_.bucket_count = 1 << options_.bucket_count;
        db_meta_.key_bucekt_mapping = options_.key_bucket_mapping;
        db_meta_.gen_key_mode = options_.generate_key_mode;
        db_meta_.opt_bucket = options_.opt_bucket_type;
    }

    StackDB::~StackDB() {
        mutex_.Lock();
        shutting_down_.store(true, std::memory_order_release);
        while (background_compaction_scheduled_) {
            background_work_finished_signal_.Wait();
        }
        mutex_.Unlock();


        for (StoreReader* reader : reader_cache_) {
            delete reader;
        }
        reader_cache_.clear();

        for (StackBucket* bucket : stack_buckets_) {
            delete bucket;
        }
        stack_buckets_.clear();

        if (mem_ != nullptr)
            mem_->Unref();

        if (imm_ != nullptr)
            imm_->Unref();

        delete tmp_batch_;
        delete log_;
        delete logfile_;
        delete store_cache_;
        delete store_index_;

        if (db_lock_ != nullptr) {
            env_->UnlockFile(db_lock_);
        }
    }

    void StackDB::MetaEncodeTo(std::string* dst) const {
        PutFixed32(dst, kStack_DB_Magic);
        PutFixed32(dst, kDB_Meta_Size);
        PutFixed64(dst, db_meta_.create_ts);
        PutFixed32(dst, db_meta_.bucket_count);
        PutFixed32(dst, db_meta_.key_bucekt_mapping);
        PutFixed32(dst, db_meta_.gen_key_mode);
        PutFixed32(dst, db_meta_.opt_bucket);

        uint32_t crc = crc32c::Value(dst->data(), dst->size());
        PutFixed32(dst, crc32c::Mask(crc));
    }

    Status StackDB::MetaDecodeFrom(const Slice& src) {
        if (src.size() < 8) {
            return  Status::Corruption("db meta too small");
        }
        int pos = 4;
        const char* ptr = src.data();
        uint32_t magic = DecodeFixed32(ptr);
        uint32_t size = DecodeFixed32(&ptr[pos]); pos += 4;

        if (size != kDB_Meta_Size || size != src.size() || magic != kStack_DB_Magic) {
            return  Status::Corruption("invalid db meta");
        }

        db_meta_.create_ts = DecodeFixed32(&ptr[pos]); pos += 4;
        db_meta_.bucket_count = DecodeFixed32(&ptr[pos]); pos += 4;
        db_meta_.key_bucekt_mapping = DecodeFixed32(&ptr[pos]); pos += 4;
        db_meta_.gen_key_mode = DecodeFixed32(&ptr[pos]); pos += 4;
        db_meta_.opt_bucket = DecodeFixed32(&ptr[pos]); pos += 4;

        uint32_t saved_crc = DecodeFixed32(&ptr[pos]); pos += 4;
        uint32_t crc = crc32c::Value(src.data(), src.size());
        if (crc32c::Unmask(saved_crc) != crc) {
            return  Status::Corruption("invalid db meta crc");
        }

        return Status::OK();
    }

    Status DB::Open(const Options& options, const std::string& dbname, DB** dbptr) {
        *dbptr = nullptr;

        StackDB* impl = new StackDB(options, dbname);
        impl->mutex_.Lock();

        // Recover handles create_if_missing, error_if_exists
        Status s = impl->Recover();
        if (s.ok() && impl->mem_ == nullptr) {
            // Create new log and a corresponding memtable.
            uint32_t new_log_number = impl->logfile_number_ + 1;
            WritableFile* lfile;
            s = options.env->NewWritableFile(LogFileName(dbname, new_log_number),
                &lfile);
            if (s.ok()) {
                impl->prev_logfile_number_ = 0;
                impl->logfile_ = lfile;
                impl->logfile_number_ = new_log_number;
                impl->log_ = new log::Writer(lfile);
                impl->mem_ = new MemLog();
                impl->mem_->Ref();
            }
        }
        
        impl->mutex_.Unlock();
        if (s.ok()) {
            assert(impl->mem_ != nullptr);
            *dbptr = impl;
        }
        else {
            delete impl;
        }
        return s;
    }

    Status StackDB::NewDB() {
        Status s = env_->CreateDir(StoreDBDir(dbname_));
        if (!s.ok()) {
            return s;
        }

        const std::string meta_file = DBMetaFileName(dbname_);
        //write db meta file
        WritableFile* file;
        s = env_->NewWritableFile(meta_file, &file);
        if (s.ok()){
            std::string record;
            MetaEncodeTo(&record);
            s = file->Append(record);
            if (s.ok()) {
                s = file->Close();
            }
            delete file;
        }

        //create index file(sqlite db file)
        if (s.ok()) {
            s = store_index_->NewIndexDB(DBIndexFileName(dbname_));
        }
       
        //create bucket£¨storefile) dir
        if (s.ok()) {
            for (uint32_t i = 0; i < db_meta_.bucket_count; i++) {
                s = env_->CreateDir(StoreFileDir(dbname_, i + 1));
                if (!s.ok()) break;
            }
        }

        //Error: remove the above created
        if (!s.ok()) {
            for (uint32_t i = 0; i < db_meta_.bucket_count; i++) {
                s = env_->RemoveDir(StoreFileDir(dbname_, i + 1));
            }
            s = env_->RemoveDir(StoreDBDir(dbname_));
        }
        return s;
    }

    Status StackDB::Recover() {
        mutex_.AssertHeld();

        // Ignore error from CreateDir since the creation of the DB is
        // committed only when the descriptor is created, and this directory
        // may already exist from a previous failed creation attempt.
        Status s = env_->CreateDir(dbname_);
        assert(db_lock_ == nullptr);
        s = env_->LockFile(LockFileName(dbname_), &db_lock_);
        if (!s.ok()) {
            return s;
        }

        if (!env_->FileExists(DBMetaFileName(dbname_))) {
            if (options_.create_if_missing) {
                s = NewDB();
                if (!s.ok()) {
                    return s;
                }
            }
            else {
                return Status::InvalidArgument(
                    dbname_, "does not exist (create_if_missing is false)");
            }
        }
        else {
            if (options_.error_if_exists) {
                return Status::InvalidArgument(dbname_,
                    "exists (error_if_exists is true)");
            }
        }

        s = store_index_->OpenIndexDB(DBIndexFileName(dbname_));
        if (!s.ok()) {
            return s;
        }

        // Recover from all newer log file, filenumber greater than min_log
        uint32_t min_log = store_index_->GetMaxLogFileNumber();
        std::vector<std::string> filenames;
        s = env_->GetChildren(StoreDBDir(dbname_), &filenames);
        if (!s.ok()) {
            return s;
        }
        
        uint32_t number = 0;
        FileType type = FileType::kDBLockFile;
        std::vector<uint32_t> logs;
        for (size_t i = 0; i < filenames.size(); i++) {
            if (ParseFileName(filenames[i], &number, &type)) {
                if (type == FileType::kLogFile && number > min_log)
                    logs.push_back(number);
            }
        }

        // Recover in the order in which the logs were generated
        if (logs.size() > 0) {
            std::sort(logs.begin(), logs.end());
            for (size_t i = 0; i < logs.size(); i++) {
                s = RecoverLogFile(logs[i]);
                if (!s.ok()) {
                    return s;
                }
            }
            RemoveLogFile(logs[logs.size() - 1]);

            min_log = logs[logs.size() - 1];
        }

        s = RecoverBucket();
        if (!s.ok()) {
            return s;
        }

        prev_logfile_number_ = 0;
        logfile_number_ = min_log;

        return Status::OK();
    }

    Status StackDB::RecoverBucket() {
        //Check if bucket exists
        for (int i = 0; i < db_meta_.bucket_count; i++) {
            if (!env_->FileExists(StoreFileDir(dbname_, i + 1))) {
                char buf[50];
                snprintf(buf, sizeof(buf), "%d missing files; e.g.",
                    static_cast<int>(i + 1));
                return Status::Corruption(buf, StoreFileDir(dbname_, i + 1));
            }
        }

        //new bucket
        Status s;
        for (int i = 0; i < db_meta_.bucket_count; i++) {
            StackBucket* bucket = new StackBucket(dbname_, &options_, store_cache_, store_index_, i + 1);
            stack_buckets_.push_back(bucket);
            s = store_index_->Recover(bucket);
            if (!s.ok()) return s;
        }

        return s;
    }

    Status StackDB::RecoverLogFile(uint32_t log_number) {
        struct LogReporter : public log::Reader::Reporter {
            Env* env;
            Logger* info_log;
            const char* fname;
            Status* status;  // null if options_.paranoid_checks==false
            void Corruption(size_t bytes, const Status& s) override {
                Log(info_log, "%s%s: dropping %d bytes; %s",
                    (this->status == nullptr ? "(ignoring error) " : ""), fname,
                    static_cast<int>(bytes), s.ToString().c_str());
                if (this->status != nullptr && this->status->ok()) *this->status = s;
            }
        };

        mutex_.AssertHeld();

        // Open the log file
        std::string fname = LogFileName(dbname_, log_number);
        SequentialFile* file;
        Status status = env_->NewSequentialFile(fname, &file);
        if (!status.ok()) {
            MaybeIgnoreError(&status);
            return status;
        }

        // Create the log reader.
        LogReporter reporter;
        reporter.env = env_;
        reporter.info_log = options_.info_log;
        reporter.fname = fname.c_str();
        reporter.status = (options_.paranoid_checks ? &status : nullptr);
        // We intentionally make log::Reader do checksumming even if
        // paranoid_checks==false so that corruptions cause entire commits
        // to be skipped instead of propagating bad information (like overly
        // large sequence numbers).
        log::Reader reader(file, &reporter, true /*checksum*/, 0 /*initial_offset*/);
        Log(options_.info_log, "Recovering log #%u", log_number);

        // Read all the records and add to a memtable
        std::string scratch;
        Slice record;
        LogBatch batch;
        MemLog* mem = new MemLog();
        mem->Ref();
        while (reader.ReadRecord(&record, &scratch) && status.ok()) {
            if (record.size() < kLog_Batch_Header) {
                reporter.Corruption(record.size(),
                    Status::Corruption("log record too small"));
                continue;
            }
            batch.SetContents(record);
            mem->Add(batch);
        }
        delete file;

        uint64_t ts = env_->NowMicros();
        if (mem->ItemCount() > 0) {
            status = store_index_->MergeLog(mem, log_number, ts);
        }
        mem->Unref();
       
        return status;
    }

    void StackDB::RemoveLogFile(uint32_t log_number) {
        std::vector<std::string> filenames;
        Status s = env_->GetChildren(StoreDBDir(dbname_), &filenames);
        if (!s.ok()) return;
         
        uint32_t number = 0;
        FileType type;
        std::vector<uint32_t> logs;
        for (const std::string& filename : filenames) {
            if (ParseFileName(filename, &number, &type)) {
                if (type == kLogFile && number <= log_number)
                    env_->RemoveFile(StoreDBDir(dbname_) + "/" + filename);
            }
        }
    }

    inline StackBucket* StackDB::GetBucket(uint64_t key) const {
        return stack_buckets_[key & (db_meta_.bucket_count - 1)];
    }

    bool StackDB::GetBlobIndex(uint64_t key, StackBucket* bucket, BlobDataIndex& blobindex) {
        MutexLock l(&mutex_);
        MemLog* mem = mem_;
        MemLog* imm = imm_;
        mem->Ref();
        if (imm != nullptr) imm->Ref();
        
        mutex_.Unlock();
        bool isFind = false;
        bool isDeleted = false;
        BlobLogItem logItem;
        if (mem->Get(key, logItem)) {
            if (logItem.type == kTypeValue) {
                ToBlobIndex(logItem, blobindex);
                isFind = true;
            }
            else {
                isDeleted = true;
            }
        }
        else if (imm != nullptr && imm->Get(key, logItem)) {
            if (logItem.type == kTypeValue) {
                ToBlobIndex(logItem, blobindex);
                isFind = true;
            }
            else {
                isDeleted = true;
            }
        }

        // Before not find and not deleted to continue
        if (!isFind && !isDeleted) {
            if (bucket->GetBlobIndex(key, blobindex))
                isFind = true;
        }
        mutex_.Lock();

        mem->Unref();
        if (imm != nullptr) imm->Ref();

        return isFind;
    }

    Status StackDB::Write(StackBucket* bucket, StoreWriter* writer, uint64_t key,
        const Slice& value, uint64_t ts, LogBatch& batch) {

        uint32_t flags = 0;
        Status s = writer->Append(key, ts, flags, value);
        if (s.ok()) {
            BlobDataIndex blobindex;
            blobindex.ts = ts;
            blobindex.file_number = bucket->CurrentFileNumber();
            blobindex.pos = writer->DataPos();
            blobindex.size = writer->DataSize();
            blobindex.block_count = writer->BblockCount();
            blobindex.flags = flags;

            batch.Add(blobindex, key, kTypeValue, bucket->BucketId());
        }
        return s;
    }

    Status StackDB::Add(const WriteOptions& options, uint64_t key, const Slice& value) {
        StackBucket* bucket = GetBucket(key);
        WriterWait writer_wait(&max_writer_sem_, bucket);

        BlobDataIndex blobindex;
        if (GetBlobIndex(key, bucket, blobindex)) {
            return Status::AlreadyExists(Slice());
        }

        LogBatch batch;

        uint64_t ts = env_->NowMicros();
        //write to data and fill logbatch
        Status s = Write(bucket, writer_wait.Writer(), key, value, ts, batch);
        if (!s.ok()) return s;

        //Write to log
        return Write(options, &batch);
    }

    Status StackDB::Set(const WriteOptions& options, uint64_t key, const Slice& value) {        
        StackBucket* bucket = GetBucket(key);
        WriterWait writer_wait(&max_writer_sem_, bucket);

        BlobDataIndex blobindex;
        bool isExists = GetBlobIndex(key, bucket, blobindex);

        uint64_t ts = env_->NowMicros();
        LogBatch batch;
        if (isExists) {
            if (ts <= blobindex.ts) ts += kTs_Offset;
            blobindex.ts = ts;
            // set exists blob deletion flag
            batch.Add(blobindex, key, kTypeDeletion, bucket->BucketId());
        }

        ts += kTs_Offset;
        Status s = Write(bucket, writer_wait.Writer(), key, value, ts, batch);
        if (!s.ok()) return s;
        return Write(options, &batch);
    }

    Status StackDB::Replace(const WriteOptions& options, uint64_t key, const Slice& value) {
        StackBucket* bucket = GetBucket(key);
        WriterWait writer_wait(&max_writer_sem_, bucket);

        BlobDataIndex blobindex;
        if (!GetBlobIndex(key, bucket, blobindex)) {
            return Status::NotFound(Slice());
        }
        
        uint64_t ts = env_->NowMicros();
        if (ts <= blobindex.ts) ts += kTs_Offset;
        blobindex.ts = ts;

        LogBatch batch;
        //delete preve blob data
        batch.Add(blobindex, key, kTypeDeletion, bucket->BucketId());

        ts += kTs_Offset;
        Status s = Write(bucket, writer_wait.Writer(), key, value, ts, batch);
        if (!s.ok()) return s;
        return Write(options, &batch);
    }

    Status StackDB::Delete(const WriteOptions& options, uint64_t key) {
        //Delete operation not limit
        //WriterWait writer_wait(&max_writer_sem_, nullptr);
    
        StackBucket* bucket = GetBucket(key);
        BlobDataIndex blobindex;
        if (!GetBlobIndex(key, bucket, blobindex)) {
            return Status::NotFound(Slice());
        }

        uint64_t ts = env_->NowMicros();
        if (ts <= blobindex.ts) ts += kTs_Offset;
        blobindex.ts = ts;

        //Only write to log
        LogBatch batch;
        batch.Add(blobindex, key, kTypeDeletion, bucket->BucketId());
        return Write(options, &batch);
    }

    Status StackDB::GetReader(const ReadOptions& options, uint64_t key, Reader** reader) {
        //limit max readers by sem
        max_reader_sem_.Wait();

        *reader = nullptr;
        StackBucket* bucket = GetBucket(key);
        BlobDataIndex blobindex;
        if (!GetBlobIndex(key, bucket, blobindex)) {
            max_reader_sem_.Signal();
            return Status::NotFound(Slice());
        }

        StoreAccessFile* file;
        Status s = store_cache_->Get(bucket->BucketId(), blobindex.file_number, &file);
        if (!s.ok()) {
            max_reader_sem_.Signal();
            return s;
        }

        StoreReader *sr = NewReader();
        sr->Setup(file, key, blobindex, true);
        *reader = (Reader*)sr;
        return s;
    }

    Status StackDB::GetWriter(const WriteOptions& options, Writer** writer) {
        max_writer_sem_.WaitBatch();
        *writer = nullptr;
        DBWriter* dbwriter = new DBWriter(this);
        *writer = (Writer*)dbwriter;

        return Status::OK();
    }

    StoreReader* StackDB::NewReader() {       
        StoreReader* reader = nullptr;
        mutex_.Lock();
        if (reader_cache_.size() > 0) {
            reader = reader_cache_.back();
            reader_cache_.pop_back();
            mutex_.Unlock();
        }
        else {
            mutex_.Unlock();
            reader = new StoreReader(this, store_cache_);
        }
        return reader;
    }

    bool StackDB::CahceReader(StoreReader* reader) {
        assert(reader != nullptr);
        max_reader_sem_.Signal();

        MutexLock l(&mutex_);
        if (reader_cache_.size() < kMax_ReaderCache) {            
            reader_cache_.push_back(reader);
            return true;
        }
        return false; //not cache
    }

    Status StackDB::Write(const WriteOptions& options, LogBatch* updates) {
        Log_Writer w(&mutex_);
        w.batch = updates;
        w.sync = options.sync;
        w.done = false;

        MutexLock l(&mutex_);
        writers_.push_back(&w);
        while (!w.done && &w != writers_.front()) {
            w.cv.Wait();
        }
        if (w.done) {
            return w.status;
        }

        // May temporarily unlock and wait.
        Status status = MakeRoomForWrite(updates == nullptr);
        Log_Writer* last_writer = &w;
        // nullptr batch is for compactions
        if (status.ok() && updates != nullptr) { 
            LogBatch* write_batch = BuildBatchGroup(&last_writer);
           
            // Add to log.  We can release the lock
            // during this phase since &w is currently responsible for logging
            // and protects against concurrent loggers and concurrent writes
            // into mem_.
            {
                mutex_.Unlock();

                status = log_->AddRecord(write_batch->Contents());
                bool sync_error = false;
                if (status.ok() && options.sync) {
                    status = logfile_->Sync();
                    if (!status.ok()) {
                        sync_error = true;
                    }
                }
                if (status.ok()) {
                    mem_->Add(*write_batch);
                }
                mutex_.Lock();
                if (sync_error) {
                    // The state of the log file is indeterminate: the log record we
                    // just added may or may not show up when the DB is re-opened.
                    // So we force the DB into a mode where all future writes fail.
                    RecordBackgroundError(status);
                }
            }
            if (write_batch == tmp_batch_) tmp_batch_->Clear();
        }

        while (true) {
            Log_Writer* ready = writers_.front();
            writers_.pop_front();
            if (ready != &w) {
                ready->status = status;
                ready->done = true;
                ready->cv.Signal();
            }
            if (ready == last_writer) break;
        }

        // Notify new head of write queue
        if (!writers_.empty()) {
            writers_.front()->cv.Signal();
        }

        return status;
    }

    Status StackDB::MakeRoomForWrite(bool force) {
        mutex_.AssertHeld();
        bool allow_delay = !force;
        Status s;
        while (true) {
            if (!bg_error_.ok()) {
                // Yield previous error
                s = bg_error_;
                break;
            }
            else if (!force &&
                (mem_->ItemCount() <= options_.log_buffer_count)) {
                // There is room in current memlog
                break;
            }
            else if (imm_ != nullptr) {
                // We have filled up the current memtable, but the previous
                // one is still being compacted, so we wait.
                Log(options_.info_log, "Current memtable full; waiting...\n");
                background_work_finished_signal_.Wait();
            }
            else {
                // Attempt to switch to a new memtable and trigger compaction of old
                uint32_t new_log_number = logfile_number_ + 1;
                WritableFile* lfile = nullptr;
                s = env_->NewWritableFile(LogFileName(dbname_, new_log_number), &lfile);
                if (!s.ok()) {
                    break;
                }
                delete log_;
                delete logfile_;

                logfile_ = lfile;
                prev_logfile_number_ = logfile_number_;
                logfile_number_ = new_log_number;
                log_ = new log::Writer(lfile);
                imm_ = mem_;
                has_imm_.store(true, std::memory_order_release);
                mem_ = new MemLog();
                mem_->Ref();
                force = false;  // Do not force another compaction if have room
                MaybeScheduleCompaction();
            }
        }
        return s;
    }

    // REQUIRES: Writer list must be non-empty
    // REQUIRES: First writer must have a non-null batch
    LogBatch* StackDB::BuildBatchGroup(Log_Writer** last_writer) {
        mutex_.AssertHeld();
        assert(!writers_.empty());
        Log_Writer* first = writers_.front();
        LogBatch* result = first->batch;
        assert(result != nullptr);

        int count = first->batch->Count();
        // Allow the group to grow up to a maximum count.
        size_t max_count = 1000;
      
        *last_writer = first;
        std::deque<Log_Writer*>::iterator iter = writers_.begin();
        ++iter;  // Advance past "first"
        for (; iter != writers_.end(); ++iter) {
            Log_Writer* w = *iter;
            if (w->sync && !first->sync) {
                // Do not include a sync write into a batch handled by a non-sync write.
                break;
            }

            if (w->batch != nullptr) {
                count += w->batch->Count();
                if (count > max_count) {
                    // Do not make batch too big
                    break;
                }

                // Append to *result
                if (result == first->batch) {
                    // Switch to temporary batch instead of disturbing caller's batch
                    result = tmp_batch_;
                    assert(result->Count() == 0);
                    result->Append(*first->batch);
                }
                result->Append(*w->batch);
            }
            *last_writer = w;
        }
        return result;
    }


    void StackDB::CompactMemLog() {
        mutex_.AssertHeld();
        assert(imm_ != nullptr);

        uint64_t ts = env_->NowMicros();
        mutex_.Unlock();
        Status s = store_index_->MergeLog(imm_, prev_logfile_number_, ts);
        if (s.ok()) {
            for (int i = 0; i < db_meta_.bucket_count; i++) {
                StackBucket* bucket = stack_buckets_[i];
                bucket->MergeLog(imm_);
            }
        }
        mutex_.Lock();

        if (s.ok() && shutting_down_.load(std::memory_order_acquire)) {
            s = Status::IOError("Deleting DB during memtable compaction");
        }

        if (s.ok()) {
            // Commit to the new state
            imm_->Unref();
            imm_ = nullptr;
            prev_logfile_number_ = 0;
            has_imm_.store(false, std::memory_order_release);
        }
        else {
            RecordBackgroundError(s);
        }
    }

    void StackDB::BGWork(void* db) {
        reinterpret_cast<StackDB*>(db)->BackgroundCall();
    }

    void StackDB::BackgroundCall() {
        MutexLock l(&mutex_);
        assert(background_compaction_scheduled_);
        if (shutting_down_.load(std::memory_order_acquire)) {
            // No more background work when shutting down.
        }
        else if (!bg_error_.ok()) {
            // No more background work after a background error.
        }
        else {
            BackgroundCompaction();
        }

        background_compaction_scheduled_ = false;

        // Previous compaction may have produced too many files in a level,
        // so reschedule another compaction if needed.
        MaybeScheduleCompaction();
        background_work_finished_signal_.SignalAll();
    }

    void StackDB::MaybeScheduleCompaction() {
        mutex_.AssertHeld();
        if (background_compaction_scheduled_) {
            // Already scheduled
        }
        else if (shutting_down_.load(std::memory_order_acquire)) {
            // DB is being deleted; no more background compactions
        }
        else if (!bg_error_.ok()) {
            // Already got an error; no more changes
        }
        else {
            background_compaction_scheduled_ = true;
            env_->Schedule(&StackDB::BGWork, this);
        }
    }

    void StackDB::BackgroundCompaction() {
        mutex_.AssertHeld();

        if (imm_ != nullptr) {
            CompactMemLog();
            return;
        }

        uint64_t imm_micros = 0;
        Status s;
        uint64_t ts = env_->NowMicros();
        for (int i = 0; i < db_meta_.bucket_count; i++) {

            // Prioritize immutable compaction work
            if (has_imm_.load(std::memory_order_relaxed)) {
                const uint64_t imm_start = env_->NowMicros();
                mutex_.Lock();
                if (imm_ != nullptr) {
                    CompactMemLog();
                    // Wake up MakeRoomForWrite() if necessary.
                    background_work_finished_signal_.SignalAll();
                }
                mutex_.Unlock();
                imm_micros += (env_->NowMicros() - imm_start);
            }

            StackBucket* bucket = stack_buckets_[i];
            s = bucket->Compact(ts);
            if (!s.ok()) break;

        }

        if (!s.ok()) {
            RecordBackgroundError(s);
        }
    }

    void StackDB::MaybeIgnoreError(Status* s) const {
        if (s->ok() || options_.paranoid_checks) {
            // No change needed
        }
        else {
            Log(options_.info_log, "Ignoring error %s", s->ToString().c_str());
            *s = Status::OK();
        }
    }

    void StackDB::RecordBackgroundError(const Status& s) {
        mutex_.AssertHeld();
        if (bg_error_.ok()) {
            bg_error_ = s;
            background_work_finished_signal_.SignalAll();
        }
    }
}  // namespace stackfiledb