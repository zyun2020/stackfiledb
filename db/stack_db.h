
#pragma once

#include <vector>
#include <deque>

#include "stackfiledb/env.h"
#include "stackfiledb/reader.h"
#include "stackfiledb/options.h"
#include "stackfiledb/db.h"

#include "port/port.h"
#include "port/thread_annotations.h"
#include "util/semaphore.h"

#include "db/log_writer.h"
#include "db/log_batch.h"
#include "db/store_cache.h"
#include "db/stack_bucket.h"
#include "db/sem_bucket.h"
#include "db/store_reader.h"
#include "db/store_index.h"


namespace stackfiledb {

    struct DBMeta {
        uint64_t create_ts;
        uint32_t gen_key_mode;       // 0 user generate key  1 db generate key
        uint32_t key_bucekt_mapping; // 0 hash. 1  low in key  byte  2 in key high byte in key
        uint32_t bucket_count;
        uint32_t opt_bucket;
    };

    class StackDB : public DB {
    public:
        StackDB(const Options& raw_options, const std::string& dbname);

        StackDB(const StackDB&) = delete;
        StackDB& operator=(const StackDB&) = delete;

        ~StackDB() override;

        Status Add(const WriteOptions& options, uint64_t key, const Slice& value) override;
        Status Set(const WriteOptions& options, uint64_t key, const Slice& value) override;
        Status Replace(const WriteOptions& options, uint64_t key, const Slice& value) override;
        Status Delete(const WriteOptions& options, uint64_t key) override;

        Status GetReader(const ReadOptions& options, uint64_t key, Reader** reader) override;
        Status GetWriter(const WriteOptions& options, Writer** writer) override;
    private:
        struct Log_Writer;

        Status RecoverLogFile(uint32_t log_number);
        Status RecoverBucket();

        void RemoveLogFile(uint32_t log_number);

        Status MakeRoomForWrite(bool force);

        StoreReader* NewReader();
        //Internal use, use by StoreReader
        bool CahceReader(StoreReader* reader);

        Status Write(StackBucket* bucket, StoreWriter* writer, uint64_t key, const Slice& value, uint64_t ts, LogBatch& batch);
        LogBatch* BuildBatchGroup(Log_Writer** last_writer);

        StackBucket* GetBucket(uint64_t key) const;
        bool GetBlobIndex(uint64_t key, StackBucket* bucket, BlobDataIndex& blobindex);
        Status Write(const WriteOptions& options, LogBatch* updates);

        void RecordBackgroundError(const Status& s);
        void MaybeIgnoreError(Status* s) const;
        static void BGWork(void* db);
        void BackgroundCall();

        void MaybeScheduleCompaction();
        void BackgroundCompaction();
        void CompactMemLog();

        Status NewDB();
        Status Recover();
        void MetaEncodeTo(std::string* dst) const;
        Status MetaDecodeFrom(const Slice& src);
    private:
        friend class StoreReader;
        friend class DBWriter;
        friend class DB;

        // Constant after construction
        Env* const env_;
        const Options options_;
        const bool owns_info_log_;
        const bool owns_cache_;

        const std::string dbname_;
        const std::string volumeid_;

        FileLock* db_lock_;

        // State below is protected by mutex_
        port::Mutex mutex_;
        std::atomic<bool> shutting_down_;
        port::CondVar background_work_finished_signal_;
        MemLog* mem_;
        MemLog* imm_;             // Memtable being compacted
        
        std::atomic<bool> has_imm_;       // So bg thread can detect non-null imm_
        WritableFile* logfile_;
        uint32_t logfile_number_;
        uint32_t prev_logfile_number_;
        log::Writer* log_;

        bool background_compaction_scheduled_;
        Status bg_error_;

        // Log queue of writers.
        std::deque<Log_Writer*> writers_;
        LogBatch* tmp_batch_;
      
        SemWriter max_writer_sem_;
        Semaphore max_reader_sem_;
        std::vector<StoreReader*> reader_cache_;
        int reader_count_;

        // List of buckets in db
        std::vector<StackBucket*> stack_buckets_;
        DBMeta db_meta_;
        StoreIndex* const store_index_;

        // store_cache_ provides its own synchronization
        StoreFileCache* const store_cache_;
    };

}  // namespace stackfiledb

