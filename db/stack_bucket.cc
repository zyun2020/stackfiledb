
#include "db/stack_bucket.h"

#include "db/filename.h"
#include "db/store_reader.h"
#include <set>

namespace stackfiledb {
 
    StackBucket::StackBucket(const std::string& dbname, const Options* options,
        StoreFileCache* store_cache, StoreIndex* store_index, uint32_t bucket_id)
        : env_(options->env),
        dbname_(dbname),
        options_(options),
        store_cache_(store_cache),
        store_index_(store_index),
        bucket_id_(bucket_id),
        blobindex_map_(nullptr),
        store_writer_(nullptr),
        access_handle_(nullptr),
        write_file_(nullptr),
        file_number_(0),
        file_size_(0),
        is_writing_(false),
        stroe_file_meta_(nullptr) {
    }

    StackBucket::~StackBucket() {
        if (blobindex_map_ != nullptr) {
            delete blobindex_map_;
        }

        if (stroe_file_meta_ != nullptr) {
            delete stroe_file_meta_;
        }

        ReleaseFile();
    }

    void StackBucket::ReleaseFile() {
        if (store_writer_ != nullptr) {
            delete store_writer_;
            store_writer_ = nullptr;
        }
        if (write_file_ != nullptr) {
            delete write_file_;
            write_file_ = nullptr;
        }
        if (access_handle_ != nullptr) {
            store_cache_->Release(access_handle_);
            access_handle_ = nullptr;
        }
    }

    StoreWriter* StackBucket::GetStoreWriter() {
        if (is_writing_.load(std::memory_order_consume)) {
            return nullptr;
        }

        is_writing_.store(true, std::memory_order_release);
        if (file_size_ > options_->max_file_size) {
            ReleaseFile();
        }
       
        if(store_writer_ == nullptr){
            uint64_t ts = env_->NowMicros();
            uint32_t file_number = file_number_.fetch_add(1);
            uint32_t next_file_number = file_number + 1;
            Status s = store_index_->CreateStoreFile(bucket_id_, file_number, file_size_, next_file_number, ts);
            if (!s.ok()) {
                is_writing_.store(false, std::memory_order_release);
                file_number_.compare_exchange_strong(next_file_number, file_number);
                return nullptr;
            }

            std::string file_name = StoreFileName(dbname_, bucket_id_, next_file_number);
            s = env_->NewWritableFile(file_name, &write_file_);
            if (s.ok()) {
                store_writer_ = new StoreWriter(write_file_);
            }
            if (s.ok()) {
                s = store_cache_->Add(bucket_id_, next_file_number, &access_handle_);
            }

            //file_number_ = next_file_number;
            file_size_ = 0;

            StoreFileMeta file_meta;
            file_meta.file_number = next_file_number;
            file_meta.file_size = 0;

            //store file meta
            WriterLock lock(&rwlock_);
            stroe_file_meta_->insert(std::make_pair(next_file_number, file_meta));
        }

        StoreWriter* writer = store_writer_;
        writer->SetDataPos(file_size_);

        return writer;
    }

    void StackBucket::ReleaseStoreWriter(StoreWriter* writer) {
        assert(writer == store_writer_);
        file_size_ += writer->DataPos();
        is_writing_.store(false, std::memory_order_release);
    }

    bool StackBucket::GetBlobIndex(uint64_t key, BlobDataIndex& blobindex) {
        ReaderLock lock(&rwlock_);
        if (blobindex_map_ == nullptr) return false;
        auto iter = blobindex_map_->find(key);
        if (iter != blobindex_map_->end()) {
            blobindex = iter->second;
            return true;
        }
        return false;
    }

    void StackBucket::MergeLog(const MemLog* mem) {
        assert(mem != nullptr);
        BlobLogItem logItem;

        //cal add count to this bucket 
        //cal deleted or updated count from this bucket 
        MemLog::LogSkipList::Iterator* iter = mem->NewIterator();
        int added = 0, deleted = 0;
        for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
            logItem = iter->key();
            if (logItem.bucketId != bucket_id_) continue;
            if (logItem.type == kTypeValue) added++;        
            else deleted++;           
        }

        //1. read old items from blobindex_map_ insert into blob_index
        //   remove deleted or updated items
        BlobDataIndexMap* blob_index = nullptr;
        rwlock_.lock_read();
        if (blobindex_map_ != nullptr) {
            size_t count = blobindex_map_->size() + added - deleted + 8;
            blob_index = new BlobDataIndexMap(count);
            for (auto it = blobindex_map_->begin(); it != blobindex_map_->end(); ++it) {
                //blobdata has deleted not to insert;
                //dont need check logItem.type is kTypeDeletion, 
                //item exist in mem, this item if deleted then type = kTypeDeletion
                //if updated then type = kValueType
                if (mem->Get(it->first, logItem)){
                    continue;
                }
                blob_index->insert(std::make_pair(it->first, it->second));
            }
        }
        rwlock_.unlock_read();

        //2. add new items to blob_index
        BlobDataIndex idxItem;
        for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
            logItem = iter->key();
            if (logItem.bucketId != bucket_id_)
                continue;
            if (logItem.type == kTypeValue) {
                ToBlobIndex(logItem, idxItem);
                blob_index->insert(std::make_pair(logItem.key, idxItem));
            }
            // add deleted item
            else if (logItem.type == kTypeDeletion) {
                //
            }
            else {
                assert(logItem.type > kTypeValue);
            }
        }
        delete iter;
        

        //reset indexmap pointer
        if (blob_index != nullptr) {
            WriterLock lock(&rwlock_);
            delete blobindex_map_;
            blobindex_map_ = blob_index;
            blob_index = nullptr;
        }
    }

    Status StackBucket::Recover(BlobDataIndexMap* blobindex_map, StoreFileMetaMap* filemeta_map, uint32_t file_number, uint64_t log_file_size) {
        blobindex_map_ = blobindex_map;
        stroe_file_meta_ = filemeta_map;

        file_number_ = file_number;
        file_size_ = log_file_size;
        if (file_number == 0) return Status::OK();
       
        Status s;
        std::string file_name = StoreFileName(dbname_, bucket_id_, file_number_);
        
        if (file_size_ == 0) {
            s = env_->NewWritableFile(file_name, &write_file_);
        }
        else {
            uint64_t actual_file_size = 0;
            s = env_->GetFileSize(file_name.c_str(), &actual_file_size);
            if (s.ok()) {
                if (file_size_ < actual_file_size) {
                    s = env_->TruncateFile(file_name, file_size_);
                }
            }
            if (s.ok()) {
                s = env_->NewAppendableFile(file_name, &write_file_);
            }
        }
        if (s.ok()) {
            store_writer_ = new StoreWriter(write_file_);
        }
        if (s.ok()) {
            s = store_cache_->Add(bucket_id_, file_number_, &access_handle_);
        }
        if (!s.ok())  return s;

        //
        std::vector<std::string> filenames;
        s = env_->GetChildren(StoreFileDir(dbname_, bucket_id_), &filenames);
        if (!s.ok()) {
            return s;
        }
        std::set<uint32_t> expected;
        for (auto it = stroe_file_meta_->begin(); it != stroe_file_meta_->end(); ++it) {
            expected.insert(it->second.file_number);
        }
        uint32_t number;
        FileType type;
        for (size_t i = 0; i < filenames.size(); i++) {
            if (ParseFileName(filenames[i], &number, &type)) {
                if (type == kStoreFile)  
                    expected.erase(number);
            }
        }
        if (!expected.empty()) {
            char buf[50];
            snprintf(buf, sizeof(buf), "%d missing files; e.g.",
                static_cast<int>(expected.size()));
            s = Status::Corruption(buf, StoreFileName(dbname_, bucket_id_, *(expected.begin())));
        }

        RemoveObsoleteFiles();

        return s;
    }

    Status StackBucket::Compact(uint64_t ts) {
        std::vector<uint32_t> file_numbers;
        uint32_t new_file_count = 0;
        uint64_t max_file_size = 0;
        Status s = store_index_->GetCompactFile(bucket_id_, ts, *options_, file_numbers, new_file_count, max_file_size);
        if (!s.ok()) return s;

        if (new_file_count == 0 || blobindex_map_ == nullptr) {
            return Status::OK();
        }

        std::vector<BlobLogItem> deletion_items;
        s = store_index_->GetReserveDeletion(bucket_id_, ts,  file_numbers, deletion_items);
        if (!s.ok()) return s;

        std::vector<BlobLogItem> log_items;
        BlobLogItem item;
        rwlock_.lock_read();
        for (auto it = blobindex_map_->begin(); it != blobindex_map_->end(); ++it) {
            ToLogItem(it->second, it->first, kTypeValue, bucket_id_, item);
            log_items.push_back(item);
        }
        rwlock_.unlock_read();

        std::sort(log_items.begin(), log_items.end(), [](const BlobLogItem& a, const BlobLogItem& b) {
            int r = 0;
            if (a.file_number > b.file_number) r = +1;
            else if (a.file_number < b.file_number) - 1;
            if (r == 0) {
                if (a.pos > b.pos) {
                    r = +1;
                }
                else if (a.pos < b.pos) {
                    r = -1;
                }
            }
            return r;
        });
        int value_count = log_items.size();
        log_items.insert(log_items.end(), deletion_items.begin(), deletion_items.end());

        std::vector<BlobLogItem> new_items;
        new_items.reserve(log_items.size());

        std::vector<StoreFileMeta> new_files;
        new_files.reserve(new_file_count);

        int next_to_read = 0;
        StoreAccessFile* read_file = nullptr;
        WritableFile* write_file = nullptr;
        StoreWriter writer(write_file);
        StoreReader reader(nullptr, store_cache_);

        uint32_t read_file_number = 0;
        uint32_t write_file_number = file_number_.fetch_add(new_file_count);
        uint32_t last_write_file = write_file_number + new_file_count;
        uint64_t file_size = 0;
        BlobDataIndex data_index;

        for (size_t i = 0; i < log_items.size(); i++) {
            item = log_items[i];
            ToBlobIndex(item, data_index);
            if (item.file_number != read_file_number) {
                reader.Close();
                read_file_number = file_numbers[next_to_read];
                Status s = store_cache_->Get(bucket_id_, read_file_number, &read_file);
                if (!s.ok()) break;
                next_to_read++;                
            }
            if (write_file == nullptr || (file_size > max_file_size && write_file_number != last_write_file)) {
                if (write_file != nullptr) {
                    delete write_file;  write_file = nullptr;
                }
                if (file_size > 0) {
                    StoreFileMeta meta;
                    meta.file_number = write_file_number;
                    meta.file_size = file_size;
                    new_files.push_back(meta);
                }
                write_file_number++;
                std::string file_name = StoreFileName(dbname_, bucket_id_, write_file_number);
                s = env_->NewWritableFile(file_name, &write_file);
                if (!s.ok()) break;
                writer.Setup(write_file);
            }

            //update file number and pos
            item.file_number = write_file_number;
            item.pos = file_size;

            //Read data
            reader.Setup(read_file, item.key, data_index, true);
            Slice result;
            int block_size = 0;
            uint32_t copy_size = 0;
            while (copy_size < data_index.size) {
                block_size = reader.Read(&result);
                if (block_size == Read_Error) {
                    s = CompactError(bucket_id_, data_index.file_number, item.key, data_index.pos);
                    break;
                }
                if (copy_size == 0) {
                    writer.Append(item.key, data_index.ts, data_index.flags, static_cast<uint32_t>(block_size));
                }
                else {
                    writer.Append(static_cast<uint32_t>(block_size));
                }
                copy_size += (uint32_t)block_size;
                if (copy_size == data_index.size) {
                    writer.Append(result, false);
                }
                else {
                    writer.Append(result, true);
                    file_size += writer.StoreSize();
                }
            }
            if (!s.ok()) break;
            assert(file_size == store_size(data_index.size, data_index.block_count));
            if (file_size != store_size(data_index.size, data_index.block_count)) {
                s = CompactError(bucket_id_, data_index.file_number, item.key, data_index.pos);
                break;
            }

            new_items.push_back(item);
        }

        reader.Close();
        if (write_file != nullptr) {
            delete write_file;  write_file = nullptr;
        }
        if (s.ok()) {
            if (file_size > 0) {
                StoreFileMeta meta;
                meta.file_size = file_size;
                meta.file_number = write_file_number;
                new_files.push_back(meta);
                assert(new_files.size() == new_file_count);
            }
            s = store_index_->Compact(bucket_id_, new_items, value_count, file_numbers, new_files, ts);
        }
        if (s.ok()) {
            CompactUpdate(new_items, file_numbers, new_files);
            RemoveObsoleteFiles();
        }
        else {
            file_number_.compare_exchange_strong(last_write_file, last_write_file - new_file_count);
        }
        
        return s;
    }

    void StackBucket::CompactUpdate(const std::vector<BlobLogItem>& items, const std::vector<uint32_t>& files,
        const std::vector<StoreFileMeta>& new_files) {
        WriterLock lock(&rwlock_);
        for (const BlobLogItem& item : items) {
            if (item.type == kTypeDeletion) continue;

            auto iter = blobindex_map_->find(item.key);
            if (iter != blobindex_map_->end()) {
                iter->second.file_number = item.file_number;
                iter->second.pos = item.pos;
            }
        }

        for (uint32_t file : files) {
            stroe_file_meta_->erase(file);
        }

        for (const StoreFileMeta& item : new_files) {
            stroe_file_meta_->insert(std::make_pair(item.file_number, item));
        }
    }

    void StackBucket::RemoveObsoleteFiles() {
        std::vector<std::string> filenames;
        Status s = env_->GetChildren(dbname_, &filenames);
        if (!s.ok()) {
            return;
        }

        std::vector<uint32_t> file_numbers;
        std::vector<std::string> files_to_delete;
        uint32_t number;
        FileType type;
        for (const std::string& filename : files_to_delete) {
            if (ParseFileName(filename, &number, &type)) {
                if (type == kStoreFile && stroe_file_meta_->find(number) == stroe_file_meta_->end()) {
                    file_numbers.push_back(number);
                }
                else {
                    files_to_delete.push_back(filename);
                }
            }
            else {
                files_to_delete.push_back(filename);
            }
        }

        for (uint32_t file_number : file_numbers) {
            if (!store_cache_->IsExists(bucket_id_, file_number)) {
                env_->RemoveFile(StoreFileName(dbname_, bucket_id_, file_number));
            }
        }

        for (const std::string& filename : files_to_delete) {
            env_->RemoveFile(StoreFileDir(dbname_, bucket_id_) +"/" + filename);
        }
    }

    Status StackBucket::CompactError(uint32_t bucketId, uint32_t filenumber, uint64_t key, uint64_t pos) {
        char buf[128];
        snprintf(buf, sizeof(buf), "Bucket:%d, File: %d, Key: %llu, Position: %llu",
            bucketId, filenumber, key, pos);
        return Status::Corruption(Slice(buf));
    }
}  // namespace stackfiledb