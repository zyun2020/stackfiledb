
#pragma once

#include <unordered_map>
#include <mutex> 

#include "db/store_format.h"
#include "db/mem_log.h"
#include "db/store_cache.h"
#include "db/store_writer.h"
#include "db/store_index.h"

#include "util/rwlock.h"

namespace stackfiledb {

    using BlobDataIndexMap = std::unordered_map<uint64_t, BlobDataIndex>;
    using StoreFileMetaMap = std::unordered_map<uint32_t, StoreFileMeta>;

    class StackDB;
    class StoreFileCache;

    class StackBucket
    {
    public:
        StackBucket(const std::string& dbname, const Options* options,
            StoreFileCache* store_cache, StoreIndex* store_index, uint32_t bucket_id);

        StackBucket(const StackBucket&) = delete;
        StackBucket& operator=(const StackBucket&) = delete;

        ~StackBucket();

        StoreWriter* GetStoreWriter();
        void ReleaseStoreWriter(StoreWriter* writer);

        bool GetBlobIndex(uint64_t key, BlobDataIndex& blobindex);

        void MergeLog(const MemLog* mem);
      
        uint32_t BucketId() const { return bucket_id_; }
        uint32_t CurrentFileNumber() const { return file_number_; }
     
        Status Recover(BlobDataIndexMap* blobindex_map, StoreFileMetaMap* filemeta_map,
            uint32_t file_number, uint64_t log_file_size);

        Status Compact(uint64_t ts);
    private:
        void ReleaseFile();
        Status CompactError(uint32_t bucketId, uint32_t filenumber, uint64_t key, uint64_t pos);
        void CompactUpdate(const std::vector<BlobLogItem>& items, const std::vector<uint32_t>& files,
            const std::vector<StoreFileMeta>& new_files);
        void RemoveObsoleteFiles();
    private:
        const uint32_t bucket_id_;
        Env* const env_;
        const std::string dbname_;
        const Options* const options_;
        // store_cache_ provides its own synchronization
        StoreFileCache* const store_cache_;
        StoreIndex* const store_index_;
        
        //Current Stroe AccessFile and WriteFile
        std::atomic<bool> is_writing_;
        std::atomic<uint32_t> file_number_;

        uint64_t file_size_;
        WritableFile* write_file_;
        StoreWriter* store_writer_;
        Cache::Handle* access_handle_;

        //protect blobindex_map_ mutiple read and one write
        RWLock rwlock_;
        //typeValue blob data index
        BlobDataIndexMap* blobindex_map_;
        StoreFileMetaMap* stroe_file_meta_;
    };

}  // namespace stackfiledb

