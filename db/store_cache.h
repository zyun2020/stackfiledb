
#pragma once

#include "stackfiledb/env.h"
#include "stackfiledb/options.h"
#include "stackfiledb/cache.h"

#include "util/arena.h"

namespace stackfiledb {

    struct StoreAccessFile {
        RandomAccessFile* file;
        Cache::Handle* handle;
    };

    class StoreFileCache
    {
    public:
        StoreFileCache(const std::string& dbname, const Options& options, int entries);
        ~StoreFileCache();

        Status Get(uint32_t bucketId, uint32_t file_number, StoreAccessFile** accessfile);
        //When create new store file, insert into buffer;
        Status Add(uint32_t bucketId, uint32_t file_number, Cache::Handle** handle);

        bool IsExists(uint32_t bucketId, uint32_t file_number);

        void Release(Cache::Handle* handle);
    private:
        Env* const env_;
        const std::string dbname_;
        const Options& options_;
        Cache* cache_;
    };

}  // namespace stackfiledb

