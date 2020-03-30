
#include "db/store_cache.h"
#include "db/filename.h"
#include "util/coding.h"

namespace stackfiledb {

    static void DeleteEntry(const Slice& key, void* value) {
        StoreAccessFile* tf = reinterpret_cast<StoreAccessFile*>(value);
        delete tf->file;
        delete tf;
    }

    StoreFileCache::StoreFileCache(const std::string& dbname, const Options& options,
        int entries)
        : env_(options.env),
        dbname_(dbname),
        options_(options),
        cache_(NewLRUCache(entries)) {}

    StoreFileCache::~StoreFileCache() { delete cache_; }
    
    void StoreFileCache::Release(Cache::Handle* handle) {
        if(handle != nullptr)
            cache_->Release(handle);
    }

    bool StoreFileCache::IsExists(uint32_t bucketId, uint32_t file_number) {
        char buf[8];
        EncodeFixed32(buf, bucketId);
        EncodeFixed32(buf + 4, file_number);

        Slice key(buf, sizeof(buf));
        Cache::Handle* handle = cache_->Lookup(key);
        if (handle == nullptr) return false;

        cache_->Release(handle);
        return true;
    }
    Status StoreFileCache::Get(uint32_t bucketId, uint32_t file_number, StoreAccessFile** accessfile) {
        *accessfile = nullptr;
        char buf[8];
        EncodeFixed32(buf, bucketId);
        EncodeFixed32(buf + 4, file_number);

        Status status;
        Slice key(buf, sizeof(buf));
        Cache::Handle* handle = cache_->Lookup(key);
        if (handle == nullptr) {
            std::string fname = StoreFileName(dbname_, bucketId, file_number);
            RandomAccessFile* file = nullptr;
            //here need lock to prevent other compact and delete this file
            status = env_->NewRandomAccessFile(fname, &file);
            if (status.ok()) {

            }
            if (!status.ok()) {
                if(file != nullptr) delete file;
                // We do not cache error results so that if the error is transient,
                // or somebody repairs the file, we recover automatically.
            }
            else {
                StoreAccessFile* tf = new StoreAccessFile();               
                handle = cache_->Insert(key, tf, 1, &DeleteEntry);
                tf->file = file;
                tf->handle = handle;
            }
        }
        if (status.ok()) {
            *accessfile = reinterpret_cast<StoreAccessFile*>(cache_->Value(handle));
        }
        return status;
    }

    Status StoreFileCache::Add(uint32_t bucketId, uint32_t file_number, Cache::Handle** handle) {
        *handle = nullptr;
        char buf[8];
        EncodeFixed32(buf, bucketId);
        EncodeFixed32(buf + 4, file_number);

        Status status;
        Slice key(buf, sizeof(buf));
        Cache::Handle* h = cache_->Lookup(key);
        if (h == nullptr) {
            std::string fname = StoreFileName(dbname_, bucketId, file_number);
            RandomAccessFile* file = nullptr;
            status = env_->NewRandomAccessFile(fname, &file, false);
            if (!status.ok()) {
                delete file;
                // We do not cache error results so that if the error is transient,
                // or somebody repairs the file, we recover automatically.
            }
            else {
                StoreAccessFile* tf = new StoreAccessFile();                
                *handle = cache_->Insert(key, tf, 1, &DeleteEntry);
                tf->file = file;
                tf->handle = *handle;

                //here handle ref == 2
            }
        }
        else {
            cache_->Release(h);
            status = Status::AlreadyExists("New Store file AlreadyExists");
        }
        
        return status;
    }

}  // namespace stackfiledb