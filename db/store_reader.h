
#pragma once

#include "stackfiledb/env.h"
#include "stackfiledb/reader.h"
#include "db/store_format.h"
#include "db/store_cache.h"
#include "util/semaphore.h"

namespace stackfiledb {

class StackDB;
class StoreReader : public Reader
{
public:
    StoreReader(StackDB* db, StoreFileCache* cache);
    virtual ~StoreReader();

    StoreReader(const StoreReader&) = delete;
    StoreReader& operator=(const StoreReader&) = delete;

    void Setup(StoreAccessFile* storeFile, uint64_t key, const BlobDataIndex& blob_index_, bool checksum);

    int Read(char** buffer) override;
    int Read(Slice* record) override;
    int Read(char* buffer, int buf_size) override;
    void Close() override;

    uint64_t Key() const override { return key_; }
    uint32_t Size() const override { return blob_index_.size; }

    Status status() const override { return status_; }
private:
    bool ReadHeader();
    bool BlockHeader(const char* ptr);

    bool Read(uint64_t offset, size_t n, Slice* result, char* scratch);
private:
    RandomAccessFile* file_;
    StoreAccessFile* storeFile_;
    char* buf_;
    uint32_t buf_size_;

    uint64_t key_;
    uint64_t pos_;
    BlobDataIndex blob_index_;
    bool checksum_;

    uint32_t saved_crc_;
    uint32_t cal_crc_;
    uint32_t readed_;
    uint32_t block_size_;
    uint32_t block_offset_;
    Status status_;

    StackDB* db_;
    StoreFileCache* cache_;
};

}  // namespace stackfiledb
