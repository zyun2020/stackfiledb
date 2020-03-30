
#pragma once

#include <unordered_map>

#include "stackfiledb/env.h"
#include "stackfiledb/slice.h"
#include "stackfiledb/status.h"
#include "stackfiledb/reader.h"
#include "db/store_format.h"
#include "db/log_batch.h"

namespace stackfiledb {

    class StoreWriter
    {
    public:
        StoreWriter(WritableFile* dest);

        StoreWriter(const StoreWriter&) = delete;
        StoreWriter& operator=(const StoreWriter&) = delete;

        void Setup(WritableFile* dest);

        Status Append(uint64_t key, uint64_t ts, uint32_t flags, const Slice& slice);
        Status Append(uint64_t key, uint64_t ts, uint32_t flags, uint32_t block_size);

        Status Append(uint32_t size);
        Status Append(const Slice& slice, bool is_end);
     
        uint64_t DataPos() const { return pos_; }
        void SetDataPos(uint64_t pos) { pos_ = pos; }
        uint32_t DataSize() const { return total_size_; }
        uint32_t StoreSize() const { return store_size(total_size_, block_count_); }
        uint32_t BblockCount() const { return block_count_; }

        ~StoreWriter();
    private:
        WritableFile* dest_;

        uint64_t pos_;
        uint32_t crc_;
        uint32_t written_;
        uint32_t block_size_;
        uint32_t total_size_;
        uint16_t block_count_;
    };

    //Not thread safe
    class StackDB;
    class StackBucket;
    class SemWriter;
    class DBWriter : public Writer
    {
      public:
        DBWriter(StackDB* db);
        
        DBWriter(const DBWriter&) = delete;
        DBWriter& operator=(const DBWriter&) = delete;

        bool Add(uint64_t key, const Slice& slice) override;
        bool Set(uint64_t key, const Slice& slice) override;
        bool Replace(uint64_t key, const Slice& slice) override;

        bool Add(uint64_t key, uint32_t block_size) override;
        bool Set(uint64_t key, uint32_t block_size) override;
        bool Replace(uint64_t key, uint32_t block_size) override;

        bool Append(uint32_t block_size) override;
        bool Append(const Slice& slice, bool is_end) override;

        bool Delete(uint64_t key) override;
        bool Commit() override;
        bool Rollback() override;

        void Close() override;

        uint32_t Crc() const override { return crc_;  }
        uint32_t Size() const override { return size_; }
        uint64_t Key() const override { return key_; }
        Status status() const override { return status_; }
    private:
        StoreWriter* GetWriter(StackBucket* bucket);
        bool End();
    protected:
        ~DBWriter();
    private:
        StackDB* const db_;
        Env* const env_;
        SemWriter* const sem_writer_;

        struct BucketWriter {
            BucketWriter() : bucket(nullptr),
                writer(nullptr) {
            }
            BucketWriter(StackBucket* b, StoreWriter* w)
                : bucket(b),
                writer(w) {
            }
            StackBucket* bucket;
            StoreWriter* writer;
        };
        std::unordered_map<uint32_t, BucketWriter> bucket_writer_;

        StackBucket* bucket_;
        StoreWriter* writer_;
       
        LogBatch log_batch_;
        uint64_t ts_;

        uint32_t crc_;
        uint32_t size_;
        uint64_t key_;
        Status status_;
    };

}  // namespace stackfiledb

