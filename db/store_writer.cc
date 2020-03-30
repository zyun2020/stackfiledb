
#include <stdint.h>

#include "db/store_writer.h"
#include "db/stack_db.h" 
#include "db/stack_bucket.h" 
#include "db/sem_bucket.h"

#include "util/coding.h"
#include "util/crc32c.h"

namespace stackfiledb {
 
    //--------------------------------------------------------------------------------------
    //  StoreWriter
    //--------------------------------------------------------------------------------------
    StoreWriter::StoreWriter(WritableFile* dest)
        : dest_(dest),
        pos_(0),
        written_(0),
        crc_(0),
        block_count_(0),
        block_size_(0),
        total_size_(0) {
    }

    StoreWriter::~StoreWriter() = default;

    void StoreWriter::Setup(WritableFile* dest) {
        assert(block_size_ == 0);
        dest_ = dest;
    }

    Status StoreWriter::Append(uint64_t key, uint64_t ts, uint32_t flags, const Slice& slice) {   
        uint32_t block_size = (uint32_t)slice.size();
        Status s = Append(key, ts, flags, block_size);
        if (!s.ok()) return s;
        return Append(slice, true);
    }
 
    Status StoreWriter::Append(uint64_t key, uint64_t ts, uint32_t flags, uint32_t block_size) {
        assert(block_size_ == 0);
        char buffer[kBlob_HeaderSize + 4];

        total_size_ = 0;
        block_size_ = block_size;
        block_count_ = 0;
        written_ = 0;
        crc_ = 0;

        //Header
        size_t pos = 0;
        EncodeFixed32(buffer + pos, kStack_DB_Magic);  pos += 4;
        EncodeFixed32(buffer + pos, flags);  pos += 4;
        EncodeFixed64(buffer + pos, key);  pos += 8;
        EncodeFixed64(buffer + pos, ts);  pos += 8;
        // First block size
        EncodeFixed32(buffer + pos, block_size);  pos += 4;
        assert(pos == kBlob_HeaderSize + 4);

        return dest_->Append(Slice(buffer, pos));
    }

    //new block
    Status StoreWriter::Append(uint32_t size) {
        if (written_ < block_size_) {
            return Status::Error("Block data not completed");
        }
        assert(written_ == block_size_);
        //pre block
        total_size_ += block_size_;
        block_count_++;

        size_t pos = 0;
        char buffer[kBlob_HeaderSize];

        //pre block data crc
        EncodeFixed32(buffer + pos, crc32c::Mask(crc_));  pos += 4;
        //new block size
        EncodeFixed32(buffer + pos, size);  pos += 4;

        //new block
        block_size_ = size;
        written_ = 0;
        crc_ = 0;

        return dest_->Append(Slice(buffer, pos));
    }

    Status StoreWriter::Append(const Slice& slice, bool is_end) {
        uint32_t data_size = (uint32_t)slice.size();
        if ((data_size + written_) > block_size_) {
            return Status::InvalidArgument("Too much data");
        }
        if (is_end && (data_size + written_) < block_size_) {
            return Status::Error("Block data not completed");
        }
        
        Status s = dest_->Append(slice);
        if (s.ok()) {
            if (written_ == 0) {
                crc_ = crc32c::Value(slice.data(), slice.size());
            }
            else {
                crc_ = crc32c::Extend(crc_, slice.data(), slice.size());
            }
            written_ += (uint32_t)slice.size();
        }

        if (is_end && s.ok()) {
            assert(written_ == block_size_);

            //pre block
            total_size_ += block_size_;
            block_count_++;

            size_t pos = 0;
            char buffer[kBlob_HeaderSize];

            //pre block data crc
            EncodeFixed32(buffer, crc32c::Mask(crc_));  pos += 4;

            //last block
            block_count_++;
            uint32_t last_block = block_count_ | SD_BLOCKLAST_FLAG;
            EncodeFixed32(buffer + pos, last_block);  pos += 4;
            //Total Size
            EncodeFixed32(buffer + pos, total_size_);  pos += 4;

            block_size_ = 0;
            written_ = 0;
            crc_ = 0;

            s = dest_->Append(Slice(buffer, pos));
            if (s.ok()) {
                s = dest_->Sync();
            }
        }
        return s;
    }

    //--------------------------------------------------------------------------------------
    //  DBWriter
    //--------------------------------------------------------------------------------------
    DBWriter::DBWriter(StackDB* db)
        : db_(db),
         env_(db->env_),
        sem_writer_(&db->max_writer_sem_),
        bucket_(nullptr),
        writer_(nullptr),
        ts_(0),
        crc_(0),
        key_(0),
        size_(0) {
    }

    DBWriter::~DBWriter() {
        Close();
    }

    StoreWriter* DBWriter::GetWriter(StackBucket* bucket) {
        auto iter = bucket_writer_.find(bucket->BucketId());
        StoreWriter* writer = nullptr;
        if (iter == bucket_writer_.end()) {
            writer = sem_writer_->WaitBatch(bucket);
            bucket_writer_[bucket->BucketId()] = BucketWriter(bucket, writer);
        }
        else{
            writer = iter->second.writer;
        }
        return writer;
    }

    bool DBWriter::Add(uint64_t key, const Slice& slice) {
        bucket_ = db_->GetBucket(key);
        writer_ = GetWriter(bucket_);
        if (writer_ == nullptr) {
            status_ = Status::IOError("new store file failed");
            return false;
        }

        BlobDataIndex blobindex;
        if (!db_->GetBlobIndex(key, bucket_, blobindex)) {
            status_ = Status::AlreadyExists(Slice());
            return false;
        }

        uint32_t flags = 0;
        ts_ = env_->NowMicros();
        status_ = writer_->Append(key, ts_, flags, slice);
        if (!status_.ok()) return false;

        return End();
    }
    bool DBWriter::Set(uint64_t key, const Slice& slice) {
        bucket_ = db_->GetBucket(key);
        writer_ = GetWriter(bucket_);
        if (writer_ == nullptr) {
            status_ = Status::IOError("new store file failed");
            return false;
        }

        BlobDataIndex blobindex;
        bool isExists = db_->GetBlobIndex(key, bucket_, blobindex);

        ts_ = env_->NowMicros();
        if (isExists) {
            if (ts_ <= blobindex.ts) ts_ += kTs_Offset;
            blobindex.ts = ts_;
            // set exists blob deletion flag
            log_batch_.Add(blobindex, key, kTypeDeletion, bucket_->BucketId());
        }

        uint32_t flags = 0;
        ts_ += kTs_Offset;
        status_ = writer_->Append(key, ts_, flags, slice);
        if (!status_.ok()) return false;

        return End();

    }
    bool DBWriter::Replace(uint64_t key, const Slice& slice) {
        bucket_ = db_->GetBucket(key);
        writer_ = GetWriter(bucket_);
        if (writer_ == nullptr) {
            status_ = Status::IOError("new store file failed");
            return false;
        }

        BlobDataIndex blobindex;
        if (!db_->GetBlobIndex(key, bucket_, blobindex)) {
            status_ = Status::NotFound(Slice());
            return false;
        }

        ts_ = env_->NowMicros();
        if (ts_ <= blobindex.ts) ts_ += kTs_Offset;
        blobindex.ts = ts_;

        //delete prev blob data
        log_batch_.Add(blobindex, key, kTypeDeletion, bucket_->BucketId());

        uint32_t flags = 0;
        ts_ += kTs_Offset;
        status_ = writer_->Append(key, ts_, flags, slice);
        if (!status_.ok()) return false;

        return End();
    }

    bool DBWriter::Add(uint64_t key, uint32_t block_size) {
        bucket_ = db_->GetBucket(key);
        writer_ = GetWriter(bucket_);
        if (writer_ == nullptr) {
            status_ = Status::IOError("new store file failed");
            return false;
        }

        BlobDataIndex blobindex;
        if (!db_->GetBlobIndex(key, bucket_, blobindex)) {
            status_ = Status::AlreadyExists(Slice());
            return false;
        }

        uint32_t flags = 0;
        ts_ = env_->NowMicros();
        //Append header
        status_ = writer_->Append(key, ts_, flags, block_size);
        if (!status_.ok()) return false;

        return true;
    }
    bool DBWriter::Set(uint64_t key, uint32_t block_size) {
        bucket_ = db_->GetBucket(key);
        writer_ = GetWriter(bucket_);
        if (writer_ == nullptr) {
            status_ = Status::IOError("new store file failed");
            return false;
        }

        BlobDataIndex blobindex;
        bool isExists = db_->GetBlobIndex(key, bucket_, blobindex);

        ts_ = env_->NowMicros();
        if (isExists) {
            if (ts_ <= blobindex.ts) ts_ += kTs_Offset;
            blobindex.ts = ts_;
            // set exists blob deletion flag
            log_batch_.Add(blobindex, key, kTypeDeletion, bucket_->BucketId());
        }

        uint32_t flags = 0;
        ts_ += kTs_Offset;
        //Append header
        status_ = writer_->Append(key, ts_, flags, block_size);
        if (!status_.ok()) return false;

        return true;

    }
    bool DBWriter::Replace(uint64_t key, uint32_t block_size) {
        bucket_ = db_->GetBucket(key);
        writer_ = GetWriter(bucket_);
        if (writer_ == nullptr) {
            status_ = Status::IOError("new store file failed");
            return false;
        }

        BlobDataIndex blobindex;
        if (!db_->GetBlobIndex(key, bucket_, blobindex)) {
            status_ = Status::NotFound(Slice());
            return false;
        }

        ts_ = env_->NowMicros();
        if (ts_ <= blobindex.ts) ts_ += kTs_Offset;
        blobindex.ts = ts_;

        //delete preve blob data
        log_batch_.Add(blobindex, key, kTypeDeletion, bucket_->BucketId());

        uint32_t flags = 0;
        ts_ += kTs_Offset;
        //Append header
        status_ = writer_->Append(key, ts_, flags, block_size);
        if (!status_.ok()) return false;

        return true;
    }

    bool DBWriter::Append(uint32_t block_size) {
        if (writer_ == nullptr) return false;
        status_ = writer_->Append(block_size);
        if (!status_.ok()) return false;
        return true;
    }

    bool DBWriter::Append(const Slice& slice, bool is_end) {
        if (writer_ == nullptr) return false;
        status_ = writer_->Append(slice, is_end);
        if (!status_.ok()) return false;

        if (is_end) {
            End();
        }

        return true;
    }

    inline bool DBWriter::End() {       
        uint32_t flags = 0;
        BlobDataIndex blobindex;
        blobindex.ts = ts_;
        blobindex.file_number = bucket_->CurrentFileNumber();
        blobindex.pos = writer_->DataPos();
        blobindex.size = writer_->DataSize();
        blobindex.block_count = writer_->BblockCount();
        blobindex.flags = flags;

        log_batch_.Add(blobindex, key_, kTypeValue, bucket_->BucketId());

        //A new key may be is in different bucket
        writer_ = nullptr;
        bucket_ = nullptr;
        return true;
    }

    bool DBWriter::Delete(uint64_t key) {
        StackBucket* bucket = db_->GetBucket(key);
        BlobDataIndex blobindex;
        if (!db_->GetBlobIndex(key, bucket, blobindex)) {
            status_ = Status::NotFound("key not found");
            return false;
        }

        uint64_t ts = env_->NowMicros();
        if (ts <= blobindex.ts) ts += kTs_Offset;
        blobindex.ts = ts;

        //Only write to log
        log_batch_.Add(blobindex, key, kTypeDeletion, bucket->BucketId());
        return true;
    }

    void DBWriter::Close() {
        sem_writer_->SignalBatch();
        delete this;
    }

    bool DBWriter::Commit() {
        if (status_.ok()) {
            WriteOptions options;
            options.sync = true;
            status_ = db_->Write(options, &log_batch_);
        }
        return Rollback();
    }

    bool DBWriter::Rollback() {
        log_batch_.Clear();

        //release writer lock
        for (auto it = bucket_writer_.begin(); it != bucket_writer_.end(); ++it) {
            BucketWriter bw = it->second;
            sem_writer_->SignalBatch(bw.bucket, bw.writer);
        }
        bucket_writer_.clear();

        return status_.ok() ? true : false;
    }

}  // namespace stackfiledb