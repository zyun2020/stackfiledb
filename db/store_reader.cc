
#ifndef NOMINMAX
#define NOMINMAX
#endif  // ifndef NOMINMAX

#include <stdint.h>
#include "db/store_reader.h"
#include "db/stack_db.h"

#include "util/coding.h"
#include "util/crc32c.h"

namespace stackfiledb {

    StoreReader::StoreReader(StackDB* db, StoreFileCache* cache)
        : db_(db),
        cache_(cache),
        storeFile_(nullptr),
        readed_(0),
        pos_(0),
        buf_(nullptr){
    }
   
    StoreReader::~StoreReader() {
        db_ = nullptr;
        Close();
    }

    void StoreReader::Close() {
        key_ = 0;
        file_ = nullptr;

        if (buf_ != nullptr) {
            delete[] buf_;
            buf_ = nullptr;
        }
        if (cache_ != nullptr && storeFile_ != nullptr) {
            cache_->Release(storeFile_->handle);
        }
        if (db_ != nullptr && !db_->CahceReader(this)) {
            //not cache and now delete
            delete this;
        }
    }

    void StoreReader::Setup(StoreAccessFile* storeFile, uint64_t key, const BlobDataIndex& blob_index, bool checksum){
        storeFile_ = storeFile;
        file_ = storeFile_->file;
       
        key_ = key;
        pos_ = blob_index.pos;
        blob_index_ = blob_index;
        checksum_ = checksum;

        cal_crc_ = 0;
        readed_ = 0;
        status_ = Status::OK();
    }

    bool StoreReader::ReadHeader() {
        size_t read_size = kBlob_HeaderSize + 4;
        char header[kBlob_HeaderSize + 4];
        Slice result;
        status_ = file_->Read(pos_, read_size, &result, header);
        if (!status_.ok()) {
            return false;
        }
        else if (result.size() != read_size) {
            status_ = Status::Corruption("Cant read enough data");
            return false;
        }

        const char* ptr = result.data();
        uint32_t magic = DecodeFixed32(ptr);
        if (magic != kStack_DB_Magic) {
            status_ = Status::Corruption("Bad file magic number");
            return false;
        }

        uint64_t key = DecodeFixed64(&ptr[8]);
        if (key != key_) {
            status_ = Status::Corruption("Key is not equal");
            return false;
        }

        block_size_ = DecodeFixed32(&ptr[kBlob_HeaderSize]);
        block_size_ = block_size_ & SD_BLOCKSIZE_FLAG;
        if (block_size_ > blob_index_.size) {
            status_ = Status::Corruption("Block size is invalid");
            return false;
        }
        block_offset_ = 0;
        pos_ += read_size;
        
        return true;
    }

    bool StoreReader::BlockHeader(const char* ptr) {
        saved_crc_ = crc32c::Unmask(DecodeFixed32(ptr));
        block_size_ = DecodeFixed32(&ptr[4]);
        if (SD_BLOCKLAST_FLAG & block_size_) {
            uint32_t block_count = block_size_ & SD_BLOCKCOUNT_FLAG;
            uint32_t total_size = DecodeFixed32(&ptr[8]);
            block_size_ = 0; //last block
        }
        else {
            block_size_ = block_size_ & SD_BLOCKSIZE_FLAG;
        }
        return true;
    }

    inline bool StoreReader::Read(uint64_t offset, size_t n, Slice* result, char* scratch) {
        status_ = file_->Read(offset, n, result, scratch);
        if (!status_.ok()) {
            return false;
        }
        else if (result->size() != n) {
            status_ = Status::Corruption("Cant read enough data");
            return false;
        }
        return true;
    }

    int StoreReader::Read(char** buffer) {
        *buffer = nullptr;
        if (file_ == nullptr) return Read_Error;

        char *buf = new char[blob_index_.size];
        if (buf == nullptr) {
            status_ = Status::Error("memory allocation failed");
            return Read_Error;
        }

        if (readed_ == 0) { //read header
            if (!ReadHeader()) return Read_Error;
        }

        //Data read completed
        char ptr[kBlob_HeaderSize];
        uint32_t dataRead, block_size;
        Slice result;
        while (block_size_ > 0) {
            block_size = block_size_;
            if (!Read(pos_, block_size, &result, buf))
                return Read_Error;

            memcpy(&buffer[readed_], result.data(), block_size);
            readed_ += block_size;
            pos_ += block_size;

            //next block
            dataRead = kBlock_MetaSize;
            if (readed_ + block_size == blob_index_.size) {
                dataRead += 4; //total size
            }
            if (!Read(pos_, dataRead, &result, ptr))
                return Read_Error;

            pos_ += dataRead;
            BlockHeader(ptr);
            
            if (checksum_) {
                cal_crc_ = crc32c::Value(result.data(), block_size);
                if (cal_crc_ != saved_crc_) {
                    status_ = Status::Corruption("data checksum mismatch");
                    return Read_Error;
                }
            }
        }
        return readed_;
    }

    int StoreReader::Read(Slice* record) {
        if (file_ == nullptr) return Read_Error;

        if (readed_ == 0) { //read header
            if (!ReadHeader()) return Read_Error;
        }

        //Data read completed
        if (block_size_ == 0) {
            return 0;
        }
        uint32_t block_size = block_size_;
      
        Slice result;
        uint32_t dataRead = block_size + kBlock_MetaSize;
        if (readed_ + block_size == blob_index_.size) {
            dataRead += 4; //total size
        }

        if (!file_->IsMmap()) {
            if (buf_ != nullptr && buf_size_ < dataRead) {
                delete[] buf_;
                buf_ = nullptr;
            }
            if (buf_ == nullptr) {
                buf_ = new char[dataRead];
            }
        }

        if(!Read(pos_, dataRead, &result, buf_))
           return Read_Error;

        const char* ptr = result.data();
        pos_ += dataRead;       
        readed_ += block_size;
        *record = Slice(ptr, block_size);

        BlockHeader(&ptr[block_size]);

        if (checksum_) {
            cal_crc_ = crc32c::Value(ptr, block_size);
            if (cal_crc_ != saved_crc_) {
                status_ = Status::Corruption("Blob data checksum mismatch");
                return Read_Error;
            }
        }

        return block_size;
    }

    int StoreReader::Read(char* buffer, int buf_size) {
        if (file_ == nullptr) return Read_Error;
        if (buf_size <= 0 || buffer == nullptr) {
            status_ = Status::InvalidArgument("buffer is invalid");
            return Read_Error;
        }

        if (readed_ == 0) { //read header
           if (!ReadHeader()) return Read_Error;
        }

        //Data read completed
        if (block_size_ == 0) {
           return 0;
        }
        uint32_t block_size = block_size_;
        Slice result;
        uint32_t dataRead = std::min(block_size - block_offset_, (uint32_t)buf_size);
        if (!Read(pos_, dataRead, &result, buffer))
            return Read_Error;
       
        if (checksum_) {
            if (readed_ == 0) {
                cal_crc_ = crc32c::Value(result.data(), dataRead);
            }
            else {
                cal_crc_ = crc32c::Extend(cal_crc_, result.data(), dataRead);
            }
        }

        block_offset_ += dataRead;
        readed_ += dataRead;
        pos_ += dataRead;

        //Block read completed
        if (block_offset_ == block_size) {
            char ptr[kBlob_HeaderSize];
            uint32_t block_header = kBlock_MetaSize;
            if (readed_ + block_size == blob_index_.size) {
                block_header += 4; //total size
            }
            if (!Read(pos_, block_header, &result, ptr))
                return Read_Error;

            pos_ += block_header;
            BlockHeader(ptr);
        }

        if (checksum_ && cal_crc_ != saved_crc_) {
            status_ = Status::Corruption("data checksum mismatch");
            return Read_Error;
        }

        if (result.data() != buffer) {
            memcpy(buffer, result.data(), dataRead);
        }
        return dataRead;
    }

}  // namespace stackfiledb