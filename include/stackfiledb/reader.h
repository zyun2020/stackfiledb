
#pragma once

#include "stackfiledb/slice.h"
#include "stackfiledb/status.h"

namespace stackfiledb {

    #define Read_Error -1
    //Not thread safe
    class Reader
    {
    public:
        Reader() = default;

        virtual int Read(char** buffer) = 0;
        virtual int Read(Slice* record) = 0;
        virtual int Read(char* buffer, int bufSize) = 0;
        //must close to free resource and delete this
        virtual void Close() = 0;

        virtual uint32_t Size() const = 0;
        virtual uint64_t Key() const = 0;
        virtual Status status() const = 0;
    protected:
        virtual ~Reader() = default;
    };

    //Not thread safe
    class Writer
    {
    public:
        Writer() = default;

        virtual bool Add(uint64_t key, const Slice& slice) = 0;
        virtual bool Set(uint64_t key, const Slice& slice) = 0;
        virtual bool Replace(uint64_t key, const Slice& slice) = 0;

        virtual bool Add(uint64_t key, uint32_t block_sizee) = 0;
        virtual bool Set(uint64_t key, uint32_t block_size) = 0;
        virtual bool Replace(uint64_t key, uint32_t block_size) = 0;

        virtual bool Append(uint32_t block_size) = 0;
        virtual bool Append(const Slice& slice, bool is_end) = 0;

        virtual bool Delete(uint64_t key) = 0;

        virtual bool Commit() = 0;
        virtual bool Rollback() = 0;

        virtual void Close() = 0;

        virtual uint32_t Crc() const = 0;
        virtual uint32_t Size() const = 0;
        virtual uint64_t Key() const = 0;
        virtual Status status() const = 0;
    protected:
        virtual ~Writer() = default;
    };



}  // namespace stackfiledb
