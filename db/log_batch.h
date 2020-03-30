
#pragma once

#include <string>

#include "stackfiledb/status.h"
#include "stackfiledb/slice.h"
#include "db/store_format.h" 

namespace stackfiledb {

// LogBatch header has an 4byte type followed by a 4-byte count.
static const size_t kLog_Batch_Header = 8;

class LogBatch {
 public:
     LogBatch();
     LogBatch(size_t reserve);

     // Intentionally copyable.
     LogBatch(const LogBatch&) = default;
     LogBatch& operator=(const LogBatch&) = default;

    ~LogBatch();

    // Store item to log 
    void Add(const BlobLogItem& item);
    void Add(const BlobDataIndex& blobindex, uint64_t key, uint32_t type, uint32_t bucketId);

    // Clear all updates buffered in this batch.
    void Clear();

    void Append(const LogBatch& src);

    const BlobLogItem* BlobLogItems() const;

    // Return the number of entries in the batch.
    uint32_t Count() const;

    // Set the count for the number of entries in the batch.
    void SetCount(uint32_t n);

    uint32_t GetType() const;
    void SetType(uint32_t t);

    Slice Body() const;

    Slice Contents() const { return Slice(rep_); }
    void SetContents(const Slice& contents);
 private:
     std::string rep_;
};

}  // namespace stackfiledb

