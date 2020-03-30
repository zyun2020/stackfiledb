// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/log_batch.h"
#include "util/coding.h"

namespace stackfiledb {

 

    //*************************************************************************************
    // LogBatch
    //*************************************************************************************
    LogBatch::LogBatch(){
        rep_.reserve(kLog_Batch_Header + 8 * kLog_ItemSize);
        Clear();
    }

    LogBatch::LogBatch(size_t reserve) {
        rep_.reserve(kLog_Batch_Header + reserve * kLog_ItemSize);
        Clear();
    }

    LogBatch::~LogBatch() = default;

    void LogBatch::Clear() {
        rep_.clear();
        rep_.resize(kLog_Batch_Header);
    }

    void LogBatch::Add(const BlobLogItem& item) {
        if (port::kLittleEndian) {
            rep_.append(reinterpret_cast<const char*>(&item), kLog_ItemSize);
            SetCount(Count() + 1);
            return;
        }

        PutFixed64(&rep_, item.key);
        PutFixed64(&rep_, item.ts);
        PutFixed32(&rep_, item.pos);
        PutFixed32(&rep_, item.size);
        PutFixed32(&rep_, item.flags);
        PutFixed32(&rep_, item.file_number);
        PutFixed32(&rep_, item.block_count);
        PutFixed32(&rep_, item.type);
        PutFixed32(&rep_, item.bucketId);

        SetCount(Count() + 1);
    }

    void LogBatch::Add(const BlobDataIndex& item, uint64_t key, uint32_t type, uint32_t bucketId) {
        BlobLogItem logItem;
        logItem.key = key;
        logItem.ts = item.ts;        
        logItem.pos = item.pos;
        logItem.size = item.size;
        logItem.flags = item.flags;
        logItem.file_number = item.file_number;
        logItem.block_count = item.block_count;

        logItem.type = type;
        logItem.bucketId = bucketId;
        
        Add(logItem);
    }

    void LogBatch::Append(const LogBatch& src) {
        assert(src.rep_.size() >= kLog_Batch_Header);
        SetCount(Count() + src.Count());
        rep_.append(src.rep_.data() + kLog_Batch_Header, src.rep_.size() - kLog_Batch_Header);
    }

    void LogBatch::SetContents(const Slice& contents) {
        assert(contents.size() >= kLog_Batch_Header);
        rep_.assign(contents.data(), contents.size());
    }

    const BlobLogItem* LogBatch::BlobLogItems() const {
        return reinterpret_cast<const BlobLogItem*>(rep_.data() + kLog_Batch_Header);
    }

    uint32_t LogBatch::Count() const {
        return DecodeFixed32(rep_.data() + 4);
    }

    void LogBatch::SetCount(uint32_t n) {
        EncodeFixed32(&rep_[4], n);
    }

    uint32_t LogBatch::GetType() const {
        return DecodeFixed32(rep_.data());
    }

    void LogBatch::SetType(uint32_t t) {
        EncodeFixed32(&rep_[0], t);
    }

    Slice LogBatch::Body() const {
        return Slice(rep_.data() + kLog_Batch_Header, rep_.size() - kLog_Batch_Header);
    }

}  // namespace stackfiledb
