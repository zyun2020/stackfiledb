// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/mem_log.h"

namespace stackfiledb {

    int MemLog::KeyComparator::operator()(const BlobLogItem& a, const BlobLogItem& b) const {
        // Order by:
        //    increasing key
        //    decreasing ts
        int r = 0;
        if (a.key > b.key) r = +1;
        else if (a.key < b.key) - 1;

        if (r == 0) {
            if (a.ts > b.ts) {
                r = -1;
            }
            else if (a.ts < b.ts) {
                r = +1;
            }
        }
        return r;
    }

    MemLog::MemLog()
      : refs_(0),
        item_count_(0),
        skipList_(comparator_, &arena_) {}

    MemLog::~MemLog() { assert(refs_ == 0); }

    size_t MemLog::ApproximateMemoryUsage() const { return arena_.MemoryUsage(); }

    MemLog::LogSkipList::Iterator* MemLog::NewIterator() const{
        MemLog::LogSkipList::Iterator* iter = new MemLog::LogSkipList::Iterator(&skipList_);
        return iter;
    }

    void MemLog::Add(const BlobLogItem& item) {
        item_count_.fetch_add(1, std::memory_order_relaxed);
        skipList_.Insert(item);
    }

    void MemLog::Add(const LogBatch& batch) {
        item_count_.fetch_add(batch.Count(), std::memory_order_relaxed);
        const BlobLogItem* log_items = batch.BlobLogItems();
        for (uint32_t i = 0; i < batch.Count(); i++) {
            skipList_.Insert(log_items[i]);
        }
    }

    bool MemLog::Get(uint64_t key, BlobLogItem& item) const {
        LogSkipList::Iterator iter(&skipList_);

        BlobLogItem logItem(key, UINT64_MAX);
        iter.Seek(logItem);

        if (iter.Valid()) {        
            item = iter.key();
            if (item.key == key) return true;
        }
        return false;
    }

    bool MemLog::Get(uint64_t key, uint64_t ts, BlobLogItem& item) const {
        LogSkipList::Iterator iter(&skipList_);

        BlobLogItem logItem(key, ts);
        iter.Seek(logItem);

        if (iter.Valid()) {
            item = iter.key();
            if (item.key == key && item.ts == ts) return true;
        }
        return false;
    }

}  // namespace leveldb
