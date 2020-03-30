// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <string>

#include "db/skiplist.h"
#include "db/store_format.h"
#include "db/log_batch.h"

namespace stackfiledb {

class MemLog {
 public:
  // MemLogs are reference counted.  The initial reference count
  // is zero and the caller must call Ref() at least once.
  MemLog();

  MemLog(const MemLog&) = delete;
  MemLog& operator=(const MemLog&) = delete;

  // Increase reference count.
  void Ref() { ++refs_; }

  // Drop reference count.  Delete if no more references exist.
  void Unref() {
    --refs_;
    assert(refs_ >= 0);
    if (refs_ <= 0) {
      delete this;
    }
  }

  int ItemCount() const { return item_count_.load(std::memory_order_relaxed); }

  // Returns an estimate of the number of bytes of data in use by this
  // data structure. It is safe to call when MemTable is being modified.
  size_t ApproximateMemoryUsage() const;

  // Add an entry into memlog
  void Add(const BlobLogItem& item);
  void Add(const LogBatch& batch);

  // If memlog contains a value for key, store it in &item and return true.
  // Else, return false.
  bool Get(uint64_t key, BlobLogItem& item) const;
  bool Get(uint64_t key, uint64_t ts, BlobLogItem& item) const;
  
  struct KeyComparator {
      KeyComparator() {}
      int operator()(const BlobLogItem& a, const BlobLogItem& b) const;
  };
  typedef SkipList<BlobLogItem, KeyComparator> LogSkipList;
  // Return an iterator that yields the contents of the memlog.
 //
 // The caller must ensure that the underlying memlog remains live
 // while the returned iterator is live.
  LogSkipList::Iterator* NewIterator() const;
 private:

  ~MemLog();  // Private since only Unref() should be used to delete it

  std::atomic<int> item_count_;
  
  KeyComparator comparator_;
  int refs_;
  Arena arena_;
  LogSkipList skipList_;
};

}  // namespace stackfiledb
