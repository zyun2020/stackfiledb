// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STACKFILE_DB_INCLUDE_DB_H_
#define STACKFILE_DB_INCLUDE_DB_H_

#include <stdint.h>
#include <stdio.h>

#include "stackfiledb/export.h"
#include "stackfiledb/options.h"
#include "stackfiledb/reader.h"

namespace stackfiledb {

// Update CMakeLists.txt if you change these
static const int kMajorVersion = 0;
static const int kMinorVersion = 202;

struct Options;
struct ReadOptions;
struct WriteOptions;

// A DB is safe for concurrent access from multiple threads without
// any external synchronization.
class STACKFILEDB_EXPORT DB {
 public:
  // Open the database with the specified "name".
  // Stores a pointer to a heap-allocated database in *dbptr and returns
  // OK on success.
  // Stores nullptr in *dbptr and returns a non-OK status on error.
  // Caller should delete *dbptr when it is no longer needed.
  static Status Open(const Options& options, const std::string& name, DB** dbptr);

  DB() = default;

  DB(const DB&) = delete;
  DB& operator=(const DB&) = delete;

  virtual ~DB();


  virtual Status Add(const WriteOptions& options, uint64_t key, const Slice& value) = 0;
  virtual Status Set(const WriteOptions& options, uint64_t key, const Slice& value) = 0;
  virtual Status Replace(const WriteOptions& options, uint64_t key, const Slice& value) = 0;
  virtual Status Delete(const WriteOptions& options, uint64_t key) = 0;
  virtual Status GetWriter(const WriteOptions& options, Writer** writer) = 0;

  virtual Status GetReader(const ReadOptions& options, uint64_t key, Reader** reader) = 0;
 
};


}  // namespace stackfiledb

#endif  // STACKFILE_DB_INCLUDE_DB_H_
