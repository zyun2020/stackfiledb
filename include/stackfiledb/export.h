// Copyright (c) 2017 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STACKFILE_DB_INCLUDE_EXPORT_H_
#define STACKFILE_DB_INCLUDE_EXPORT_H_

#if !defined(STACKFILEDB_EXPORT)

#if defined(STACKFILEDB_SHARED_LIBRARY)
#if defined(_WIN32)

#if defined(STACKFILEDB_COMPILE_LIBRARY)
#define STACKFILEDB_EXPORT __declspec(dllexport)
#else
#define STACKFILEDB_EXPORT __declspec(dllimport)
#endif  // defined(LEVELDB_COMPILE_LIBRARY)

#else  // defined(_WIN32)
#if defined(LEVELDB_COMPILE_LIBRARY)
#define STACKFILEDB_EXPORT __attribute__((visibility("default")))
#else
#define STACKFILEDB_EXPORT
#endif
#endif  // defined(_WIN32)

#else  // defined(STACKFILEDB_SHARED_LIBRARY)
#define STACKFILEDB_EXPORT
#endif

#endif  // !defined(STACKFILEDB_EXPORT)

#endif  // STACKFILE_DB_INCLUDE_EXPORT_H_
