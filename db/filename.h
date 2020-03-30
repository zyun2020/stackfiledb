// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// File names used by DB code

#ifndef STORAGE_LEVELDB_DB_FILENAME_H_
#define STORAGE_LEVELDB_DB_FILENAME_H_

#include <stdint.h>

#include <string>

#include "stackfiledb/slice.h"
#include "stackfiledb/status.h"
#include "port/port.h"

namespace stackfiledb {

class Env;

enum FileType {
  kLogFile,
  kDBLockFile,
  kDBMetaFile,
  kIndexFile,
  kStoreFile,
  kInfoLogFile  // Either the current one, or an old one
};

//  db file structure
//
//  dbname \-- info.log(info_old.log)
//  dbname \-- storedb 
//                  \--- zyun.dbm
//                  \--- storeindex.db
//                  \--- [0-9].log
//                  \--- [0-9].log
//                 \-- - [0-9].tmp
//  dbname \-- [0-9]001 
//                  \--- [0-9].lsf
//                  \--- [0-9].lsf
//              .                 
//              .                
//              .  
//  dbname \-- [0-9]256
//

// Return the name of the log file with the specified number
// in the db named by "dbname".  The result will be prefixed with
// "dbname" 
// dbname + "/storedb" + "/[0-9].log"
std::string LogFileName(const std::string& dbname, uint32_t number);
// dbname + "/storedb/zyun.dbm"
std::string DBMetaFileName(const std::string& dbname);
// dbname + "/storedb/storeindex.db"
std::string DBIndexFileName(const std::string& dbname);
//dbname + "/storedb"
std::string StoreDBDir(const std::string& dbname);

// Return the name of the sstable with the specified number
// in the db named by "dbname".  The result will be prefixed with
// "dbname".
// dbname + "/bucketId" + "/[0-9].lsf"
std::string StoreFileName(const std::string& dbname, uint32_t bucketId, uint32_t number);
std::string StoreFileDir(const std::string& dbname, uint32_t bucketId);

// Return the name of the lock file for the db named by
// "dbname".  The result will be prefixed with "dbname".
std::string LockFileName(const std::string& dbname);

// Return the name of the info log file for "dbname".
std::string InfoLogFileName(const std::string& dbname);
std::string OldInfoLogFileName(const std::string& dbname);

// If filename is a leveldb file, store the type of the file in *type.
// The number encoded in the filename is stored in *number.  If the
// filename was successfully parsed, returns true.  Else return false.
bool ParseFileName(const std::string& filename, uint32_t* number,
                   FileType* type);

}  // namespace stackfiledb

#endif  // STORAGE_LEVELDB_DB_FILENAME_H_
