// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/filename.h"

#include <ctype.h>
#include <stdio.h>

#include "stackfiledb/env.h"
#include "util/logging.h"

namespace stackfiledb {


    std::string StoreFileDir(const std::string& dbname, uint32_t bucketId) {
        assert(bucketId > 0);
        char buf[100];
        snprintf(buf, sizeof(buf), "/%03u", bucketId);
        return dbname + buf;
    }

    std::string StoreFileName(const std::string& dbname, uint32_t bucketId, uint32_t number) {
      assert(number > 0 && bucketId > 0);
      char buf[100];
      snprintf(buf, sizeof(buf), "/%03u/%06u.lsf", bucketId, number);
      return dbname + buf;
    }

    std::string StoreDBDir(const std::string& dbname) {
        return dbname + "/storedb";
    }

    std::string LogFileName(const std::string& dbname, uint32_t number) {
        assert(number > 0);
        char buf[100];
        snprintf(buf, sizeof(buf), "/storedb/%08u.log", number);
        return dbname + buf;
    }

    std::string DBMetaFileName(const std::string& dbname) {
        return dbname + "/storedb/zyun.dbm";
    }

    std::string DBIndexFileName(const std::string& dbname) {
        return dbname + "/storedb/storeindex.db";
    }
 
    std::string LockFileName(const std::string& dbname) { return dbname + "/LOCK"; }

    std::string InfoLogFileName(const std::string& dbname) {
      return dbname + "/info.log";
    }

    // Return the name of the old info log file for "dbname".
    std::string OldInfoLogFileName(const std::string& dbname) {
        return dbname + "/info_old.log";
    }

    // Owned filenames have the form:
    //    dbname/CURRENT
    //    dbname/LOCK
    //    dbname/LOG
    //    dbname/LOG.old
    //    dbname/MANIFEST-[0-9]+
    //    dbname/[0-9]+.(log|sst|ldb)
    bool ParseFileName(const std::string& filename, uint32_t* number,
                       FileType* type) {
      Slice rest(filename);
      if (rest == "zyun.dbm") {
        *number = 0;
        *type = kDBMetaFile;
      } else if (rest == "storeindex.db") {
          *number = 0;
          *type = kIndexFile;
      }
      else if (rest == "LOCK") {
        *number = 0;
        *type = kDBLockFile;
      } else if (rest == "info.log" || rest == "info_old.log") {
        *number = 0;
        *type = kInfoLogFile;
      }  else {
        // Avoid strtoull() to keep filename format independent of the
        // current locale
        uint64_t num;
        if (!ConsumeDecimalNumber(&rest, &num)) {
          return false;
        }
        Slice suffix = rest;
        if (suffix == Slice(".log")) {
          *type = kLogFile;
        } else if (suffix == Slice(".lsf")) {
          *type = kStoreFile;
        }  else {
          return false;
        }
        *number = num;
      }
      return true;
    }


}  // namespace stackfiledb
