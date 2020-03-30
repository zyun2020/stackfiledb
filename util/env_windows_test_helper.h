// Copyright 2018 (c) The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STACKFILE_DB_UTIL_ENV_WINDOWS_TEST_HELPER_H_
#define STACKFILE_DB_UTIL_ENV_WINDOWS_TEST_HELPER_H_

namespace stackfiledb {

class EnvWindowsTest;

// A helper for the Windows Env to facilitate testing.
class EnvWindowsTestHelper {
 private:
  friend class CorruptionTest;
  friend class EnvWindowsTest;

  // Set the maximum number of read-only files that will be mapped via mmap.
  // Must be called before creating an Env.
  static void SetReadOnlyMMapLimit(int limit);
};

}  // namespace stackfiledb

#endif  // STACKFILE_DB_UTIL_ENV_WINDOWS_TEST_HELPER_H_
