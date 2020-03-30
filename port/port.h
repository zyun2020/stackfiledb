// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STACKFILE_DB_PORT_PORT_H_
#define STACKFILE_DB_PORT_PORT_H_

// Include the appropriate platform specific file below.  If you are
// porting to a new platform, see "port_example.h" for documentation
// of what the new port_<platform>.h file must provide.
#if defined(STACKFILEDB_PLATFORM_POSIX) || defined(STACKFILEDB_PLATFORM_WINDOWS)
#include "port/port_stdcxx.h"
#endif

#endif  // STACKFILE_DB_PORT_PORT_H_
