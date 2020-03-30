
cmake_minimum_required (VERSION 3.9)

# Keep the version below in sync with the one in db.h
project(stackfiledb VERSION 0.202 LANGUAGES C CXX)

# C standard can be overridden when this is used as a sub-project.
if(NOT CMAKE_C_STANDARD)
  # This project can use C11, but will gracefully decay down to C89.
  set(CMAKE_C_STANDARD 11)
  set(CMAKE_C_STANDARD_REQUIRED OFF)
  set(CMAKE_C_EXTENSIONS OFF)
endif(NOT CMAKE_C_STANDARD)

# C++ standard can be overridden when this is used as a sub-project.
if(NOT CMAKE_CXX_STANDARD)
  # This project requires C++11.
  set(CMAKE_CXX_STANDARD 11)
  set(CMAKE_CXX_STANDARD_REQUIRED ON)
  set(CMAKE_CXX_EXTENSIONS OFF)
endif(NOT CMAKE_CXX_STANDARD)

if (WIN32)
  set(STACKFILEDB_PLATFORM_NAME STACKFILEDB_PLATFORM_WINDOWS)
  # TODO(cmumford): Make UNICODE configurable for Windows.
  add_definitions(-D_UNICODE -DUNICODE)
else (WIN32)
  set(STACKFILEDB_PLATFORM_NAME STACKFILEDB_PLATFORM_POSIX)
endif (WIN32)

option(STACKFILEDB_BUILD_TESTS "Build StackFileDB's unit tests" ON)
option(STACKFILEDB_BUILD_BENCHMARKS "Build StackFileDB's benchmarks" ON)
option(STACKFILEDB_INSTALL "Install StackFileDB's header and library" ON)

include(TestBigEndian)
test_big_endian(LEVELDB_IS_BIG_ENDIAN)

include(CheckIncludeFile)
check_include_file("unistd.h" HAVE_UNISTD_H)

include(CheckLibraryExists)
check_library_exists(crc32c crc32c_value "" HAVE_CRC32C)
check_library_exists(snappy snappy_compress "" HAVE_SNAPPY)
check_library_exists(tcmalloc malloc "" HAVE_TCMALLOC)

include(CheckCXXSymbolExists)
# Using check_cxx_symbol_exists() instead of check_c_symbol_exists() because
# we're including the header from C++, and feature detection should use the same
# compiler language that the project will use later. Principles aside, some
# versions of do not expose fdatasync() in <unistd.h> in standard C mode
# (-std=c11), but do expose the function in standard C++ mode (-std=c++11).
check_cxx_symbol_exists(fdatasync "unistd.h" HAVE_FDATASYNC)
check_cxx_symbol_exists(F_FULLFSYNC "fcntl.h" HAVE_FULLFSYNC)
check_cxx_symbol_exists(O_CLOEXEC "fcntl.h" HAVE_O_CLOEXEC)

if(CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")
  # Disable C++ exceptions.
  string(REGEX REPLACE "/EH[a-z]+" "" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /EHs-c-")
  add_definitions(-D_HAS_EXCEPTIONS=0)

  # Disable RTTI.
  string(REGEX REPLACE "/GR" "" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /GR-")
else(CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")
  # Enable strict prototype warnings for C code in clang and gcc.
  if(NOT CMAKE_C_FLAGS MATCHES "-Wstrict-prototypes")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wstrict-prototypes")
  endif(NOT CMAKE_C_FLAGS MATCHES "-Wstrict-prototypes")

  # Disable C++ exceptions.
  string(REGEX REPLACE "-fexceptions" "" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-exceptions")

  # Disable RTTI.
  string(REGEX REPLACE "-frtti" "" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-rtti")
endif(CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")

# Test whether -Wthread-safety is available. See
# https://clang.llvm.org/docs/ThreadSafetyAnalysis.html
include(CheckCXXCompilerFlag)
check_cxx_compiler_flag(-Wthread-safety HAVE_CLANG_THREAD_SAFETY)

# Used by googletest.
check_cxx_compiler_flag(-Wno-missing-field-initializers
                        LEVELDB_HAVE_NO_MISSING_FIELD_INITIALIZERS)

include(CheckCXXSourceCompiles)

# Test whether C++17 __has_include is available.
check_cxx_source_compiles("
#if defined(__has_include) &&  __has_include(<string>)
#include <string>
#endif
int main() { std::string str; return 0; }
" HAVE_CXX17_HAS_INCLUDE)

set(STACKFILEDB_PUBLIC_INCLUDE_DIR "include/stackfiledb")
set(STACKFILEDB_PORT_CONFIG_DIR "include/port")

configure_file(
  "port/port_config.h.in"
  "${PROJECT_BINARY_DIR}/${STACKFILEDB_PORT_CONFIG_DIR}/port_config.h"
)

include_directories(
  "${PROJECT_BINARY_DIR}/include"
  "."
)

if(BUILD_SHARED_LIBS)
  # Only export STACKFILEDB_EXPORT symbols from the shared library.
  add_compile_options(-fvisibility=hidden)
endif(BUILD_SHARED_LIBS)

# Must be included before CMAKE_INSTALL_INCLUDEDIR is used.
include(GNUInstallDirs)

add_library(stackfiledb)
target_sources(stackfiledb
  PRIVATE
    "${PROJECT_BINARY_DIR}/${STACKFILEDB_PORT_CONFIG_DIR}/port_config.h"
    "port/port_stdcxx.h"
    "port/port.h"
    "port/thread_annotations.h"
    "db/stack_db.cc"
    "db/stack_db.h"
    "db/sem_bucket.h"
    "db/stack_bucket.cc"
    "db/stack_bucket.h"
    "db/store_cache.cc"
    "db/store_cache.h"
    "db/store_format.cc"
    "db/store_format.h"
    "db/store_index.cc"
    "db/store_index.h"
    "db/store_reader.cc"
    "db/store_reader.h"
    "db/store_writer.cc"
    "db/store_writer.h"
    "db/filename.cc"
    "db/filename.h"
    "db/log_format.h"
    "db/log_reader.cc"
    "db/log_reader.h"
    "db/log_writer.cc"
    "db/log_writer.h"
    "db/mem_log.cc"
    "db/mem_log.h"
    "db/skiplist.h"
    "db/log_batch.cc"
    "db/log_batch.h"
    "util/arena.cc"
    "util/arena.h"
    "util/cache.cc"
    "util/coding.cc"
    "util/coding.h"
    "util/crc32c.cc"
    "util/crc32c.h"
    "util/env.cc"
    "util/hash.cc"
    "util/hash.h"
    "util/logging.cc"
    "util/logging.h"
    "util/mutexlock.h"
    "util/no_destructor.h"
    "util/options.cc"
    "util/random.h"
    "util/status.cc"
    "util/sqlite_db.cc"
    "util/sqlite_db.h" 
    "util/rwlock.h"
    "util/semaphore.h"

  # Only CMake 3.3+ supports PUBLIC sources in targets exported by "install".
  $<$<VERSION_GREATER:CMAKE_VERSION,3.2>:PUBLIC>   
    "${STACKFILEDB_PUBLIC_INCLUDE_DIR}/env.h"
    "${STACKFILEDB_PUBLIC_INCLUDE_DIR}/export.h"
    "${STACKFILEDB_PUBLIC_INCLUDE_DIR}/slice.h"
    "${STACKFILEDB_PUBLIC_INCLUDE_DIR}/status.h"
    "${STACKFILEDB_PUBLIC_INCLUDE_DIR}/options.h"
    "${STACKFILEDB_PUBLIC_INCLUDE_DIR}/cache.h"
    "${STACKFILEDB_PUBLIC_INCLUDE_DIR}/reader.h"
)

if (WIN32)
  target_sources(stackfiledb
    PRIVATE
      "util/env_windows.cc"
      "util/windows_logger.h"
  )
else (WIN32)
  target_sources(stackfiledb
    PRIVATE
      "util/env_posix.cc"
      "util/posix_logger.h"
  )
endif (WIN32)

# MemEnv is not part of the interface and could be pulled to a separate library.
target_sources(stackfiledb
  PRIVATE
    "helpers/memenv/memenv.cc"
    "helpers/memenv/memenv.h"
)

# add the sqlite library
add_subdirectory("third_party/sqlite")
target_link_libraries(stackfiledb sqlite3)

target_include_directories(stackfiledb
  PUBLIC
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

set_target_properties(stackfiledb
  PROPERTIES VERSION ${PROJECT_VERSION} SOVERSION ${PROJECT_VERSION_MAJOR})

target_compile_definitions(stackfiledb
  PRIVATE
    # Used by include/export.h when building shared libraries.
    STACKFILEDB_COMPILE_LIBRARY
    # Used by port/port.h.
    ${STACKFILEDB_PLATFORM_NAME}=1
)
if (NOT HAVE_CXX17_HAS_INCLUDE)
  target_compile_definitions(stackfiledb
    PRIVATE
      STACKFILEDB_HAS_PORT_CONFIG_H=1
  )
endif(NOT HAVE_CXX17_HAS_INCLUDE)

if(BUILD_SHARED_LIBS)
  target_compile_definitions(stackfiledb
    PUBLIC
      # Used by include/export.h.
      STACKFILEDB_SHARED_LIBRARY
  )
endif(BUILD_SHARED_LIBS)

if(HAVE_CLANG_THREAD_SAFETY)
  target_compile_options(stackfiledb
    PUBLIC
      -Werror -Wthread-safety)
endif(HAVE_CLANG_THREAD_SAFETY)

if(HAVE_CRC32C)
  target_link_libraries(stackfiledb crc32c)
endif(HAVE_CRC32C)
if(HAVE_SNAPPY)
  target_link_libraries(stackfiledb snappy)
endif(HAVE_SNAPPY)
if(HAVE_TCMALLOC)
  target_link_libraries(stackfiledb tcmalloc)
endif(HAVE_TCMALLOC)

# Needed by port_stdcxx.h
find_package(Threads REQUIRED)
target_link_libraries(stackfiledb Threads::Threads)

add_executable(db_util
  "db/db_util.cc"
)
target_link_libraries(db_util stackfiledb)
