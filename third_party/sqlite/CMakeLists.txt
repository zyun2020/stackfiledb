
cmake_minimum_required(VERSION 3.9)

project(SQLite3
    DESCRIPTION "sqlite3, built with CMake"
    VERSION 3.31.1
    LANGUAGES C)

# Uncomment this for single-threaded variant (faster)
#add_definitions(-DSQLITE_THREADSAFE=0)

# Library
add_library(sqlite3 STATIC)
target_sources(sqlite3
    PRIVATE
    src/sqlite3.c)
target_include_directories(sqlite3
    INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src>)
set_target_properties(sqlite3 PROPERTIES
    OUTPUT_NAME sqlite3
    VERSION ${PROJECT_VERSION})