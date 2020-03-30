
#include <stdio.h>

#include "stackfiledb/env.h"
#include "stackfiledb/status.h"
#include "stackfiledb/reader.h"
#include "stackfiledb/options.h"
#include "stackfiledb/db.h"

using namespace stackfiledb;


int main(int argc, char** argv) {
    bool ok = true;

    DB* db = nullptr;
    Options options;
    options.create_if_missing = true;
    Status status = DB::Open(options, "f:/stack_db", &db);
    if(!status.ok()) ok = false;

    
    Env* env = options.env;

    RandomAccessFile* access_file = nullptr;
    status = env->NewRandomAccessFile("F:/stack_db_test/31.jpg", &access_file);
    uint64_t file_size = 0;
    status = env->GetFileSize("F:/stack_db_test/31.jpg", &file_size);
    char* buf = new char[file_size];
    
    Slice result;
    status = access_file->Read(0, file_size, &result, buf);
    assert(result.size() == file_size);
   
   
    uint64_t key = 20;
    WriteOptions write_options;
    for (int i = 0; i < 1; i++) {
        status = db->Add(write_options, key, result);
        //key++;
    }

    delete access_file;

    WritableFile* write_file = nullptr;
    status = env->NewWritableFile("F:/stack_db_test/out.jpg", &write_file);
    ReadOptions read_options;
    Reader* reader = nullptr;
    status = db->GetReader(read_options, key, &reader);

    uint32_t readed = 0;
    uint32_t total = 0;
    if (status.ok()) {
        while (total < reader->Size()) {
            readed = reader->Read(&result);
            if (readed > 0) {
                total += readed;
                write_file->Append(result);
            }
            else {
                break;
            }
        }
        if (total == reader->Size()) {
            write_file->Close();
        }
        reader->Close();
    }

    return (ok ? 0 : 1);
}
