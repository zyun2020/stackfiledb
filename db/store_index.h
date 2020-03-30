#pragma once

#include "stackfiledb/status.h"
#include "stackfiledb/env.h"
#include "stackfiledb/options.h"

#include "util/sqlite_db.h"
#include "db/store_format.h"
#include "db/mem_log.h"

namespace stackfiledb {

    enum CompactState {
        kNo = 0,
        kCompacting = 1,
        kCompacted = 2
    };

    class StackBucket;
	class StoreIndex {
    public:
        StoreIndex();

        StoreIndex(const StoreIndex&) = delete;
        StoreIndex& operator=(const StoreIndex&) = delete;

        ~StoreIndex();

        Status NewIndexDB(const std::string& dbfile);
        Status OpenIndexDB(const std::string& dbfile);
        uint32_t GetMaxLogFileNumber();

        Status Recover(StackBucket* bucket);
        Status MergeLog(const MemLog* mem, uint32_t log_file_number, uint64_t ts);

        Status CreateStoreFile(uint32_t bucketId, uint32_t old_file_number, uint64_t old_file_size, uint32_t file_number, uint64_t ts);
        Status GetCompactFile(uint32_t bucketId, uint64_t ts, const Options& options, std::vector<uint32_t>& file_numbers,
            uint32_t& new_file_count, uint64_t& max_file_size);
        Status GetReserveDeletion(uint32_t bucketId, uint64_t ts, const std::vector<uint32_t>& files, std::vector<BlobLogItem>& log_items);

        Status Compact(uint32_t bucketId, const std::vector<BlobLogItem>& items, int value_count, const std::vector<uint32_t>& files,
            const std::vector<StoreFileMeta>& new_files, uint64_t ts);
    private:
        void Close();
        bool InsertLog(SqliteStatement& stat, const BlobLogItem& item);
        void ReadLog(SqliteStatement& stat, BlobLogItem& item);
        void ReadLog(SqliteStatement& stat, uint64_t& key, BlobDataIndex& item);
        void ReadMeta(SqliteStatement& stat, StoreFileMeta& item);

        bool GetStoreFile(uint32_t bucketId, std::vector<StoreFileMeta> files);
        Status DbError() const;
    private:
        SqliteConnection* sqlite_cnn_;
	};

}//stackfiledb