
#include "db/store_index.h"
#include "db/stack_bucket.h"

#include "db/mem_log.h"
#include "db/filename.h"

namespace stackfiledb {

	#define STORE_TABLE_CREATE "CREATE TABLE zy_store_index\
	(skey INTEGER PRIMARY KEY,ts INTEGER,pos INTEGER,size INTEGER,flags INTEGER,file_number INTEGER,block_count INTEGER,value_type INTEGER,bucketId INTEGER)"

	#define DELETION_TABLE_CREATE "CREATE TABLE zy_deletion_index\
	(skey INTEGER,ts INTEGER,pos INTEGER,size INTEGER,flags INTEGER,file_number INTEGER,block_count INTEGER,value_type INTEGER,bucketId INTEGER,PRIMARY KEY(bucketId,skey,ts)) WITHOUT ROWID"

	#define FILEMETA_TABLE_CREATE "CREATE TABLE zy_store_file\
	(bucketId INTEGER,file_number INTEGER,file_size INTEGER,creation_ts INTEGER,completion_ts INTEGER,PRIMARY KEY(bucketId,file_number)) WITHOUT ROWID"

	#define LOGFILE_TABLE_CREATE "CREATE TABLE zy_log_file\
	(log_file_number INTEGER PRIMARY KEY,ts INTEGER)"

    #define STORE_TABLE_INDEX "CREATE INDEX bucketId_zy_store on zy_store_index(bucketId)"
	
    #define INDEX_FIELD "skey,ts,pos,size,flags,file_number,block_count,value_type,bucketId"
    #define FILEMETA_FIELD "bucketId,file_number,file_size,creation_ts,completion_ts"

    #define INDEX_PARAM "(?,?,?,?,?,?,?,?,?)"

	//table name
	#define STORE_TABLE "zy_store_index"
	#define DELETION_TABLE "zy_deletion_index"
	#define FILEMETA_TABLE "zy_store_file"
	#define LOGFILE_TABLE "zy_log_file"

	static const int kStoreFile_MetaSize = 48;

    #define SQL_SIZE 256

	StoreIndex::StoreIndex()
		: sqlite_cnn_(nullptr) {
	}

	StoreIndex::~StoreIndex() {
		Close();
	}

	void StoreIndex::Close() {
		if (sqlite_cnn_ != nullptr) {
			sqlite_cnn_->Close();
			delete sqlite_cnn_;
			sqlite_cnn_ = nullptr;
		}
	}

	inline Status StoreIndex::DbError() const {
		return Status::SqliteError(sqlite_cnn_->LastErrorMsg());
	}

	Status StoreIndex::NewIndexDB(const std::string& dbfile) {
		Close();

		Status s;
		sqlite_cnn_ = new SqliteConnection();
		if (sqlite_cnn_->Create(dbfile.c_str())) {
			if (sqlite_cnn_->ExecuteNonQuery(STORE_TABLE_CREATE) != -1 &&
				sqlite_cnn_->ExecuteNonQuery(DELETION_TABLE_CREATE) != -1 &&
				sqlite_cnn_->ExecuteNonQuery(LOGFILE_TABLE_CREATE) != -1 &&
				sqlite_cnn_->ExecuteNonQuery(FILEMETA_TABLE_CREATE) != -1 &&
				sqlite_cnn_->ExecuteNonQuery(STORE_TABLE_INDEX) != -1) {
				return s;
			}
		}
		s = DbError();

		Close();
		return s;
	}

	Status StoreIndex::OpenIndexDB(const std::string& dbfile) {
		Close();

		Status s;
		sqlite_cnn_ = new SqliteConnection();
		if (sqlite_cnn_->Connect(dbfile.c_str())) {
			return s;
		}
		s = DbError();

		Close();
		return s;
	}

	bool StoreIndex::InsertLog(SqliteStatement& stat, const BlobLogItem& item) {
		stat.BindParamter(0, item.key);
		stat.BindParamter(1, item.ts);
		stat.BindParamter(2, item.pos);
		stat.BindParamter(3, item.size);
		stat.BindParamter(4, item.flags);
		stat.BindParamter(5, item.file_number);
		stat.BindParamter(6, item.block_count);
		stat.BindParamter(7, item.type);
		stat.BindParamter(8, item.bucketId);

		if (stat.ExecuteNonQuery() == -1) return false;
		return true;
	}

	void StoreIndex::ReadLog(SqliteStatement& stat, BlobLogItem& item) {
		item.key = stat.GetUInt64(0);
		item.ts = stat.GetUInt64(1);
		item.pos = stat.GetUInt64(2);
		item.size = stat.GetUInt32(3);
		item.flags = stat.GetUInt32(4);
		item.file_number = stat.GetUInt32(5);
		item.block_count = stat.GetUInt32(6);
		item.type = stat.GetUInt32(7);
		item.bucketId = stat.GetUInt32(8);
	}

	void StoreIndex::ReadLog(SqliteStatement& stat, uint64_t &key, BlobDataIndex& item) {
		//item.key = stat.GetUInt64(0);
		key = stat.GetUInt64(0);
		item.ts = stat.GetUInt64(1);
		item.pos = stat.GetUInt64(2);
		item.size = stat.GetUInt32(3);
		item.flags = stat.GetUInt32(4);
		item.file_number = stat.GetUInt32(5);
		item.block_count = stat.GetUInt16(6);
		//item.type = stat.GetUInt32(7);
		//item.bucketId = stat.GetUInt16(8);
	}

	void StoreIndex::ReadMeta(SqliteStatement& stat, StoreFileMeta& item) {
		//bucketId = stat.GetUInt32(0);
		item.file_number = stat.GetUInt32(1);
		item.file_size = stat.GetUInt64(2);
		item.seeks_count = 0;
	}

	uint32_t StoreIndex::GetMaxLogFileNumber() {
		assert(sqlite_cnn_ != nullptr);
		  return sqlite_cnn_->ExecScalar("select max(log_file_number) from zy_log_file");
	}

	Status StoreIndex::MergeLog(const MemLog* mem, uint32_t log_file_number, uint64_t ts) {
		assert(sqlite_cnn_ != nullptr);

		char sql[SQL_SIZE] ;
		snprintf(sql, SQL_SIZE, "insert into %s (%s) values %s", STORE_TABLE, INDEX_FIELD, INDEX_PARAM);
		SqliteStatement store_index_stat;
		if (!store_index_stat.Prepare(sqlite_cnn_, sql)) return DbError();

		snprintf(sql, SQL_SIZE, "insert into %s (%s) values %s", DELETION_TABLE, INDEX_FIELD, INDEX_PARAM);
		SqliteStatement deletion_index_stat;
		if (!deletion_index_stat.Prepare(sqlite_cnn_, sql)) return DbError();
		
		//Transaction
		if (!sqlite_cnn_->BeginTransaction()) return DbError();
			
		BlobLogItem item, item0;
		bool is_ok = true;
		MemLog::LogSkipList::Iterator* iter = mem->NewIterator();
		for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
			item = iter->key();
			if (item.type == kTypeValue) {
				is_ok = InsertLog(store_index_stat, item);
			}
			else {
				snprintf(sql, SQL_SIZE, "select %s from %s where skey=%llu", INDEX_FIELD, STORE_TABLE, item.key);
				SqliteStatement stat;
				is_ok = stat.Execute(sqlite_cnn_, sql);
				if (is_ok && !stat.Fetch()) is_ok = false;
				ReadLog(stat, item0);
				if (item0.file_number != item.file_number) {
					item.file_number = item0.file_number;
					item.pos = item0.pos;
				}

				if (is_ok) {
					is_ok = InsertLog(deletion_index_stat, item);
				}
				if (is_ok) {
					snprintf(sql, SQL_SIZE, "delete from %s where skey=%llu", STORE_TABLE, item.key);
					is_ok = sqlite_cnn_->ExecuteNonQuery(sql) > 0 ? true : false;
				}
			}
			if (!is_ok) break;
		}
		delete iter;

		//merge log file number
		if (is_ok) {
			snprintf(sql, SQL_SIZE, "insert into zy_log_file (log_file_number,ts) values (%d,%llu)", log_file_number, ts);
			is_ok = sqlite_cnn_->ExecuteNonQuery(sql) > 0 ? true : false;
		}
		if (is_ok) {
			is_ok = sqlite_cnn_->Commit();
		}
		else {
			sqlite_cnn_->Rollback();
		}

		Status s;
		if (!is_ok) {
			s = DbError();
		}

		return s;
	}

	//bucketId,file_number,file_size,creation_ts,completion_ts
	Status StoreIndex::CreateStoreFile(uint32_t bucketId, uint32_t old_file_number, uint64_t old_file_size, uint32_t file_number, uint64_t ts) {
		if (!sqlite_cnn_->BeginTransaction()) return DbError();
		char sql[SQL_SIZE] ;

		snprintf(sql, SQL_SIZE, "insert into zy_store_file (bucketId,file_number,file_size,creation_ts,completion_ts) values (%d,%d,%d,%llu,%d)",
			bucketId, file_number, 0, ts, 0);
		sqlite_cnn_->ExecuteNonQuery(sql);

		if (old_file_number > 0) {
			snprintf(sql, SQL_SIZE, "update zy_store_file set file_size=%llu,completion_ts=%llu where bucketId=%d and file_number=%d",
				old_file_size, ts, bucketId, old_file_number);
			sqlite_cnn_->ExecuteNonQuery(sql);
		}
		if (sqlite_cnn_->Commit()) {
			return Status::OK();
		}
		return DbError();
	}

	Status StoreIndex::Recover(StackBucket* bucket) {
		assert(sqlite_cnn_ != nullptr);

		SqliteStatement stat;
		char sql[SQL_SIZE] ;
		//store file meta info
		snprintf(sql, SQL_SIZE, "select %s from %s where bucketId=%d", FILEMETA_FIELD, FILEMETA_TABLE, bucket->BucketId());
		if (!stat.Execute(sqlite_cnn_, sql))  return DbError();

		StoreFileMetaMap* file_metas = new StoreFileMetaMap();
		uint32_t file_number = 0;
		uint64_t file_size = 0;;
		while (stat.Fetch()) {
			StoreFileMeta meta;
			ReadMeta(stat, meta);
			if (meta.file_number > file_number) {
				file_number = meta.file_number;
				file_size = meta.file_size;
			}
			file_metas->insert(std::make_pair(meta.file_number, meta));
		}
		assert(file_size == 0);
		
		//blob data index
		snprintf(sql, SQL_SIZE, "select %s from %s where bucketId=%d", INDEX_FIELD, STORE_TABLE, bucket->BucketId());
		if (!stat.Execute(sqlite_cnn_, sql))  return DbError();

		BlobDataIndexMap* blob_index = new BlobDataIndexMap();
		BlobDataIndex idxItem;
		uint64_t key;

		while (stat.Fetch()) {
			ReadLog(stat, key, idxItem);
			blob_index->insert(std::make_pair(key, idxItem));

			if (idxItem.file_number == file_number) {
				if (file_size < (idxItem.pos + store_size(idxItem.size, idxItem.block_count))) {
					file_size = idxItem.pos + store_size(idxItem.size, idxItem.block_count);
				}
			}
		}
	
		//deletion data index
		snprintf(sql, SQL_SIZE, "select %s from %s where bucketId=%d", INDEX_FIELD, DELETION_TABLE, bucket->BucketId());
		if (!stat.Execute(sqlite_cnn_, sql))  return DbError();

		BlobLogItem logItem;
		while (stat.Fetch())
		{
			ReadLog(stat, logItem);
			if (logItem.file_number == file_number) {
				if (file_size < (logItem.pos + store_size(logItem.size, logItem.block_count))) {
					file_size = logItem.pos + store_size(logItem.size, logItem.block_count);
				}
			}
		}

		return  bucket->Recover(blob_index, file_metas, file_number, file_size);
	}

	Status StoreIndex::GetCompactFile(uint32_t bucketId, uint64_t ts, const Options& options, std::vector<uint32_t>& file_numbers,
		uint32_t& new_file_count, uint64_t& max_file_size) {
		std::vector<StoreFileMeta> files;
		if (!GetStoreFile(bucketId, files)) return DbError();

		char sql[SQL_SIZE] ;
		SqliteStatement stat;

		std::vector<StoreFileMeta> files_compact;
		StoreFileMeta file;

		uint64_t size;
		uint32_t count, itemcount;
		for (auto it = files.begin(); it != files.end(); ++it) {
			//store file meta info
			snprintf(sql, SQL_SIZE, "select sum(size),sum(block_count),count(*) from %s where bucketId=%d and file_number=%d and ts<=%llu order by file_number", 
				DELETION_TABLE, bucketId, it->file_number, ts);
			if (stat.Execute(sqlite_cnn_, sql)) {
				size = stat.GetUInt32(0);
				count = stat.GetUInt32(1);
				itemcount = stat.GetUInt32(2);

				if ((size + count * kBlock_MetaSize + itemcount * kBlob_HeaderSize) >= static_cast<uint64_t>(it->file_size * options.percent_compact)) {
					file.file_number = it->file_number;
					file.file_size = it->file_size - (size + count * kBlock_MetaSize + itemcount * kBlob_HeaderSize);
					files_compact.push_back(file);
				}
			}
		}

		uint64_t tolerance = static_cast<uint64_t>(options.max_file_size * options.file_size_tolerance);
		uint64_t remainder;
		auto end = files_compact.end();
		size = 0;
		count = 0;
		itemcount = 0;
		for (auto it = files_compact.begin(); it != files_compact.end(); ++it) {
			size += it->file_size;
			count++;
			itemcount = size / options.max_file_size;
			if (itemcount > 0) {
				remainder = size % options.max_file_size;
				if (remainder < tolerance * itemcount) {					
					if (itemcount < count) {
						end = it;
						max_file_size = options.max_file_size + remainder / itemcount;
						break;
					}
				}
				else if ((options.max_file_size - remainder) < tolerance * (itemcount + 1)) {
					end = it;
					itemcount++;
					if (itemcount < count) {
						max_file_size = options.max_file_size - remainder / itemcount;
						break;
					}
				}
			}
		}
		new_file_count = itemcount;
		if (new_file_count > 0) {
			for (auto it = files_compact.begin(); it <= end; ++it) {
				file_numbers.push_back(it->file_number);
			}
		}

		return Status::OK();
	}

	Status StoreIndex::GetReserveDeletion(uint32_t bucketId, uint64_t ts, const std::vector<uint32_t>& files, std::vector<BlobLogItem>& log_items) {
		SqliteStatement stat;
		char sql[SQL_SIZE] ;

		for (size_t i = 0; i < files.size(); i++) {
			//deletion data index
			snprintf(sql, SQL_SIZE, "select %s from %s where bucketId=%d and ts>%llu and file_number=%d order by pos", INDEX_FIELD, DELETION_TABLE, bucketId, ts, files[i]);
			if (!stat.Execute(sqlite_cnn_, sql))  return DbError();

			MemLog* memlog = new MemLog();
			BlobLogItem logItem;
			while (stat.Fetch())
			{
				ReadLog(stat, logItem);
				log_items.push_back(logItem);
			}
		}
		return Status::OK();
	}

	bool StoreIndex::GetStoreFile(uint32_t bucketId, std::vector<StoreFileMeta> files) {
		char sql[SQL_SIZE] ;
		SqliteStatement stat;

		//store file meta info
		snprintf(sql, SQL_SIZE, "select file_number,file_size from %s where bucketId=%d", FILEMETA_TABLE, bucketId);
		if (!stat.Execute(sqlite_cnn_, sql))  return  false;

		StoreFileMeta file;
		while (stat.Fetch()) {
			file.file_number = stat.GetUInt32(0);
			file.file_size = stat.GetUInt64(1);

			files.push_back(file);
		}
		return true;
	}

	Status StoreIndex::Compact(uint32_t bucketId, const std::vector<BlobLogItem>& items, int value_count, const std::vector<uint32_t>& files,
		const std::vector<StoreFileMeta>& new_files, uint64_t ts) {
		assert(sqlite_cnn_ != nullptr);

		char sql[SQL_SIZE] ;
		snprintf(sql, SQL_SIZE, "update %s set file_number=?,set pos=? where skey=?", STORE_TABLE);
		
		//Transaction
		if (!sqlite_cnn_->BeginTransaction()) return DbError();

		SqliteStatement stat;
		if (!stat.Prepare(sqlite_cnn_, sql)) return DbError();

		bool is_ok = true;
		BlobLogItem item;
		for (size_t i = 0; i < value_count; i++) {
			item = items[i];
			if (item.type == kTypeDeletion) continue;
			stat.BindParamter(2, item.file_number);
			stat.BindParamter(1, item.pos);
			stat.BindParamter(0, item.key);
			if (stat.ExecuteNonQuery() == -1) {
				is_ok = false;
				break;
			}
		}

		snprintf(sql, SQL_SIZE, "update %s set file_number=?,set pos=? where skey=?", DELETION_TABLE);
		for (size_t i = value_count; i < items.size(); i++) {
			item = items[i];
			if (item.type == kTypeValue) continue;
			stat.BindParamter(2, item.file_number);
			stat.BindParamter(1, item.pos);
			stat.BindParamter(0, item.key);
			if (stat.ExecuteNonQuery() == -1) {
				is_ok = false;
				break;
			}
		}

		for (size_t i = 0; i < files.size(); i++) {
			snprintf(sql, SQL_SIZE, "delete from zy_store_file where bucketId=%d and file_number=%d", bucketId, files[i]);
			if (sqlite_cnn_->ExecuteNonQuery(sql) == -1) {
				is_ok = false;
				break;
			}

			snprintf(sql, SQL_SIZE, "delete from zy_deletion_index where bucketId=%d and ts<%llu and file_number=%d", bucketId, ts, files[i]);
			if (sqlite_cnn_->ExecuteNonQuery(sql) == -1) {
				is_ok = false;
				break;
			}
		}

		for (size_t i = 0; i < new_files.size(); i++) {
			snprintf(sql, SQL_SIZE, "insert into zy_store_file (bucketId,file_number,file_size,creation_ts,completion_ts) values (%d,%d,%llu,%llu,%llu))",
				bucketId, new_files[i].file_number, new_files[i].file_size, ts, ts);
			if (sqlite_cnn_->ExecuteNonQuery(sql) == -1) {
				is_ok = false;
				break;
			}
		}

		if (is_ok) {
			is_ok = sqlite_cnn_->Commit();
		}
		else {
			sqlite_cnn_->Rollback();
		}

		Status s;
		if (!is_ok) {
			s = DbError();
		}

		return s;
	}

}//stackfiledb