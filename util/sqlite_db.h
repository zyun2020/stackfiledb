#pragma once

#include "sqlite3.h"
#include "stdint.h"
#include <string>

namespace stackfiledb
{
	class SqliteError
	{
	public:
		SqliteError(void);
		~SqliteError(void);
	public:
		SqliteError(sqlite3 *pSqlite);
		bool GetLastError(sqlite3 *pSqlite);

		const char* GetErrorMessage() const { return err_msg; }
		int GetErrorCode() const { return err_code; }
	private:
		char err_msg[128];
		int err_code;
	};

	class SqliteConnection
	{
	public:
		SqliteConnection(void);
		virtual ~SqliteConnection(void);

	public:
		SqliteConnection(const char* cnn);

		bool Connect(const char* cnn, bool readonly = false);
		bool Create(const char* cnn);

		bool Connected() const;
		void Close();

		bool BeginTransaction();
		bool Commit();
		bool Rollback();

		int ExecuteNonQuery(const char* sql);
		int ExecScalar(const char* sql);
		bool TableIsExist(const char* table);

		sqlite3* NativeSqlite3() { return sqlite_; }

		bool LastError(SqliteError &oErr);
		SqliteError* LastError();

		const char* LastErrorMsg();
	private:
		sqlite3* sqlite_;			//!< Handle of enviroment

		// Uncopiable
		SqliteConnection(const SqliteConnection&) = delete;
		SqliteConnection& operator=(const SqliteConnection&) = delete;
	};

	class SqliteStatement
	{
	public:
		SqliteStatement(void);
		~SqliteStatement(void);
	public:
		SqliteStatement(SqliteConnection* cnn, const char* sql);
		bool Prepare(SqliteConnection* cnn, const char* sql);

		int ExecuteNonQuery();
		int ExecuteNonQuery(SqliteConnection* cnn, const char* sql);

		bool Execute(SqliteConnection* cnn, const char* sql);
		bool Fetch();

		int NumParams();
		int NumColumns();
		void Close();

		//----------------------------------------------------------------------------
		// 获取值
		int32_t GetInt32(int nIndex);
		uint32_t GetUInt32(int nIndex);

		int16_t GetInt16(int nIndex);
		uint16_t GetUInt16(int nIndex);
		
		int64_t GetInt64(int nIndex);
		uint64_t GetUInt64(int nIndex);
		double GetDouble(int nIndex);
		float GetFloat(int nIndex);
		char* GetString(int nIndex);
		int GetString(int nIndex, char* buf, int buf_size);
		char* GetBlob(int nIndex, int& nLen);
		int GetBlob(int nIndex, char* buf, int buf_size);
		const char* GetStaticBlob(int nIndex, int& size);

		//----------------------------------------------------------------------------
		// 设置参数
		bool BindParamter(int nIndex, int32_t val);
		bool BindParamter(int nIndex, uint32_t val);
		bool BindParamter(int nIndex, int16_t val);
		bool BindParamter(int nIndex, uint16_t val);
		bool BindParamter(int nIndex, int64_t val);
		bool BindParamter(int nIndex, uint64_t val);
		bool BindParamter(int nIndex, double val);
		bool BindParamter(int nIndex, float val);
		bool BindParamter(int nIndex, const char *val, int len = -1);
		bool BindParamter(int nIndex, const unsigned char* val, int nLen);

		//----------------------------------------------------------------------------
		bool LastError(SqliteError &oErr);
		SqliteError* LastError();
	private:
		sqlite3_stmt* sqlite_stmt_;
		sqlite3* sqlite_;
	};

}