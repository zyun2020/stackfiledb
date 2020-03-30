
#include "util/sqlite_db.h"
#include "stdio.h"

namespace stackfiledb
{
	//------------------------------------------------------------------------------------------
	// SqliteError
	//------------------------------------------------------------------------------------------
	SqliteError::SqliteError(void)
	{
		err_msg[0] = '\0';
		err_code = SQLITE_OK;
	}

	SqliteError::~SqliteError(void)
	{
	}

	SqliteError::SqliteError(sqlite3* sqlite)
	{
		err_msg[0] = '\0';
		err_code = SQLITE_OK;
		GetLastError(sqlite);
	}

	bool SqliteError::GetLastError(sqlite3* sqlite)
	{
		if (sqlite == nullptr)
			return false;

		err_msg[0] = '\0';
		err_code = sqlite3_errcode(sqlite);
		if (err_code != SQLITE_OK) {
#ifdef _WIN32
			strcpy_s(err_msg, 128, (char*)sqlite3_errmsg(sqlite));
#else
			strcpy(err_msg, (char*)sqlite3_errmsg(pSqlite));
#endif
		}

		return true;
	}

	//-----------------------------------------------------------------------------------------------------
	//
	//  class  SqliteConnection
	//
	//-----------------------------------------------------------------------------------------------------
	SqliteConnection::SqliteConnection(void)
	{
		sqlite_ = nullptr;
	}

	SqliteConnection::~SqliteConnection(void)
	{
		Close();
	}

	bool SqliteConnection::Connected() const
	{
		return sqlite_ == nullptr ? false : true;
	}

	SqliteConnection::SqliteConnection(const char* cnn)
	{
		sqlite_ = nullptr;
		Connect(cnn);
	}

	const char* SqliteConnection::LastErrorMsg() {
		if (sqlite_ != nullptr) {
			return sqlite3_errmsg(sqlite_);
		}
		return "Unknow error";
	}

	bool SqliteConnection::Connect(const char* cnn, bool readonly)
	{
		Close();

		int rc;
		if (readonly)
			rc = sqlite3_open_v2(cnn, &sqlite_, SQLITE_OPEN_READONLY | SQLITE_OPEN_NOMUTEX, nullptr);
		else
			rc = sqlite3_open_v2(cnn, &sqlite_, SQLITE_OPEN_READWRITE | SQLITE_OPEN_NOMUTEX, nullptr);
		if (rc != SQLITE_OK)
		{
			Close();
			return false;
		}
		return true;
	}

	bool SqliteConnection::Create(const char* cnn)
	{
		int rc = sqlite3_open_v2(cnn, &sqlite_, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_NOMUTEX, nullptr);
		if (rc != SQLITE_OK)
		{
			Close();
			return false;
		}
		return true;
	}

	void SqliteConnection::Close()
	{
		if (sqlite_ != nullptr)
		{
			sqlite3_close(sqlite_);
			sqlite_ = nullptr;
		}
	}

	bool SqliteConnection::BeginTransaction()
	{
		if (ExecuteNonQuery("begin transaction;") == -1)
		{
			return false;
		}
		return true;
	}

	bool SqliteConnection::Commit()
	{
		if (ExecuteNonQuery("commit transaction;") == -1)
		{
			return false;
		}
		return true;
	}

	bool SqliteConnection::Rollback()
	{
		if (ExecuteNonQuery("rollback transaction;") == -1)
		{
			return false;
		}
		return true;
	}

	int SqliteConnection::ExecuteNonQuery(const char* sql)
	{
		if (!Connected()) return -1;
		SqliteStatement oStatement;
		return oStatement.ExecuteNonQuery(this, sql);
	}

	int SqliteConnection::ExecScalar(const char* sql)
	{
		SqliteStatement oStatement;
		if (!oStatement.Prepare(this, sql)) return 0;
		if (!oStatement.Fetch()) return 0;

		return oStatement.GetInt32(0);
	}

	bool SqliteConnection::TableIsExist(const char* table)
	{
		char szSQL[256];
		snprintf(szSQL, 256, "select count(*) from sqlite_master where type='table' and name='%s'", table);
		int nRet = ExecScalar(szSQL);

		return (nRet > 0);
	}


	bool SqliteConnection::LastError(SqliteError &oErr)
	{
		if (sqlite_ == nullptr)
			return false;
		return oErr.GetLastError(sqlite_);
	}

	SqliteError* SqliteConnection::LastError()
	{
		SqliteError*pErr = new SqliteError(sqlite_);
		return  pErr;
	}

	//-----------------------------------------------------------------------------------------------------
	//
	//  class  SqliteStatement
	//
	//-----------------------------------------------------------------------------------------------------
	SqliteStatement::SqliteStatement(void)
	{
		sqlite_ = nullptr;
		sqlite_stmt_ = nullptr;
	}

	SqliteStatement::~SqliteStatement(void)
	{
		Close();
	}

	void SqliteStatement::Close()
	{
		if (sqlite_stmt_ != nullptr)
		{
			sqlite3_finalize(sqlite_stmt_);
			sqlite_stmt_ = nullptr;
		}
	}

	SqliteStatement::SqliteStatement(SqliteConnection* cnn, const char* sql)
	{
		sqlite_ = nullptr;
		sqlite_stmt_ = nullptr;

		Prepare(cnn, sql);
	}

	bool SqliteStatement::Prepare(SqliteConnection* cnn, const char* sql)
	{
		if (cnn == nullptr) return false;

		Close();

		sqlite_ = cnn->NativeSqlite3();
		if (sqlite_ == nullptr) return false;

		const char* tail = nullptr;
		sqlite3_stmt* pVM = nullptr;
		int nRet = sqlite3_prepare_v2(sqlite_, sql, -1, &pVM, &tail);
		if (nRet != SQLITE_OK)
		{
			return false;
		}

		sqlite_stmt_ = pVM;

		return true;
	}

	int SqliteStatement::ExecuteNonQuery()
	{
		if (sqlite_stmt_ == nullptr) return -1;

		int nRowsChanged = -1;

		int nRet = sqlite3_step(sqlite_stmt_);
		if (nRet == SQLITE_DONE)
		{
			nRowsChanged = sqlite3_changes(sqlite_);
		}

		sqlite3_reset(sqlite_stmt_);

		return nRowsChanged;
	}

	bool SqliteStatement::Execute(SqliteConnection* cnn, const char* sql)
	{
		if (cnn == nullptr) return false;

		Close();

		sqlite_ = cnn->NativeSqlite3();
		if (sqlite_ == nullptr) return false;

		const char* szTail = nullptr;
		sqlite3_stmt* pVM = nullptr;
		int nRet = sqlite3_prepare_v2(sqlite_, sql, -1, &pVM, &szTail);
		if (nRet != SQLITE_OK)
		{
			return false;
		}
		sqlite_stmt_ = pVM;

		return true;
	}

	int SqliteStatement::ExecuteNonQuery(SqliteConnection* cnn, const char* sql)
	{
		if (cnn == nullptr) return false;

		Close();

		sqlite_ = cnn->NativeSqlite3();
		if (sqlite_ == nullptr) return false;

		const char* szTail = nullptr;
		sqlite3_stmt* pVM = nullptr;
		int nRet = sqlite3_prepare_v2(sqlite_, sql, -1, &pVM, &szTail);
		if (nRet != SQLITE_OK)
		{
			return -1;
		}

		int nRowsChanged = -1;

		nRet = sqlite3_step(pVM);
		if (nRet == SQLITE_DONE)
		{
			nRowsChanged = sqlite3_changes(sqlite_);
		}

		sqlite_stmt_ = pVM;

		return nRowsChanged;
	}

	bool SqliteStatement::Fetch()
	{
		int nRet = sqlite3_step(sqlite_stmt_);
		if (nRet == SQLITE_ROW)
		{
			return true;
		}
		else if (nRet == SQLITE_BUSY)
		{
			if (sqlite3_step(sqlite_stmt_) == SQLITE_ROW)
				return true;
		}

		return false;
	}

	int SqliteStatement::NumParams()
	{
		if (sqlite_stmt_ == nullptr) return -1;
		return sqlite3_bind_parameter_count(sqlite_stmt_);
	}

	int SqliteStatement::NumColumns()
	{
		if (sqlite_stmt_ == nullptr) return -1;
		return sqlite3_column_count(sqlite_stmt_);
	}

	int32_t SqliteStatement::GetInt32(int nIndex)
	{
		return sqlite3_column_int(sqlite_stmt_, nIndex);
	}

	uint32_t SqliteStatement::GetUInt32(int nIndex)
	{
		return (uint32_t)sqlite3_column_int(sqlite_stmt_, nIndex);
	}

	int16_t SqliteStatement::GetInt16(int nIndex)
	{
		return (int16_t)sqlite3_column_int(sqlite_stmt_, nIndex);
	}

	uint16_t SqliteStatement::GetUInt16(int nIndex)
	{
		return (uint16_t)sqlite3_column_int(sqlite_stmt_, nIndex);
	}

	int64_t SqliteStatement::GetInt64(int nIndex)
	{
		return (int64_t)sqlite3_column_int64(sqlite_stmt_, nIndex);
	}

	uint64_t SqliteStatement::GetUInt64(int nIndex)
	{
		return (uint64_t)sqlite3_column_int64(sqlite_stmt_, nIndex);
	}

	double SqliteStatement::GetDouble(int nIndex)
	{
		return sqlite3_column_double(sqlite_stmt_, nIndex);
	}

	float SqliteStatement::GetFloat(int nIndex)
	{
		return (float)sqlite3_column_double(sqlite_stmt_, nIndex);
	}

	char* SqliteStatement::GetString(int nIndex)
	{
		int nChar = sqlite3_column_bytes(sqlite_stmt_, nIndex) + 1;
		const unsigned char* pData = sqlite3_column_text(sqlite_stmt_, nIndex);
		if (nChar == 0 || pData == nullptr)
			return nullptr;

		char *pDst = new char[nChar];
		memcpy(pDst, pData, nChar);

		return pDst;
	}

	int SqliteStatement::GetString(int nIndex, char* buf, int buf_size)
	{
		if (buf == nullptr) return false;

		const char *pTemp = (const char*)sqlite3_column_text(sqlite_stmt_, nIndex);
		if (pTemp == nullptr) return false;
#ifdef _WIN32
		strcpy_s(buf, buf_size, pTemp);
#else
		strcpy(lpszBuffer, pTemp);
#endif // _Win32

		return true;
	}

	char* SqliteStatement::GetBlob(int nIndex, int& nLen)
	{
		nLen = sqlite3_column_bytes(sqlite_stmt_, nIndex);
		const char* pData = (const char*)sqlite3_column_blob(sqlite_stmt_, nIndex);
		if (nLen == 0 || pData == nullptr)
			return nullptr;

		char*pDst = new char[nLen];
		memcpy(pDst, pData, nLen);

		return pDst;
	}

	int SqliteStatement::GetBlob(int nIndex, char* buf, int buf_size)
	{
		int nLen = sqlite3_column_bytes(sqlite_stmt_, nIndex);
		if (nLen > buf_size) return nLen;

		const char* pData = (const char*)sqlite3_column_blob(sqlite_stmt_, nIndex);
		if (nLen == 0 || pData == nullptr)
			return 0;

		memcpy(buf, pData, nLen);

		return nLen;
	}

	const char* SqliteStatement::GetStaticBlob(int nIndex, int& nLen)
	{
		nLen = sqlite3_column_bytes(sqlite_stmt_, nIndex);
		return (const char*)sqlite3_column_blob(sqlite_stmt_, nIndex);
	}

	bool SqliteStatement::BindParamter(int nIndex, int32_t val)
	{
		if (sqlite3_bind_int(sqlite_stmt_, nIndex + 1, val) == SQLITE_OK)
			return true;
		return false;
	}


	bool SqliteStatement::BindParamter(int nIndex, uint32_t val)
	{
		if (sqlite3_bind_int(sqlite_stmt_, nIndex + 1, val) == SQLITE_OK)
			return true;
		return false;
	}

	bool SqliteStatement::BindParamter(int nIndex, uint16_t val)
	{
		if (sqlite3_bind_int(sqlite_stmt_, nIndex + 1, val) == SQLITE_OK)
			return true;
		return false;
	}

	bool SqliteStatement::BindParamter(int nIndex, int16_t val)
	{
		if (sqlite3_bind_int(sqlite_stmt_, nIndex + 1, val) == SQLITE_OK)
			return true;
		return false;
	}


	bool SqliteStatement::BindParamter(int nIndex, int64_t val)
	{
		if (sqlite3_bind_int64(sqlite_stmt_, nIndex + 1, val) == SQLITE_OK)
			return true;
		return false;
	}

	bool SqliteStatement::BindParamter(int nIndex, uint64_t val)
	{
		if (sqlite3_bind_int64(sqlite_stmt_, nIndex + 1, val) == SQLITE_OK)
			return true;
		return false;
	}

	bool SqliteStatement::BindParamter(int nIndex, double val)
	{
		if (sqlite3_bind_double(sqlite_stmt_, nIndex + 1, val) == SQLITE_OK)
			return true;
		return false;
	}

	bool SqliteStatement::BindParamter(int nIndex, float val)
	{
		if (sqlite3_bind_double(sqlite_stmt_, nIndex + 1, val) == SQLITE_OK)
			return true;
		return false;
	}

	bool SqliteStatement::BindParamter(int nIndex, const char *pVal, int nLen)
	{
		if (sqlite3_bind_text(sqlite_stmt_, nIndex + 1, pVal, nLen, SQLITE_STATIC) == SQLITE_OK)
			return true;
		return false;
	}

	bool SqliteStatement::BindParamter(int nIndex, const unsigned char* pVal, int nLen)
	{
		if (sqlite3_bind_blob(sqlite_stmt_, nIndex + 1, pVal, nLen, SQLITE_STATIC) == SQLITE_OK)
			return true;
		return false;
	}

	bool SqliteStatement::LastError(SqliteError &oErr)
	{
		if (sqlite_ == nullptr)
			return false;
		return oErr.GetLastError(sqlite_);
	}

	SqliteError *SqliteStatement::LastError()
	{
		SqliteError *pErr = new SqliteError(sqlite_);
		return pErr;
	}
}