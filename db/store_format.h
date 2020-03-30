
#pragma once

#include <cstddef>
#include <cstdint>

namespace stackfiledb {

	// Blob data store format in storefile
	// Header  24 bytes
		// magic (4 bytes) flags (4 bytes)
		// key   (8 bytes) ts    (8 bytes)  
	// block
        // size  (4 bytes)  = n
	    // Data     n  bytes
	    // crc  (4 bytes)
	// block
	    // size  (4 bytes)  = n
	    // Data   n  bytes
	    // crc  (4 bytes)
	// block (last)
	    // size  (4 bytes)  = 0  
	// Footer 4 bytes
	    // total_size  (4 bytes)

	//Size uses 4 bytes It contains 3 fields
	// Noraml_Block  Block_Type  Block_Size
	//  bit(0) 31    bits 30     bits 29 - 0

	//Last_Block Block_Type  Block_Count
	//  bit(1) 31             bits 15 - 0

	// Block_Type 
	// 0 if data in the block is uncompressed
	// 1 if data in the block is compressed
	
	#define SD_BLOCKLAST_FLAG       0x80000000U
	#define SD_BLOCKCOMPRESSED_FLAG 0x40000000U
	#define SD_BLOCKSIZE_FLAG       0x0FFFFFFFU      //max block size
	#define SD_BLOCKCOUNT_FLAG      0x0000FFFFU

	// Contains the size(4 bytes) of the first block of data
	static const uint32_t kBlob_HeaderSize = 24U;
	static const uint32_t kBlock_MetaSize = 8U;

	static const size_t kTs_Offset = 100U;

	//actual occupy size in store file, include Header, Footer
	//vsize is data actual length
	inline uint32_t store_size(uint32_t vsize, uint32_t block_count) {
		return vsize + block_count * kBlock_MetaSize + kBlob_HeaderSize;
	}

	// magic
	static const uint32_t kStack_DB_Magic = 0xD312B4D6U;
	static const uint32_t kDefault_Crc_Size = 4;

	struct BlobDataIndex {
		BlobDataIndex() : ts(0), file_number(0), pos(0), size(0), flags(0) {}
		uint64_t ts;
		uint64_t pos;
		uint32_t size;
		uint32_t flags;
		uint32_t file_number;
		uint16_t block_count;
		uint16_t seeks_count;
	};

	enum ValueType { kTypeDeletion = 0x0, kTypeValue = 0x1 };
	static const ValueType kValueTypeForSeek = kTypeValue;

	struct BlobLogItem {
		BlobLogItem()  {}
		BlobLogItem(uint64_t k, uint64_t t) : key(k), ts(t) {}

		uint64_t key;
		uint64_t ts;
		uint64_t pos;
		uint32_t size;
		uint32_t flags;
		uint32_t file_number;
		uint32_t block_count;
		uint32_t type;		
		uint32_t bucketId;
		 
	};
	static const int kLog_ItemSize = 48;

	struct StoreFileMeta {
		StoreFileMeta()
			: file_size(0),
			file_number(0),
			seeks_count(0) {
		}

		uint64_t file_size;
		uint32_t file_number;
		uint32_t seeks_count;
	};
	
	void ToBlobIndex(const BlobLogItem& item, BlobDataIndex& blobindex);
	void ToLogItem(const BlobDataIndex& blobindex, uint64_t key, uint32_t type, uint32_t bucketId, BlobLogItem& logItem);

}  // namespace stackfiledb

