
#include "db/store_format.h"

namespace stackfiledb {

	void ToBlobIndex(const BlobLogItem& item, BlobDataIndex& blobindex) {
		blobindex.ts = item.ts;
		blobindex.file_number = item.file_number;
		blobindex.pos = item.pos;
		blobindex.size = item.size;
		blobindex.flags = item.flags;
		blobindex.block_count = item.block_count;
	}

	void ToLogItem(const BlobDataIndex& item, uint64_t key, uint32_t type, uint32_t bucketId, BlobLogItem& logItem) {
		logItem.key = key;
		logItem.ts = item.ts;
		logItem.pos = item.pos;
		logItem.size = item.size;
		logItem.flags = item.flags;
		logItem.file_number = item.file_number;
		logItem.block_count = item.block_count;

		logItem.type = type;
		logItem.bucketId = bucketId;
	}
} // namespace stackfiledb