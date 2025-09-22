#include <string.h>
#include "krnl_host.h"

// footer

char *EncodeVarint64(char *dst, uint64_t v)
{
    static const unsigned int B = 128;
    unsigned char *ptr = reinterpret_cast<unsigned char *>(dst);
    while (v >= B)
    {
        *(ptr++) = (v & (B - 1)) | B;
        v >>= 7;
    }
    *(ptr++) = static_cast<unsigned char>(v);
    return reinterpret_cast<char *>(ptr);
}

char* PutBytesPtr(char* p, char* value, uint32_t size){
    for (uint32_t i = 0; i < size; i++)
    {
        *(reinterpret_cast<unsigned char *>(p)) = value[i];
        p++;
    }
    return reinterpret_cast<char *>(p);
}

char* putHash(char checksum_type, uint32_t hash, char *buffer)
{
	// put checksum type
	buffer[0] = checksum_type;
	// put hash
    // buffer[1] = hash;
    // buffer[2] = hash >> 8;
    // buffer[3] = hash >> 16;
    // buffer[4] = hash >> 24;
    buffer[1] = hash&0xff;
    buffer[2] = 0x00;
    buffer[3] = 0x00;
    buffer[4] = 0x00;

    return buffer + 5;
}

void putFooter(uint64_t index_block_offset, uint64_t index_block_size, uint64_t metaindex_block_offset, uint64_t metaindex_block_size,char* footer_buffer)
{
    static uint64_t kMagicNumber = 9863518390377041911ULL;
    // static uint32_t kExtendedMagicNumber = 7995454;
    // static uint64_t kNewFooterSize = 53;
    // static uint64_t kXXH3checksum = 4;
    static uint64_t kNoChecksum = 0;
    uint32_t version = 5;
    // assume version 1-5
    // put checksum type
    footer_buffer = EncodeVarint64(footer_buffer, kNoChecksum);
    // part begin
    char *begin = footer_buffer;
    // put metaindex handle
    footer_buffer = EncodeVarint64(footer_buffer, metaindex_block_offset);
    footer_buffer = EncodeVarint64(footer_buffer, metaindex_block_size);
    // put index handle
    footer_buffer = EncodeVarint64(footer_buffer, index_block_offset);
    footer_buffer = EncodeVarint64(footer_buffer, index_block_size);
    // put zero padding
    while (footer_buffer < begin + 40)
    {
        *(footer_buffer) = 0;
        ++footer_buffer;
    }
    // part end
    // put format version
    footer_buffer = PutBytesPtr(footer_buffer, reinterpret_cast<char *>(&version), sizeof(uint32_t));
    // put magic number
    uint64_t magic = kMagicNumber;
    footer_buffer = PutBytesPtr(footer_buffer, reinterpret_cast<char *>(&magic), sizeof(uint64_t));
    // return footer_buffer;
}
// properties

// 0:data_size 1:index_size

char *PutBytesPtr(char *p, const char *value, uint32_t size)
{
    for (uint32_t i = 0; i < size; i++)
    {
        *(reinterpret_cast<unsigned char *>(p)) = value[i];
        p++;
    }
    return reinterpret_cast<char *>(p);
}

char *EncodeVarint32(char *dst, uint32_t v)
{
    static const unsigned int B = 128;
    unsigned char *ptr = reinterpret_cast<unsigned char *>(dst);
    while (v >= B)
    {
        *(ptr++) = (v & (B - 1)) | B;
        v >>= 7;
    }
    *(ptr++) = static_cast<unsigned char>(v);
    return reinterpret_cast<char *>(ptr);
}

// kIndexType value is a int32 value (4 bytes)
std::string kIndexType = "rocksdb.block.based.table.index.type";
// default value is kBinarySearch
// kBinarySearch = 0x00,
// kHashSearch = 0x01,
// kTwoLevelIndexSearch = 0x02,
// kBinarySearchWithFirstKey = 0x03,
std::string kPrefixFiltering = "rocksdb.block.based.table.prefix.filtering";
std::string kWholeKeyFiltering = "rocksdb.block.based.table.whole.key.filtering";
// TODO
std::string kHashIndexPrefixesBlock = "rocksdb.hashindex.prefixes";
// TODO
std::string kHashIndexPrefixesMetadataBlock = "rocksdb.hashindex.metadata";
// reserved
std::string kPropTrue = "1";
// reserved
std::string kPropFalse = "0";

std::string kColumnFamilyId = "rocksdb.column.family.id";
std::string kColumnFamilyName = "rocksdb.column.family.name";
std::string kComparator = "rocksdb.comparator";
std::string kCompression = "rocksdb.compression";
std::string kCompressionOptions = "rocksdb.compression_options";
std::string kDbId = "rocksdb.creating.db.identity";
std::string kDbHostId = "rocksdb.creating.host.identity";
std::string kDbSessionId = "rocksdb.creating.session.identity";
std::string kCreationTime = "rocksdb.creation.time";
std::string kDataSize = "rocksdb.data.size";
std::string kDeletedKeys = "rocksdb.deleted.keys";
// rocksdb.external_sst_file.global_seqno
// rocksdb.external_sst_file.version
//
std::string kFileCreationTime = "rocksdb.file.creation.time";
std::string kFilterPolicy = "rocksdb.filter.policy";
//
std::string kFilterSize = "rocksdb.filter.size";
std::string kFixedKeyLen = "rocksdb.fixed.key.length";
std::string kFormatVersion = "rocksdb.format.version";
std::string kIndexKeyIsUserKey = "rocksdb.index.key.is.user.key";
// only when index type is kTwoLevelIndexSearch
std::string kIndexPartitions = "rocksdb.index.partitions";
std::string kIndexSize = "rocksdb.index.size";
std::string kIndexValueIsDeltaEncoded = "rocksdb.index.value.is.delta.encoded";
std::string kMergeOperands = "rocksdb.merge.operands";
std::string kMergeOperator = "rocksdb.merge.operator";
std::string kNumDataBlocks = "rocksdb.num.data.blocks";
std::string kNumEntries = "rocksdb.num.entries";
std::string kNumFilterEntries = "rocksdb.num.filter_entries";
std::string kNumRangeDeletions = "rocksdb.num.range-deletions";
std::string kOldestKeyTime = "rocksdb.oldest.key.time";
std::string kOriginalFileNumber = "rocksdb.original.file.number";
std::string kPrefixExtractorName = "rocksdb.prefix.extractor.name";
std::string kPropertyCollectors = "rocksdb.property.collectors";
std::string kRawKeySize = "rocksdb.raw.key.size";
std::string kRawValueSize = "rocksdb.raw.value.size";
//
std::string kFastCompressionEstimatedDataSize = "rocksdb.sample_for_compression.fast.data.size";
//
std::string kSlowCompressionEstimatedDataSize = "rocksdb.sample_for_compression.slow.data.size";
// not found
std::string kSequenceNumberTimeMapping = "rocksdb.seqno.time.map";
std::string kTailStartOffset = "rocksdb.tail.start.offset";
// only when index type is kTwoLevelIndexSearch
std::string kTopLevelIndexSize = "rocksdb.top-level.index.size";
// explicitly written to meta properties block when it's false
std::string kUserDefinedTimestampsPersisted = "rocksdb.user.defined.timestamps.persisted";

struct TableProperties
{
    uint64_t index_type = 0;
    // the file number at creation time, or 0 for unknown. When known,
    // combining with db_session_id must uniquely identify an SST file.
    uint64_t orig_file_number = 0;
    // the total size of all data blocks.
    uint64_t data_size = 0;
    // the size of index block.
    uint64_t index_size = 0;
    // Total number of index partitions if kTwoLevelIndexSearch is used
    uint64_t index_partitions = 0;
    // Size of the top-level index if kTwoLevelIndexSearch is used
    uint64_t top_level_index_size = 0;
    // Whether the index key is user key. Otherwise it includes 8 byte of sequence
    // number added by internal key format.
    uint64_t index_key_is_user_key = 0;
    // Whether delta encoding is used to encode the index values.
    uint64_t index_value_is_delta_encoded = 0;
    // the size of filter block.
    uint64_t filter_size = 0;
    // total raw (uncompressed, undelineated) key size
    uint64_t raw_key_size = 0;
    // total raw (uncompressed, undelineated) value size
    uint64_t raw_value_size = 0;
    // the number of blocks in this table
    uint64_t num_data_blocks = 0;
    // the number of entries in this table
    uint64_t num_entries = 0;
    // the number of unique entries (keys or prefixes) added to filters
    uint64_t num_filter_entries = 0;
    // the number of deletions in the table
    uint64_t num_deletions = 0;
    // the number of merge operands in the table
    uint64_t num_merge_operands = 0;
    // the number of range deletions in this table
    uint64_t num_range_deletions = 0;
    // format version, reserved for backward compatibility
    uint64_t format_version = 0;
    // If 0, key is variable length. Otherwise number of bytes for each key.
    uint64_t fixed_key_len = 0;
    // ID of column family for this SST file, corresponding to the CF identified
    // by column_family_name.
    uint64_t column_family_id = INT32_MAX;

    // Oldest ancester time. 0 means unknown.
    //
    // For flush output file, oldest ancestor time is the oldest key time in the
    // file.  If the oldest key time is not available, flush time is used.
    //
    // For compaction output file, oldest ancestor time is the oldest
    // among all the oldest key time of its input files, since the file could be
    // the compaction output from other SST files, which could in turn be outputs
    // for compact older SST files. If that's not available, creation time of this
    // compaction output file is used.
    //
    // TODO(sagar0): Should be changed to oldest_ancester_time ... but don't know
    // the full implications of backward compatibility. Hence retaining for now.
    uint64_t creation_time = 0;

    // Timestamp of the earliest key. 0 means unknown.
    uint64_t oldest_key_time = 0;
    // Actual SST file creation time. 0 means unknown.
    uint64_t file_creation_time = 0;
    // Estimated size of data blocks if compressed using a relatively slower
    // compression algorithm (see `ColumnFamilyOptions::sample_for_compression`).
    // 0 means unknown.
    uint64_t slow_compression_estimated_data_size = 0;
    // Estimated size of data blocks if compressed using a relatively faster
    // compression algorithm (see `ColumnFamilyOptions::sample_fo r_compression`).
    // 0 means unknown.
    uint64_t fast_compression_estimated_data_size = 0;
    // Offset of the value of the property "external sst file global seqno" in the
    // file if the property exists.
    // 0 means not exists.
    uint64_t external_sst_file_global_seqno_offset = 0;

    // Offset where the "tail" part of SST file starts
    // "Tail" refers to all blocks after data blocks till the end of the SST file
    uint64_t tail_start_offset = 0;

    // Value of the `AdvancedColumnFamilyOptions.persist_user_defined_timestamps`
    // when the file is created. Default to be true, only when this flag is false,
    // it's explicitly written to meta properties block.
    uint64_t user_defined_timestamps_persisted = 1;

    std::string prefix_filtering = "0";
    std::string whole_key_filtering = "1";
    // DB identity
    // db_id is an identifier generated the first time the DB is created
    // If DB identity is unset or unassigned, `db_id` will be an empty string.
    std::string db_id;

    // DB session identity
    // db_session_id is an identifier that gets reset every time the DB is opened
    // If DB session identity is unset or unassigned, `db_session_id` will be an
    // empty string.
    std::string db_session_id;

    // Location of the machine hosting the DB instance
    // db_host_id identifies the location of the host in some form
    // (hostname by default, but can also be any string of the user's choosing).
    // It can potentially change whenever the DB is opened
    std::string db_host_id;

    // Name of the column family with which this SST file is associated.
    // If column family is unknown, `column_family_name` will be an empty string.
    std::string column_family_name;

    // The name of the filter policy used in this table.
    // If no filter policy is used, `filter_policy_name` will be an empty string.
    std::string filter_policy_name;

    // The name of the comparator used in this table.
    std::string comparator_name;

    // The name of the merge operator used in this table.
    // If no merge operator is used, `merge_operator_name` will be "nullptr".
    std::string merge_operator_name;

    // The name of the prefix extractor used in this table
    // If no prefix extractor is used, `prefix_extractor_name` will be "nullptr".
    std::string prefix_extractor_name;

    // The names of the property collectors factories used in this table
    // separated by commas
    // {collector_name[1]},{collector_name[2]},{collector_name[3]} ..
    std::string property_collectors_names;

    // The compression algo used to compress the SST files.
    std::string compression_name;

    // Compression options used to compress the SST files.
    std::string compression_options;

    // Sequence number to time mapping, delta encoded.
    std::string seqno_to_time_mapping;
};

TableProperties properties;

// update type
// type > 0 => use data block kv format
//  type == 1 => exactly data block
//  type == 2 => properties block
//  type == 3 => metaindex block
// type == 0 => use index block kv format
char *put_kv_remove_prefix(std::string &key, std::string &pre_key, std::string &value, int type, char *block_pointer)
{
    // remove prefix
    uint32_t i = 0;
    for (; i < key.size() && i < pre_key.size(); i++)
    {
        if (key[i] != pre_key[i])
            break;
    }
    // put key to data block
    // shared | unshared | value | key | value
    if (type > 0)
    {
        // if (type == 1)
        // {
        //     // update properties.raw_key_size RAW!
        //     properties.raw_key_size += key.size();
        //     // update properties.raw_value_size
        //     properties.raw_value_size += value.size();
        //     // update properties.num_entries
        //     ++properties.num_entries;
        // }
        block_pointer = EncodeVarint32(block_pointer, i);
        block_pointer = EncodeVarint32(block_pointer, key.size() - i);
        block_pointer = EncodeVarint32(block_pointer, value.size());
        block_pointer = PutBytesPtr(block_pointer, key.c_str() + i, key.size() - i);
        block_pointer = PutBytesPtr(block_pointer, value.c_str(), value.size());
    }
    else if (type == 0)
    {
        // shared | unshared | key | value
        block_pointer = EncodeVarint32(block_pointer, i);
        block_pointer = EncodeVarint32(block_pointer, key.size() - i);
        block_pointer = PutBytesPtr(block_pointer, key.c_str() + i, key.size() - i);
        block_pointer = PutBytesPtr(block_pointer, value.c_str(), value.size());
    }
    return block_pointer;
}

char *putKV(std::string &key, std::string& pre_key, std::string &value, char *head, char *footer_buffer, uint32_t *data_block_restart_point, uint32_t data_block_restart_point_num)
{
    // static uint32_t restart_point_count = 0;
    static uint32_t entry_size = 1;
    static const int restart = 16;

    // put kv pair
    // check if need restart
    if (entry_size % restart == 0)
    {
        data_block_restart_point[data_block_restart_point_num] = footer_buffer - head;
        data_block_restart_point_num++;
        pre_key.clear();
    }
    footer_buffer = put_kv_remove_prefix(key, pre_key, value, 1, footer_buffer);
    pre_key = key;
    entry_size++;
    return footer_buffer;
}

char *putRestartPoint(uint32_t *restart_point, uint32_t &restart_point_num, char *footer_buffer)
{
    // put restart point
    // restart point array
    for (uint32_t i = 0; i < restart_point_num; i++)
    {
        footer_buffer = PutBytesPtr(footer_buffer, reinterpret_cast<char *>(&restart_point[i]), sizeof(uint32_t));
    }
    // restart point num
    footer_buffer = PutBytesPtr(footer_buffer, reinterpret_cast<char *>(&restart_point_num), sizeof(uint32_t));
    return footer_buffer;
}

void to_string(std::string &str, uint64_t offset, uint64_t size)
{
    static char buf[10];
    static char *p;
    p = buf;
    p = EncodeVarint32(p, offset);
    p = EncodeVarint32(p, size);
    str.resize(p - buf);
    // memcpy(str.data(), buf, p - buf);
    for (int i = 0; i < p - buf; ++i)
        str[i] = buf[i];
    return;
}

void to_string(std::string &str, uint64_t num)
{
    static char buf[10];
    static char *p;
    p = buf;
    p = EncodeVarint32(p, num);
    str.resize(p - buf);
    // memcpy(str.data(), buf, p - buf);
    for (int i = 0; i < p - buf; ++i)
        str[i] = buf[i];
    return;
}

void initProperties(const std::string& db_id,const std::string& db_session_id,uint64_t orig_file_number,uint64_t index_block_length)
{
    properties.orig_file_number = orig_file_number;
    properties.data_size = 0;
    properties.index_size = index_block_length;

    // now don't support kTwoLevelIndexSearch
    properties.index_partitions = 0;
    // same as above
    properties.top_level_index_size = 0;
    // "index key isn't user key" as default, recall internal key
    properties.index_key_is_user_key = 1;
    // index value refers to the offset of data block
    // now don't support
    properties.index_value_is_delta_encoded = 1;
    // TODO
    properties.filter_size = 0;

    properties.raw_key_size = 0;
    properties.num_data_blocks = 0;
    properties.num_entries = 0;

    // TODO
    properties.num_filter_entries = 0;

    // after compaction, there won't be any deletions
    properties.num_deletions = 0;

    // TODO: figure out how merge works, specially during compaction
    properties.num_merge_operands = 0;

    properties.num_range_deletions = 0;

    // format version in footer now, reserved for backward compatibility
    properties.format_version = 0;

    // TODO
    properties.fixed_key_len = 0;

    // when unknown, set to INT32_MAX
    // TODO: copy from any input sst
    properties.column_family_id = INT32_MAX;
    // TODO: creation_time is the oldest among the input sst files
    properties.creation_time = 0;
    // TODO
    properties.oldest_key_time = 0;
    // TODO
    properties.file_creation_time = 0;
    // TODO
    properties.slow_compression_estimated_data_size = 0;
    // same as above
    properties.fast_compression_estimated_data_size = 0;
    // TODO
    properties.external_sst_file_global_seqno_offset = 0;
    properties.tail_start_offset = 0;
    //////// write only when false
    properties.user_defined_timestamps_persisted = 1;

    // TODO
    // the following properties can directly copy from the opened db
    // empty for now
    properties.db_id = db_id;
    properties.db_session_id = db_session_id;
    properties.db_host_id = "";
    // id == 0 -> name = default
    properties.column_family_name = "defalut";
    properties.filter_policy_name = "";
    properties.comparator_name = "leveldb.BytewiseComparator";
    properties.merge_operator_name = "";
    properties.prefix_extractor_name = "nullptr";
    properties.property_collectors_names = "[]";
    properties.compression_name = "NoCompression";
    properties.compression_options = "";
    properties.seqno_to_time_mapping = "";
}
void putProperties(char *pp_pointer, int &pp_index, char *metaindex_block_buffer, int &metaindexblock_index, int prev_size, uint64_t *pps)
{
    // set properties
    properties.num_data_blocks = pps[CSD_PPS_DATABLOCK_NUM_OFF];
    properties.num_entries = pps[CSD_PPS_ENTRIES_OFF];
    properties.data_size = pps[CSD_PPS_DATASIZE_OFF];
    properties.raw_key_size = pps[CSD_PPS_RAWKEYSIZE_OFF];
    properties.raw_value_size = pps[CSD_PPS_RAWVALUESIZE_OFF];
    properties.tail_start_offset = pps[CSD_PPS_INDEXBLOCK_OFFSET_OFF];
    // init data block
    // pp_pointer = putHash(0,0, pp_pointer);
    char *head = pp_pointer;
    uint32_t data_block_restart_point[16], data_block_restart_point_num = 0;
    data_block_restart_point_num = 1;
    data_block_restart_point[0] = 0;

    // TODO add blockbased table properties
    std::string temp;
    static std::string pre_key;
    pre_key.clear();
    to_string(temp,properties.index_type);
    pp_pointer = putKV(kIndexType,pre_key,temp,head,pp_pointer,data_block_restart_point,data_block_restart_point_num);
    pp_pointer = putKV(kPrefixFiltering,pre_key,properties.prefix_filtering,head,pp_pointer,data_block_restart_point,data_block_restart_point_num);
    pp_pointer = putKV(kWholeKeyFiltering,pre_key,properties.whole_key_filtering,head,pp_pointer,data_block_restart_point,data_block_restart_point_num);
    to_string(temp, properties.column_family_id);
    pp_pointer = putKV(kColumnFamilyId, pre_key, temp, head, pp_pointer, data_block_restart_point, data_block_restart_point_num);
    pp_pointer = putKV(kColumnFamilyName, pre_key, properties.column_family_name, head, pp_pointer, data_block_restart_point, data_block_restart_point_num);
    pp_pointer = putKV(kComparator, pre_key, properties.comparator_name, head, pp_pointer, data_block_restart_point, data_block_restart_point_num);
    pp_pointer = putKV(kCompression, pre_key, properties.compression_name, head, pp_pointer, data_block_restart_point, data_block_restart_point_num);
    pp_pointer = putKV(kCompressionOptions, pre_key, properties.compression_options, head, pp_pointer, data_block_restart_point, data_block_restart_point_num);
    pp_pointer = putKV(kDbId, pre_key, properties.db_id, head, pp_pointer, data_block_restart_point, data_block_restart_point_num);
    pp_pointer = putKV(kDbHostId, pre_key, properties.db_host_id, head, pp_pointer, data_block_restart_point, data_block_restart_point_num);
    pp_pointer = putKV(kDbSessionId, pre_key, properties.db_session_id, head, pp_pointer, data_block_restart_point, data_block_restart_point_num);
    to_string(temp, properties.creation_time);
    pp_pointer = putKV(kCreationTime, pre_key, temp, head, pp_pointer, data_block_restart_point, data_block_restart_point_num);
    to_string(temp, properties.data_size);
    pp_pointer = putKV(kDataSize, pre_key, temp, head, pp_pointer, data_block_restart_point, data_block_restart_point_num);
    to_string(temp, properties.num_deletions);
    pp_pointer = putKV(kDeletedKeys, pre_key, temp, head, pp_pointer, data_block_restart_point, data_block_restart_point_num);
    to_string(temp, properties.file_creation_time);
    pp_pointer = putKV(kFileCreationTime, pre_key, temp, head, pp_pointer, data_block_restart_point, data_block_restart_point_num);
    // pp_pointer = putKV(kFilterPolicy, pre_key, properties.filter_policy_name, head, pp_pointer, data_block_restart_point, data_block_restart_point_num);
    to_string(temp, properties.filter_size);
    pp_pointer = putKV(kFilterSize, pre_key, temp, head, pp_pointer, data_block_restart_point, data_block_restart_point_num);
    to_string(temp, properties.fixed_key_len);
    pp_pointer = putKV(kFixedKeyLen, pre_key, temp, head, pp_pointer, data_block_restart_point, data_block_restart_point_num);
    to_string(temp, properties.format_version);
    pp_pointer = putKV(kFormatVersion, pre_key, temp, head, pp_pointer, data_block_restart_point, data_block_restart_point_num);
    to_string(temp, properties.index_key_is_user_key);
    pp_pointer = putKV(kIndexKeyIsUserKey, pre_key, temp, head, pp_pointer, data_block_restart_point, data_block_restart_point_num);
    // to_string(temp, properties.index_partitions);
    // pp_pointer = putKV(kIndexPartitions,temp pre_key,);
    to_string(temp, properties.index_size);
    pp_pointer = putKV(kIndexSize, pre_key, temp, head, pp_pointer, data_block_restart_point, data_block_restart_point_num);
    to_string(temp, properties.index_value_is_delta_encoded);
    pp_pointer = putKV(kIndexValueIsDeltaEncoded, pre_key, temp, head, pp_pointer, data_block_restart_point, data_block_restart_point_num);
    to_string(temp, properties.num_merge_operands);
    pp_pointer = putKV(kMergeOperands, pre_key, temp, head, pp_pointer, data_block_restart_point, data_block_restart_point_num);
    pp_pointer = putKV(kMergeOperator, pre_key, properties.merge_operator_name, head, pp_pointer, data_block_restart_point, data_block_restart_point_num);
    to_string(temp, properties.num_data_blocks);
    pp_pointer = putKV(kNumDataBlocks, pre_key, temp, head, pp_pointer, data_block_restart_point, data_block_restart_point_num);
    to_string(temp, properties.num_entries);
    pp_pointer = putKV(kNumEntries, pre_key, temp, head, pp_pointer, data_block_restart_point, data_block_restart_point_num);
    to_string(temp, properties.num_filter_entries);
    pp_pointer = putKV(kNumFilterEntries, pre_key, temp, head, pp_pointer, data_block_restart_point, data_block_restart_point_num);
    to_string(temp, properties.num_range_deletions);
    pp_pointer = putKV(kNumRangeDeletions, pre_key, temp, head, pp_pointer, data_block_restart_point, data_block_restart_point_num);
    to_string(temp, properties.oldest_key_time);
    pp_pointer = putKV(kOldestKeyTime, pre_key, temp, head, pp_pointer, data_block_restart_point, data_block_restart_point_num);
    to_string(temp,properties.orig_file_number);
    pp_pointer = putKV(kOriginalFileNumber, pre_key, temp, head, pp_pointer, data_block_restart_point, data_block_restart_point_num);
    pp_pointer = putKV(kPrefixExtractorName, pre_key, properties.prefix_extractor_name, head, pp_pointer, data_block_restart_point, data_block_restart_point_num);
    pp_pointer = putKV(kPropertyCollectors, pre_key, properties.property_collectors_names, head, pp_pointer, data_block_restart_point, data_block_restart_point_num);
    to_string(temp, properties.raw_key_size);
    pp_pointer = putKV(kRawKeySize, pre_key, temp, head, pp_pointer, data_block_restart_point, data_block_restart_point_num);
    to_string(temp, properties.raw_value_size);
    pp_pointer = putKV(kRawValueSize, pre_key, temp, head, pp_pointer, data_block_restart_point, data_block_restart_point_num);
    // to_string(temp, properties.fast_compression_estimated_data_size);
    // pp_pointer = putKV(kFastCompressionEstimatedDataSize, pre_key, temp, head, pp_pointer, data_block_restart_point, data_block_restart_point_num);
    // to_string(temp, properties.slow_compression_estimated_data_size);
    // pp_pointer = putKV(kSlowCompressionEstimatedDataSize, pre_key, temp, head, pp_pointer, data_block_restart_point, data_block_restart_point_num);
    // pp_pointer = putKV(kSequenceNumberTimeMapping, pre_key, properties.seqno_to_time_mapping);
    to_string(temp, properties.tail_start_offset);
    pp_pointer = putKV(kTailStartOffset, pre_key, temp, head, pp_pointer, data_block_restart_point, data_block_restart_point_num);
    // to_string(temp, properties.top_level_index_size);
    // pp_pointer = putKV(kTopLevelIndexSize,temp pre_key,);
    // to_string(temp, properties.user_defined_timestamps_persisted);
    // pp_pointer = putKV(kUserDefinedTimestampsPersisted,temp pre_key,);

    pp_pointer = putRestartPoint(data_block_restart_point, data_block_restart_point_num, pp_pointer);
    pp_index = pp_pointer - head;
    // block handle (in meta index block)
    // init index block buffer an meta index block buffer
    // index_block_pointer = index_block_buffer;
    uint64_t properties_offset = prev_size, properties_size = pp_pointer - head;
    pp_pointer = putHash(0,0, pp_pointer);

    // std::cout<<"properties: \n=============\n";
    // for(int i=0;i<pp_index+5;i++)
    // {
    //     std::cout<<(int)head[i]<<" ";
    // }
    // std::cout<<"\n===========\n";

    metaindexblock_index = 0;
    std::string name = "rocksdb.properties";
    metaindex_block_buffer[metaindexblock_index++] = 0;
    metaindex_block_buffer[metaindexblock_index++] = 18;
    to_string(temp, properties_offset, properties_size);
    metaindex_block_buffer[metaindexblock_index++] = temp.length();
    for (uint32_t i = 0; i < name.length(); ++i)
        metaindex_block_buffer[metaindexblock_index++] = name[i];
    for (uint32_t i = 0; i < temp.length(); ++i)
        metaindex_block_buffer[metaindexblock_index++] = temp[i];

    uint32_t restart = 0, restart_size = 1;
    PutBytesPtr(metaindex_block_buffer + metaindexblock_index, (char *)&restart, sizeof(uint32_t));
    metaindexblock_index += 4;
    PutBytesPtr(metaindex_block_buffer + metaindexblock_index, (char *)&restart_size, sizeof(uint32_t));
    metaindexblock_index += 4;
    for(int i=metaindexblock_index;i<metaindexblock_index+5;i++)
        metaindex_block_buffer[i]=0;
    // for(int i=0;i<metaindexblock_index;i++)
    //     std::cout<<(int)metaindex_block_buffer[i]<<" ";
    // std::cout<<" host metablock write\n";
    pp_pointer = putHash(0,0, pp_pointer);

    *(pp_pointer+1)=0;
    *(pp_pointer+2)=0;
    *(pp_pointer+3)=0;
    *(pp_pointer+4)=0;
    *(pp_pointer+5)=0;
    // 不再对index进行+=5,因为hash不包含在block_size中，而所有记录block_index的变量都会被下一个block的offset使用
    // metaindexblock_index += 5;
    // index_block_pointer = putBlockHandle(name, properties_block_handle, 1);
    // // xxh3
    // pp_pointer = PutBytesPtr(pp_pointer, (char *)&kXXH3, sizeof(uint8_t));
    // uint32_t hash = Lower32of64(XXH3_64bits(sst_buffer + properties_block_handle.offset, properties_block_handle.size));
    // pp_pointer = PutBytesPtr(pp_pointer, (char *)&hash, sizeof(uint32_t));
}