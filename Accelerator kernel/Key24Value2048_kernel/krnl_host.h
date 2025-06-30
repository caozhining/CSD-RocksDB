#ifndef __KRNL_HOST_H__
#define __KRNL_HOST_H__

#define index_block_buffer_size (1024 * 1024 * 8)
#define footer_size 53
// table_size_multiplier
#define MAX_OUTPUT_FILE_NUM 4
#define MAX_INPUT_FILE_NUM 4

// danger: 目前的处理逻辑要求key的长度为KEY_WIDTH(16字节)整数倍，而且要求KEY_WIDTH为8字节的整数倍
#define KEY_BITS 5
#define KEY_LENGTH (1 << KEY_BITS)  // key bytes
#define KEY_WIDTH_BITS 4    // 用于计算key的宽度，方便offset和index的计算，写死宽度为16字节（128位）
#define KEY_WIDTH_MASK ((1 << KEY_WIDTH_BITS) - 1)
#define KEY_WIDTH (1 << KEY_WIDTH_BITS)     // 每个数组元素16字节宽，对齐128位
#define KEY_BITWIDTH (KEY_WIDTH * 8)
#define KEY_ARRAY_LENGTH ((KEY_LENGTH * 8) / KEY_BITWIDTH)
#define KEY_LENGTH_BITWIDTH (KEY_BITS + 1)  // key长度需要用KEY_BITS+1位表示

#define VALUE_BITS 11
#define VALUE_LENGTH (1 << VALUE_BITS)  // value bytes
#define VALUE_WIDTH_BITS 4    // 写死宽度为16字节（128位）
#define VALUE_WIDTH_MASK ((1 << VALUE_WIDTH_BITS) - 1)
#define VALUE_WIDTH (1 << VALUE_WIDTH_BITS)     // 每个数组元素16字节宽，对齐128位
#define VALUE_BITWIDTH (VALUE_WIDTH * 8)
#define VALUE_ARRAY_LENGTH ((VALUE_LENGTH * 8) / VALUE_BITWIDTH)
#define VALUE_LENGTH_BITWIDTH (VALUE_BITS + 1)  // value长度需要用VALUE_BITS+1位表示

#define SEQ_LENGTH 8
#define SEQ_MASK ((ap_uint<SEQ_LENGTH*8>)((1 << SEQ_LENGTH) - 1))
#define USER_KEY_LENGTH (KEY_LENGTH - SEQ_LENGTH)

#define PPS_KERNEL_SINGEL_SIZE 128
#define PPS_KERNEL_SIZE (PPS_KERNEL_SINGEL_SIZE * MAX_OUTPUT_FILE_NUM)

// #define PPS_DATABLOCK_NUM_OFF       0
// #define PPS_ENTRIES_OFF             1
// #define PPS_DATASIZE_OFF            2
// #define PPS_RAWKEYSIZE_OFF          3
// #define PPS_RAWVALUESIZE_OFF        4
// #define PPS_INDEXBLOCK_OFFSET_OFF   5
// #define PPS_SMALLESTKEY_OFF         6
// #define PPS_SMALLESTKEY_LENGTH_OFF  8
// #define PPS_LARGESTKEY_OFF          9
// #define PPS_LARGESTKEY_LENGTH_OFF   11
// #define PPS_MINSEQ_OFF              12
// #define PPS_MAXSEQ_OFF              13

#define PPS_DATABLOCK_NUM_OFF       0
#define PPS_ENTRIES_OFF             1
#define PPS_DATASIZE_OFF            2
#define PPS_RAWKEYSIZE_OFF          3
#define PPS_RAWVALUESIZE_OFF        4
#define PPS_INDEXBLOCK_OFFSET_OFF   5
#define PPS_MINSEQ_OFF              6
#define PPS_MAXSEQ_OFF              7
#define PPS_SMALLESTKEY_LENGTH_OFF  8
#define PPS_LARGESTKEY_LENGTH_OFF   9
#define PPS_SMALLESTKEY_OFF         10
#define PPS_LARGESTKEY_OFF          (PPS_SMALLESTKEY_OFF + (KEY_LENGTH / 8))  // KEY长度为128字节时，需要16个uint64_t
// 结束位置：10+16+16=42

#endif