/*

** CAUTION: **

This code was copied from /czn_krnl_test/krnl_vadd_extreme_speed.cpp

Characteristics:
== Parallism :
    -- index block :  32
== Index block buffer :
    -- turn to ring buffer

==============

*/

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
// #include <sstream>

#include <iostream>
#include "ap_int.h"
#include "hls_task.h"
#include "hls_stream.h"
#include <chrono>


#include "krnl_host.h"

// for encoder
#define restart_point_max 64
#define restart 16

// #define TIME_DEBUG

#ifdef TIME_DEBUG
std::chrono::high_resolution_clock::time_point prev_time = std::chrono::high_resolution_clock::now();
void chrono_print_time(const char *str)
{
    std::chrono::high_resolution_clock::time_point now_time = std::chrono::high_resolution_clock::now();
    uint64_t time_diff = std::chrono::duration_cast<std::chrono::nanoseconds>(now_time - prev_time).count();
    prev_time = now_time;
    // all_time += time_diff;
    printf("****** %s time: %.2f ms\n", str, time_diff / 1000000.0);
    // ss << "****** " << str << " time: " << time_diff / 1000000.0 << " ms\n"; 
}

std::chrono::high_resolution_clock::time_point merge_prev_time = std::chrono::high_resolution_clock::now();;;
void chrono_print_merge_time(const char *str)
{
    std::chrono::high_resolution_clock::time_point now_time = std::chrono::high_resolution_clock::now();
    uint64_t time_diff = std::chrono::duration_cast<std::chrono::nanoseconds>(now_time - prev_time).count();
    merge_prev_time = now_time;
    // all_time += time_diff;
    printf("****** %s time: %.2f ms\n", str, time_diff / 1000000.0);
    // ss << "****** " << str << " time: " << time_diff / 1000000.0 << " ms\n"; 
}
#else
void chrono_print_time(const char *str) {}
void chrono_print_merge_time(const char *str) {}
#endif

// TODO : ap_uint<KEY_BITWIDTH>和ap_uint<KEY_BITWIDTH> c[KEY_ARRAY_LENGTH]可以写成宏或结构体

ap_uint<KEY_BITWIDTH> key_byte_reverse(const ap_uint<KEY_BITWIDTH> &key_element) {
#pragma HLS INLINE
    ap_uint<KEY_BITWIDTH> result = 0;
//     for (ap_uint<KEY_WIDTH_BITS+1> i = 0; i < KEY_WIDTH; ++i) {
// #pragma HLS UNROLL
//         // result |= (key_element.range((i + 1) * 8 - 1, i * 8) << (KEY_BITWIDTH - (i + 1) * 8));
//         result.range(i * 8 + 7, i * 8) = key_element.range((KEY_WIDTH - i) * 8 - 1, (KEY_WIDTH - i - 1) * 8);
//     }
//     return result;
    // danger: 现在写死KEY_BITWIDTH为128
    result.range(7, 0) = key_element.range(127, 120);
    result.range(15, 8) = key_element.range(119, 112);
    result.range(23, 16) = key_element.range(111, 104);
    result.range(31, 24) = key_element.range(103, 96);
    result.range(39, 32) = key_element.range(95, 88);
    result.range(47, 40) = key_element.range(87, 80);
    result.range(55, 48) = key_element.range(79, 72);
    result.range(63, 56) = key_element.range(71, 64);
    result.range(71, 64) = key_element.range(63, 56);
    result.range(79, 72) = key_element.range(55, 48);
    result.range(87, 80) = key_element.range(47, 40);
    result.range(95, 88) = key_element.range(39, 32);
    result.range(103, 96) = key_element.range(31, 24);
    result.range(111, 104) = key_element.range(23, 16);
    result.range(119, 112) = key_element.range(15, 8);
    result.range(127, 120) = key_element.range(7, 0);
    return result;
}

struct keystring
{
    ap_uint<KEY_BITWIDTH> c[KEY_ARRAY_LENGTH];
    ap_uint<KEY_LENGTH_BITWIDTH> length;

    void set_empty()
    {
        length = 0;
    }

    void print_key()
    {
        // printf("Key Length: %d\n", length.to_uint());
        printf("Key: ");
        for(uint32_t j = 0; j < length / KEY_WIDTH; j++)
        {
            for (uint32_t k = 0; k < KEY_WIDTH; k++)
            {
                printf("%02x", (uint32_t)c[j].range(k*8+7, k*8) & 0xFF);
            }
        }
        printf("\n");
    }

    // 传入的key，高8字节为seq，低120字节为key
    // seq的比较优先级最低，顺序存储，在c[KEY_ARRAY_LENGTH - 1]中
    // key的比较优先级最高，逆序存储, 从c[0]开始
    bool operator<(const keystring &rhs) const {
        ap_uint<KEY_BITWIDTH> key1;
        ap_uint<KEY_BITWIDTH> key2;
        for (int i = 0; i < KEY_ARRAY_LENGTH-1; ++i) {
    #pragma HLS UNROLL
    #pragma ALLOCATION instances=key_byte_reverse limit=2 function  // key_byte_reverse函数实例化两次
            key1 = key_byte_reverse(c[i]);  // TODO: 四个key一共比较三次，六次逆序，有两次多余
            key2 = key_byte_reverse(rhs.c[i]);
            if (key1 != key2) {
                return key1 < key2;
            }
        }
        // ap_uint<KEY_BITWIDTH> key1 = key_byte_reverse(c[KEY_ARRAY_LENGTH - 1]);
        // ap_uint<KEY_BITWIDTH> key2 = key_byte_reverse(rhs.c[KEY_ARRAY_LENGTH - 1]);
        // key1.range(SEQ_LENGTH * 8 - 1, 0) = ~c[KEY_ARRAY_LENGTH - 1].range(KEY_BITWIDTH - 1, KEY_BITWIDTH - SEQ_LENGTH * 8);  // seq顺序存储，取反后比较
        // key2.range(SEQ_LENGTH * 8 - 1, 0) = ~rhs.c[KEY_ARRAY_LENGTH - 1].range(KEY_BITWIDTH - 1, KEY_BITWIDTH - SEQ_LENGTH * 8);
        // danger: 现在写死KEY_BITWIDTH为128，这里又写死了SEQ_LENGTH为8字节（64位）
        key1.range(63, 0) = ~c[KEY_ARRAY_LENGTH - 1].range(127, 64);
        key1.range(71, 64) = c[KEY_ARRAY_LENGTH - 1].range(63, 56);
        key1.range(79, 72) = c[KEY_ARRAY_LENGTH - 1].range(55, 48);
        key1.range(87, 80) = c[KEY_ARRAY_LENGTH - 1].range(47, 40);
        key1.range(95, 88) = c[KEY_ARRAY_LENGTH - 1].range(39, 32);
        key1.range(103, 96) = c[KEY_ARRAY_LENGTH - 1].range(31, 24);
        key1.range(111, 104) = c[KEY_ARRAY_LENGTH - 1].range(23, 16);
        key1.range(119, 112) = c[KEY_ARRAY_LENGTH - 1].range(15, 8);
        key1.range(127, 120) = c[KEY_ARRAY_LENGTH - 1].range(7, 0);
        key2.range(63, 0) = ~rhs.c[KEY_ARRAY_LENGTH - 1].range(127, 64);
        key2.range(71, 64) = rhs.c[KEY_ARRAY_LENGTH - 1].range(63, 56);
        key2.range(79, 72) = rhs.c[KEY_ARRAY_LENGTH - 1].range(55, 48);
        key2.range(87, 80) = rhs.c[KEY_ARRAY_LENGTH - 1].range(47, 40);
        key2.range(95, 88) = rhs.c[KEY_ARRAY_LENGTH - 1].range(39, 32);
        key2.range(103, 96) = rhs.c[KEY_ARRAY_LENGTH - 1].range(31, 24);
        key2.range(111, 104) = rhs.c[KEY_ARRAY_LENGTH - 1].range(23, 16);
        key2.range(119, 112) = rhs.c[KEY_ARRAY_LENGTH - 1].range(15, 8);
        key2.range(127, 120) = rhs.c[KEY_ARRAY_LENGTH - 1].range(7, 0);
        return key1 < key2; 
    }

    friend std::ostream& operator<<(std::ostream& os, const keystring& ks) {
        os << "Key Length: " << std::dec << ks.length.to_uint() << "\n";
        os << "Keys: ";
        for (int i = 0; i < ks.length.to_uint() / KEY_WIDTH; ++i) {
            
            os << std::hex << ks.c[i] << " ";
        }
        os << "\n";
        os << std::dec; // Reset to decimal
        return os;
    }
};

struct valuestring
{
    ap_uint<VALUE_BITWIDTH> c[VALUE_ARRAY_LENGTH];
    ap_uint<VALUE_LENGTH_BITWIDTH> length;

    void print_value()
    {
        // printf("Value Length: %d\n", length.to_uint());
        printf("Value: ");
        for(uint32_t j = 0; j < length / VALUE_WIDTH; j++)
        {
            for (uint32_t k = 0; k < VALUE_WIDTH; k++)
            {
                printf("%02x", (uint32_t)c[j].range(k*8+7, k*8) & 0xFF);
            }
        }
        printf("\n");
    }

    friend std::ostream& operator<<(std::ostream& os, const valuestring& vs) {
        os << "Value Length: " << std::dec << vs.length.to_uint() << "\n";
        os << "Values: ";
        for (int i = 0; i < vs.length.to_uint(); ++i) {
            os << std::hex << vs.c[i] << " ";
        }
        os << "\n";
        os << std::dec; // Reset to decimal
        return os;
    }
};

struct fifo_key_meta{
    keystring key;
    ap_uint<VALUE_LENGTH_BITWIDTH> value_length;

    friend std::ostream& operator<<(std::ostream& os, const fifo_key_meta& km) {
        os << "fifo_key_meta:\n";
        os << km.key;
        os << "value_length: " << std::dec << km.value_length.to_uint() << "\n";
        return os;
    }
};

struct fifo_encoder_receive{
    fifo_key_meta km;
    ap_uint<2> decoder_num;

    friend std::ostream& operator<<(std::ostream& os, const fifo_encoder_receive& ker) {
        os << ker.km;
        os << "Decoder Num: " << std::dec << ker.decoder_num.to_uint() << "\n";
        return os;
    }
};

struct fifo_value_slice{
    ap_uint<128> c[4096/128];
    friend std::ostream& operator<<(std::ostream& os, const fifo_value_slice& vs) {
        os << "Value Slice: ";
        for (int i = 0; i < 4096/128; ++i) {
            os << std::hex << vs.c[i] << " ";
        }
        os << "\n";
        os << std::dec; // Reset to decimal
        return os;
    }
};

inline void set_max_key(keystring &key) {
    key.length = KEY_LENGTH;
    for (int i = 0; i < KEY_ARRAY_LENGTH; ++i) {
#pragma HLS UNROLL
        key.c[i] = -1;
    }
    key.c[KEY_ARRAY_LENGTH - 1].range(KEY_BITWIDTH-1, KEY_BITWIDTH-SEQ_LENGTH*8) = 0;
}

inline bool is_max_key(const keystring &key) {
    for (int i = 0; i < KEY_ARRAY_LENGTH - 1; ++i) {
#pragma HLS UNROLL
        if (key.c[i].and_reduce() != 1) {  // 不全是1
            return false;
        }
    }
    if (key.c[KEY_ARRAY_LENGTH - 1].range(KEY_BITWIDTH-SEQ_LENGTH*8-1, 0).and_reduce() != 1) {
        return false;
    }
    return key.c[KEY_ARRAY_LENGTH - 1].range(KEY_BITWIDTH-1, KEY_BITWIDTH-SEQ_LENGTH*8) == 0;
}

inline void set_signal_key(keystring &key) {
    set_max_key(key);
    key.c[0] -= 1;
}

inline bool is_signal_key(const keystring &key) {
#if KEY_ARRAY_LENGTH == 1
    if (key.c[0].range(0,0) == 1 || key.c[0].range(KEY_BITWIDTH-SEQ_LENGTH*8-1, 1).and_reduce() != 1) {
        return false;
    }
    return key.c[KEY_ARRAY_LENGTH - 1].range(KEY_BITWIDTH-1, KEY_BITWIDTH-SEQ_LENGTH*8) == 0;
#else
    if (key.c[0].range(0,0) == 1 || key.c[0].range(KEY_BITWIDTH-1,1).and_reduce() != 1) {
        return false;
    }
    for (int i = 1; i < KEY_ARRAY_LENGTH - 1; ++i) {
#pragma HLS UNROLL
        if (key.c[i].and_reduce() != 1) {
            return false;
        }
    }
    if (key.c[KEY_ARRAY_LENGTH - 1].range(KEY_BITWIDTH-SEQ_LENGTH*8-1, 0).and_reduce() != 1) {
        return false;
    }
    return key.c[KEY_ARRAY_LENGTH - 1].range(KEY_BITWIDTH-1, KEY_BITWIDTH-SEQ_LENGTH*8) == 0;
#endif
}

/*
=======================================================================================================
=======================================================================================================
==                                                                                                   ==
==                                      footer-decoder area                                          ==
==                                                                                                   ==
=======================================================================================================
==                                                                                                   ==
==                                                                                                   ==
*/


ap_uint<64> ap32get64(ap_uint<32> *buf, ap_uint<8> index)
{
    ap_uint<64> tem;
    ap_uint<2> offset;
    ap_uint<6> buf_offset;
    offset=index.range(1,0);
    buf_offset=index.range(7,2);
    switch(offset)
    {
    case 0b00:
        tem.range(31,0)=buf[buf_offset].range(31,0);
        tem.range(63,32)=buf[buf_offset+1].range(31,0);
        break;
    case 0b01:
        tem.range(23,0)=buf[buf_offset].range(31,8);
        tem.range(55,24)=buf[buf_offset+1].range(31,0);
        tem.range(63,56)=buf[buf_offset+2].range(7,0);
        break;
    case 0b10:
        tem.range(15,0)=buf[buf_offset].range(31,16);
        tem.range(47,16)=buf[buf_offset+1].range(31,0);
        tem.range(63,48)=buf[buf_offset+2].range(15,0);
        break;
    case 0b11:
        tem.range(7,0)=buf[buf_offset].range(31,24);
        tem.range(39,8)=buf[buf_offset+1].range(31,0);
        tem.range(63,40)=buf[buf_offset+2].range(23,0);
        break;
    }
    return tem;
}

ap_uint<8> GetVarintfromuint64_for_footer_decoder(ap_uint<64> input, ap_uint<64> &value)
{
    ap_uint<64> result; // wire
    ap_uint<8> delta_offset;

    ap_uint<64> msk_code;

    ap_uint<2> ctrl_hi;
    ap_uint<2> ctrl_lo;
    ap_uint<4> ctrl_over32;

    ctrl_lo.range(1, 1) = input.range(15, 15);
    ctrl_lo.range(0, 0) = input.range(7, 7);

    ctrl_hi.range(1, 1) = input.range(31, 31);
    ctrl_hi.range(0, 0) = input.range(23, 23);

    ctrl_over32.range(0,0) = input.range(39,39);
    ctrl_over32.range(1,1) = input.range(47,47);
    ctrl_over32.range(2,2) = input.range(55,55);
    ctrl_over32.range(3,3) = input.range(63,63);

    //Copy data
    //0-31
    result.range(6, 0) = input.range(6, 0);
    result.range(13, 7) = input.range(14, 8);
    result.range(20, 14) = input.range(22, 16);
    result.range(27, 21) = input.range(30, 24);
    //31-64
    result.range(34, 28) = input.range(38, 32);
    result.range(41, 35) = input.range(46, 40);
    result.range(48, 42) = input.range(54, 48);
    result.range(55, 49) = input.range(62, 56);

    result.range(63, 56) = 0;

    switch (ctrl_lo)
    {
    case 0b00:
        msk_code = 0b00000000000000000000000001111111;
        delta_offset = 1;
        break;
    case 0b01:
        msk_code = 0b00000000000000000011111111111111;
        delta_offset = 2;
        break;
    case 0b10:
        msk_code = 0b00000000000000000000000001111111;
        delta_offset = 1;
        break;
    case 0b11:
        switch (ctrl_hi)
        {
        case 0b00:
            msk_code = 0b00000000000111111111111111111111;
            delta_offset = 3;
            break;
        case 0b01:
            msk_code = 0b00001111111111111111111111111111;
            delta_offset = 4;
            break;
        case 0b10:
            msk_code = 0b00000000000111111111111111111111;
            delta_offset = 3;
            break;
        case 0b11:
            switch(ctrl_over32){
            case 0b0000:
                msk_code = 0b000000011111111111111111111111111111111111;
                //           654321065432106543210654321065432106543210
                delta_offset = 5;
                break;
            case 0b0001:
                msk_code = 0b111111111111111111111111111111111111111111;
                //           654321065432106543210654321065432106543210
                delta_offset = 6;
                break;
            case 0b0010:
                msk_code = 0b000000011111111111111111111111111111111111;
                //           654321065432106543210654321065432106543210
                delta_offset = 5;
                break;
            case 0b0011:
                msk_code = 0b1111111111111111111111111111111111111111111111111;
                //           6543210654321065432106543210654321065432106543210
                delta_offset = 7;
                break;
            case 0b0100:
                msk_code = 0b000000011111111111111111111111111111111111;
                //           654321065432106543210654321065432106543210
                delta_offset = 5;
                break;
            case 0b0101:
                msk_code = 0b111111111111111111111111111111111111111111;
                //           654321065432106543210654321065432106543210
                delta_offset = 6;
                break;
            case 0b0110:
                msk_code = 0b000000011111111111111111111111111111111111;
                //           654321065432106543210654321065432106543210
                delta_offset = 5;
                break;
            case 0b0111:
                msk_code = 0b1111111111111111111111111111111111111111111111111;
                //           6543210654321065432106543210654321065432106543210
                delta_offset = 8;
                break;
            case 0b1000:
                msk_code = 0b000000011111111111111111111111111111111111;
                //           654321065432106543210654321065432106543210
                delta_offset = 5;
                break;
            case 0b1001:
                msk_code = 0b111111111111111111111111111111111111111111;
                //           654321065432106543210654321065432106543210
                delta_offset = 6;
                break;
            case 0b1010:
                msk_code = 0b000000011111111111111111111111111111111111;
                //           654321065432106543210654321065432106543210
                delta_offset = 5;
                break;
            case 0b1011:
                msk_code = 0b1111111111111111111111111111111111111111111111111;
                //           6543210654321065432106543210654321065432106543210
                delta_offset = 7;
                break;
            case 0b1100:
                msk_code = 0b000000011111111111111111111111111111111111;
                //           654321065432106543210654321065432106543210
                delta_offset = 5;
                break;
            case 0b1110:
                msk_code = 0b000000011111111111111111111111111111111111;
                //           654321065432106543210654321065432106543210
                delta_offset = 5;
                break;
            default:
                msk_code = 0b00000000000000000000000000000000;
                delta_offset = 0;
            }
            break;
        default:
            msk_code = 0b00000000000000000000000000000000;
            delta_offset = 0;
        }
        break;
    default:
        msk_code = 0b00000000000000000000000000000000;
        delta_offset = 0;
    }

    value = result & msk_code;

    return delta_offset;
}

ap_uint<32> getint32_direct_from_dram(ap_uint<32>* buf, ap_uint<32> index)
{
    ap_uint<32> tem;
    ap_uint<2> offset;
    ap_uint<30> buf_index;
    offset = index.range(1,0);
    buf_index = index.range(31,2);
    switch (offset)
    {
    case 0b00:
        tem = buf[buf_index];
        break;
    case 0b01:
        tem.range(23, 0) = buf[buf_index].range(31, 8);
        tem.range(31, 24) = buf[buf_index + 1].range(7,0);
        break;
    case 0b10:
        tem.range(15, 0) = buf[buf_index].range(31, 16);
        tem.range(31, 16) = buf[buf_index+1].range(15, 0);
        break;
    case 0b11:
        tem.range(7, 0) = buf[buf_index].range(31, 24);
        tem.range(31, 8) = buf[buf_index+1].range(23, 0);
        break;
    default:
        break;
    }
    return tem;
}

void footer_decoder(ap_uint<32> *buf, int size,
                    // uint64_t &metaindex_handle_offset, uint64_t &metaindex_handle_size,
                    // uint64_t &index_handle_offset,
                    int &index_handle_pointer,
                    // uint64_t &index_handle_size,
                    int &limit_point)
{
    if (size == 0)
    {
        index_handle_pointer = 0;
        limit_point=0;
    }
    else
    {
        ap_uint<32> footer_buf[footer_size + 10];

        int start_input_index = (size - footer_size + 1) / 4;
        for (int i = 0; i < 15; i++){
            footer_buf[i] = buf[start_input_index + i];
        }

        ap_uint<8> pointer_buf = (size - footer_size + 1) % 4;
        ap_uint<64> index_handle_offset, index_handle_size;
        ap_uint<64> metaindex_handle_offset, metaindex_handle_size;
        uint32_t restart_point_num;

        ap_uint<64> gen64num;

        gen64num = ap32get64(footer_buf, pointer_buf);
        pointer_buf+=GetVarintfromuint64_for_footer_decoder(gen64num, metaindex_handle_offset);

        gen64num = ap32get64(footer_buf, pointer_buf);
        pointer_buf+=GetVarintfromuint64_for_footer_decoder(gen64num, metaindex_handle_size);

        gen64num = ap32get64(footer_buf, pointer_buf);
        pointer_buf+=GetVarintfromuint64_for_footer_decoder(gen64num, index_handle_offset);

        gen64num = ap32get64(footer_buf, pointer_buf);
        pointer_buf+=GetVarintfromuint64_for_footer_decoder(gen64num, index_handle_size);

        index_handle_pointer= index_handle_offset;
        restart_point_num = getint32_direct_from_dram(buf, index_handle_offset + index_handle_size - 4);
        limit_point = index_handle_offset + index_handle_size - (1 + restart_point_num) * sizeof(uint32_t);
    }
}

/*
==                                                                                                   ==
==                                                                                                   ==
=======================================================================================================
==                                                                                                   ==
==                                       footer decoder end                                          ==
==                                                                                                   ==
=======================================================================================================
=======================================================================================================
*/

/*
=======================================================================================================
=======================================================================================================
==                                                                                                   ==
==                                         Decoder area                                              ==
==                                                                                                   ==
=======================================================================================================
==                                                                                                   ==
==                                                                                                   ==
*/

ap_uint<4> GetVarint32(ap_uint<32> input, ap_uint<32> &value)
{
    ap_uint<32> result; // wire
    ap_uint<4> delta_offset;

    ap_uint<32> msk_code;

    ap_uint<2> ctrl_hi;
    ap_uint<2> ctrl_lo;

    ctrl_lo.range(1, 1) = input.range(15, 15);
    ctrl_lo.range(0, 0) = input.range(7, 7);

    ctrl_hi.range(1, 1) = input.range(31, 31);
    ctrl_hi.range(0, 0) = input.range(23, 23);

    result.range(6, 0) = input.range(6, 0);
    result.range(13, 7) = input.range(14, 8);
    result.range(20, 14) = input.range(22, 16);
    result.range(27, 21) = input.range(30, 24);
    result.range(31, 28) = 0;

    switch (ctrl_lo)
    {
    case 0b00:
        msk_code = 0b00000000000000000000000001111111;
        delta_offset = 1;
        break;
    case 0b01:
        msk_code = 0b00000000000000000011111111111111;
        delta_offset = 2;
        break;
    case 0b10:
        msk_code = 0b00000000000000000000000001111111;
        delta_offset = 1;
        break;
    case 0b11:
        switch (ctrl_hi)
        {
        case 0b00:
            msk_code = 0b00000000000111111111111111111111;
            delta_offset = 3;
            break;
        case 0b01:
            msk_code = 0b00001111111111111111111111111111;
            delta_offset = 4;
            break;
        case 0b10:
            msk_code = 0b00000000000111111111111111111111;
            delta_offset = 3;
            break;
        default:
            msk_code = 0b00000000000000000000000000000000;
            delta_offset = 0;
        }
        break;
    default:
        msk_code = 0b00000000000000000000000000000000;
        delta_offset = 0;
    }

    value = result & msk_code;

    return delta_offset;
}

ap_uint<4> GetVarint64(ap_uint<64> input, ap_uint<35> &value)
{
    // warning: 目前仅解码40位
    ap_uint<40> result; // wire
    ap_uint<4> delta_offset;

    ap_uint<40> msk_code;

    ap_uint<2> ctrl_l1;
    ap_uint<2> ctrl_l2;
    ap_uint<1> ctrl_l3;

    ctrl_l1.range(1, 1) = input.range(15, 15);
    ctrl_l1.range(0, 0) = input.range(7, 7);

    ctrl_l2.range(1, 1) = input.range(31, 31);
    ctrl_l2.range(0, 0) = input.range(23, 23);

    ctrl_l3.range(0, 0) = input.range(39, 39);
    // ctrl_l3.range(1, 1) = input.range(47, 47);

    result.range(6, 0) = input.range(6, 0);
    result.range(13, 7) = input.range(14, 8);
    result.range(20, 14) = input.range(22, 16);
    result.range(27, 21) = input.range(30, 24);
    result.range(34, 28) = input.range(38, 32);
    // result.range(39, 35) = 0;


    switch (ctrl_l1)
    {
    case 0b00:
        msk_code = 0b0000000000000000000000000000000001111111;
        delta_offset = 1;
        break;
    case 0b01:
        msk_code = 0b0000000000000000000000000011111111111111;
        delta_offset = 2;
        break;
    case 0b10:
        msk_code = 0b0000000000000000000000000000000001111111;
        delta_offset = 1;
        break;
    case 0b11:
        switch (ctrl_l2)
        {
        case 0b00:
            msk_code = 0b0000000000000000000111111111111111111111;
            delta_offset = 3;
            break;
        case 0b01:
            msk_code = 0b0000000000001111111111111111111111111111;
            delta_offset = 4;
            break;
        case 0b10:
            msk_code = 0b0000000000000000000111111111111111111111;
            delta_offset = 3;
            break;
        case 0b11:
            switch (ctrl_l3)
            {
            case 0b0:
                msk_code = 0b0000011111111111111111111111111111111111;
                delta_offset = 5;
                break;
            case 0b1:
                printf("Error: GetVarint64 ctrl_l3上溢\n");
                msk_code = 0b0000011111111111111111111111111111111111;;
                delta_offset = 5;
                break;
            default:
                msk_code = 0b0000000000000000000000000000000000000000;
                delta_offset = 0;
                break;
            }
            break;
        default:
            msk_code = 0b0000000000000000000000000000000000000000;
            delta_offset = 0;
        }
        break;
    default:
        msk_code = 0b0000000000000000000000000000000000000000;
        delta_offset = 0;
    }

    value = result & msk_code;

    return delta_offset;
}

ap_uint<32> ap32to32_for_decoder_index_block(ap_uint<32>* buf, ap_uint<8> index)
{
    ap_uint<32> tem=0;
    ap_uint<2> offset=index.range(1,0);
    ap_uint<6> buf_index=index.range(7,2);
    switch(offset)
    {
        case 0:
            tem=buf[buf_index];
            break;
        case 1:
            tem.range(23,0)=buf[buf_index].range(31,8);
            tem.range(31,24)=buf[buf_index+1].range(7,0);
            break;
        case 2:
            tem.range(15,0)=buf[buf_index].range(31,16);
            tem.range(31,16)=buf[buf_index+1].range(15,0);
            break;
        case 3:
            tem.range(7,0)=buf[buf_index].range(31,24);
            tem.range(31,8)=buf[buf_index+1].range(23,0);
            break;
        default:
            tem=0;
            break;
    }
    return tem;
}

ap_uint<64> ap32to64_for_decoder_index_block(ap_uint<32>* buf, ap_uint<8> index)
{
    ap_uint<64> tem=0;
    ap_uint<2> offset=index.range(1,0);
    ap_uint<6> buf_index=index.range(7,2);
    switch(offset)
    {
        case 0:
            tem.range(31,0)=buf[buf_index];
            tem.range(63,32)=buf[buf_index+1];
            break;
        case 1:
            tem.range(23,0)=buf[buf_index].range(31,8);
            tem.range(55,24)=buf[buf_index+1].range(31,0);
            tem.range(63,56)=buf[buf_index+2].range(7,0);
            break;
        case 2:
            tem.range(15,0)=buf[buf_index].range(31,16);
            tem.range(47,16)=buf[buf_index+1].range(31,0);
            tem.range(63,48)=buf[buf_index+2].range(15,0);
            break;
        case 3:
            tem.range(7,0)=buf[buf_index].range(31,24);
            tem.range(39,8)=buf[buf_index+1].range(31,0);
            tem.range(63,40)=buf[buf_index+2].range(23,0);
            break;
        default:
            tem=0;
            break;
    }
    return tem;
}

ap_uint<32> ap32to32_for_decoder_data_block(ap_uint<32>* buf, ap_uint<16> index)
{
    ap_uint<32> tem=0;
    ap_uint<2> offset=index.range(1,0);
    ap_uint<14> buf_index=index.range(15,2);
    switch(offset)
    {
        case 0:
            tem=buf[buf_index];
            break;
        case 1:
            tem.range(23,0)=buf[buf_index].range(31,8);
            tem.range(31,24)=buf[buf_index+1].range(7,0);
            break;
        case 2:
            tem.range(15,0)=buf[buf_index].range(31,16);
            tem.range(31,16)=buf[buf_index+1].range(15,0);
            break;
        case 3:
            tem.range(7,0)=buf[buf_index].range(31,24);
            tem.range(31,8)=buf[buf_index+1].range(23,0);
            break;
        default:
            tem=0;
            break;
    }
    return tem;
}

void ap32to32_copy_datablock(ap_uint<32> *input, ap_uint<32>* output, ap_uint<35> offset, ap_uint<32> size)
{
    ap_uint<30> readlength = size.range(31,2);
    ap_uint<33> readoffset = offset.range(34,2);
    readlength+=2;
    for(ap_uint<30> i=0;i<readlength;i++)
    {
        output[i]=input[i+readoffset];
    }
}

// void Get_key_for_decoder(ap_uint<32>* input, keystring &shared_key, ap_uint<12> index, ap_uint<8> unshared_key_length)
// {
//     // 取用index指向的数据块
//     ap_uint<2> start_offset = index.range(1,0);
//     ap_uint<10> start_index = index.range(11,2);

//     ap_uint<KEY_WIDTH_BITS> result_start_offset = shared_key.length.range(KEY_WIDTH_BITS-1,0);  // 取决于数据宽度的字节数，64位就是8字节，KEY_WIDTH_BITS=3
//     ap_uint<KEY_LENGTH_BITWIDTH-KEY_WIDTH_BITS> result_start_index = shared_key.length.range(KEY_LENGTH_BITWIDTH-1,KEY_WIDTH_BITS);  // 高位表示index

//     for (ap_uint<8> i = 0; i < unshared_key_length; i++)  // 共拷贝unshared_key_length字节
//     {
// #pragma HLS UNROLL factor=8  // UNROLL成一次处理8字节
//         shared_key.c[result_start_index].range(result_start_offset * 8 + 7, result_start_offset * 8) = input[start_index].range(start_offset * 8 + 7, start_offset * 8);
//         start_offset++;
//         result_start_offset++;
//         if (start_offset == 0)
//         {
//             start_index++;
//         }
//         if (result_start_offset == 0)
//         {
//             result_start_index++;
//         }
//     }
    
// }

// TODO: 验证优化结果
void Get_key_for_decoder(ap_uint<32>* input, keystring &shared_key, ap_uint<16> index, ap_uint<8> unshared_key_length)
{
    // 拷贝输入到tem
    ap_uint<KEY_BITWIDTH> tem[KEY_ARRAY_LENGTH];
    // 将unshared_key_length对齐到128位（16字节），得到需要拷贝的数组长度
    ap_uint<KEY_LENGTH_BITWIDTH-KEY_WIDTH_BITS> array_cnt = ((unshared_key_length + KEY_WIDTH_MASK) >> KEY_WIDTH_BITS);
    ap_uint<KEY_LENGTH_BITWIDTH-KEY_WIDTH_BITS> i;

    // danger: 这里KEY_BITWIDTH一定要是32的倍数，已经写死KEY_BITWIDTH=128
    // assert(KEY_BITWIDTH == 128);
    ap_uint<2> input_offset = index.range(1,0);
    ap_uint<14> input_index = index.range(15,2);

    for(i = 0; i < array_cnt; i++, input_index+=4)
    {
// #pragma HLS PIPELINE
        switch(input_offset)  // danger: 写死KEY_BITWIDTH=128
        {
        case 0:
            tem[i].range(31,0)=input[input_index].range(31,0);
            tem[i].range(63,32)=input[input_index+1].range(31,0);
            tem[i].range(95,64)=input[input_index+2].range(31,0);
            tem[i].range(127,96)=input[input_index+3].range(31,0);
            break;
        case 1:
            tem[i].range(23,0)=input[input_index].range(31,8);
            tem[i].range(55,24)=input[input_index+1].range(31,0);
            tem[i].range(87,56)=input[input_index+2].range(31,0);
            tem[i].range(119,88)=input[input_index+3].range(31,0);
            tem[i].range(127,120)=input[input_index+4].range(7,0);
            break;
        case 2:
            tem[i].range(15,0)=input[input_index].range(31,16);
            tem[i].range(47,16)=input[input_index+1].range(31,0);
            tem[i].range(79,48)=input[input_index+2].range(31,0);
            tem[i].range(111,80)=input[input_index+3].range(31,0);
            tem[i].range(127,112)=input[input_index+4].range(15,0);
            break;
        case 3:
            tem[i].range(7,0)=input[input_index].range(31,24);
            tem[i].range(39,8)=input[input_index+1].range(31,0);
            tem[i].range(71,40)=input[input_index+2].range(31,0);
            tem[i].range(103,72)=input[input_index+3].range(31,0);
            tem[i].range(127,104)=input[input_index+4].range(23,0);
            break;
        default:
            tem[i]=0;
        }
    }

    // 将tem偏移shared_key_length，拷贝到unshared_key位置
    ap_uint<KEY_WIDTH_BITS> key_offset = shared_key.length.range(KEY_WIDTH_BITS-1,0);
    ap_uint<KEY_LENGTH_BITWIDTH-KEY_WIDTH_BITS> key_index = shared_key.length.range(KEY_LENGTH_BITWIDTH-1,KEY_WIDTH_BITS);
    if (key_offset == 0)
    {
        for (i = 0; i < array_cnt; i++, key_index++)
        {
            shared_key.c[key_index] = tem[i];
        }
    }
    else  // key_offset != 0，此时需要额外处理最后一个元素
    {
        for (i = 0; i < array_cnt-1; i++, key_index++)
        {
    #pragma HLS PIPELINE
            switch(key_offset)
            {
            case 1:
                shared_key.c[key_index].range(127,8) = tem[i].range(119,0);
                shared_key.c[key_index+1].range(7,0) = tem[i].range(127,120);
                break;
            case 2:
                shared_key.c[key_index].range(127,16) = tem[i].range(111,0);
                shared_key.c[key_index+1].range(15,0) = tem[i].range(127,112);
                break;
            case 3:
                shared_key.c[key_index].range(127,24) = tem[i].range(103,0);
                shared_key.c[key_index+1].range(23,0) = tem[i].range(127,104);
                break;
            case 4:
                shared_key.c[key_index].range(127,32) = tem[i].range(95,0);
                shared_key.c[key_index+1].range(31,0) = tem[i].range(127,96);
                break;
            case 5:
                shared_key.c[key_index].range(127,40) = tem[i].range(87,0);
                shared_key.c[key_index+1].range(39,0) = tem[i].range(127,88);
                break;
            case 6:
                shared_key.c[key_index].range(127,48) = tem[i].range(79,0);
                shared_key.c[key_index+1].range(47,0) = tem[i].range(127,80);
                break;
            case 7:
                shared_key.c[key_index].range(127,56) = tem[i].range(71,0);
                shared_key.c[key_index+1].range(55,0) = tem[i].range(127,56);
                break;
            case 8:
                shared_key.c[key_index].range(127,64) = tem[i].range(63,0);
                shared_key.c[key_index+1].range(63,0) = tem[i].range(127,64);
                break;
            case 9:
                shared_key.c[key_index].range(127,72) = tem[i].range(55,0);
                shared_key.c[key_index+1].range(55,0) = tem[i].range(127,56);
                break;
            case 10:
                shared_key.c[key_index].range(127,80) = tem[i].range(47,0);
                shared_key.c[key_index+1].range(47,0) = tem[i].range(127,48);
                break;
            case 11:
                shared_key.c[key_index].range(127,88) = tem[i].range(39,0);
                shared_key.c[key_index+1].range(39,0) = tem[i].range(127,40);
                break;
            case 12:
                shared_key.c[key_index].range(127,96) = tem[i].range(31,0);
                shared_key.c[key_index+1].range(31,0) = tem[i].range(127,32);
                break;
            case 13:
                shared_key.c[key_index].range(127,104) = tem[i].range(23,0);
                shared_key.c[key_index+1].range(23,0) = tem[i].range(127,24);
                break;
            case 14:
                shared_key.c[key_index].range(127,112) = tem[i].range(15,0);
                shared_key.c[key_index+1].range(15,0) = tem[i].range(127,16);
                break;
            case 15:
                shared_key.c[key_index].range(127,120) = tem[i].range(7,0);
                shared_key.c[key_index+1].range(7,0) = tem[i].range(127,8);
                break;
            default:
                break;
            }
        }
        switch(key_offset)
        {
        case 1:
            shared_key.c[key_index].range(127,8) = tem[i].range(119,0);
            break;
        case 2:
            shared_key.c[key_index].range(127,16) = tem[i].range(111,0);
            break;
        case 3:
            shared_key.c[key_index].range(127,24) = tem[i].range(103,0);
            break;
        case 4:
            shared_key.c[key_index].range(127,32) = tem[i].range(95,0);
            break;
        case 5:
            shared_key.c[key_index].range(127,40) = tem[i].range(87,0);
            break;
        case 6:
            shared_key.c[key_index].range(127,48) = tem[i].range(79,0);
            break;
        case 7:
            shared_key.c[key_index].range(127,56) = tem[i].range(71,0);
            break;
        case 8:
            shared_key.c[key_index].range(127,64) = tem[i].range(63,0);
            break;
        case 9:
            shared_key.c[key_index].range(127,72) = tem[i].range(55,0);
            break;
        case 10:
            shared_key.c[key_index].range(127,80) = tem[i].range(47,0);
            break;
        case 11:
            shared_key.c[key_index].range(127,88) = tem[i].range(39,0);
            break;
        case 12:
            shared_key.c[key_index].range(127,96) = tem[i].range(31,0);
            break;
        case 13:
            shared_key.c[key_index].range(127,104) = tem[i].range(23,0);
            break;
        case 14:
            shared_key.c[key_index].range(127,112) = tem[i].range(15,0);
            break;
        case 15:
            shared_key.c[key_index].range(127,120) = tem[i].range(7,0);
            break;
        default:
            break;
        }
    }
    
//     // 低效率，待优化
//     ap_uint<KEY_LENGTH_BITWIDTH> result_index = shared_key.length;
//     ap_uint<8> byte;
//     for (ap_uint<12> idxtmp = index; index < idxtmp + unshared_key_length; index++, result_index++)  // 共拷贝unshared_key_length字节
//     {
// #pragma HLS UNROLL factor=16
//         byte = input[index >> 2].range((index&0x3) * 8 + 7, (index&0x3) * 8);

//         shared_key.c[result_index >> KEY_WIDTH_BITS].range((result_index & KEY_WIDTH_MASK) * 8 + 7, (result_index & KEY_WIDTH_MASK) * 8) = byte;
//     }
}

void Get_value_for_decoder(ap_uint<32>* input, valuestring &value, ap_uint<16> index)
{
    ap_uint<2> offset=index.range(1,0);
    ap_uint<14> start_index=index.range(15,2);
    // warning: 8位宽度可以表示128*16=2048字节，现在定下value为1024字节，所以没问题
    ap_uint<8> copy_array_length = (value.length + VALUE_WIDTH-1) >> VALUE_WIDTH_BITS;  // (length+15)/16，对齐到16字节后除以16字节为数组长度

    for(ap_uint<8> i=0;i<copy_array_length;i++)
    {
        ap_uint<VALUE_BITWIDTH> tem;  // 写死宽度为128位
        switch(offset)  // TODO：存在一个32位分两次读取的问题，可优化？
        {
        case 0:
            tem.range(31,0)=input[start_index].range(31,0);
            tem.range(63,32)=input[start_index+1].range(31,0);
            tem.range(95,64)=input[start_index+2].range(31,0);
            tem.range(127,96)=input[start_index+3].range(31,0);
            break;
        case 1:
            tem.range(23,0)=input[start_index].range(31,8);
            tem.range(55,24)=input[start_index+1].range(31,0);
            tem.range(87,56)=input[start_index+2].range(31,0);
            tem.range(119,88)=input[start_index+3].range(31,0);
            tem.range(127,120)=input[start_index+4].range(7,0);
            break;
        case 2:
            tem.range(15,0)=input[start_index].range(31,16);
            tem.range(47,16)=input[start_index+1].range(31,0);
            tem.range(79,48)=input[start_index+2].range(31,0);
            tem.range(111,80)=input[start_index+3].range(31,0);
            tem.range(127,112)=input[start_index+4].range(15,0);
            break;
        case 3:
            tem.range(7,0)=input[start_index].range(31,24);
            tem.range(39,8)=input[start_index+1].range(31,0);
            tem.range(71,40)=input[start_index+2].range(31,0);
            tem.range(103,72)=input[start_index+3].range(31,0);
            tem.range(127,104)=input[start_index+4].range(23,0);
            break;
        default:
            tem=0;
        }
        start_index+=4;
        value.c[i]=tem;
    }

}

void decoder(ap_uint<32> *buf,  ap_uint<32> kv_sum, ap_uint<40> file_size, hls::stream<fifo_key_meta> &km_stream, hls::stream<fifo_value_slice> &value_stream)
{
    /*
        footer decoder
    */
    ap_uint<32> index_block_pointer, index_block_limit;

    if (file_size == 0)
    {
        index_block_pointer = 0;
        index_block_limit=0;
    }
    else
    {
        ap_uint<32> footer_buf[footer_size + 10];

        ap_uint<40> start_input_index = (file_size - footer_size + 1) / 4;
        for (int i = 0; i < 15; i++){
            footer_buf[i] = buf[start_input_index + i];
        }
        // std::cout << "start_input_index: " << (uint32_t)start_input_index << std::endl;
        // for (int i = 0; i < 15; i++){
        //     printf("footer_buf[%d]: %02x %02x %02x %02x\n", i, footer_buf[i].range(7,0).to_uint(), footer_buf[i].range(15,8).to_uint(), footer_buf[i].range(23,16).to_uint(), footer_buf[i].range(31,24).to_uint());
        // }

        ap_uint<8> pointer_buf = (file_size - footer_size + 1) % 4;
        // std::cout << "pointer_buf: " << (uint32_t)pointer_buf << std::endl;
        ap_uint<64> index_handle_offset, index_handle_size;
        ap_uint<64> metaindex_handle_offset, metaindex_handle_size;
        uint32_t restart_point_num;

        ap_uint<64> gen64num;

        gen64num = ap32get64(footer_buf, pointer_buf);
        // for (int i = 0; i < 8; i++)
        // {
        //     printf("%02x ", gen64num.range(7+8*i,8*i).to_uint());
        // }
        // printf("\n");
        pointer_buf+=GetVarintfromuint64_for_footer_decoder(gen64num, metaindex_handle_offset);

        gen64num = ap32get64(footer_buf, pointer_buf);
        pointer_buf+=GetVarintfromuint64_for_footer_decoder(gen64num, metaindex_handle_size);

        gen64num = ap32get64(footer_buf, pointer_buf);
        pointer_buf+=GetVarintfromuint64_for_footer_decoder(gen64num, index_handle_offset);

        gen64num = ap32get64(footer_buf, pointer_buf);
        pointer_buf+=GetVarintfromuint64_for_footer_decoder(gen64num, index_handle_size);

        // std::cout << "metaindex_handle_offset: " << metaindex_handle_offset << std::endl;
        // std::cout << "metaindex_handle_size: " << metaindex_handle_size << std::endl;
        // std::cout << "index_handle_offset: " << (uint32_t)index_handle_offset << std::endl;
        // std::cout << "index_handle_size: " << (uint32_t)index_handle_size << std::endl;

        index_block_pointer= index_handle_offset;
        restart_point_num = getint32_direct_from_dram(buf, index_handle_offset + index_handle_size - 4);
        index_block_limit = index_handle_offset + index_handle_size - (1 + restart_point_num) * sizeof(uint32_t);
    }


    /*
        =============
        ** CAUTION **
        =============
        !!!!!!!!!!!!!
        DON'T DEFINE STATIC VARIABLES
        !!!!!!!!!!!!!
    */

    // warning: 1300<<2=5200，支持的最大data block size为5200字节
    ap_uint<32> bram_buffer[1300]={0};
#pragma HLS BIND_STORAGE variable=bram_buffer impl=uram

    keystring shared_key_buffer;
    shared_key_buffer.length = 0;

    /*
        Change:
        -- index_block_buffer to 32 bit width (all varint is 32 bit width)
        -- depth 32 (32*32=1024=512*2)
        -- index_block_buffer turn to circular buffer
    */
    ap_uint<32> index_block_buffer[32];
#pragma HLS ARRAY_PARTITION dim=1 factor=2 type=cyclic variable=index_block_buffer

    ap_uint<16> data_block_pointer = 0;
    ap_uint<16> data_block_limit = 0;

    {
        // std::cout << "decoder writing start singal" << std::endl;
        fifo_key_meta SIGNAL;
        set_signal_key(SIGNAL.key);
        km_stream.write(SIGNAL);
    }

    ap_uint<32> data_block_num = 0;
    // std::cout << "index block pointer: " << (uint32_t)index_block_pointer << "  index block limit: " << (uint32_t)index_block_limit << std::endl;
    chrono_print_time("decoder prepare end");
    for (int i=0; i<kv_sum; i++)
    {
        chrono_print_time("decoder start");
        if (i % 10000 == 0)
        {
            std::cout << "-----------" << i+1 << " / " << kv_sum << "------------" << std::endl;
        }
        if(data_block_pointer>=data_block_limit)
        {
            /*
                The current data block has been read out
            */
            if (index_block_pointer < index_block_limit)
            {
                /*
                    Now get a new data block from index block.
                */
                ap_uint<32> shared_key_length, unshared_key_length;
                ap_uint<35> data_block_handle_offset;
                ap_uint<32> data_block_handle_size;

                ap_uint<8> index_block_delta=0;
                ap_uint<8> index_block_index_for_read=0;
                ap_uint<30> index_block_read_start_pos=index_block_pointer.range(31,2);

                index_block_index_for_read.range(1,0)=index_block_pointer.range(1,0);
                index_block_index_for_read.range(7,2)=0;
                
                // std::cout << "index_block_pointer: " << (uint32_t)index_block_pointer << "  index_block_limit: " << (uint32_t)index_block_limit << std::endl;
                // std::cout << "index_block_read_start_pos: " << (uint32_t)index_block_read_start_pos << std::endl;
                // std::cout << "index_block_index_for_read: " << (uint32_t)index_block_index_for_read << std::endl;

                //buffer index block
                index_block_buffer[0]=buf[index_block_read_start_pos];
                index_block_buffer[1]=buf[index_block_read_start_pos+1];
                index_block_buffer[2]=buf[index_block_read_start_pos+2];
                index_block_buffer[3]=buf[index_block_read_start_pos+3];
                index_block_buffer[4]=buf[index_block_read_start_pos+4];
                index_block_buffer[5]=buf[index_block_read_start_pos+5];

                ap_uint<32> genint32_num;
                ap_uint<64> getint64_num;
                ap_uint<8> delta_tem;

                genint32_num=ap32to32_for_decoder_index_block(index_block_buffer, index_block_index_for_read);
                delta_tem=GetVarint32(genint32_num, shared_key_length);
                index_block_delta+=delta_tem;
                index_block_index_for_read+=delta_tem;

                genint32_num=ap32to32_for_decoder_index_block(index_block_buffer, index_block_index_for_read);
                delta_tem=GetVarint32(genint32_num, unshared_key_length);
                index_block_delta+=delta_tem;
                index_block_index_for_read+=delta_tem;

                index_block_delta += unshared_key_length;
                index_block_index_for_read += unshared_key_length;

                // offset过大，有上溢风险，目前解码前40位
                getint64_num=ap32to64_for_decoder_index_block(index_block_buffer, index_block_index_for_read);
                delta_tem=GetVarint64(getint64_num, data_block_handle_offset);
                index_block_delta+=delta_tem;
                index_block_index_for_read+=delta_tem;

                genint32_num=ap32to32_for_decoder_index_block(index_block_buffer, index_block_index_for_read);
                delta_tem=GetVarint32(genint32_num, data_block_handle_size);
                index_block_delta+=delta_tem;
                index_block_index_for_read+=delta_tem;

                std::cout << "shared_key_length: " << shared_key_length << " unshared_key_length: " << unshared_key_length << " data_block_handle_offset: " << data_block_handle_offset << " data_block_handle_size: " << data_block_handle_size << std::endl;

                index_block_pointer+=index_block_delta;

                ap32to32_copy_datablock(buf, bram_buffer, data_block_handle_offset, data_block_handle_size);

                data_block_pointer.range(1,0) = data_block_handle_offset.range(1,0);
                data_block_pointer.range(15,2) = 0;
                data_block_limit = data_block_handle_size.range(15,0) + data_block_pointer;

                //get restart point num
                genint32_num = ap32to32_for_decoder_data_block(bram_buffer, data_block_limit - sizeof(uint32_t));

                data_block_limit -= (1+genint32_num) *4;
                
                data_block_num++;
                // printf("============================================\n");
                // printf("Data Block # %d | offset: %d | size: %d\n", data_block_num, data_block_handle_offset, data_block_handle_size);
                // std::cout << "index_block_pointer: " << (uint32_t)index_block_pointer << "  index_block_limit: " << (uint32_t)index_block_limit << std::endl;
            }
            else
            {
                /*
                    The index block has been read out. We should finish this task.
                */
                break;
            }
            chrono_print_time("decoder get new data block");
        }
        // printf("data_block_pointer: %d  data_block_limit: %d\n", data_block_pointer, data_block_limit);
        ap_uint<32> shared_key_length, unshared_key_length;
        ap_uint<32> genint32_num;
        fifo_key_meta tem_km;
        valuestring tem_value;
        // ap_uint<128> key;
        ap_uint<32> value_length;

        genint32_num = ap32to32_for_decoder_data_block(bram_buffer, data_block_pointer);
        data_block_pointer += GetVarint32(genint32_num, shared_key_length);
        genint32_num = ap32to32_for_decoder_data_block(bram_buffer, data_block_pointer);
        data_block_pointer += GetVarint32(genint32_num, unshared_key_length);
        genint32_num = ap32to32_for_decoder_data_block(bram_buffer, data_block_pointer);
        data_block_pointer += GetVarint32(genint32_num, value_length);
        tem_km.value_length=value_length;
        tem_value.length=value_length;
        // printf("value_length: %d\n", value_length);
        chrono_print_time("decoder get key and value length");

        // danger: key length目前为128字节，这里最大支持到255字节，再翻倍要改
        shared_key_buffer.length = shared_key_length.range(7,0);
        Get_key_for_decoder(bram_buffer, shared_key_buffer, data_block_pointer, unshared_key_length.range(7,0));
        data_block_pointer += unshared_key_length;
        chrono_print_time("decoder get key");


        Get_value_for_decoder(bram_buffer, tem_value, data_block_pointer);
        // tem_value.print_value();
        data_block_pointer += value_length;
        chrono_print_time("decoder get value");

        shared_key_buffer.length += unshared_key_length.range(7,0);
        // std::cout << "shared_key_length: " << shared_key_length << "  unshared_key_length: " << unshared_key_length << std::endl;
        // printf("%d ", i);
        // shared_key_buffer.print_key();
        // tem_kv.value.print_value();
        tem_km.key = shared_key_buffer;
        km_stream.write(tem_km);
        // tem_km.key.print_key();

        ap_uint<4> j;
        fifo_value_slice tem;
        // TODO: 这里是写死更快还是根据value_length计算更快？
        for (j = 0; j < VALUE_LENGTH / 512; j++)
        {
            // 一次写512字节（位宽写死16字节，所以一次写32个数据）
            for (ap_uint<6> k = 0; k < 32; k++)
            {
        #pragma HLS UNROLL
                tem.c[k] = tem_value.c[j*32+k];
                // for(int t = 0; t < 16; t++)
                // {
                //     printf("%02x", (uint32_t)tem.c[k].range((t+1)*8-1, t*8) & 0xFF);
                // }
            }
            value_stream.write(tem);
        }
        // printf("\n");
#if VALUE_LENGTH % 512 != 0
        // 一次写512字节（位宽写死16字节，所以一次写32个数据）
        for (ap_uint<6> k = 0; k < (VALUE_LENGTH % 512) / 16; k++)
        {
            tem.c[k] = tem_value.c[j*32+k];
        }
        value_stream.write(tem);
#endif

        chrono_print_time("decoder write kv pair");
    }

    {
        // std::cout << "decoder writing end singal" << std::endl;
        fifo_key_meta MAX_KEY;
        set_max_key(MAX_KEY.key);
        km_stream.write(MAX_KEY);
    }

}

/*
==                                                                                                   ==
==                                                                                                   ==
=======================================================================================================
==                                                                                                   ==
==                                          Decoder end                                              ==
==                                                                                                   ==
=======================================================================================================
=======================================================================================================
*/



/*
=======================================================================================================
=======================================================================================================
==                                                                                                   ==
==                                         Merger area                                               ==
==                                                                                                   ==
=======================================================================================================
==                                                                                                   ==
==                                                                                                   ==
*/

void merge(hls::stream<fifo_key_meta> km_input[MAX_INPUT_FILE_NUM], hls::stream<fifo_key_meta> &km_output, hls::stream<ap_uint<2> > &merge_result)
{
    static fifo_key_meta tem_km[4];
    static ap_uint<2> smallest_num;

    ap_uint<2> smaller_of_0and1_num;
    ap_uint<2> smaller_of_2and3_num;

    fifo_key_meta tem;
    chrono_print_merge_time("merge reading");

    tem = km_input[smallest_num].read();
    // tem.key.print_key();

    // std::cout << "merge reading: ";
    // std::cout << tem.key << std::endl;

    if(is_signal_key(tem.key))
    {
        // std::cout << "read SIGNAL" << std::endl;
        if(smallest_num!=0)
        {
            tem = km_input[0].read();
            tem = km_input[0].read();
            tem_km[0] = tem;
        }
        if(smallest_num!=1)
        {
            tem = km_input[1].read();
            tem = km_input[1].read();
            tem_km[1] = tem;
        }
        if(smallest_num!=2)
        {
            tem = km_input[2].read();
            tem = km_input[2].read();
            tem_km[2] = tem;
        }
        if(smallest_num!=3)
        {
            tem = km_input[3].read();
            tem = km_input[3].read();
            tem_km[3] = tem;
        }
        // result.write(0);
        tem = km_input[smallest_num].read();
        // std::cout << "read SIGNAL done" << std::endl;
        // std::cout << "now num:" << smallest_num << std::endl;
        chrono_print_merge_time("merge signal handle");
    }

    tem_km[smallest_num] = tem;

    keystring smaller_of_0and1_key;
    keystring smaller_of_2and3_key;
    if(tem_km[0].key<tem_km[1].key){
        smaller_of_0and1_num=0;
        smaller_of_0and1_key=tem_km[0].key;
    }else{
        smaller_of_0and1_num=1;
        smaller_of_0and1_key=tem_km[1].key;
    }
    chrono_print_merge_time("merge compare 0 and 1");

    if(tem_km[2].key<tem_km[3].key){
        smaller_of_2and3_num=2;
        smaller_of_2and3_key=tem_km[2].key;
    }else{
        smaller_of_2and3_num=3;
        smaller_of_2and3_key=tem_km[3].key;
    }
    chrono_print_merge_time("merge compare 2 and 3");

    if(smaller_of_0and1_key<smaller_of_2and3_key){
        smallest_num=smaller_of_0and1_num;
    }else{
        smallest_num=smaller_of_2and3_num;
    }
    chrono_print_merge_time("merge compare last");

    // std::cout << "comparing:" << std::endl;
    // for (int i = 0; i < 4; i++)
    // {
    //     tem_km[i].key.print_key();
    // }
    // std::cout << "smallest: " << smallest_num << std::endl;
    km_output.write(tem_km[smallest_num]);
    merge_result.write(smallest_num);
//     ap_uint<4> j;
//     for (j = 0; j < VALUE_LENGTH / 512; j++)
//     {
//         // danger: 最后一个输出的是max_key，没有传value，退出内核前这里是阻塞的
//         value_output.write(value_input[smallest_num].read());
//     }
// #if VALUE_LENGTH % 512 != 0
//     value_output.write(value_input[smallest_num].read());
// #endif
    chrono_print_merge_time("merge writing");
}

/*
==                                                                                                   ==
==                                                                                                   ==
=======================================================================================================
==                                                                                                   ==
==                                           Merger end                                              ==
==                                                                                                   ==
=======================================================================================================
=======================================================================================================
*/


/*
=======================================================================================================
=======================================================================================================
==                                                                                                   ==
==                                         Encoder area                                              ==
==                                                                                                   ==
=======================================================================================================
==                                                                                                   ==
==                                                                                                   ==
*/

bool same_user_key(const keystring &key1, const keystring &key2)
{
    if (key1.length != key2.length)
    {
        return false;
    }
    ap_uint<4> key_array_length = key1.length >> KEY_WIDTH_BITS;
    for (ap_uint<8> i = 0; i < key_array_length - 1; i++)  // 128 / 16 - 1 = 7
    {
#pragma HLS LOOPTRIPCOUNT min=15 max=15 avg=15
#pragma HLS UNROLL
        if (key1.c[i] != key2.c[i])
        {
            return false;
        }
    }

    return key1.c[key_array_length-1].range(KEY_BITWIDTH-SEQ_LENGTH*8-1, 0) == key2.c[key_array_length-1].range(KEY_BITWIDTH-SEQ_LENGTH*8-1, 0);
}

void copy_user_key(const keystring &ori_key, keystring &new_key)
{
    new_key.length = ori_key.length;
    for (ap_uint<8> i = 0; i < (ori_key.length >> KEY_WIDTH_BITS); i++)  // 128 / 16 = 8，连seq一块拷贝了
    {
#pragma HLS LOOPTRIPCOUNT min=15 max=15 avg=15
#pragma HLS UNROLL
        new_key.c[i] = ori_key.c[i];
    }
}

uint64_t getseqno(ap_uint<128> key, int key_length){
    ap_uint<64> res;
    res.range(63,56)=0;
    switch (key_length)
    {
    case 8:
        res.range(55,0)=key.range(55,0);
        break;
    case 9:
        res.range(55,0)=key.range(63,8);
        break;
    case 10:
        res.range(55,0)=key.range(71,16);
        break;
    case 11:
        res.range(55,0)=key.range(79,24);
        break;
    case 12:
        res.range(55,0)=key.range(87,32);
        break;
    case 13:
        res.range(55,0)=key.range(95,40);
        break;
    case 14:
        res.range(55,0)=key.range(103,48);
        break;
    case 15:
        res.range(55,0)=key.range(111,56);
        break;
    case 16:
        res.range(55,0)=key.range(119,64);
        break;
    default:
        res.range(55,0)=0;
        break;
    }
    return res;
}

void ap8to128_encoder(ap_uint<8> input, ap_uint<128>* output, ap_uint<16> index)
{
    ap_uint<4> offset=index.range(3,0);
    ap_uint<12> output_index=index.range(15,4);

    ap_uint<8> tem0;
    ap_uint<8> tem1;
    ap_uint<8> tem2;
    ap_uint<8> tem3;
    ap_uint<8> tem4;
    ap_uint<8> tem5;
    ap_uint<8> tem6;
    ap_uint<8> tem7;
    ap_uint<8> tem8;
    ap_uint<8> tem9;
    ap_uint<8> tem10;
    ap_uint<8> tem11;
    ap_uint<8> tem12;
    ap_uint<8> tem13;
    ap_uint<8> tem14;
    ap_uint<8> tem15;

    if(offset==0){
        tem0=input;
    }else{
        tem0=output[output_index].range(7,0);
    }

    if(offset==1){
        tem1=input;
    }else{
        tem1=output[output_index].range(15,8);
    }

    if(offset == 2) {
        tem2 = input;
    } else {
        tem2 = output[output_index].range(23, 16);
    }

    if(offset == 3) {
        tem3 = input;
    } else {
        tem3 = output[output_index].range(31, 24);
    }

    if(offset == 4) {
        tem4 = input;
    } else {
        tem4 = output[output_index].range(39, 32);
    }

    if(offset == 5) {
        tem5 = input;
    } else {
        tem5 = output[output_index].range(47, 40);
    }

    if(offset == 6) {
        tem6 = input;
    } else {
        tem6 = output[output_index].range(55, 48);
    }

    if(offset == 7) {
        tem7 = input;
    } else {
        tem7 = output[output_index].range(63, 56);
    }

    if(offset == 8) {
        tem8 = input;
    } else {
        tem8 = output[output_index].range(71, 64);
    }

    if(offset == 9) {
        tem9 = input;
    } else {
        tem9 = output[output_index].range(79, 72);
    }

    if(offset == 10) {
        tem10 = input;
    } else {
        tem10 = output[output_index].range(87, 80);
    }

    if(offset == 11) {
        tem11 = input;
    } else {
        tem11 = output[output_index].range(95, 88);
    }

    if(offset == 12) {
        tem12 = input;
    } else {
        tem12 = output[output_index].range(103, 96);
    }

    if(offset == 13) {
        tem13 = input;
    } else {
        tem13 = output[output_index].range(111, 104);
    }

    if(offset == 14) {
        tem14 = input;
    } else {
        tem14 = output[output_index].range(119, 112);
    }

    if(offset == 15) {
        tem15 = input;
    } else {
        tem15 = output[output_index].range(127, 120);
    }

    output[output_index].range(7,0) = tem0;
    output[output_index].range(15,8) = tem1;
    output[output_index].range(23,16) = tem2;
    output[output_index].range(31,24) = tem3;

    output[output_index].range(39,32) = tem4;
    output[output_index].range(47,40) = tem5;
    output[output_index].range(55,48) = tem6;
    output[output_index].range(63,56) = tem7;

    output[output_index].range(71,64) = tem8;
    output[output_index].range(79,72) = tem9;
    output[output_index].range(87,80) = tem10;
    output[output_index].range(95,88) = tem11;

    output[output_index].range(103,96) = tem12;
    output[output_index].range(111,104) = tem13;
    output[output_index].range(119,112) = tem14;
    output[output_index].range(127,120) = tem15;
}

void ap8to128_encoder_index_block(ap_uint<8> input, ap_uint<128>* output, ap_uint<32> index)
{
    ap_uint<4> offset=index.range(3,0);
    ap_uint<28> output_index=index.range(31,4);

    ap_uint<8> tem0;
    ap_uint<8> tem1;
    ap_uint<8> tem2;
    ap_uint<8> tem3;
    ap_uint<8> tem4;
    ap_uint<8> tem5;
    ap_uint<8> tem6;
    ap_uint<8> tem7;
    ap_uint<8> tem8;
    ap_uint<8> tem9;
    ap_uint<8> tem10;
    ap_uint<8> tem11;
    ap_uint<8> tem12;
    ap_uint<8> tem13;
    ap_uint<8> tem14;
    ap_uint<8> tem15;

    if(offset==0){
        tem0=input;
    }else{
        tem0=output[output_index].range(7,0);
    }

    if(offset==1){
        tem1=input;
    }else{
        tem1=output[output_index].range(15,8);
    }

    if(offset == 2) {
        tem2 = input;
    } else {
        tem2 = output[output_index].range(23, 16);
    }

    if(offset == 3) {
        tem3 = input;
    } else {
        tem3 = output[output_index].range(31, 24);
    }

    if(offset == 4) {
        tem4 = input;
    } else {
        tem4 = output[output_index].range(39, 32);
    }

    if(offset == 5) {
        tem5 = input;
    } else {
        tem5 = output[output_index].range(47, 40);
    }

    if(offset == 6) {
        tem6 = input;
    } else {
        tem6 = output[output_index].range(55, 48);
    }

    if(offset == 7) {
        tem7 = input;
    } else {
        tem7 = output[output_index].range(63, 56);
    }

    if(offset == 8) {
        tem8 = input;
    } else {
        tem8 = output[output_index].range(71, 64);
    }

    if(offset == 9) {
        tem9 = input;
    } else {
        tem9 = output[output_index].range(79, 72);
    }

    if(offset == 10) {
        tem10 = input;
    } else {
        tem10 = output[output_index].range(87, 80);
    }

    if(offset == 11) {
        tem11 = input;
    } else {
        tem11 = output[output_index].range(95, 88);
    }

    if(offset == 12) {
        tem12 = input;
    } else {
        tem12 = output[output_index].range(103, 96);
    }

    if(offset == 13) {
        tem13 = input;
    } else {
        tem13 = output[output_index].range(111, 104);
    }

    if(offset == 14) {
        tem14 = input;
    } else {
        tem14 = output[output_index].range(119, 112);
    }

    if(offset == 15) {
        tem15 = input;
    } else {
        tem15 = output[output_index].range(127, 120);
    }

    output[output_index].range(7,0) = tem0;
    output[output_index].range(15,8) = tem1;
    output[output_index].range(23,16) = tem2;
    output[output_index].range(31,24) = tem3;

    output[output_index].range(39,32) = tem4;
    output[output_index].range(47,40) = tem5;
    output[output_index].range(55,48) = tem6;
    output[output_index].range(63,56) = tem7;

    output[output_index].range(71,64) = tem8;
    output[output_index].range(79,72) = tem9;
    output[output_index].range(87,80) = tem10;
    output[output_index].range(95,88) = tem11;

    output[output_index].range(103,96) = tem12;
    output[output_index].range(111,104) = tem13;
    output[output_index].range(119,112) = tem14;
    output[output_index].range(127,120) = tem15;
}

void ap40to128_encoder(ap_uint<40> input, ap_uint<128>* output, ap_uint<16> index)
{
    ap_uint<4> offset=index.range(3,0);
    ap_uint<12> output_index=index.range(15,4);

    switch(offset)
    {
    case 0:
        output[output_index].range(39,0) = input;
        break;
    case 1:
        output[output_index].range(47,8) = input;
        break;
    case 2:
        output[output_index].range(55,16) = input;
        break;
    case 3:
        output[output_index].range(63,24) = input;
        break;
    case 4:
        output[output_index].range(71,32) = input;
        break;
    case 5:
        output[output_index].range(79,40) = input;
        break;
    case 6:
        output[output_index].range(87,48) = input;
        break;
    case 7:
        output[output_index].range(95,56) = input;
        break;
    case 8:
        output[output_index].range(103,64) = input;
        break;
    case 9:
        output[output_index].range(111,72) = input;
        break;
    case 10:
        output[output_index].range(119,80) = input;
        break;
    case 11:
        output[output_index].range(127,88) = input;
        break;
    case 12:
        output[output_index].range(127,96) = input;
        output[output_index+1].range(7,0) = input.range(39,32);
        break;
    case 13:
        output[output_index].range(127,104) = input.range(23,0);
        output[output_index+1].range(15,0) = input.range(39,24);
        break;
    case 14:
        output[output_index].range(127,112) = input.range(15,0);
        output[output_index+1].range(23,0) = input.range(39,16);
        break;
    case 15:
        output[output_index].range(127,120) = input.range(7,0);
        output[output_index+1].range(31,0) = input.range(39,8);
        break;
    }
}

void ap32to128_encoder(ap_uint<32> input, ap_uint<128>* output, ap_uint<16> index)
{
    ap_uint<4> offset=index.range(3,0);
    ap_uint<12> output_index=index.range(15,4);

    switch(offset)
    {
    case 0:
        output[output_index].range(31,0) = input;
        break;
    case 1:
        output[output_index].range(39,8) = input;
        break;
    case 2:
        output[output_index].range(47,16) = input;
        break;
    case 3:
        output[output_index].range(55,24) = input;
        break;
    case 4:
        output[output_index].range(63,32) = input;
        break;
    case 5:
        output[output_index].range(71,40) = input;
        break;
    case 6:
        output[output_index].range(79,48) = input;
        break;
    case 7:
        output[output_index].range(87,56) = input;
        break;
    case 8:
        output[output_index].range(95,64) = input;
        break;
    case 9:
        output[output_index].range(103,72) = input;
        break;
    case 10:
        output[output_index].range(111,80) = input;
        break;
    case 11:
        output[output_index].range(119,88) = input;
        break;
    case 12:
        output[output_index].range(127,96) = input;
        break;
    case 13:
        output[output_index].range(127,104) = input.range(23,0);
        output[output_index+1].range(7,0) = input.range(31,24);
        break;
    case 14:
        output[output_index].range(127,112) = input.range(15,0);
        output[output_index+1].range(15,0) = input.range(31,16);
        break;
    case 15:
        output[output_index].range(127,120) = input.range(7,0);
        output[output_index+1].range(23,0) = input.range(31,8);
        break;
    }
}

void ap32to128_encoder_index_block(ap_uint<32> input, ap_uint<128>* output, ap_uint<32> index)
{
    ap_uint<4> offset=index.range(3,0);
    ap_uint<28> output_index=index.range(31,4);

    switch(offset)
    {
    case 0:
        output[output_index].range(31,0) = input;
        break;
    case 1:
        output[output_index].range(39,8) = input;
        break;
    case 2:
        output[output_index].range(47,16) = input;
        break;
    case 3:
        output[output_index].range(55,24) = input;
        break;
    case 4:
        output[output_index].range(63,32) = input;
        break;
    case 5:
        output[output_index].range(71,40) = input;
        break;
    case 6:
        output[output_index].range(79,48) = input;
        break;
    case 7:
        output[output_index].range(87,56) = input;
        break;
    case 8:
        output[output_index].range(95,64) = input;
        break;
    case 9:
        output[output_index].range(103,72) = input;
        break;
    case 10:
        output[output_index].range(111,80) = input;
        break;
    case 11:
        output[output_index].range(119,88) = input;
        break;
    case 12:
        output[output_index].range(127,96) = input;
        break;
    case 13:
        output[output_index].range(127,104) = input.range(23,0);
        output[output_index+1].range(7,0) = input.range(31,24);
        break;
    case 14:
        output[output_index].range(127,112) = input.range(15,0);
        output[output_index+1].range(15,0) = input.range(31,16);
        break;
    case 15:
        output[output_index].range(127,120) = input.range(7,0);
        output[output_index+1].range(23,0) = input.range(31,8);
        break;
    }
}

void ap16to128_encoder_index_block(ap_uint<16> input, ap_uint<128>* output, ap_uint<32> index)
{
    ap_uint<4> offset=index.range(3,0);
    ap_uint<28> output_index=index.range(31,4);

    switch(offset)
    {
    case 0:
        output[output_index].range(15,0) = input;
        break;
    case 1:
        output[output_index].range(23,8) = input;
        break;
    case 2:
        output[output_index].range(31,16) = input;
        break;
    case 3:
        output[output_index].range(39,24) = input;
        break;
    case 4:
        output[output_index].range(47,32) = input;
        break;
    case 5:
        output[output_index].range(55,40) = input;
        break;
    case 6:
        output[output_index].range(63,48) = input;
        break;
    case 7:
        output[output_index].range(71,56) = input;
        break;
    case 8:
        output[output_index].range(79,64) = input;
        break;
    case 9:
        output[output_index].range(87,72) = input;
        break;
    case 10:
        output[output_index].range(95,80) = input;
        break;
    case 11:
        output[output_index].range(103,88) = input;
        break;
    case 12:
        output[output_index].range(111,96) = input;
        break;
    case 13:
        output[output_index].range(119,104) = input;
        break;
    case 14:
        output[output_index].range(127,112) = input;
        break;
    case 15:
        output[output_index].range(127,120) = input.range(7,0);
        output[output_index+1].range(7,0) = input.range(15,8);
        break;
    }
}

void putRestartPoint_encoder_for_datablock(ap_uint<32>* restart_point, ap_uint<8> restart_point_num, ap_uint<128> *sst_pointer, ap_uint<16> index)
{
	// put restart point
	// restart point array
    encoder_putRestartPoint_for_datablock_loop:
	for (ap_uint<8> i = 0; i < restart_point_num; i++)
	{
#pragma HLS LOOP_TRIPCOUNT max = 16 avg = 16 min = 1
		ap32to128_encoder(restart_point[i], sst_pointer, index + 4 * i);
	}
	// restart point num
	ap32to128_encoder(restart_point_num, sst_pointer, index + 4 * restart_point_num);
}

void putRestartPoint_encoder_for_indexblock(ap_uint<32>* restart_point, ap_uint<16> restart_point_num, ap_uint<128> *sst_pointer, ap_uint<32> index)
{
	// put restart point
	// restart point array
    encoder_putRestartPoint_for_indexblock_loop:
	for (ap_uint<16> i = 0; i < restart_point_num; i++)
	{
		ap32to128_encoder_index_block(restart_point[i], sst_pointer, index + 4 * i);
	}
	// restart point num
	ap32to128_encoder_index_block(restart_point_num, sst_pointer, index + 4 * restart_point_num);
}

void ap128to128_encoder(ap_uint<128>* input, ap_uint<128>* output, ap_uint<40> sst_index, ap_uint<16> datablock_size)
{
    ap_uint<12> copy_times=datablock_size.range(15,4);
    ap_uint<36> output_start_index=sst_index.range(39,4);

    encoder_ap128to128_loop:
    for(ap_uint<13> i=0; i<copy_times; i++)
    {
        output[output_start_index+i]=input[i];
    }
}

ap_uint<8> gen_shared_key_length(const keystring &pre_key, const keystring &now_key)
{
    ap_uint<8> prefix_len = 0;

    encoder_gen_shared_key_length_loop:
    for (ap_uint<6> i = 0; i < now_key.length / KEY_WIDTH; i++) {
#pragma HLS UNROLL
        for (ap_uint<8> j = 0; j < KEY_WIDTH; j++) {
            ap_uint<8> byte1 = pre_key.c[i].range((j+1)*8-1, j*8);
            ap_uint<8> byte2 = now_key.c[i].range((j+1)*8-1, j*8);

            if (byte1 == byte2) {
                prefix_len++;
            } else {
                return prefix_len;
            }
        }
    }
    return prefix_len;
}

ap_uint<8> gen_shared_key_length_index_block(ap_uint<64> pre_key, ap_uint<64> now_key)
{
    ap_uint<8> prefix_len = 0;

    encoder_gen_shared_key_length_for_index_block_loop:
    for (int i = 0; i < 8; i++) {
        #pragma HLS unroll
        ap_uint<8> byte1 = pre_key.range((i+1)*8-1, i*8);
        ap_uint<8> byte2 = now_key.range((i+1)*8-1, i*8);

        if (byte1 == byte2) {
            prefix_len++;
        } else {
            break;
        }
    }
    return prefix_len;
}

ap_uint<16> valuelength_to_16(ap_uint<VALUE_LENGTH_BITWIDTH> value_length, ap_uint<4> &index_increase)
{
    ap_uint<16> result;
#if VALUE_LENGTH_BITWIDTH <= 8-1
    result = value_length;
    result.range(7,7) = 0;
    index_increase += 1;
#elif VALUE_LENGTH_BITWIDTH == 8
    if(value_length.range(7,7))
    {
        result.range(6,0)=value_length.range(6,0);
        result.range(8,7)=0b11;
        result.range(15,9)=0;
        index_increase += 2;
    }
    else
    {
        result.range(7,0)=value_length.range(7,0);
        result.range(15,8)=0;
        index_increase += 1;
    }
#elif VALUE_LENGTH_BITWIDTH <= 16-2
    if (value_length.range(VALUE_LENGTH_BITWIDTH-1,7) != 0)
    {
        result.range(6,0) = value_length.range(6,0);
        result.range(7,7) = 0b1;
        result.range(VALUE_LENGTH_BITWIDTH,8) = value_length.range(VALUE_LENGTH_BITWIDTH-1,7);
        result.range(15,VALUE_LENGTH_BITWIDTH+1) = 0;
        index_increase += 2;
    }
    else
    {
        result.range(7,0)=value_length.range(7,0);
        result.range(15,8)=0;
        index_increase += 1;
    }
#else
    printf("VALUE_LENGTH_BITWIDTH is too large\n");
    result = 0b0111111111111111;
    index_increase += 2;
#endif
    return result;
}

ap_uint<128> leftmove_key_putkv(ap_uint<128> input, ap_uint<8> offset)
{
    ap_uint<128> target;
    switch (offset)
    {
    case 0:
        target.range(127, 0) = input.range(127, 0);
        break;
    case 1:
        target.range(119, 0) = input.range(127, 8);
        target.range(127, 120) = 0;
        break;
    case 2:
        target.range(111, 0) = input.range(127, 16);
        target.range(127, 112) = 0;
        break;
    case 3:
        target.range(103, 0) = input.range(127, 24);
        target.range(127, 104) = 0;
        break;
    case 4:
        target.range(95, 0) = input.range(127, 32);
        target.range(127, 96) = 0;
        break;
    case 5:
        target.range(87, 0) = input.range(127, 40);
        target.range(127, 88) = 0;
        break;
    case 6:
        target.range(79, 0) = input.range(127, 48);
        target.range(127, 80) = 0;
        break;
    case 7:
        target.range(71, 0) = input.range(127, 56);
        target.range(127, 72) = 0;
        break;
    case 8:
        target.range(63, 0) = input.range(127, 64);
        target.range(127, 64) = 0;
        break;
    case 9:
        target.range(55, 0) = input.range(127, 72);
        target.range(127, 56) = 0;
        break;
    case 10:
        target.range(47, 0) = input.range(127, 80);
        target.range(127, 48) = 0;
        break;
    case 11:
        target.range(39, 0) = input.range(127, 88);
        target.range(127, 40) = 0;
        break;
    case 12:
        target.range(31, 0) = input.range(127, 96);
        target.range(127, 32) = 0;
        break;
    case 13:
        target.range(23, 0) = input.range(127, 104);
        target.range(127, 24) = 0;
        break;
    case 14:
        target.range(15, 0) = input.range(127, 112);
        target.range(127, 16) = 0;
        break;
    case 15:
        target.range(7, 0) = input.range(127, 120);
        target.range(127, 8) = 0;
        break;
    default:
        target = 0;
    }
    return target;
}

// void putkey_data(keystring key, ap_uint<8> shared, ap_uint<128> *output, ap_uint<16> output_index)
// {
//     ap_uint<8> byte;
//     // 从shared字节开始，写到key.length字节
//     for (ap_uint<8> i = shared; i < key.length; i++)
//     {
// #pragma HLS UNROLL factor=8  // UNROLL成一次处理8字节
//         byte = key.c[i >> KEY_WIDTH_BITS].range((i & KEY_WIDTH_MASK) * 8 + 7, (i & KEY_WIDTH_MASK) * 8);

//         output[output_index >> 4].range((output_index & 0xF) * 8 + 7, (output_index & 0xF) * 8) = byte;
//     }
// }

void putkey_data(const keystring &key, ap_uint<8> shared, ap_uint<128> *output, ap_uint<16> index)
{
    ap_uint<KEY_WIDTH_BITS> left_offset = shared.range(KEY_WIDTH_BITS-1, 0);
    ap_uint<KEY_LENGTH_BITWIDTH-KEY_WIDTH_BITS> key_index = shared.range(KEY_LENGTH_BITWIDTH-1, KEY_WIDTH_BITS);
    ap_uint<4> right_offset = index.range(3, 0);
    ap_uint<12> output_index = index.range(15, 4);
    // 将key左移shared.range(3,0)位，再右移output_index.range(3,0)位
    ap_int<6> move = left_offset - right_offset;
    switch(move)  // 第一步处理首部数据
    {  // 为负值时，表示整体右移
    case -15:  // 0-15=-15，可以确定两个offset
        output[output_index].range(127, 120) = key.c[key_index].range(7, 0);
        output[output_index+1].range(119, 0) = key.c[key_index].range(127, 8);
        output_index++;
        break;
    case -14:  // 从-14开始无法确定两个offset的值
        output[output_index].range(127, right_offset*8) = key.c[key_index].range(15, left_offset*8);
        output[output_index+1].range(111, 0) = key.c[key_index].range(127, 16);
        output_index++;
        break;
    case -13:
        output[output_index].range(127, right_offset*8) = key.c[key_index].range(23, left_offset*8);
        output[output_index+1].range(103, 0) = key.c[key_index].range(127, 24);
        output_index++;
        break;
    case -12:
        output[output_index].range(127, right_offset*8) = key.c[key_index].range(31, left_offset*8);
        output[output_index+1].range(95, 0) = key.c[key_index].range(127, 32);
        output_index++;
        break;
    case -11:
        output[output_index].range(127, right_offset*8) = key.c[key_index].range(39, left_offset*8);
        output[output_index+1].range(87, 0) = key.c[key_index].range(127, 40);
        output_index++;
        break;
    case -10:
        output[output_index].range(127, right_offset*8) = key.c[key_index].range(47, left_offset*8);
        output[output_index+1].range(79, 0) = key.c[key_index].range(127, 48);
        output_index++;
        break;
    case -9:
        output[output_index].range(127, right_offset*8) = key.c[key_index].range(55, left_offset*8);
        output[output_index+1].range(71, 0) = key.c[key_index].range(127, 56);
        output_index++;
        break;
    case -8:
        output[output_index].range(127, right_offset*8) = key.c[key_index].range(63, left_offset*8);
        output[output_index+1].range(63, 0) = key.c[key_index].range(127, 64);
        output_index++;
        break;
    case -7:
        output[output_index].range(127, right_offset*8) = key.c[key_index].range(71, left_offset*8);
        output[output_index+1].range(55, 0) = key.c[key_index].range(127, 72);
        output_index++;
        break;
    case -6:
        output[output_index].range(127, right_offset*8) = key.c[key_index].range(79, left_offset*8);
        output[output_index+1].range(47, 0) = key.c[key_index].range(127, 80);
        output_index++;
        break;
    case -5:
        output[output_index].range(127, right_offset*8) = key.c[key_index].range(87, left_offset*8);
        output[output_index+1].range(39, 0) = key.c[key_index].range(127, 88);
        output_index++;
        break;
    case -4:
        output[output_index].range(127, right_offset*8) = key.c[key_index].range(95, left_offset*8);
        output[output_index+1].range(31, 0) = key.c[key_index].range(127, 96);
        output_index++;
        break;
    case -3:
        output[output_index].range(127, right_offset*8) = key.c[key_index].range(103, left_offset*8);
        output[output_index+1].range(23, 0) = key.c[key_index].range(127, 104);
        output_index++;
        break;
    case -2:
        output[output_index].range(127, right_offset*8) = key.c[key_index].range(111, left_offset*8);
        output[output_index+1].range(15, 0) = key.c[key_index].range(127, 112);
        output_index++;
        break;
    case -1:
        output[output_index].range(127, right_offset*8) = key.c[key_index].range(119, left_offset*8);
        output[output_index+1].range(7, 0) = key.c[key_index].range(127, 120);
        output_index++;
        break;
    // 无需移动，直接拷贝
    case 0:
        output[output_index].range(127, right_offset*8) = key.c[key_index].range(127, left_offset*8);
        output_index++;
        break;
    // 为正值时，表示整体左移，只有左移不需要++output_index
    case 1:
        output[output_index].range(119, right_offset*8) = key.c[key_index].range(127, left_offset*8);
        break;
    case 2:
        output[output_index].range(111, right_offset*8) = key.c[key_index].range(127, left_offset*8);
        break;
    case 3:
        output[output_index].range(103, right_offset*8) = key.c[key_index].range(127, left_offset*8);
        break;
    case 4:
        output[output_index].range(95, right_offset*8) = key.c[key_index].range(127, left_offset*8);
        break;
    case 5:
        output[output_index].range(87, right_offset*8) = key.c[key_index].range(127, left_offset*8);
        break;
    case 6:
        output[output_index].range(79, right_offset*8) = key.c[key_index].range(127, left_offset*8);
        break;
    case 7:
        output[output_index].range(71, right_offset*8) = key.c[key_index].range(127, left_offset*8);
        break;
    case 8:
        output[output_index].range(63, right_offset*8) = key.c[key_index].range(127, left_offset*8);
        break;
    case 9:
        output[output_index].range(55, right_offset*8) = key.c[key_index].range(127, left_offset*8);
        break;
    case 10:
        output[output_index].range(47, right_offset*8) = key.c[key_index].range(127, left_offset*8);
        break;
    case 11:
        output[output_index].range(39, right_offset*8) = key.c[key_index].range(127, left_offset*8);
        break;
    case 12:
        output[output_index].range(31, right_offset*8) = key.c[key_index].range(127, left_offset*8);
        break;
    case 13:
        output[output_index].range(23, right_offset*8) = key.c[key_index].range(127, left_offset*8);
        break;
    case 14:
        output[output_index].range(15, right_offset*8) = key.c[key_index].range(127, left_offset*8);
        break;
    case 15:  // 15-0=15，可以确定两个offset
        output[output_index].range(7, 0) = key.c[key_index].range(127, 120);
        break;
    default:
        break;
    }

    // 之后就是平移key.c[key_index]的内容
    for (key_index++; key_index < KEY_ARRAY_LENGTH; key_index++, output_index++)
    {
        switch (move)
        {
        case -15:
            output[output_index].range(127, 120) = key.c[key_index].range(7, 0);
            output[output_index+1].range(119, 0) = key.c[key_index].range(127, 8);
            break;
        case -14:
            output[output_index].range(127, 112) = key.c[key_index].range(15, 0);
            output[output_index+1].range(111, 0) = key.c[key_index].range(127, 16);
            break;
        case -13:
            output[output_index].range(127, 104) = key.c[key_index].range(23, 0);
            output[output_index+1].range(103, 0) = key.c[key_index].range(127, 24);
            break;
        case -12:
            output[output_index].range(127, 96) = key.c[key_index].range(31, 0);
            output[output_index+1].range(95, 0) = key.c[key_index].range(127, 32);
            break;
        case -11:
            output[output_index].range(127, 88) = key.c[key_index].range(39, 0);
            output[output_index+1].range(87, 0) = key.c[key_index].range(127, 40);
            break;
        case -10:
            output[output_index].range(127, 80) = key.c[key_index].range(47, 0);
            output[output_index+1].range(79, 0) = key.c[key_index].range(127, 48);
            break;
        case -9:
            output[output_index].range(127, 72) = key.c[key_index].range(55, 0);
            output[output_index+1].range(71, 0) = key.c[key_index].range(127, 56);
            break;
        case -8:
            output[output_index].range(127, 64) = key.c[key_index].range(63, 0);
            output[output_index+1].range(63, 0) = key.c[key_index].range(127, 64);
            break;
        case -7:
            output[output_index].range(127, 56) = key.c[key_index].range(71, 0);
            output[output_index+1].range(55, 0) = key.c[key_index].range(127, 72);
            break;
        case -6:
            output[output_index].range(127, 48) = key.c[key_index].range(79, 0);
            output[output_index+1].range(47, 0) = key.c[key_index].range(127, 80);
            break;
        case -5:
            output[output_index].range(127, 40) = key.c[key_index].range(87, 0);
            output[output_index+1].range(39, 0) = key.c[key_index].range(127, 88);
            break;
        case -4:
            output[output_index].range(127, 32) = key.c[key_index].range(95, 0);
            output[output_index+1].range(31, 0) = key.c[key_index].range(127, 96);
            break;
        case -3:
            output[output_index].range(127, 24) = key.c[key_index].range(103, 0);
            output[output_index+1].range(23, 0) = key.c[key_index].range(127, 104);
            break;
        case -2:
            output[output_index].range(127, 16) = key.c[key_index].range(111, 0);
            output[output_index+1].range(15, 0) = key.c[key_index].range(127, 112);
            break;
        case -1:
            output[output_index].range(127, 8) = key.c[key_index].range(119, 0);
            output[output_index+1].range(7, 0) = key.c[key_index].range(127, 120);
            break;
        case 0:
            output[output_index].range(127, 0) = key.c[key_index].range(127, 0);
            break;
        case 1:
            output[output_index].range(127, 120) = key.c[key_index].range(7, 0);
            output[output_index+1].range(119, 0) = key.c[key_index].range(127, 8);
            break;
        case 2:
            output[output_index].range(127, 112) = key.c[key_index].range(15, 0);
            output[output_index+1].range(111, 0) = key.c[key_index].range(127, 16);
            break;
        case 3:
            output[output_index].range(127, 104) = key.c[key_index].range(23, 0);
            output[output_index+1].range(103, 0) = key.c[key_index].range(127, 24);
            break;
        case 4:
            output[output_index].range(127, 96) = key.c[key_index].range(31, 0);
            output[output_index+1].range(95, 0) = key.c[key_index].range(127, 32);
            break;
        case 5:
            output[output_index].range(127, 88) = key.c[key_index].range(39, 0);
            output[output_index+1].range(87, 0) = key.c[key_index].range(127, 40);
            break;
        case 6:
            output[output_index].range(127, 80) = key.c[key_index].range(47, 0);
            output[output_index+1].range(79, 0) = key.c[key_index].range(127, 48);
            break;
        case 7:
            output[output_index].range(127, 72) = key.c[key_index].range(55, 0);
            output[output_index+1].range(71, 0) = key.c[key_index].range(127, 56);
            break;
        case 8:
            output[output_index].range(127, 64) = key.c[key_index].range(63, 0);
            output[output_index+1].range(63, 0) = key.c[key_index].range(127, 64);
            break;
        case 9:
            output[output_index].range(127, 56) = key.c[key_index].range(71, 0);
            output[output_index+1].range(55, 0) = key.c[key_index].range(127, 72);
            break;
        case 10:
            output[output_index].range(127, 48) = key.c[key_index].range(79, 0);
            output[output_index+1].range(47, 0) = key.c[key_index].range(127, 80);
            break;
        case 11:
            output[output_index].range(127, 40) = key.c[key_index].range(87, 0);
            output[output_index+1].range(39, 0) = key.c[key_index].range(127, 88);
            break;
        case 12:
            output[output_index].range(127, 32) = key.c[key_index].range(95, 0);
            output[output_index+1].range(31, 0) = key.c[key_index].range(127, 96);
            break;
        case 13:
            output[output_index].range(127, 24) = key.c[key_index].range(103, 0);
            output[output_index+1].range(23, 0) = key.c[key_index].range(127, 104);
            break;
        case 14:
            output[output_index].range(127, 16) = key.c[key_index].range(111, 0);
            output[output_index+1].range(15, 0) = key.c[key_index].range(127, 112);
            break;
        case 15:
            output[output_index].range(127, 8) = key.c[key_index].range(119, 0);
            output[output_index+1].range(7, 0) = key.c[key_index].range(127, 120);
            break;
        default:
            break;
        }
    }

    // if (left_offset < right_offset) // 整体右移，首部分为2部分
    // {  // 假设应左移10字节，右移12字节，则整体右移2字节，(111,80)->(127,96) (127,112)->下一个的(15,0)
    //     ap_uint<4> move = right_offset - left_offset;
    //     output[output_index].range(127, right_offset*8) = key.c[key_index].range(KEY_BITWIDTH-move*8-1, left_offset*8);
    //     output[output_index+1].range(move*8-1, 0) = key.c[key_index].range(KEY_BITWIDTH-1, KEY_BITWIDTH-move*8);
    //     // 之后就是平移key.c[key_index]的内容为两部分 (111,0)->(127,16) (127,112)->下一个的(15,0)
    //     for (key_index++, output_index++; key_index < KEY_ARRAY_LENGTH; key_index++, output_index++)
    //     {
    //         output[output_index].range(127, move*8) = key.c[key_index].range(KEY_BITWIDTH-move*8-1, 0);
    //         output[output_index+1].range(move*8-1, 0) = key.c[key_index].range(KEY_BITWIDTH-1, KEY_BITWIDTH-move*8);
    //     }
    // }
    // else if (left_offset > right_offset)  // 整体左移，首部仍为一部分
    // {  // 假设应左移12字节，右移10字节，则整体左移2字节，(127,96)->(111,80)
    //     ap_uint<4> move = left_offset - right_offset;
    //     output[output_index].range(127-move*8, right_offset*8) = key.c[key_index].range(KEY_BITWIDTH-1, left_offset*8);
    //     // 之后就是平移key.c[key_index]的内容为两部分，(15,0)->(127,112) (127,16)->下一个的(111,0)
    //     for (key_index++; key_index < KEY_ARRAY_LENGTH; key_index++, output_index++)
    //     {
    //         output[output_index].range(127, 128-move*8) = key.c[key_index].range(move*8-1, 0);
    //         output[output_index+1].range(127-move*8, 0) = key.c[key_index].range(KEY_BITWIDTH-1, move*8);
    //     }
    // }
    // else  // 无需移动，直接拷贝
    // {
    //     output[output_index].range(127, right_offset*8) = key.c[key_index].range(KEY_BITWIDTH-1, left_offset*8);
    //     for (key_index++, output_index++; key_index < KEY_ARRAY_LENGTH; key_index++, output_index++)
    //     {
    //         output[output_index] = key.c[key_index];
    //     }
    // }
    
//     // TODO: 低效率，待优化
//     ap_uint<8> byte;
//     for (ap_uint<8> i = shared; i < key.length; i++, index++)  // 从shared字节开始，写到key.length字节
//     {
// //#pragma HLS UNROLL factor=16  // UNROLL反而变慢？
//         byte = key.c[i >> KEY_WIDTH_BITS].range((i & KEY_WIDTH_MASK) * 8 + 7, (i & KEY_WIDTH_MASK) * 8);

//         output[index >> 4].range((index & 0xF) * 8 + 7, (index & 0xF) * 8) = byte;
//     }
}

void putkey_ap128to128_index(ap_uint<128> input, ap_uint<128> *output, ap_uint<32> output_index)
{
    ap_uint<4> offset=output_index.range(3,0);
    ap_uint<28> index=output_index.range(31,4);
    switch(offset)
    {
    case 0:
        output[index]=input;
        break;
    case 1:
        output[index].range(127,8)=input.range(119,0);
        output[index+1].range(7,0)=input.range(127,120);
        break;
    case 2:
        output[index].range(127,16)=input.range(111,0);
        output[index+1].range(15,0)=input.range(127,112);
        break;
    case 3:
        output[index].range(127,24)=input.range(103,0);
        output[index+1].range(23,0)=input.range(127,104);
        break;
    case 4:
        output[index].range(127,32)=input.range(95,0);
        output[index+1].range(31,0)=input.range(127,96);
        break;
    case 5:
        output[index].range(127,40)=input.range(87,0);
        output[index+1].range(39,0)=input.range(127,88);
        break;
    case 6:
        output[index].range(127,48)=input.range(79,0);
        output[index+1].range(47,0)=input.range(127,80);
        break;
    case 7:
        output[index].range(127,56)=input.range(71,0);
        output[index+1].range(55,0)=input.range(127,72);
        break;
    case 8:
        output[index].range(127,64)=input.range(63,0);
        output[index+1].range(63,0)=input.range(127,64);
        break;
    case 9:
        output[index].range(127,72)=input.range(55,0);
        output[index+1].range(71,0)=input.range(127,56);
        break;
    case 10:
        output[index].range(127,80)=input.range(47,0);
        output[index+1].range(79,0)=input.range(127,48);
        break;
    case 11:
        output[index].range(127,88)=input.range(39,0);
        output[index+1].range(87,0)=input.range(127,40);
        break;
    case 12:
        output[index].range(127,96)=input.range(31,0);
        output[index+1].range(95,0)=input.range(127,32);
        break;
    case 13:
        output[index].range(127,104)=input.range(23,0);
        output[index+1].range(103,0)=input.range(127,24);
        break;
    case 14:
        output[index].range(127,112)=input.range(15,0);
        output[index+1].range(111,0)=input.range(127,16);
        break;
    case 15:
        output[index].range(127,120)=input.range(7,0);
        output[index+1].range(119,0)=input.range(127,8);
        break;
    }
}

void putvalue_data(const valuestring &input, ap_uint<128> *output, ap_uint<16> output_index)
{
    ap_uint<4> offset=output_index.range(3,0);
    ap_uint<12> index=output_index.range(15,4);
    // warning: 8位宽度可以表示128*16=2048字节，现在定下value为1024字节，所以没问题
    ap_uint<8> copy_array_length = (input.length + VALUE_WIDTH-1) >> VALUE_WIDTH_BITS;
    if(offset==0)
    {
        for(ap_uint<8> i=0;i<copy_array_length;i++)
        {
            output[index+i]=input.c[i];
        }
    }
    else
    {
        ap_uint<8> max_offset=8*offset;
        output[index].range(127,max_offset)=input.c[0].range(127-max_offset, 0);
        encoder_putvalue_data_loop:
        for(ap_uint<8> i=1;i<copy_array_length;i++)
        {
            ap_uint<128> tem;
            tem.range(max_offset-1, 0)=input.c[i-1].range(127,128-max_offset);
            tem.range(127, max_offset)=input.c[i].range(127-max_offset, 0);
            output[index+i]=tem;
        }
        output[index + copy_array_length].range(max_offset-1, 0)=input.c[copy_array_length-1].range(127, 128-max_offset);
    }
}

void putKV(const keystring &pre_key, const fifo_key_meta &now_km, const valuestring &now_value, ap_uint<128> *sst_pointer, ap_uint<16> &datablock_index, ap_uint<16> &index)
{
    // assert(db_key_length == 16);
    // assert(db_value_length == 128);
    ap_uint<8> shared = 0;

    if (is_max_key(pre_key))
    {
        shared = 0;
    }
    else
    {
        shared = gen_shared_key_length(pre_key, now_km.key);
    }
    // std::cout << pre_key << std::endl;
    // std::cout << now_kv.key << std::endl;
    // printf("shared: %d\n", shared);

    ap_uint<KEY_LENGTH_BITWIDTH> unshared = now_km.key.length - shared;  // key length需要用7+1位表示
    ap_uint<VALUE_LENGTH_BITWIDTH> value_length = now_km.value_length;  // value length需要用10+1位表示
    // put key
    // shared | unshared | value | key | value
    // shared==0时占1字节，unshared=128使用varint，占2字节，共3字节
    // shared!=0时，各占1字节，共2字节
    // value_length>=128时都占2字节(一直到16383)
    // midres可能占4字节，也可能占5字节
    // danger: 目前假定最大key不可能超过128，那么只有key=128且shared=0的唯一情况下midres才会占5字节
#if KEY_LENGTH == 128  // danger: 如果增大key需要修改为>=和.range(23,8)，而减小key不需要
    if (shared == 0 && unshared == 128)
    {
        ap_uint<40> midres = 0;
        ap_uint<4> index_increase = 3;

        midres.range(7,0) = shared;
        midres.range(23,8) = 0x0180;
        midres.range(39,24) = valuelength_to_16(value_length, index_increase);
        ap40to128_encoder(midres, sst_pointer, index);
        index+=index_increase;
        datablock_index+=index_increase;
    }
    else
#endif
    {
        ap_uint<32> midres = 0;
        ap_uint<4> index_increase = 2;

        midres.range(7,0) = shared;
        midres.range(15,8) = unshared;
        midres.range(31,16) = valuelength_to_16(value_length, index_increase);
        ap32to128_encoder(midres, sst_pointer, index);
        index+=index_increase;
        datablock_index+=index_increase;
    }

    // ap_uint<128> tmp_key;
    // tmp_key = leftmove_key_putkv(now_kv.key, pos);
    putkey_data(now_km.key, shared, sst_pointer, index);
    index += unshared;

    // TODO: 目前好像默认所有情况value长度固定，所以没有用到value_length
    putvalue_data(now_value, sst_pointer, index);
    index += value_length;

    datablock_index+=unshared+value_length;
}

ap_uint<128> packtwo64(ap_uint<64> in1, ap_uint<64> in2, ap_uint<8> length)
{
    ap_uint<128> res;
    switch(length)
    {
    case 0:
        res.range(63,0)=in2;
        res.range(127,64)=0;
        break;
    case 1:
        res.range(7,0)=in1.range(7,0);
        res.range(71,8)=in2;
        res.range(127,72)=0;
        break;
    case 2:
        res.range(15,0)=in1.range(15,0);
        res.range(79,16)=in2;
        res.range(127,80)=0;
        break;
    case 3:
        res.range(23,0)=in1.range(23,0);
        res.range(87,24)=in2;
        res.range(127,88)=0;
        break;
    case 4:
        res.range(31,0)=in1.range(31,0);
        res.range(95,32)=in2;
        res.range(127,96)=0;
        break;
    case 5:
        res.range(39,0)=in1.range(39,0);
        res.range(103,40)=in2;
        res.range(127,104)=0;
        break;
    case 6:
        res.range(47,0)=in1.range(47,0);
        res.range(111,48)=in2;
        res.range(127,112)=0;
        break;
    case 7:
        res.range(55,0)=in1.range(55,0);
        res.range(119,56)=in2;
        res.range(127,120)=0;
        break;
    case 8:
        res.range(63,0)=in1.range(63,0);
        res.range(127,64)=in2;
        break;
    default:
        res=0;
    }
    return res;
}

void putKV_index(/*ap_uint<64> pre_key, */keystring key, ap_uint<64> value, ap_uint<5> value_length, ap_uint<128> *sst_pointer, ap_uint<32> &index)
{
    // assert(db_key_length == 16);
    // assert(db_value_length == 128);
    ap_uint<8> pos = 0;

    // pos = gen_shared_key_length_index_block(pre_key, key);
    // if (pre_key == MAX_USER_KEY)
    // {
    //     pos = 0;
    // }
    // ap_uint<8> unshared = key.length - SEQ_LENGTH - pos;
    // TODO: 这里db_bench为什么只用了低8字节？
    const ap_uint<8> unshared = 8;  // danger:写死使用user key的低8字节，和db_bench默认结果相同

    // put key
    // shared | unshared | key | value
    ap_uint<16> midres1;

    midres1.range(7,0) = pos;
    midres1.range(15,8) = unshared;

    ap16to128_encoder_index_block(midres1, sst_pointer, index);
    index+=2;

    // ap_uint<64> tmp_key = 0;

    // tmp_key=leftmove_key_putkv_index(key, pos);

    ap_uint<128> tem;
    tem=packtwo64(key.c[0].range(63,0), value, unshared);  // danger:写死了用8字节

    putkey_ap128to128_index(tem, sst_pointer, index);
    index += unshared + value_length;
}

ap_uint<24> size_to_varint(ap_uint<16> input, ap_uint<4> &length)
{
    ap_uint<24> res;
    res.range(6,0)=input.range(6,0);
    res.range(14,8)=input.range(13,7);
    res.range(17,16)=input.range(15,14);
    res.range(23,18)=0;

    if(input.range(15,14)==0)
    {
        if(input.range(13,7)==0)
        {
            length=1;
            res.range(7,7)=0;
            res.range(15,15)=0;
        }
        else
        {
            length=2;
            res.range(7,7)=1;
            res.range(15,15)=0;
        }
    }
    else
    {
        length=3;
        res.range(7,7)=1;
        res.range(15,15)=1;
    }
    return res;
}

ap_uint<40> offset_to_varint(ap_uint<35> input, ap_uint<4> &length)
{
    ap_uint<40> res;
    // warning: 目前确保sst大小不超过1G，所以只需要40位varint（即35位offset）
    res.range(6,0)=input.range(6,0);
    res.range(7,7)=1;
    res.range(14,8)=input.range(13,7);
    res.range(15,15)=1;
    res.range(22,16)=input.range(20,14);
    res.range(23,23)=1;
    res.range(30,24)=input.range(27,21);
    res.range(31,31)=1;
    res.range(38,32)=input.range(34,28);
    res.range(39,39)=0;


    // 只需要确定在哪里置0
    // danger: 这样只能保证低地址是所需的varint，高地址会存在1
    if(input.range(34,28))
    {
        length=5;
    }
    else
    {
        if(input.range(27,21))
        {
            length=4;
            res.range(31,31)=0;
        }
        else
        {
            if(input.range(20,14))
            {
                length=3;
                res.range(23,23)=0;
            }
            else
            {
                if(input.range(13,7))
                {
                    length=2;
                    res.range(15,15)=0;
                }
                else
                {
                    length=1;
                    res.range(7,7)=0;
                }
            }
        
        }
    }
    
    return res;
}

ap_uint<64> pack_two_varint_encoder(ap_uint<40> input1, ap_uint<24> input2, ap_uint<4> length1)
{
    ap_uint<64> out;

    if(length1==1)
    {
        out.range(7,0)=input1.range(7,0);
        out.range(31,8)=input2;
        out.range(63,32)=0;
    }
    else if(length1==2)
    {
        out.range(15,0)=input1.range(15,0);
        out.range(39,16)=input2;
        out.range(63,40)=0;
    }
    else if(length1==3)
    {
        out.range(23,0)=input1.range(23,0);
        out.range(47,24)=input2;
        out.range(63,48)=0;
    }
    else if(length1==4)
    {
        out.range(31,0)=input1.range(31,0);
        out.range(55,32)=input2;
        out.range(63,56)=0;
    }
    else if(length1==5)
    {
        out.range(39,0)=input1;
        out.range(63,40)=input2;
    }
    return out;
}

ap_uint<64> putvalue_offset_size_to_string(ap_uint<35> offset, ap_uint<16> size, ap_uint<5> &length)
{
    ap_uint<4> length1, length2;

    ap_uint<64> result;
    ap_uint<40> tem1=offset_to_varint(offset, length1);
    ap_uint<24> tem2=size_to_varint(size, length2);

    length=length1+length2;

    result=pack_two_varint_encoder(tem1, tem2, length1);
    return result;
}

void putBlockHandle(const keystring &key, /* ap_uint<64> pre_key, */ap_uint<16> &entry_size, 
                    ap_uint<35> value_offset, ap_uint<16> value_size,
                    ap_uint<32> &index_block_index, ap_uint<128> *index_block_buffer, ap_uint<32> index_block_offset,
                    ap_uint<32> *index_block_restart_point, ap_uint<16> &index_block_restart_point_num)
{
    // new index block
    ap_uint<64> value_str;
    ap_uint<5> value_length;

    // index block is unique

    if (entry_size % restart == 0)
    {
        entry_size = 1;
        index_block_restart_point[index_block_restart_point_num] = index_block_index-index_block_offset;
        index_block_restart_point_num++;
        // pre_key = MAX_USER_KEY;
    }

    value_str=putvalue_offset_size_to_string(value_offset, value_size, value_length);
    // std::cout<<"to string finish\n";

    ap_uint<32> index_tmp = index_block_index.range(31,0);

    putKV_index(/*pre_key, */key, value_str, value_length, index_block_buffer, index_block_index);

    // pre_key = key;
    entry_size++;
}


void encoder(hls::stream<fifo_key_meta> &input_km, hls::stream<fifo_value_slice> input_value[MAX_INPUT_FILE_NUM],
             hls::stream<ap_uint<2> > &merge_result, ap_uint<32> kv_sum,
             ap_uint<128>* sst_buffer, ap_uint<128>* index_block_result,
             uint64_t *output_data, ap_uint<40> file_limit
            )
{
    //Detect same key
    keystring last_user_key;
    last_user_key.set_empty();

    //Meta data transfer to host
    ap_uint<32> top_index_block_index[MAX_OUTPUT_FILE_NUM] = {0};
    ap_uint<35> top_sst_index[MAX_OUTPUT_FILE_NUM] = {0};

    for (int i = 0; i < MAX_OUTPUT_FILE_NUM; i++)
    {
        top_index_block_index[i] = 0;
        top_sst_index[i] = 0;
    }
    uint64_t pps_kernel[PPS_KERNEL_SIZE] = {0};

    int pps_offset=0;
    ap_uint<40> sst_offset=0;  // 至少3倍sst大小，开大一点

    // static ap_uint<128> data_block_buffer_bram[270];
    // TODO: 删掉bram
    ap_uint<128> data_block_buffer_bram[325];  // warning: 325<<4=5200，支持的最大data block大小
    keystring pre_key;
    set_max_key(pre_key);
    // keystring index_block_pre_user_key;
    // set_max_key(index_block_pre_user_key);


    //restart point area
    ap_uint<8> data_block_restart_point_num = 0;
    ap_uint<32> data_block_restart_point[restart_point_max] = {0};
    ap_uint<16> index_block_restart_point_num = 0;
    ap_uint<32> index_block_restart_point[restart_point_max*restart_point_max*4];
    ap_uint<8> entry_size = 0;
    ap_uint<16> index_block_entry_size = 0;

    uint64_t minSeqno = 72057594037927935UL;
    uint64_t maxSeqno = 0;

    //index area
    ap_uint<4> output_file_num=0;
    ap_uint<16> data_block_index=0;
    ap_uint<35> sst_index=0;  // 目前仅使用40位varint，即35位offset
    ap_uint<32> index_block_index=0;
    ap_uint<32> index_block_offset=0;
    ap_uint<16> bram_read_index=0;
    ap_uint<4> pre_page_change_remain;
    pre_page_change_remain = 0;
    ap_uint<32> i = 0;
    // int invalid_key_num = 0;

    chrono_print_time("encoder prepare end");
    encoder_top_loop:
    for(i=0; i<kv_sum; i++)
    {
        chrono_print_time("encoder loop start");
        if (i % 10000 == 0)
        {
            std::cout << "-----------" << i+1 << " / " << kv_sum << "------------" << std::endl;
        }

        // std::cout << "--------------------------------------------" << std::endl;
        // std::cout << "encoder read " << std::dec << i << ": ";
        fifo_key_meta now_km = input_km.read();
        // printf("received value_length: %d\n", now_km.value_length);
        ap_uint<2> now_decoder_num = merge_result.read();
        // std::cout << now_kv.key << std::endl;

        // KV_Transfer work
        // if (now_km.key.length == 0 || is_max_key(now_km.key))  // warning: 假设不会接收到max key
        // {
        //     last_user_key.set_empty();
        //     now_km.key.set_empty();
        // }
        // find a "deletion" key record
        if (last_user_key.length == now_km.key.length && same_user_key(last_user_key, now_km.key))
        {
            now_km.key.set_empty();
        }
        else
        {
            copy_user_key(now_km.key, last_user_key);
        }

        chrono_print_time("encoder kvTrans");

        if(now_km.key.length == 0)
        {
            // std::cout << "encoder reading invalid key" << std::endl;
            // invalid key
            // invalid_key_num++;
            // printf("invalid key num: %d\n", invalid_key_num);
            ap_uint<4> j;
            for (j = 0; j < VALUE_LENGTH / 512; j++)
            {
                // 一次读512字节
                input_value[now_decoder_num].read();
            }
#if VALUE_LENGTH % 512 != 0
            input_value[now_decoder_num].read();
#endif
            
        }
        else
        {
            //valid key
            valuestring now_value{.length = now_km.value_length};
            fifo_value_slice tem;
            // printf("received value: ");
            for (ap_uint<4> j = 0; j < VALUE_LENGTH / 512; j++)
            {
                // 一次读512字节
                tem = input_value[now_decoder_num].read();
                for (ap_uint<6> k = 0; k < 32; k++)
                {
            #pragma HLS UNROLL
                    now_value.c[j*32+k] = tem.c[k];
                    // for(int t = 0; t < 16; t++)
                    // {
                    //     printf("%02x", (uint32_t)tem.c[k].range((t+1)*8-1, t*8) & 0xFF);
                    // }
                }
            }
            // printf("\n");
            // now_value.print_value();
#if VALUE_LENGTH % 512 != 0
            tem = input_value[now_decoder_num].read();
            for (ap_uint<6> k = 0; k < (VALUE_LENGTH % 512) / 16; k++)
            {
        #pragma HLS UNROLL
                now_value.c[k] = tem.c[k];
            }
#endif
            
            //Now put kv to bram

            // new data block
            if (data_block_index == 0)
            {
                if (sst_index == 0)
                {
                    // pps_kernel[PPS_SMALLESTKEY_OFF+pps_offset] = now_kv.key.range(63, 0);
                    // pps_kernel[7+pps_offset] = now_kv.key.range(127, 64);
                    pps_kernel[PPS_SMALLESTKEY_LENGTH_OFF+pps_offset] = now_km.key.length;
                    for (ap_uint<8> i = 0; i < KEY_ARRAY_LENGTH; i++)
                    {
                #pragma HLS UNROLL
                        // danger: 目前KEY_BITWIDTH写死128位
                        pps_kernel[PPS_SMALLESTKEY_OFF+pps_offset+i*KEY_BITWIDTH/64] = now_km.key.c[i].range(63, 0);
                        pps_kernel[PPS_SMALLESTKEY_OFF+pps_offset+i*KEY_BITWIDTH/64+1] = now_km.key.c[i].range(127, 64);
                    }
                    // std::cout << "smallest key: " << std::endl;
                    // now_km.key.print_key();
                }
                // clear restart point array
                data_block_restart_point_num = 1;
                data_block_restart_point[0] = 0;
                entry_size = 1;
                set_max_key(pre_key);
            }
            else
            {
                // put kv pair
                // check if need restart
                if (entry_size % restart == 0)
                {
                    data_block_restart_point[data_block_restart_point_num] = data_block_index;
                    data_block_restart_point_num++;
                    set_max_key(pre_key);
                }
                entry_size++;
            }

            ap_uint<64> nowseqno;
            // nowseqno = getseqno(now_kv.key, now_kv.key_length);
            // danger: 目前KEY_BITWIDTH写死128位，SEQ_LENGTH写死64位
            nowseqno = now_km.key.c[KEY_ARRAY_LENGTH-1].range(KEY_BITWIDTH-1, KEY_BITWIDTH-SEQ_LENGTH*8);

            if (nowseqno < minSeqno){
                minSeqno = nowseqno;
            }
            if (nowseqno > maxSeqno){
                maxSeqno = nowseqno;
            }

            chrono_print_time("encoder new kv prepare");

            // now_kv.key.print_key();
            // now_value.print_value();
            putKV(pre_key, now_km, now_value, data_block_buffer_bram, data_block_index, bram_read_index);

            chrono_print_time("encoder put kv");

            pre_key = now_km.key;
            pps_kernel[PPS_ENTRIES_OFF+pps_offset]++;
            pps_kernel[PPS_RAWKEYSIZE_OFF+pps_offset] += now_km.key.length;
            pps_kernel[PPS_RAWVALUESIZE_OFF+pps_offset] += now_km.value_length;


            //check if data block is full or file is full
            // if (now_km.key.length + now_km.value_length + 80 + data_block_index >= 4096)
            if (data_block_index >= 4096)
            {
                // data_block_restart_point[data_block_restart_point_num] = data_block_index;
                // data_block_restart_point_num++;

                putRestartPoint_encoder_for_datablock(data_block_restart_point, data_block_restart_point_num, data_block_buffer_bram, bram_read_index);
                data_block_index += (data_block_restart_point_num + 1) * 4;
                bram_read_index += (data_block_restart_point_num + 1) * 4;

                ap_uint<35> data_block_handle_offset;
                ap_uint<16> data_block_handle_size;
                data_block_handle_offset = sst_index;
                data_block_handle_size = data_block_index;

                // to do
                // std::cout << "----------------------------------" << std::endl;
                // std::cout << "index block put block handle" << std::endl;
                putBlockHandle(pre_key, /* index_block_pre_key, */index_block_entry_size,
                                    data_block_handle_offset, data_block_handle_size,
                                    index_block_index, index_block_result, index_block_offset,
                                    index_block_restart_point, index_block_restart_point_num);

                // putHash(0, 0, data_block_buffer_bram, data_block_index);
                ap8to128_encoder(0, data_block_buffer_bram, bram_read_index);
                ap32to128_encoder(0, data_block_buffer_bram, bram_read_index+1);
                bram_read_index+=5;
                data_block_index+=5;

                // copy data block on bram to sst buffer on dram
                // copy_write_burst_io(sst_index, data_block_index, data_block_buffer_bram, sst_buffer);
                ap128to128_encoder(data_block_buffer_bram, sst_buffer, sst_index + sst_offset, data_block_index + pre_page_change_remain);

                //copy last page to first page
                data_block_buffer_bram[0] = data_block_buffer_bram[bram_read_index.range(15,4)];

                // create new data block
                // data_block_head = sst_pointer;
                sst_index += data_block_index;
                // std::cout << "sst_index: " << sst_index << std::endl;


                // modify properties
                // properties.num_data_blocks++;
                pps_kernel[PPS_DATABLOCK_NUM_OFF+pps_offset]++;
                // properties.data_size += data_block_index->front;
                pps_kernel[PPS_DATASIZE_OFF+pps_offset] += data_block_index;
                // reset data block index
                data_block_index = 0;
                bram_read_index.range(3,0) = sst_index.range(3,0);
                bram_read_index.range(15,4) = 0;

                pre_page_change_remain = sst_index.range(3,0);
                chrono_print_time("encoder new data block");
            }

            //New file
            if ((sst_index + 5200 > file_limit && output_file_num < MAX_OUTPUT_FILE_NUM) || i == kv_sum-1)
            // if (sst_index + 4096 > file_limit && output_file_num < MAX_OUTPUT_FILE_NUM)
            {
                std::cout << "output file num: " << output_file_num << std::endl;
                // 创建新文件时，无需考虑sst_buffer，因为主机以sst_index确定文件结束位置
                // 但需要考虑最后一个data_block可能尚未写入的重启点、数量和校验和，因为index block需要data_block的最终信息
                // TODO: 还是有bug，不是这里的问题？
                if (data_block_index != 0)
                {
                    // data_block_restart_point[data_block_restart_point_num] = data_block_index;
                    // data_block_restart_point_num++;

                    putRestartPoint_encoder_for_datablock(data_block_restart_point, data_block_restart_point_num, data_block_buffer_bram, bram_read_index);
                    data_block_index += (data_block_restart_point_num + 1) * 4;
                    bram_read_index += (data_block_restart_point_num + 1) * 4;

                    ap_uint<35> data_block_handle_offset;
                    ap_uint<16> data_block_handle_size;
                    data_block_handle_offset = sst_index;
                    data_block_handle_size = data_block_index;

                    putBlockHandle(pre_key, /* index_block_pre_key, */index_block_entry_size,
                                        data_block_handle_offset, data_block_handle_size,
                                        index_block_index, index_block_result, index_block_offset,
                                        index_block_restart_point, index_block_restart_point_num);

                    // putHash(0, 0, data_block_buffer_bram, data_block_index);
                    ap8to128_encoder(0, data_block_buffer_bram, bram_read_index);
                    ap32to128_encoder(0, data_block_buffer_bram, bram_read_index+1);
                    bram_read_index+=5;
                    data_block_index+=5;

                    // copy data block on bram to sst buffer on dram
                    // copy_write_burst_io(sst_index, data_block_index, data_block_buffer_bram, sst_buffer);
                    ap128to128_encoder(data_block_buffer_bram, sst_buffer, sst_index + sst_offset, data_block_index + pre_page_change_remain + 16);

                    //copy last page to first page
                    data_block_buffer_bram[0] = data_block_buffer_bram[bram_read_index.range(15,4)];

                    // create new data block
                    // data_block_head = sst_pointer;
                    sst_index += data_block_index;

                    // modify properties
                    // properties.num_data_blocks++;
                    pps_kernel[PPS_DATABLOCK_NUM_OFF+pps_offset]++;
                    // properties.data_size += data_block_index->front;
                    pps_kernel[PPS_DATASIZE_OFF+pps_offset] += data_block_index;
                    // reset data block index
                    data_block_index = 0;
                    // bram_read_index.range(3,0) = sst_index.range(3,0);
                    // bram_read_index.range(15,4) = 0;

                    // pre_page_change_remain = sst_index.range(3,0);
                }
                else
                {
                    // ap32to128_encoder_index_block(0x0103070F, index_block_result, index_block_index+1);
                    // ap32to128_encoder_index_block(0x1F3F7FFF, index_block_result, index_block_index+1);
                    // 最后的略过的128位还没写进去
                    ap128to128_encoder(data_block_buffer_bram, sst_buffer, sst_index + sst_offset, 16);
                    // ap128to128_encoder(data_block_buffer_bram, sst_buffer, sst_index + sst_offset, 128);
                }

                pps_kernel[PPS_INDEXBLOCK_OFFSET_OFF+ pps_offset] = sst_index;
                // pps_kernel[PPS_LARGESETKEY_OFF +pps_offset] = pre_key.range(63, 0);
                // pps_kernel[10+pps_offset] = pre_key.range(127, 64);
                pps_kernel[PPS_LARGESTKEY_LENGTH_OFF+pps_offset] = pre_key.length;
                for (ap_uint<8> i = 0; i < KEY_ARRAY_LENGTH; i++)
                {
            #pragma HLS UNROLL
                    // danger: 目前KEY_BITWIDTH写死128位
                    pps_kernel[PPS_LARGESTKEY_OFF+pps_offset+i*KEY_BITWIDTH/64] = pre_key.c[i].range(63, 0);
                    pps_kernel[PPS_LARGESTKEY_OFF+pps_offset+i*KEY_BITWIDTH/64+1] = pre_key.c[i].range(127, 64);
                }
                pps_kernel[PPS_MINSEQ_OFF+pps_offset] = minSeqno;
                pps_kernel[PPS_MAXSEQ_OFF+pps_offset] = maxSeqno;

                //put index block
                // std::cout << "index block put restart point" << std::endl;
                putRestartPoint_encoder_for_indexblock(index_block_restart_point, index_block_restart_point_num, index_block_result, index_block_index);
                index_block_index += (index_block_restart_point_num + 1) * sizeof(uint32_t);
                index_block_restart_point_num=1;
                index_block_restart_point[0]=0;

                ap8to128_encoder_index_block(0, index_block_result, index_block_index);
                ap32to128_encoder_index_block(0, index_block_result, index_block_index+1);
                index_block_index+=5;

                top_index_block_index[output_file_num]=index_block_index-index_block_offset;
                top_sst_index[output_file_num]=sst_index;
                printf("sst_index: %d\n", sst_index);

                //put index block
                // putRestartPoint_encoder_for_indexblock(index_block_restart_point, index_block_restart_point_num, index_block_result, index_block_index);

                index_block_offset+=(index_block_buffer_size / MAX_OUTPUT_FILE_NUM);
                pps_offset+=PPS_KERNEL_SINGEL_SIZE;
                output_file_num++;
                sst_index=0;
                // sst_offset=(file_limit*output_file_num / 16) * 16;
                sst_offset=file_limit*output_file_num;
                printf("sst_offset: %d\n", sst_offset);
                bram_read_index=0;
                pre_page_change_remain=0;
                index_block_index=index_block_offset;

                minSeqno = 72057594037927935UL;
                maxSeqno = 0;
                chrono_print_time("encoder new file");
            }

            
        }
    }
/*
Last block
*/

//     data_block_restart_point[data_block_restart_point_num] = data_block_index;
//     data_block_restart_point_num++;

//     putRestartPoint_encoder_for_datablock(data_block_restart_point, data_block_restart_point_num, data_block_buffer_bram, bram_read_index);

//     data_block_index += (data_block_restart_point_num + 1) * 4;
//     bram_read_index += (data_block_restart_point_num + 1) * 4;

//     ap_uint<35> data_block_handle_offset;
//     ap_uint<16> data_block_handle_size;
//     data_block_handle_offset = sst_index;
//     data_block_handle_size = data_block_index;

//     // to do
//     putBlockHandle(pre_key, /* index_block_pre_key, */index_block_entry_size,
//                         data_block_handle_offset, data_block_handle_size,
//                         index_block_index, index_block_result, index_block_offset,
//                         index_block_restart_point, index_block_restart_point_num);

//     // putHash(0, 0, data_block_buffer_bram, data_block_index);
//     ap8to128_encoder(0, data_block_buffer_bram, bram_read_index);
//     ap32to128_encoder(0, data_block_buffer_bram, bram_read_index+1);
//     bram_read_index+=5;
//     data_block_index+=5;

//     // copy data block on bram to sst buffer on dram
//     // copy_write_burst_io(sst_index, data_block_index, data_block_buffer_bram, sst_buffer);
//     // 前面把最后少于128位的空间延后拷贝，这里要全写进去
//     ap128to128_encoder(data_block_buffer_bram, sst_buffer, sst_index + sst_offset, data_block_index + pre_page_change_remain + 16);
//                                                             // + ((data_block_index + pre_page_change_remain).range(3,0) > 0 ? 16 : 0));

//     // copy last page to first page
//     // data_block_buffer_bram[0] = data_block_buffer_bram[bram_read_index.range(15,4)];

//     // create new data block
//     // data_block_head = sst_pointer;
//     sst_index += data_block_index;

//     // modify properties
//     // properties.num_data_blocks++;
//     pps_kernel[PPS_DATABLOCK_NUM_OFF+pps_offset]++;
//     // properties.data_size += data_block_index->front;
//     pps_kernel[PPS_DATASIZE_OFF+pps_offset] += data_block_index;
//     // reset data block index

//     // data_block_index = 0;
//     // bram_read_index.range(3,0) = sst_index.range(3,0);
//     // bram_read_index.range(15,4) = 0;

//     pps_kernel[PPS_INDEXBLOCK_OFFSET_OFF+ pps_offset] = sst_index;
//     // pps_kernel[9 +pps_offset] = pre_key.range(63, 0);
//     // pps_kernel[10+pps_offset] = pre_key.range(127, 64);
//     pps_kernel[PPS_LARGESTKEY_LENGTH_OFF+pps_offset] = pre_key.length;
//     for (ap_uint<8> i = 0; i < KEY_ARRAY_LENGTH; i++)
//     {
// #pragma HLS UNROLL
//         // danger: 目前KEY_BITWIDTH写死128位
//         pps_kernel[PPS_LARGESTKEY_OFF+pps_offset+i*KEY_BITWIDTH/64] = pre_key.c[i].range(63, 0);
//         pps_kernel[PPS_LARGESTKEY_OFF+pps_offset+i*KEY_BITWIDTH/64+1] = pre_key.c[i].range(127, 64);
//     }
//     pps_kernel[PPS_MINSEQ_OFF+pps_offset] = minSeqno;
//     pps_kernel[PPS_MAXSEQ_OFF+pps_offset] = maxSeqno;

//     //put index block
//     putRestartPoint_encoder_for_indexblock(index_block_restart_point, index_block_restart_point_num, index_block_result, index_block_index);
//     index_block_index += (index_block_restart_point_num + 1) * sizeof(uint32_t);

//     ap8to128_encoder_index_block(0, index_block_result, index_block_index);
//     ap32to128_encoder_index_block(0, index_block_result, index_block_index+1);
//     index_block_index+=5;

//     top_index_block_index[output_file_num]=index_block_index-index_block_offset;
//     top_sst_index[output_file_num]=sst_index;
//     output_file_num++;

    // 现在decoder会输出4个SIGNAL+kv_sum个kv+4个MAX_KEY
    // merge会忽略4个SIGNAL，输出kv_sum+4-3个kv，因此有一个MAX_KEY要进行清空
    input_km.read();
    merge_result.read();
//     ap_uint<4> j;
//     for (j = 0; j < VALUE_LENGTH / 512; j++)
//     {
//         // 一次读512字节
//         input_value.read();
//     }
// #if VALUE_LENGTH % 512 != 0
//     input_value.read();
// #endif

    //put index block
    // putRestartPoint_encoder_for_indexblock(index_block_restart_point, index_block_restart_point_num, index_block_result, index_block_index);


/*
Put meta_data
*/

    encoder_copy_pps_to_host_loop:
    for (i = 0; i < PPS_KERNEL_SIZE; ++i)
    {
        output_data[i] = pps_kernel[i];
    }
    encoder_copy_sstindex_to_host_loop:
    for (i = 0; i < MAX_OUTPUT_FILE_NUM; i++)
    {
#pragma HLS LOOP_TRIPCOUNT max=8 avg=4 min=2
        output_data[PPS_KERNEL_SIZE+i] = top_sst_index[i];
    }
    encoder_copy_index_block_index_to_host_loop:
    for (i = 0; i < MAX_OUTPUT_FILE_NUM; i++)
    {
#pragma HLS LOOP_TRIPCOUNT max=8 avg=4 min=2
        output_data[PPS_KERNEL_SIZE+MAX_OUTPUT_FILE_NUM+i] = top_index_block_index[i];
    }

    output_data[PPS_KERNEL_SIZE+MAX_OUTPUT_FILE_NUM*2] = output_file_num;
}

/*
==                                                                                                   ==
==                                                                                                   ==
=======================================================================================================
==                                                                                                   ==
==                                           Encoder end                                             ==
==                                                                                                   ==
=======================================================================================================
=======================================================================================================
*/


/**
// void index_block_fetch(ap_uint<512> *buf, ap_uint<256> index_block_buffer[1024], uint64_t index_handle_offset, uint64_t index_handle_size,
//                     //    int &pointer_buf,
//                        int &limit_point)
// {
//     if (index_handle_size == 0)
//     {
//         limit_point = 0;
//         return;
//     }

// indexblock_copy_loop:
//     for (int i = 0; i <= index_handle_size / 64; i++)
//     {
//         index_block_buffer[i].range(511 - index_handle_offset % 64 * 8, 0) = buf[i + index_handle_offset / 64].range(511, index_handle_offset % 64 * 8);
//         if (index_handle_offset % 64 > 0)
//             index_block_buffer[i].range(511, 512 - index_handle_offset % 64 * 8) = buf[i + index_handle_offset / 64 + 1].range(index_handle_offset % 64 * 8 - 1, 0);
//     }

//     uint32_t restart_point_num;
//     GetBytesPtr_int32_key_HLS(index_handle_size - 4, index_block_buffer, restart_point_num);

//     limit_point = 0 + index_handle_size - (1 + restart_point_num) * sizeof(uint32_t);
// }

*/

template <typename T>
inline void clear_fifo(hls::stream<T> &input) {
    clear_fifo_loop:
    while (!input.empty()) {
        std::cout << input.read() << std::endl;
    }
}

template <typename T>
inline void check_clear_fifo(hls::stream<T> &input, std::string name) {
    if (input.empty())
    {
        std::cout << name << " is empty" << std::endl;
    }
    else
    {
        std::cout << name << " is not empty" << std::endl;
        clear_fifo(input);
    }
}
    



/*
 *
 * Top Fuction
 *
 */
extern "C"
{

    void krnl_vadd(ap_uint<32> *sst_input0, ap_uint<32> *sst_input1, ap_uint<32> *sst_input2, ap_uint<32> *sst_input3,
                    ap_uint<64> host_data[15],
                    //int host_size[5], int host_offset[5], int host_input_kv_sum[1],
                    ap_uint<128> *sst_buffer, ap_uint<128> *index_block_result,
                    // uint64_t *sstlength, uint64_t *indexlength,
                    // uint64_t *pps
                    uint64_t *output_data
                    )
    {
#pragma HLS INTERFACE mode=m_axi bundle=gmem0 depth=4096 max_read_burst_length=16 max_widen_bitwidth=512 port=sst_input0
#pragma HLS INTERFACE mode=m_axi bundle=gmem1 depth=4096 max_read_burst_length=16 max_widen_bitwidth=512 port=sst_input1
#pragma HLS INTERFACE mode=m_axi bundle=gmem2 depth=4096 max_read_burst_length=16 max_widen_bitwidth=512 port=sst_input2
#pragma HLS INTERFACE mode=m_axi bundle=gmem3 depth=4096 max_read_burst_length=16 max_widen_bitwidth=512 port=sst_input3
#pragma HLS INTERFACE mode = m_axi max_widen_bitwidth = 512 max_write_burst_length = 16 port = sst_buffer depth = 4096
#pragma HLS INTERFACE mode = m_axi max_widen_bitwidth = 512 max_write_burst_length = 16 port = index_block_result depth = 4096
#pragma HLS INTERFACE mode = m_axi port = output_data depth = 64

#pragma HLS DATAFLOW

    	ap_uint<64> buf_offset[5];
        ap_uint<64> size[4];
        ap_uint<64> input_kv[4];
        ap_uint<64> input_kv_sum;
#pragma HLS array_partition variable=buf_offset type=complete dim=1
#pragma HLS array_partition variable=size type=complete dim=1

    	buf_offset[0]=host_data[0];
    	buf_offset[1]=host_data[1];
    	buf_offset[2]=host_data[2];
    	buf_offset[3]=host_data[3];
        buf_offset[4]=host_data[4];
        size[0]=host_data[5];
        size[1]=host_data[6];
        size[2]=host_data[7];
        size[3]=host_data[8];
        input_kv_sum=host_data[9];
        input_kv[0]=host_data[10];
        input_kv[1]=host_data[11];
        input_kv[2]=host_data[12];
        input_kv[3]=host_data[13];


        // 得改一下
//         hls_thread_local hls::stream<ap_uint<128> > key_stream[MAX_INPUT_FILE_NUM];
// #pragma HLS STREAM depth = 16 variable = key_stream[0]
// #pragma HLS STREAM depth = 16 variable = key_stream[1]
// #pragma HLS STREAM depth = 16 variable = key_stream[2]
// #pragma HLS STREAM depth = 16 variable = key_stream[3]
        hls_thread_local hls::stream<fifo_key_meta> decoder_km_stream[MAX_INPUT_FILE_NUM];
#pragma HLS STREAM depth = 16 variable = decoder_km_stream[0]
#pragma HLS STREAM depth = 16 variable = decoder_km_stream[1]
#pragma HLS STREAM depth = 16 variable = decoder_km_stream[2]
#pragma HLS STREAM depth = 16 variable = decoder_km_stream[3]
//         hls_thread_local hls::stream<fifo_value_slice> decoder_value_stream[MAX_INPUT_FILE_NUM];  // value fifo直接开满4096位宽（512字节）
// #pragma HLS STREAM depth = 16 variable = decoder_value_stream[0]
// #pragma HLS STREAM depth = 16 variable = decoder_value_stream[1]
// #pragma HLS STREAM depth = 16 variable = decoder_value_stream[2]
// #pragma HLS STREAM depth = 16 variable = decoder_value_stream[3]

//         hls_thread_local hls::stream<ap_uint<2> > merger_result;
// #pragma HLS STREAM depth = 16 variable = merger_result
        hls_thread_local hls::stream<fifo_key_meta> encoder_km_stream;
#pragma HLS STREAM depth = 16 variable = encoder_km_stream
        hls_thread_local hls::stream<fifo_value_slice> encoder_value_stream[MAX_INPUT_FILE_NUM];  // value fifo直接开满4096位宽（512字节）
#pragma HLS STREAM depth = 16 variable = encoder_value_stream[0]
#pragma HLS STREAM depth = 16 variable = encoder_value_stream[1]
#pragma HLS STREAM depth = 16 variable = encoder_value_stream[2]
#pragma HLS STREAM depth = 16 variable = encoder_value_stream[3]
        hls_thread_local hls::stream<ap_uint<2> > merge_result_stream;
#pragma HLS STREAM depth = 16 variable = merge_result_stream

        int index_block_pointer[MAX_INPUT_FILE_NUM]={0};
        int index_block_limit[MAX_INPUT_FILE_NUM];
#pragma HLS array_partition variable=index_block_pointer type=complete dim=1
#pragma HLS array_partition variable=index_block_limit type=complete dim=1

        // keystring tem;
        // set_max_key(tem);
        // tem.print_key();
        // std::cout << "tem is max key: " << is_max_key(tem) << "  is signal key: " << is_signal_key(tem) << std::endl;
        // set_signal_key(tem);
        // tem.print_key();
        // std::cout << "tem is max key: " << is_max_key(tem) << "  is signal key: " << is_signal_key(tem) << std::endl;

        // for(int i = 0; i < MAX_INPUT_FILE_NUM; i++)
        // {
        //     clearap128fifo(key_stream[i]);
        //     clearkvpairfifo(decoder_kv_stream[i]);
        // }
        // clearap2fifo(merger_result);
        // clearkvpairfifo(encoder_kv_stream);        

        // footer_decoder(sst_input0, size[0],
        //                 // metaindex_handle_offset[0], metaindex_handle_size[0],
        //                 // index_handle_offset[i],
        //                 /*
        //                     ** CAUTIION **
        //                     index_handle_offset -> pointer
        //                 */
        //                 index_block_pointer[0],
        //                 // index_handle_size[i],
        //                 index_block_limit[0]);
        // footer_decoder(sst_input1, size[1],
        //                 index_block_pointer[1],
        //                 index_block_limit[1]);
        // footer_decoder(sst_input2, size[2],
        //                 index_block_pointer[2],
        //                 index_block_limit[2]);
        // footer_decoder(sst_input3, size[3],
        //                 index_block_pointer[3],
        //                 index_block_limit[3]);

        ap_uint<40> max_file_size = ((buf_offset[MAX_INPUT_FILE_NUM] / MAX_OUTPUT_FILE_NUM) + 4095) & ~4095;
        printf("max_file_size: %lu\n", max_file_size);

        hls_thread_local hls::task merge_krnl(merge, decoder_km_stream, encoder_km_stream, merge_result_stream);
        // hls_thread_local hls::task kv_transfer_krnl(KV_Transfer, merger_result, decoder_kv_stream, encoder_kv_stream);

        decoder(sst_input0, input_kv[0], size[0],
                // index_block_pointer[0], index_block_limit[0],
                // key_stream[0], 
                decoder_km_stream[0], encoder_value_stream[0]);
        decoder(sst_input1, input_kv[1], size[1],
                // index_block_pointer[1], index_block_limit[1],
                // key_stream[1], 
                decoder_km_stream[1], encoder_value_stream[1]);
        decoder(sst_input2, input_kv[2], size[2],
                // index_block_pointer[2], index_block_limit[2],
                // key_stream[2], 
                decoder_km_stream[2], encoder_value_stream[2]);
        decoder(sst_input3, input_kv[3], size[3],
                // index_block_pointer[3], index_block_limit[3],
                // key_stream[3], 
                decoder_km_stream[3], encoder_value_stream[3]);

        encoder(encoder_km_stream, encoder_value_stream, merge_result_stream, input_kv_sum, sst_buffer, index_block_result, output_data, max_file_size);

        // for (int i = 0; i < MAX_INPUT_FILE_NUM; i++)
        // {
        //     std::cout << i << std::endl;
        //     check_clear_fifo(decoder_km_stream[i], "decoder_km_stream");
        //     check_clear_fifo(encoder_value_stream[i], "encoder_value_stream");
        // }
        // // check_clear_fifo(merger_result, "merger_result");
        // check_clear_fifo(encoder_km_stream, "encoder_km_stream");
        // check_clear_fifo(merge_result_stream, "merge_result_stream");
    }
}
