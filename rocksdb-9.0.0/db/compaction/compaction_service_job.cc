//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/compaction/compaction_job.h"
#include "db/compaction/compaction_state.h"
#include "logging/logging.h"
#include "monitoring/iostats_context_imp.h"
#include "monitoring/thread_status_util.h"
#include "options/options_helper.h"
#include "rocksdb/utilities/options_type.h"

#include "host_process.h"

#include <iostream>
#include <iomanip>
// #include <thread>
#include <fcntl.h>
#include <unistd.h>
#include <sstream>
#include <errno.h>

#include "krnl_host.h"

#define CSD_ALIGN_TO_4K(x) (((x) + 4095) & ~4095)

void  Bitout( void   * pBuffer, int  nSize)   
 {   
    unsigned char *pTemp = (unsigned char*)pBuffer;   
    int i, j, nResult;   
    for(i = nSize - 1;i >= 0; i--)   
    {   
        for(j = 7;j >= 0; j--)   
        {   
            nResult   = pTemp[i] & (1 << j);   
            nResult = ( 0  != nResult);   
            std::cout << nResult;
        }   
    }
    std::cout <<"\n";
}


void readfile(std::fstream &filestream, char *file_buffer, uint64_t size)
{
    if (size <= 0)
    {
      return;
    }
    int index;
    index = 0;
    for (uint64_t j = 0; j < size / 4096; j++)
    {
        filestream.read(file_buffer + 4096 * index, 4096);
        index++;
    }
    if (size % 4096)
    {
        filestream.read(file_buffer + 4096 * index, size % 4096);
    }
}

void check_pwrite(ssize_t write_size)
{
  if (write_size == -1) {
    std::cerr << "Error in extra index write: " << strerror(errno) << std::endl;
}
}

cl_ulong all_time;
void chrono_print_time(std::chrono::high_resolution_clock::time_point &prev_time, const char *str, std::stringstream &ss)
{
  std::chrono::high_resolution_clock::time_point now_time = std::chrono::high_resolution_clock::now();
  cl_ulong time_diff = std::chrono::duration_cast<std::chrono::nanoseconds>(now_time - prev_time).count();
  prev_time = now_time;
  all_time += time_diff;
  // printf("****** %s time: %.2f ms\n", str, time_diff / 1000000.0);
  ss << "****** " << str << " time: " << time_diff / 1000000.0 << " ms\n"; 
}

// WARNING!!!
// Should be placed in namespace
// #define ALIGN_TO_4K(x) (((x) + 4095) & ~4095)

namespace ROCKSDB_NAMESPACE {

class SubcompactionState;

CompactionServiceJobStatus
CompactionJob::ProcessKeyValueCompactionOnCSD(
    SubcompactionState* sub_compact) {
  std::stringstream ss;
  ss << "-----------compaction all times-----------\n";
  all_time = 0;
  std::chrono::high_resolution_clock::time_point prev_time = std::chrono::high_resolution_clock::now();
  auto time = std::chrono::duration_cast<std::chrono::milliseconds>(prev_time.time_since_epoch());
  std::cout << time.count() << " ms at inner start\n";
  assert(sub_compact);
  assert(sub_compact->compaction);

  uint64_t num_input_range_del = 0;
  UpdateCompactionStats(&num_input_range_del);
  compaction_job_stats_->num_input_records=(compaction_stats_.stats.num_input_records - num_input_range_del);
  compaction_stats_.stats.num_input_records=0;
  
  // std::cout<<"Run on CSD\n";

  ROCKS_LOG_INFO(
      db_options_.info_log,
      "[%s] [JOB %d] Starting compaction on CSD",
      sub_compact->compaction->column_family_data()->GetName().c_str(), job_id_);

  chrono_print_time(prev_time, "program device", ss);


  const Compaction* compaction = sub_compact->compaction;
  const auto& input_table_properties = compaction->GetInputTableProperties();

  auto cfd = compaction->column_family_data();

  cl_int err;
  cl::Context context = cfd->Compaction_csd_context;
  cl::CommandQueue q = cfd->Compaction_csd_queue;
  cl::Kernel Compaction_kernel = cfd->Compaction_accelerator_kernel;

  // std::fstream filestream[4];
  int fd[4];
  uint64_t file_size_host[4]={0,0,0,0};
  uint64_t file_buffer_size[4]={4096,4096,4096,4096};
  uint64_t file_input_entries[4]={0,0,0,0};
  uint64_t gen_sst_file_limit=0;

  uint64_t output_file_id[4]={0,0,0,0};

  const std::vector<CompactionInputFiles>& inputs =
      *(compact_->compaction->inputs());
      
  int file_count=0;
  for (const auto& files_per_level : inputs) {
    for (const auto& file : files_per_level.files) {
      uint64_t input_fd_num = file->fd.GetNumber();
      std::string input_path = compaction->immutable_options()->cf_paths[file->fd.GetPathId()].path;
      std::string compaction_file_name = MakeTableFileName(input_path, input_fd_num);

      fd[file_count] = open(compaction_file_name.c_str(), O_RDWR | O_DIRECT);
      // filestream[file_count]=std::fstream(compaction_file_name, std::ios::in);

      output_file_id[file_count] = input_fd_num;

      const auto& tp = input_table_properties.find(compaction_file_name);
      // std::cout << "tp: " << tp->second->ToString() << std::endl;
      if (tp != input_table_properties.end()) {
        file_input_entries[file_count] = tp->second->num_entries;
      } else {
        file_input_entries[file_count] = 0;
      }
      file_size_host[file_count]=file->fd.file_size;
      file_buffer_size[file_count]=CSD_ALIGN_TO_4K(file->fd.file_size);
      // std::cout << "file_buffer_size: " << file_buffer_size[file_count] << std::endl;

      file_count++;
      // std::cout<<"compaction on device file name:"<<MakeTableFileName(compaction->immutable_options()->cf_paths[file->fd.GetPathId()].path,file->fd.GetNumber()) << " entries: " << tp->second->num_entries<<"\n";
    }
  }

  gen_sst_file_limit = file_size_host[0] + file_size_host[1] + file_size_host[2] + file_size_host[3] + 5200 * 4;

  if(compaction->immutable_options()->compaction_csd_gen_sst_file_size_policy != kCompactionCSDSSTavg)
  {
    if(compaction->immutable_options()->compaction_csd_gen_sst_file_size_policy == KCompactionCSDSSTabove64)
    {
      uint64_t tune_size=64*1024*1024*4;
      if(gen_sst_file_limit<tune_size)
      {
        std::cout<<" file size tune policy : KCompactionCSDSSTabove64 "<<gen_sst_file_limit<< " -> "<<tune_size<<"\n";
        gen_sst_file_limit=tune_size;
      }
    }
    if(compaction->immutable_options()->compaction_csd_gen_sst_file_size_policy == KCompactionCSDSSTlayer)
    {
      uint64_t tune_size=64*1024*1024*4*(1<<(compaction->output_level()-1));
      if(tune_size>=128*1024*1024*4)
      {
        tune_size = 128*1024*1024*4;
      }
      if(gen_sst_file_limit<tune_size)
      {
        std::cout<<" file size tune policy : KCompactionCSDSSTlayer "<<gen_sst_file_limit<< " -> "<<tune_size<<" "<< compaction->output_level()<<"\n";
        gen_sst_file_limit=tune_size;
      }
    }
    if(compaction->immutable_options()->compaction_csd_gen_sst_file_size_policy == kCompactionCSDSSTwtosmall)
    {
      std::cout<<" file size tune policy : kCompactionCSDSSTwtosmall "<<gen_sst_file_limit<<" - > ";
      uint64_t tune_size=64*1024*1024*4*(1<<(compaction->output_level()-1));
      if(gen_sst_file_limit<tune_size)
      {
        
        uint64_t initial_file_standard = 64*1024*1024*(1<<(compaction->output_level()-1));
        uint64_t approx_last_file = gen_sst_file_limit;
        uint64_t approx_gen_file_num = 0;
        while(approx_last_file>initial_file_standard)
        {
          approx_last_file-=initial_file_standard;
          approx_gen_file_num++;
        }

        //TODO : initial_file_standard/2 maybe should be replaced by initial_file_standard*approx_gen_file_num/(approx_gen_file_num+1)

        if(approx_gen_file_num==0)
        {
          gen_sst_file_limit=tune_size;
        }
        else
        {
          if(approx_last_file < initial_file_standard/2) 
          {
            //We should increase file limit
            initial_file_standard += approx_last_file/approx_gen_file_num;
          }
          else
          {
            //We should decrease file limit
            initial_file_standard = initial_file_standard - (initial_file_standard-approx_last_file) / (approx_gen_file_num+1);
          }
          gen_sst_file_limit=initial_file_standard<<2;
        }
      }
      std::cout<<gen_sst_file_limit<<" "<< compaction->output_level()<<"\n";
    }
  }

  gen_sst_file_limit = CSD_ALIGN_TO_4K(gen_sst_file_limit);

  ROCKS_LOG_INFO(
      db_options_.info_log,
      "[%s] [JOB %d] The file number for Compaction on CSD is %lu %lu %lu %lu, file size are %lu %lu %lu %lu, entries are %lu %lu %lu %lu",
      sub_compact->compaction->column_family_data()->GetName().c_str(), job_id_,
      output_file_id[0], output_file_id[1], output_file_id[2], output_file_id[3],
      file_size_host[0], file_size_host[1], file_size_host[2], file_size_host[3],
      file_input_entries[0], file_input_entries[1], file_input_entries[2], file_input_entries[3]
      );

  chrono_print_time(prev_time, "prepare file", ss);

  uint64_t index_block_buffer_size = 1024 * 1024 * 8;
  uint64_t sst_buffer_size = 1024 * 1024 * 768;
  uint64_t host_data_size = 15;

  // Alloc device memory
  cl_mem_ext_ptr_t file0_device_ext;
  file0_device_ext = {XCL_MEM_EXT_P2P_BUFFER, nullptr, 0};
  cl::Buffer file0_device(context, CL_MEM_READ_WRITE | CL_MEM_EXT_PTR_XILINX, file_buffer_size[0], &file0_device_ext);

  cl_mem_ext_ptr_t file1_device_ext;
  file1_device_ext = {XCL_MEM_EXT_P2P_BUFFER, nullptr, 0};
  cl::Buffer file1_device(context, CL_MEM_READ_WRITE | CL_MEM_EXT_PTR_XILINX, file_buffer_size[1], &file1_device_ext);

  cl_mem_ext_ptr_t file2_device_ext;
  file2_device_ext = {XCL_MEM_EXT_P2P_BUFFER, nullptr, 0};
  cl::Buffer file2_device(context, CL_MEM_READ_WRITE | CL_MEM_EXT_PTR_XILINX, file_buffer_size[2], &file2_device_ext);

  cl_mem_ext_ptr_t file3_device_ext;
  file3_device_ext = {XCL_MEM_EXT_P2P_BUFFER, nullptr, 0};
  cl::Buffer file3_device(context, CL_MEM_READ_WRITE | CL_MEM_EXT_PTR_XILINX, file_buffer_size[3], &file3_device_ext);

  // cl::Buffer host_data(context, CL_MEM_READ_ONLY, sizeof(int)* host_data_size, NULL, &err);

  // cl_mem_ext_ptr_t sst_bufferExt;
  // sst_bufferExt = {XCL_MEM_EXT_P2P_BUFFER, nullptr, 0};
  // cl::Buffer sst_buffer(context, CL_MEM_READ_ONLY | CL_MEM_EXT_PTR_XILINX, sizeof(char) * sst_buffer_size, &sst_bufferExt,
  //                                         &err);
  // cl_mem_ext_ptr_t index_block_bufferExt;
  // index_block_bufferExt = {XCL_MEM_EXT_P2P_BUFFER, nullptr, 0};
  // cl::Buffer index_block_buffer(context, CL_MEM_READ_ONLY | CL_MEM_EXT_PTR_XILINX, sizeof(char) * index_block_buffer_size, &index_block_bufferExt,
  //                                         &err);

  // cl::Buffer output_data(context, CL_MEM_READ_WRITE, sizeof(uint64_t) * (CSD_PPS_KERNEL_SIZE + 20), NULL, &err);

  // cl::Buffer file0_device= cfd->input_sst_buffer[0];
  // cl::Buffer file1_device= cfd->input_sst_buffer[1];
  // cl::Buffer file2_device= cfd->input_sst_buffer[2];
  // cl::Buffer file3_device= cfd->input_sst_buffer[3];
  cl::Buffer host_data   = cfd->input_metadata_buffer;
  cl::Buffer sst_buffer  = cfd->output_datablock_buffer;
  cl::Buffer index_block_buffer = cfd->output_indexblock_buffer;
  cl::Buffer output_data = cfd->output_metadata_buffer;

  chrono_print_time(prev_time, "opencl", ss);

  // int narg = 0;
  Compaction_kernel.setArg(0, file0_device);
  Compaction_kernel.setArg(1, file1_device);
  Compaction_kernel.setArg(2, file2_device);
  Compaction_kernel.setArg(3, file3_device);
  // Compaction_kernel.setArg(narg++, host_data);
  // Compaction_kernel.setArg(narg++, sst_buffer);
  // Compaction_kernel.setArg(narg++, index_block_buffer);
  // Compaction_kernel.setArg(narg++, output_data);

  chrono_print_time(prev_time, "set arg", ss);

  char* file_buffer[4];

  char* sst_result;
  char* index_block_result;

  uint64_t *device_output_data;

#ifndef CSD_KEY_16
  uint64_t* host_data_buffer;
#else
  int* host_data_buffer;
#endif
  

  file_buffer[0] = (char*)q.enqueueMapBuffer(file0_device, CL_TRUE, CL_MAP_READ | CL_MAP_WRITE, 0, file_buffer_size[0], NULL, NULL, &err);
  file_buffer[1] = (char*)q.enqueueMapBuffer(file1_device, CL_TRUE, CL_MAP_READ | CL_MAP_WRITE, 0, file_buffer_size[1], NULL, NULL, &err);
  file_buffer[2] = (char*)q.enqueueMapBuffer(file2_device, CL_TRUE, CL_MAP_READ | CL_MAP_WRITE, 0, file_buffer_size[2], NULL, NULL, &err);
  file_buffer[3] = (char*)q.enqueueMapBuffer(file3_device, CL_TRUE, CL_MAP_READ | CL_MAP_WRITE, 0, file_buffer_size[3], NULL, NULL, &err);

#ifndef CSD_KEY_16
  host_data_buffer = (uint64_t*)q.enqueueMapBuffer(host_data, CL_TRUE, CL_MAP_READ | CL_MAP_WRITE, 0, sizeof(uint64_t)*host_data_size, NULL, NULL, &err);
#else
  host_data_buffer = (int*)q.enqueueMapBuffer(host_data, CL_TRUE, CL_MAP_READ | CL_MAP_WRITE, 0, sizeof(int)*host_data_size, NULL, NULL, &err);
#endif

  sst_result = (char*)q.enqueueMapBuffer(sst_buffer, CL_TRUE, CL_MAP_READ | CL_MAP_WRITE, 0, sizeof(char)*(sst_buffer_size), NULL, NULL, &err);
  index_block_result = (char*)q.enqueueMapBuffer(index_block_buffer, CL_TRUE, CL_MAP_READ | CL_MAP_WRITE, 0, sizeof(char)* index_block_buffer_size, NULL, NULL, &err);

  device_output_data = (uint64_t *)q.enqueueMapBuffer(output_data, CL_TRUE, CL_MAP_READ, 0, sizeof(uint64_t) * (CSD_PPS_KERNEL_SIZE + 20), NULL, NULL, &err);

  chrono_print_time(prev_time, "enqueueMapBuffer", ss);

#ifndef CSD_KEY_16
  uint64_t *sizes = (host_data_buffer + 5);
  uint64_t *offset = (host_data_buffer + 0);
  uint64_t *kv_sums = (host_data_buffer + 9);
  uint64_t *init = (host_data_buffer + 14);
#else
  int *sizes = (host_data_buffer + 5);
  int *offset = (host_data_buffer + 0);
  int *kv_sums = (host_data_buffer + 9);
  int *init = (host_data_buffer + 14);
#endif
  // std::cout<<"size:";
  for(int i=0;i<file_count;i++){
    sizes[i]=file_size_host[i];
    // std::cout<<sizes[i]<<" ";
  }
  // std::cout<<"\n";
  
  offset[0] = 0;
  offset[1] = 0;
  offset[2] = 0;
  offset[3] = 0;
  // offset[4] = sizes[0] + sizes[1] + sizes[2] + sizes[3];
  // int input_size = (sizes[0] + sizes[1] + sizes[2] + sizes[3]) + 4096 * 4;
  // offset[4] = ALIGN_TO_4K(input_size);
  offset[4] = gen_sst_file_limit;
  
  
  kv_sums[1] = file_input_entries[0];
  kv_sums[2] = file_input_entries[1];
  kv_sums[3] = file_input_entries[2];
  kv_sums[4] = file_input_entries[3];
  kv_sums[0] = kv_sums[1] + kv_sums[2] + kv_sums[3] + kv_sums[4];
  
  init[0] = 1;

  // std::cout << "entries: " << kv_sums[1] << " " << kv_sums[2] << " " << kv_sums[3] << " " << kv_sums[4] << std::endl;

  q.enqueueMigrateMemObjects({host_data}, 0);

  chrono_print_time(prev_time, "prepare host", ss);

  // int index;
  
  // for(int i=0;i<4;i++){
  //   index=0;
  //   for(uint64_t j=0;j<file_size_host[i]/4096;j++){
  //       filestream[i].read(file_buffer[i]+4096*index, 4096);
  //       index++;
  //   }
  //   if(file_size_host[i]%4096){
  //       filestream[i].read(file_buffer[i]+4096*index, file_size_host[i]%4096);
  //   }
  // }
  // std::thread t2(readfile, std::ref(filestream[1]), file_buffer[1], file_size_host[1]);
  // std::thread t3(readfile, std::ref(filestream[2]), file_buffer[2], file_size_host[2]);
  // std::thread t4(readfile, std::ref(filestream[3]), file_buffer[3], file_size_host[3]);
  // readfile(std::ref(filestream[0]), file_buffer[0], file_size_host[0]);
  // t2.join();
  // t3.join();
  // t4.join();
  // for(int i=0;i<file_count;i++)
  //   {
  //       // std::cout<<"--------------------\n";
  //       // std::cout<<"file "<<i<<"\n";

  //       // std::chrono::high_resolution_clock::time_point p2pStart = std::chrono::high_resolution_clock::now();
  //       std::cout << "fd: " << fd[i] << std::endl;
  //       std::cout << "file size: " << file_buffer_size[i] << std::endl;
  //       ssize_t num = pread(fd[i], file_buffer[i], file_buffer_size[i], 0);
  //       std::cout << "read " << num << " bytes\n";
  //       if (num == -1) {
  //           std::cerr << "pread failed: " << std::strerror(errno) << std::endl;
  //       }
  //       // for (int j = 0; j < 200; j++)
  //       // {
  //       //     std::cout << file_buffer[i][j];
  //       // }
  //       std::cout << std::endl;
  //       close(fd[i]);

  //       // std::chrono::high_resolution_clock::time_point p2pEnd = std::chrono::high_resolution_clock::now();
  //       // cl_ulong p2pTime = std::chrono::duration_cast<std::chrono::nanoseconds>(p2pEnd - p2pStart).count();

  //       // std::cout << "p2p time: " << p2pTime << " ns\n";
  //       // std::cout << "p2p throughput: " << ((double)file_size_host[i])/(((double)p2pTime)/1000000000.0) << " MB/s\n";
  //       // std::cout<<"--------------------\n";
  //   }

  uint64_t chunksize = 16777216;
  // uint64_t chunksize = 4096;
  // std::cout<<"--------------------\n";
  // std::cout<<"chunksize "<<chunksize<<"\n";
  ssize_t read_size;

  for(int x=0; x<file_count; x++)
  {
      std::cout<<"--------------------\n";
      for (uint64_t j = 0; j < file_size_host[x] / chunksize; j++)
      {
          read_size = pread(fd[x], file_buffer[x] + chunksize * j, chunksize, j*chunksize);
          std::cout << "sst read " << j << ": " << read_size << " bytes\n";
      }
      if(file_size_host[x] % chunksize)
      {
          uint64_t j = file_size_host[x] / chunksize;
          // uint64_t read_length=file_size_host[x] % chunksize - 4;
          uint64_t read_length=file_size_host[x] % chunksize;
          read_size = pread(fd[x], file_buffer[x] + chunksize * j, CSD_ALIGN_TO_4K(read_length), j*chunksize);
          std::cout << "sst read " << j << ": " << read_size << " bytes\n";
      }
      close(fd[x]);
  }
  std::cout<<"--------------------\n";

  chrono_print_time(prev_time, "read file", ss);

  std::vector<cl::Event> events_kernel(1);

  q.enqueueTask(Compaction_kernel, nullptr, &events_kernel[0]);
  q.finish();

  chrono_print_time(prev_time, "kernel", ss);

  // q.enqueueMigrateMemObjects({sst_buffer}, CL_MIGRATE_MEM_OBJECT_HOST, &events_kernel, nullptr);
  // q.enqueueMigrateMemObjects({output_data}, CL_MIGRATE_MEM_OBJECT_HOST);
  // q.finish();
  unsigned long kerneltime, kerneltimestart, kerneltimeend;

  events_kernel[0].getProfilingInfo(CL_PROFILING_COMMAND_START, &kerneltimestart);
  events_kernel[0].getProfilingInfo(CL_PROFILING_COMMAND_END, &kerneltimeend);
  kerneltime = (kerneltimeend - kerneltimestart);

  std::cout<<"---- Kernel Time: "<<kerneltime<<"ns -----\n";

  
  q.enqueueMigrateMemObjects({output_data}, CL_MIGRATE_MEM_OBJECT_HOST, &events_kernel, nullptr);
  // q.enqueueMigrateMemObjects({sst_buffer}, CL_MIGRATE_MEM_OBJECT_HOST);
  // q.enqueueMigrateMemObjects({index_block_buffer}, CL_MIGRATE_MEM_OBJECT_HOST);
  q.finish();

  // std::cout<<"sstlengths:"<<device_output_data[PPS_KERNEL_SIZE]<<" "<<device_output_data[PPS_KERNEL_SIZE+1]<<" "<<device_output_data[PPS_KERNEL_SIZE+2]<<" "<<device_output_data[PPS_KERNEL_SIZE+3]<<"\n";
  // std::cout<< "write speed (MB/s):"<< double(device_output_data[PPS_KERNEL_SIZE]+device_output_data[PPS_KERNEL_SIZE+1]+device_output_data[PPS_KERNEL_SIZE+2]+device_output_data[PPS_KERNEL_SIZE+3]) / 1024 / 1024 / (double(kerneltime) / 1000000000) << '\n';

  // get result
  // offset in bytes
  uint32_t offset_index[CSD_MAX_OUTPUT_FILE_NUM], offset_pps[CSD_MAX_OUTPUT_FILE_NUM], offset_sst[CSD_MAX_OUTPUT_FILE_NUM];
  for (int i = 0; i < CSD_MAX_OUTPUT_FILE_NUM; i++){
      // i * 总大小 / 输入文件数 * 输出文件数
#ifndef CSD_KEY_16
      offset_sst[i] = CSD_ALIGN_TO_4K(offset[CSD_MAX_INPUT_FILE_NUM] / CSD_MAX_OUTPUT_FILE_NUM) * i;
      // offset_sst[i] = CSD_ALIGN_TO_4K(i*(offset[CSD_MAX_INPUT_FILE_NUM] / CSD_MAX_OUTPUT_FILE_NUM));
#else
      offset_sst[i] = i * (offset[CSD_MAX_INPUT_FILE_NUM] / CSD_MAX_OUTPUT_FILE_NUM);
#endif
      // std::cout<<"offset_sst " <<offset_sst[i]<<"\n";
  }
  for (int i = 0; i < CSD_MAX_OUTPUT_FILE_NUM; i++){
      offset_index[i] = i * (index_block_buffer_size / CSD_MAX_OUTPUT_FILE_NUM);
  }
  for (int i = 0; i < CSD_MAX_OUTPUT_FILE_NUM; i++){
      offset_pps[i] = i * CSD_PPS_KERNEL_SINGEL_SIZE;
  }

  // compaction service(subcompactionservice)

  Temperature temperature = sub_compact->compaction->output_temperature();
  if (temperature == Temperature::kUnknown &&
    sub_compact->compaction->is_last_level() &&
    !sub_compact->IsCurrentPenultimateLevel()) {
      temperature =
          sub_compact->compaction->mutable_cf_options()->last_level_temperature;
    }

    
  uint64_t epoch_number = sub_compact->compaction->MinInputFileEpochNumber();
  int64_t temp_current_time = 0;
  InternalKey tmp_start, tmp_end;
  if (sub_compact->start.has_value()) {
    tmp_start.SetMinPossibleForUserKey(*(sub_compact->start));
  }
  if (sub_compact->end.has_value()) {
    tmp_end.SetMinPossibleForUserKey(*(sub_compact->end));
  }
  uint64_t oldest_ancester_time =
      sub_compact->compaction->MinInputFileOldestAncesterTime(
          sub_compact->start.has_value() ? &tmp_start : nullptr,
          sub_compact->end.has_value() ? &tmp_end : nullptr);


  char properties[4096], metaindexblock[1024];
  char footer_buffer[53];
  int pp_index = 0, metaindexblock_index = 0;

  uint64_t outputfile_total_size=0;
  uint64_t outputfile_total_entry=0;

  // int output_file_num = device_output_data[PPS_KERNEL_SIZE + CSD_MAX_OUTPUT_FILE_NUM*2];
  // std::cout << "output file num: " << output_file_num << std::endl;

  ssize_t write_size;
  char sst_buf[4096];

  chrono_print_time(prev_time, "prepare write", ss);

  for (int i = 0; i < CSD_MAX_OUTPUT_FILE_NUM; i++)
  {
    std::cout << "==============================================" << std::endl;
    uint64_t file_size;
    uint64_t sstlength, indexlength;
    sstlength=device_output_data[CSD_PPS_KERNEL_SIZE+i];
    indexlength=device_output_data[CSD_PPS_KERNEL_SIZE+CSD_MAX_OUTPUT_FILE_NUM+i];
    uint64_t entries = device_output_data[CSD_PPS_ENTRIES_OFF + offset_pps[i]];
    std::cout << "sstlength:" << sstlength << "\n";
    std::cout << "indexlength:" << indexlength << "\n";
    std::cout << "entries:" << entries << "\n";

    // for (uint64_t j = sstlength-5200; j < sstlength; j++)
    // {
    //   printf("%02x ", (uint32_t)sst_result[j + offset_sst[i]] & 0xFF);
    // }
    // printf("\n--------\n");
    // for (uint64_t j = sstlength; j < sstlength+5000; j++)
    // {
    //   printf("%02x ", (uint32_t)sst_result[j + offset_sst[i]] & 0xFF);
    // }
    // printf("\n");
    if (entries <= 0)
    {
        std::cout << "empty file entries is "<<entries<<"\n";
        continue;
    }
    if (sstlength <= 0)
    {
      std::cout << "output file num less than 4: " << i + 1 << std::endl;
      break;
    }

    //Get a new file
    uint64_t file_id = versions_->NewFileNumber();
    auto tgt_file = TableFileName(compaction->immutable_options()->cf_paths,
                                  file_id, compaction->output_path_id());

    std::cout<<"outputfile: "<<file_id<<"\n";
    auto get_time_status = db_options_.clock->GetCurrentTime(&temp_current_time);
    // Safe to proceed even if GetCurrentTime fails. So, log and proceed.
    if (!get_time_status.ok()) {
      ROCKS_LOG_WARN(db_options_.info_log,
                    "Failed to get current time. Status: %s",
                    get_time_status.ToString().c_str());
    }

    if (oldest_ancester_time == std::numeric_limits<uint64_t>::max()) {
      uint64_t current_time = static_cast<uint64_t>(temp_current_time);
      // TODO: fix DBSSTTest.GetTotalSstFilesSize and use
      //  kUnknownOldestAncesterTime
      oldest_ancester_time = current_time;
    }


    uint64_t index_block_offset = CSD_ALIGN_TO_4K(sstlength);
    std::cout << "index_block_offset: " << index_block_offset << std::endl;
    uint64_t index_block_write_size = CSD_ALIGN_TO_4K(indexlength);
    uint64_t meta_data_offset = index_block_write_size + index_block_offset;
    pp_index = 0;
    metaindexblock_index = 0;

    {
      // file_id | (path_id * (kFileNumberMask + 1))
      // initProperties(db_id_,db_session_id_,file_id, indexlength);
      initProperties(db_id_,db_session_id_,file_id, indexlength-5);
    }
    // putProperties(properties, pp_index, metaindexblock, metaindexblock_index, sstlength + indexlength + 5, device_output_data + offset_pps[i]);
    // putProperties(properties, pp_index, metaindexblock, metaindexblock_index, sstlength + indexlength, device_output_data + offset_pps[i]);

    //void putProperties(char *pp_pointer, int &pp_index, char *metaindex_block_buffer, int &metaindexblock_index, int properties_offset, uint64_t *pps)
    putProperties(properties, pp_index, metaindexblock, metaindexblock_index, meta_data_offset, device_output_data + offset_pps[i]);
    // std::cout << "pp_index " << pp_index << " metaibdexblock_index " << metaindexblock_index << '\n';

    // build footer
    // metaindexblock_offset:indexlength实际上是indexblock的index变量，所以需要加上hash长度5；pp_index同理
    // void putFooter(uint64_t index_block_offset, uint64_t index_block_size, uint64_t metaindex_block_offset, uint64_t metaindex_block_size,char* footer_buffer)
    // std::cout << "putFooter" << std::endl;
    // std::cout << "index_block_offset:" << sstlength << " index_block_size:" << indexlength-5 << " metaindex_block_offset:" << (sstlength + indexlength + pp_index + 5);
    // std::cout << " metaindex_block_size:" << metaindexblock_index << std::endl;

    // putFooter(sstlength, indexlength, sstlength + indexlength + 5 + pp_index + 5, metaindexblock_index, footer_buffer);
    // putFooter(sstlength, indexlength-5, sstlength + indexlength + pp_index + 5, metaindexblock_index, footer_buffer);

    //void putFooter(uint64_t index_block_offset, uint64_t index_block_size, uint64_t metaindex_block_offset, uint64_t metaindex_block_size,char* footer_buffer)
    putFooter(index_block_offset, indexlength-5, meta_data_offset + pp_index + 5, metaindexblock_index, footer_buffer);

    uint64_t otherlength = pp_index + 5 + metaindexblock_index + 5 + 53;

    file_size = meta_data_offset +  otherlength;

    std::cout<<"output act file size "<<file_size<<"\n";

    // write sst
    // int nvmeFd = open(tgt_file.c_str(), O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    int nvmeFd = open(tgt_file.c_str(), O_RDWR | O_CREAT | O_TRUNC | O_DIRECT, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    if (nvmeFd == -1)
    {
      std::cerr << "nvme file open failed: " << std::strerror(errno) << std::endl;
      return CompactionServiceJobStatus::kFailure;
    }

    std::cout << "-----------------------------------" << std::endl;
    std::cout << "sst length: " << index_block_offset << "\n";
    // sstlength = ALIGN_TO_4K(sstlength);
    // write_size = pwrite(nvmeFd, sst_result + (offset_sst[i]), sstlength, 0);
    // write_size = pwrite(nvmeFd, sst_result + (offset_sst[i]), index_block_offset, 0);
    // std::cout << "sst write " << write_size << " bytes\n";

    //PLAN A: chunk size 
    
    for (uint64_t j = 0; j < index_block_offset / chunksize; j++)
    {
      // do{
      write_size = pwrite(nvmeFd, sst_result + (offset_sst[i]) + chunksize * j, chunksize, j*chunksize);
      std::cout << "sst write " << j << ": " << write_size << " bytes\n";
      check_pwrite(write_size);
      // }while(write_size==-1);
    }
    if(index_block_offset % chunksize)
    {
      uint64_t j = index_block_offset / chunksize;
      uint64_t write_length = index_block_offset % chunksize;
      std::cout << "extra sst write length: " << write_length << " bytes\n";
      write_size = pwrite(nvmeFd, sst_result + (offset_sst[i]) + chunksize * j, write_length, j*chunksize);
      std::cout << "extra sst write : " << write_size << " bytes\n";
      check_pwrite(write_size);
    }
    
    //PLAN B: write all data in one time
    // write_size = pwrite(nvmeFd, sst_result + (offset_sst[i]), index_block_offset, 0);
    // std::cout << "sst write - data block" << ": " << write_size << " bytes\n";

    std::cout << "-----------------------------------" << std::endl;
    std::cout << "index length: " << index_block_write_size << "\n";
    // indexlength = ALIGN_TO_4K(indexlength);
    // write_size = pwrite(nvmeFd, index_block_result + (offset_index[i]), indexlength, sstlength);
    // std::cout << "index block offset " << index_block_offset << " bytes\n";

    // write_size = pwrite(nvmeFd, index_block_result + (offset_index[i]), index_block_write_size, index_block_offset);
    // std::cout << "index write " << write_size << " bytes\n";

    //PLAN A : chunk size
    
    for (uint64_t j = 0; j < index_block_write_size / chunksize; j++)
    {
        write_size = pwrite(nvmeFd, index_block_result + (offset_index[i]) + chunksize * j, chunksize, index_block_offset+j*chunksize);
        std::cout << "index write " << j << ": " << write_size << " bytes\n";
        check_pwrite(write_size);
    }
    if(index_block_write_size % chunksize)
    {
      uint64_t j = index_block_write_size / chunksize;
      uint64_t write_length = index_block_write_size % chunksize;
      std::cout << "extra index write length: " << write_length << " bytes\n";
      write_size = pwrite(nvmeFd, index_block_result + (offset_index[i]) + chunksize * j, write_length, index_block_offset+j*chunksize);
      std::cout << "extra index write : " << write_size << " bytes\n";
      check_pwrite(write_size);
    }
    
    std::cout << "-----------------------------------" << std::endl;

    close(nvmeFd);

    // memcpy(sst_buf, "\0\0\0\0\0", 5);
    memcpy(sst_buf, properties, pp_index + 5);
    memcpy(sst_buf + pp_index + 5, metaindexblock, metaindexblock_index + 5);
    memcpy(sst_buf + pp_index + 5 + metaindexblock_index + 5, footer_buffer, 53);

    nvmeFd = open(tgt_file.c_str(), O_RDWR);

    // write_size = pwrite(nvmeFd, sst_buf, otherlength, sstlength+indexlength);
    write_size = pwrite(nvmeFd, sst_buf, otherlength, meta_data_offset);
    
    // std::cout << "other write " << write_size << " bytes\n";

    close(nvmeFd);

    // std::ofstream fd_sst(tgt_file);
    // for (uint32_t j = 0; j < sstlength - 4096; j += 4096){
    //     // std::cout<<"write\n";
    //     fd_sst.write((char *)(sst_result + (offset_sst[i])) + j, 4096);
    // }
    // if (sstlength % 4096){
    //     fd_sst.write((char *)(sst_result + (offset_sst[i])) + (int)(sstlength / 4096) * 4096, sstlength % 4096);
    // }
    // for (uint32_t j = 0; j < indexlength - 4096; j += 4096){
    //     fd_sst.write((char *)(index_block_result + offset_index[i]) + j, 4096);
    // }
    // if (indexlength % 4096){
    //     fd_sst.write((char *)(index_block_result + offset_index[i]) + (int)(indexlength / 4096) * 4096, indexlength % 4096);
    // }
    // uint64_t otherlength = 5 + pp_index + 5 + metaindexblock_index + 5 + 53;
    // fd_sst.write("\0\0\0\0\0", 5);
    // fd_sst.write(properties, pp_index + 5);
    // fd_sst.write(metaindexblock, metaindexblock_index + 5);
    // fd_sst.write(footer_buffer, 53);
  
    // smallest key in pps[6]:pps[7], size is pps[8]
    // largest key in pps[9]:pps[10], size is pps[11]
    char smallest_key_buffer[CSD_KEY_LENGTH], largest_key_buffer[CSD_KEY_LENGTH];
    uint64_t smallest_key_length, largest_key_length;

    smallest_key_length=(device_output_data + offset_pps[i])[CSD_PPS_SMALLESTKEY_LENGTH_OFF];
    for(uint32_t j = 0; j < smallest_key_length / 8; j++)  // 128字节存在uint64_t，128/8=16，每个循环处理一个uint64_t
    {
      for (uint32_t k = 0; k < 8; k++)  // uint64_t共8字节
      {
        smallest_key_buffer[j*8+k] = (char)((((device_output_data + offset_pps[i])[CSD_PPS_SMALLESTKEY_OFF+j]) >> (k*8)) & 0xFF);
      }
    }
    largest_key_length=(device_output_data + offset_pps[i])[CSD_PPS_LARGESTKEY_LENGTH_OFF];
    for(uint32_t j = 0; j < largest_key_length / 8; j++)  // 128字节存在uint64_t，128/8=16，每个循环处理一个uint64_t
    {
      for (uint32_t k = 0; k < 8; k++)  // uint64_t共8字节
      {
        largest_key_buffer[j*8+k] = (char)((((device_output_data + offset_pps[i])[CSD_PPS_LARGESTKEY_OFF+j]) >> (k*8)) & 0xFF);
      }
    }
    // for(uint32_t j = 0; j < 8; j++){
    //   largest_key_buffer[j] = (char)((((device_output_data + offset_pps[i])[9]) >> ((j) * 8)) & 0xFF);
    //   largest_key_buffer[j + 8] = (char)((((device_output_data + offset_pps[i])[10]) >> ((j) * 8)) & 0xFF);
    // }

    // uint64_t invalid_key_sum=(device_output_data + offset_pps[i])[14];
    // uint64_t invalid_key_sum2=(device_output_data + offset_pps[i])[15];
    ROCKS_LOG_INFO(
      db_options_.info_log,
      "[%s] [JOB %d] Compaction on CSD is writing output. Now output file num is %lu. File size: %lu. Entries sum: %lu.\
      Smallest key is %d %d %d %d %d %d %d %d %d %d %d %d %d %d %d %d, length %lu\
      Largest key is %d %d %d %d %d %d %d %d %d %d %d %d %d %d %d %d, length %lu",
      sub_compact->compaction->column_family_data()->GetName().c_str(), job_id_,
      file_id, file_size ,entries,
      int(smallest_key_buffer[0]), int(smallest_key_buffer[1]), int(smallest_key_buffer[2]), int(smallest_key_buffer[3]),
      int(smallest_key_buffer[4]), int(smallest_key_buffer[5]), int(smallest_key_buffer[6]), int(smallest_key_buffer[7]),
      int(smallest_key_buffer[8]), int(smallest_key_buffer[9]), int(smallest_key_buffer[10]), int(smallest_key_buffer[11]),
      int(smallest_key_buffer[12]), int(smallest_key_buffer[13]), int(smallest_key_buffer[14]), int(smallest_key_buffer[15]),
      smallest_key_length,
      int(largest_key_buffer[0]), int(largest_key_buffer[1]), int(largest_key_buffer[2]), int(largest_key_buffer[3]),
      int(largest_key_buffer[4]), int(largest_key_buffer[5]), int(largest_key_buffer[6]), int(largest_key_buffer[7]),
      int(largest_key_buffer[8]), int(largest_key_buffer[9]), int(largest_key_buffer[10]), int(largest_key_buffer[11]),
      int(largest_key_buffer[12]), int(largest_key_buffer[13]), int(largest_key_buffer[14]), int(largest_key_buffer[15]),
      largest_key_length
      );

    printf("smallest key length: %ld\n", smallest_key_length);
    // for(uint32_t j = 0; j < smallest_key_length / 8; j++)
    // {
    //   for (uint32_t k = 0; k < 8; k++)  // uint64_t共8字节
    //   {
    //     printf("%02x ", (uint32_t)smallest_key_buffer[j*8+k] & 0xFF);
    //   }
    // }
    // printf("\n");
    printf("largest key length: %ld\n", largest_key_length);
    // for(uint32_t j = 0; j < largest_key_length / 8; j++)
    // {
    //   for (uint32_t k = 0; k < 8; k++)  // uint64_t共8字节
    //   {
    //     printf("%02x ", (uint32_t)largest_key_buffer[j*8+k] & 0xFF);
    //   }
    // }
    // printf("\n");
    Slice smallest_key(smallest_key_buffer, smallest_key_length);
    Slice largest_key(largest_key_buffer, largest_key_length);
    FileMetaData meta;
    
    meta.fd = FileDescriptor(file_id, sub_compact->compaction->output_path_id(), file_size,
                              (device_output_data + offset_pps[i])[CSD_PPS_MINSEQ_OFF], (device_output_data + offset_pps[i])[CSD_PPS_MAXSEQ_OFF]);
    
    close(nvmeFd);
    // std::cout<<"sst write close\n";

    // std::cout<<"min seqno: "<<(device_output_data + offset_pps[i])[12]<<" max seqno:"<<(device_output_data + offset_pps[i])[13]<<"\n";

    meta.smallest.DecodeFrom(smallest_key);
    meta.largest.DecodeFrom(largest_key);

    // std::cout<<"small_length "<<meta.smallest.size()<<"\n";
    // std::cout<<"largest_length "<<meta.largest.size()<<"\n";
    // if (meta.smallest.size() != 16 || meta.largest.size() != 16)
    // {
    //   std::cout << "error key length" << std::endl;
    //   exit(0);
    // }

    // std::cout<<(DecodeFixed64(meta.smallest.Encode().data()+12-8UL)&0xff)<<"\n";
    // std::cout<<(DecodeFixed64(meta.largest.Encode().data()+12-8UL)&0xff)<<"\n";
    // std::cout<<(int)(127U)<<"\n";
    // std::cout<<meta.smallest.Encode().size()<<"\n";
    // std::cout<<meta.largest.Encode().size()<<"\n";
    // meta.smallest.Valid();
    // meta.largest.Valid();
    // ExtractUserKey(meta.smallest.Encode());
    // ExtractUserKey(meta.largest.Encode());

    meta.oldest_ancester_time = oldest_ancester_time;
    meta.file_creation_time = temp_current_time;
    meta.epoch_number = epoch_number;
    //meta.marked_for_compaction = file.marked_for_compaction;

    Status s = GetSstInternalUniqueId(db_id_, db_session_id_, file_id,
                              &meta.unique_id);
    
    // std::cout<<"db_id:"<<db_id_<<"\n";
    // std::cout<<"db_session_id_:"<<db_session_id_<<"\n";
    // std::cout<<"compaction unique id"<<InternalUniqueIdToHumanString(&meta.unique_id)<<"\n";
    if (!s.ok()) {
      return CompactionServiceJobStatus::kFailure;
    }

    meta.temperature=temperature;

    sub_compact->Current().AddOutput(std::move(meta),
                                     cfd->internal_comparator(), false, true,
                                     0);
    outputfile_total_size += meta.fd.GetFileSize();
    outputfile_total_entry += entries;
    // std::cout<<"next file "<<outputfile_total_size<<" "<<outputfile_total_entry<<"\n";
  }

  chrono_print_time(prev_time, "write file", ss);

  // for(int i=0;i<CSD_MAX_OUTPUT_FILE_NUM;i++)
  // {
  //   uint64_t entries = device_output_data[CSD_PPS_ENTRIES_OFF + offset_pps[i]];
  //   if(entries>0)
  //   {
  //     assert((device_output_data + offset_pps[i])[14]==0);
  //     assert((device_output_data + offset_pps[i])[15]==0);
  //   }
  // }
      
  sub_compact->Current().SetNumOutputRecords(
      outputfile_total_entry);

  // Calc WA
  // outputfile_total_size = 0;
  sub_compact->Current().SetTotalBytes(outputfile_total_size);
  sub_compact->Current().SetCSDTotalBytes(outputfile_total_size);
  
  
  ROCKS_LOG_INFO(
      db_options_.info_log,
      "[%s] [JOB %d] Compaction on CSD finished. Total output file size is %lu. Total output entiers is %lu.",
      sub_compact->compaction->column_family_data()->GetName().c_str(), job_id_,
      outputfile_total_size, outputfile_total_size
      );


  q.enqueueUnmapMemObject(file0_device, file_buffer[0]);
  q.enqueueUnmapMemObject(file1_device, file_buffer[1]);
  q.enqueueUnmapMemObject(file2_device, file_buffer[2]);
  q.enqueueUnmapMemObject(file3_device, file_buffer[3]);
  q.enqueueUnmapMemObject(host_data, host_data_buffer);

  q.enqueueUnmapMemObject(sst_buffer, sst_result);
  q.enqueueUnmapMemObject(index_block_buffer, index_block_result);
  q.enqueueUnmapMemObject(output_data, device_output_data);
  q.finish();

  sub_compact->status=sub_compact->status.OK();
  sub_compact->io_status=sub_compact->io_status.OK();
  CompactionServiceResult compaction_result;
  sub_compact->compaction_job_stats = compaction_result.stats;
  // std::cout<<"csd compaction finish\n";
  // exit(0);

  chrono_print_time(prev_time, "tail handle", ss);
  ss << "all_time: " << all_time / 1000000.0  << " ms\n";
  ss << "----------------------------------------------\n";

  std::cout << ss.str().c_str();

  auto time2 = std::chrono::duration_cast<std::chrono::milliseconds>(prev_time.time_since_epoch());
  std::cout << time2.count() << " ms at inner end\n";

  return CompactionServiceJobStatus::kSuccess;
}


CompactionServiceJobStatus
CompactionJob::ProcessKeyValueCompactionWithCompactionService(
    SubcompactionState* sub_compact) {
  assert(sub_compact);
  assert(sub_compact->compaction);
  assert(db_options_.compaction_service);

  const Compaction* compaction = sub_compact->compaction;
  CompactionServiceInput compaction_input;
  compaction_input.output_level = compaction->output_level();
  compaction_input.db_id = db_id_;

  const std::vector<CompactionInputFiles>& inputs =
      *(compact_->compaction->inputs());
  for (const auto& files_per_level : inputs) {
    for (const auto& file : files_per_level.files) {
      compaction_input.input_files.emplace_back(
          MakeTableFileName(file->fd.GetNumber()));
    }
  }
  compaction_input.column_family.name =
      compaction->column_family_data()->GetName();
  compaction_input.column_family.options =
      compaction->column_family_data()->GetLatestCFOptions();
  compaction_input.db_options =
      BuildDBOptions(db_options_, mutable_db_options_copy_);
  compaction_input.snapshots = existing_snapshots_;
  compaction_input.has_begin = sub_compact->start.has_value();
  compaction_input.begin =
      compaction_input.has_begin ? sub_compact->start->ToString() : "";
  compaction_input.has_end = sub_compact->end.has_value();
  compaction_input.end =
      compaction_input.has_end ? sub_compact->end->ToString() : "";

  std::string compaction_input_binary;
  Status s = compaction_input.Write(&compaction_input_binary);
  if (!s.ok()) {
    sub_compact->status = s;
    return CompactionServiceJobStatus::kFailure;
  }

  std::ostringstream input_files_oss;
  bool is_first_one = true;
  for (const auto& file : compaction_input.input_files) {
    input_files_oss << (is_first_one ? "" : ", ") << file;
    is_first_one = false;
  }

  ROCKS_LOG_INFO(
      db_options_.info_log,
      "[%s] [JOB %d] Starting remote compaction (output level: %d): %s",
      compaction_input.column_family.name.c_str(), job_id_,
      compaction_input.output_level, input_files_oss.str().c_str());
  CompactionServiceJobInfo info(dbname_, db_id_, db_session_id_,
                                GetCompactionId(sub_compact), thread_pri_);
  CompactionServiceJobStatus compaction_status =
      db_options_.compaction_service->StartV2(info, compaction_input_binary);
  switch (compaction_status) {
    case CompactionServiceJobStatus::kSuccess:
      break;
    case CompactionServiceJobStatus::kFailure:
      sub_compact->status = Status::Incomplete(
          "CompactionService failed to start compaction job.");
      ROCKS_LOG_WARN(db_options_.info_log,
                     "[%s] [JOB %d] Remote compaction failed to start.",
                     compaction_input.column_family.name.c_str(), job_id_);
      return compaction_status;
    case CompactionServiceJobStatus::kUseLocal:
      ROCKS_LOG_INFO(
          db_options_.info_log,
          "[%s] [JOB %d] Remote compaction fallback to local by API Start.",
          compaction_input.column_family.name.c_str(), job_id_);
      return compaction_status;
    default:
      assert(false);  // unknown status
      break;
  }

  ROCKS_LOG_INFO(db_options_.info_log,
                 "[%s] [JOB %d] Waiting for remote compaction...",
                 compaction_input.column_family.name.c_str(), job_id_);
  std::string compaction_result_binary;
  compaction_status = db_options_.compaction_service->WaitForCompleteV2(
      info, &compaction_result_binary);

  if (compaction_status == CompactionServiceJobStatus::kUseLocal) {
    ROCKS_LOG_INFO(db_options_.info_log,
                   "[%s] [JOB %d] Remote compaction fallback to local by API "
                   "WaitForComplete.",
                   compaction_input.column_family.name.c_str(), job_id_);
    return compaction_status;
  }

  CompactionServiceResult compaction_result;
  s = CompactionServiceResult::Read(compaction_result_binary,
                                    &compaction_result);

  if (compaction_status == CompactionServiceJobStatus::kFailure) {
    if (s.ok()) {
      if (compaction_result.status.ok()) {
        sub_compact->status = Status::Incomplete(
            "CompactionService failed to run the compaction job (even though "
            "the internal status is okay).");
      } else {
        // set the current sub compaction status with the status returned from
        // remote
        sub_compact->status = compaction_result.status;
      }
    } else {
      sub_compact->status = Status::Incomplete(
          "CompactionService failed to run the compaction job (and no valid "
          "result is returned).");
      compaction_result.status.PermitUncheckedError();
    }
    ROCKS_LOG_WARN(db_options_.info_log,
                   "[%s] [JOB %d] Remote compaction failed.",
                   compaction_input.column_family.name.c_str(), job_id_);
    return compaction_status;
  }

  if (!s.ok()) {
    sub_compact->status = s;
    compaction_result.status.PermitUncheckedError();
    return CompactionServiceJobStatus::kFailure;
  }
  sub_compact->status = compaction_result.status;

  std::ostringstream output_files_oss;
  is_first_one = true;
  for (const auto& file : compaction_result.output_files) {
    output_files_oss << (is_first_one ? "" : ", ") << file.file_name;
    is_first_one = false;
  }

  ROCKS_LOG_INFO(db_options_.info_log,
                 "[%s] [JOB %d] Receive remote compaction result, output path: "
                 "%s, files: %s",
                 compaction_input.column_family.name.c_str(), job_id_,
                 compaction_result.output_path.c_str(),
                 output_files_oss.str().c_str());

  if (!s.ok()) {
    sub_compact->status = s;
    return CompactionServiceJobStatus::kFailure;
  }

  for (const auto& file : compaction_result.output_files) {
    uint64_t file_num = versions_->NewFileNumber();
    auto src_file = compaction_result.output_path + "/" + file.file_name;
    auto now_output_file_id = compact_->compaction->get_new_output_path_id();
    auto tgt_file = TableFileName(compaction->immutable_options()->cf_paths,
                                  file_num, now_output_file_id);
    s = fs_->RenameFile(src_file, tgt_file, IOOptions(), nullptr);
    if (!s.ok()) {
      sub_compact->status = s;
      return CompactionServiceJobStatus::kFailure;
    }

    FileMetaData meta;
    uint64_t file_size;
    s = fs_->GetFileSize(tgt_file, IOOptions(), &file_size, nullptr);
    if (!s.ok()) {
      sub_compact->status = s;
      return CompactionServiceJobStatus::kFailure;
    }
    meta.fd = FileDescriptor(file_num, now_output_file_id, file_size,
                             file.smallest_seqno, file.largest_seqno);
    meta.smallest.DecodeFrom(file.smallest_internal_key);
    meta.largest.DecodeFrom(file.largest_internal_key);
    meta.oldest_ancester_time = file.oldest_ancester_time;
    meta.file_creation_time = file.file_creation_time;
    meta.epoch_number = file.epoch_number;
    meta.marked_for_compaction = file.marked_for_compaction;
    meta.unique_id = file.unique_id;

    auto cfd = compaction->column_family_data();
    sub_compact->Current().AddOutput(std::move(meta),
                                     cfd->internal_comparator(), false, true,
                                     file.paranoid_hash);
  }
  sub_compact->compaction_job_stats = compaction_result.stats;
  sub_compact->Current().SetNumOutputRecords(
      compaction_result.num_output_records);
  sub_compact->Current().SetTotalBytes(compaction_result.total_bytes);
  RecordTick(stats_, REMOTE_COMPACT_READ_BYTES, compaction_result.bytes_read);
  RecordTick(stats_, REMOTE_COMPACT_WRITE_BYTES,
             compaction_result.bytes_written);
  return CompactionServiceJobStatus::kSuccess;
}

std::string CompactionServiceCompactionJob::GetTableFileName(
    uint64_t file_number) {
  return MakeTableFileName(output_path_, file_number);
}

void CompactionServiceCompactionJob::RecordCompactionIOStats() {
  compaction_result_->bytes_read += IOSTATS(bytes_read);
  compaction_result_->bytes_written += IOSTATS(bytes_written);
  CompactionJob::RecordCompactionIOStats();
}

CompactionServiceCompactionJob::CompactionServiceCompactionJob(
    int job_id, Compaction* compaction, const ImmutableDBOptions& db_options,
    const MutableDBOptions& mutable_db_options, const FileOptions& file_options,
    VersionSet* versions, const std::atomic<bool>* shutting_down,
    LogBuffer* log_buffer, FSDirectory* output_directory, Statistics* stats,
    InstrumentedMutex* db_mutex, ErrorHandler* db_error_handler,
    std::vector<SequenceNumber> existing_snapshots,
    std::shared_ptr<Cache> table_cache, EventLogger* event_logger,
    const std::string& dbname, const std::shared_ptr<IOTracer>& io_tracer,
    const std::atomic<bool>& manual_compaction_canceled,
    const std::string& db_id, const std::string& db_session_id,
    std::string output_path,
    const CompactionServiceInput& compaction_service_input,
    CompactionServiceResult* compaction_service_result)
    : CompactionJob(
          job_id, compaction, db_options, mutable_db_options, file_options,
          versions, shutting_down, log_buffer, nullptr, output_directory,
          nullptr, stats, db_mutex, db_error_handler,
          std::move(existing_snapshots), kMaxSequenceNumber, nullptr, nullptr,
          std::move(table_cache), event_logger,
          compaction->mutable_cf_options()->paranoid_file_checks,
          compaction->mutable_cf_options()->report_bg_io_stats, dbname,
          &(compaction_service_result->stats), Env::Priority::USER, io_tracer,
          manual_compaction_canceled, db_id, db_session_id,
          compaction->column_family_data()->GetFullHistoryTsLow()),
      output_path_(std::move(output_path)),
      compaction_input_(compaction_service_input),
      compaction_result_(compaction_service_result) {}

Status CompactionServiceCompactionJob::Run() {
  AutoThreadOperationStageUpdater stage_updater(
      ThreadStatus::STAGE_COMPACTION_RUN);

  auto* c = compact_->compaction;
  assert(c->column_family_data() != nullptr);
  assert(c->column_family_data()->current()->storage_info()->NumLevelFiles(
             compact_->compaction->level()) > 0);

  write_hint_ =
      c->column_family_data()->CalculateSSTWriteHint(c->output_level());
  bottommost_level_ = c->bottommost_level();

  Slice begin = compaction_input_.begin;
  Slice end = compaction_input_.end;
  compact_->sub_compact_states.emplace_back(
      c,
      compaction_input_.has_begin ? std::optional<Slice>(begin)
                                  : std::optional<Slice>(),
      compaction_input_.has_end ? std::optional<Slice>(end)
                                : std::optional<Slice>(),
      /*sub_job_id*/ 0);

  log_buffer_->FlushBufferToLog();
  LogCompaction();
  const uint64_t start_micros = db_options_.clock->NowMicros();
  // Pick the only sub-compaction we should have
  assert(compact_->sub_compact_states.size() == 1);
  SubcompactionState* sub_compact = compact_->sub_compact_states.data();

  ProcessKeyValueCompaction(sub_compact);

  compaction_stats_.stats.micros =
      db_options_.clock->NowMicros() - start_micros;
  compaction_stats_.stats.cpu_micros =
      sub_compact->compaction_job_stats.cpu_micros;

  RecordTimeToHistogram(stats_, COMPACTION_TIME,
                        compaction_stats_.stats.micros);
  RecordTimeToHistogram(stats_, COMPACTION_CPU_TIME,
                        compaction_stats_.stats.cpu_micros);

  Status status = sub_compact->status;
  IOStatus io_s = sub_compact->io_status;

  if (io_status_.ok()) {
    io_status_ = io_s;
  }

  if (status.ok()) {
    constexpr IODebugContext* dbg = nullptr;

    if (output_directory_) {
      io_s = output_directory_->FsyncWithDirOptions(IOOptions(), dbg,
                                                    DirFsyncOptions());
    }
  }
  if (io_status_.ok()) {
    io_status_ = io_s;
  }
  if (status.ok()) {
    status = io_s;
  }
  if (status.ok()) {
    // TODO: Add verify_table()
  }

  // Finish up all book-keeping to unify the subcompaction results
  compact_->AggregateCompactionStats(compaction_stats_, *compaction_job_stats_);
  UpdateCompactionStats();
  RecordCompactionIOStats();

  LogFlush(db_options_.info_log);
  compact_->status = status;
  compact_->status.PermitUncheckedError();

  // Build compaction result
  compaction_result_->output_level = compact_->compaction->output_level();
  compaction_result_->output_path = output_path_;
  for (const auto& output_file : sub_compact->GetOutputs()) {
    auto& meta = output_file.meta;
    compaction_result_->output_files.emplace_back(
        MakeTableFileName(meta.fd.GetNumber()), meta.fd.smallest_seqno,
        meta.fd.largest_seqno, meta.smallest.Encode().ToString(),
        meta.largest.Encode().ToString(), meta.oldest_ancester_time,
        meta.file_creation_time, meta.epoch_number,
        output_file.validator.GetHash(), meta.marked_for_compaction,
        meta.unique_id);
  }
  InternalStats::CompactionStatsFull compaction_stats;
  sub_compact->AggregateCompactionStats(compaction_stats);
  compaction_result_->num_output_records =
      compaction_stats.stats.num_output_records;
  compaction_result_->total_bytes = compaction_stats.TotalBytesWritten();

  return status;
}

void CompactionServiceCompactionJob::CleanupCompaction() {
  CompactionJob::CleanupCompaction();
}

// Internal binary format for the input and result data
enum BinaryFormatVersion : uint32_t {
  kOptionsString = 1,  // Use string format similar to Option string format
};

static std::unordered_map<std::string, OptionTypeInfo> cfd_type_info = {
    {"name",
     {offsetof(struct ColumnFamilyDescriptor, name), OptionType::kEncodedString,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
    {"options",
     {offsetof(struct ColumnFamilyDescriptor, options),
      OptionType::kConfigurable, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone,
      [](const ConfigOptions& opts, const std::string& /*name*/,
         const std::string& value, void* addr) {
        auto cf_options = static_cast<ColumnFamilyOptions*>(addr);
        return GetColumnFamilyOptionsFromString(opts, ColumnFamilyOptions(),
                                                value, cf_options);
      },
      [](const ConfigOptions& opts, const std::string& /*name*/,
         const void* addr, std::string* value) {
        const auto cf_options = static_cast<const ColumnFamilyOptions*>(addr);
        std::string result;
        auto status =
            GetStringFromColumnFamilyOptions(opts, *cf_options, &result);
        *value = "{" + result + "}";
        return status;
      },
      [](const ConfigOptions& opts, const std::string& name, const void* addr1,
         const void* addr2, std::string* mismatch) {
        const auto this_one = static_cast<const ColumnFamilyOptions*>(addr1);
        const auto that_one = static_cast<const ColumnFamilyOptions*>(addr2);
        auto this_conf = CFOptionsAsConfigurable(*this_one);
        auto that_conf = CFOptionsAsConfigurable(*that_one);
        std::string mismatch_opt;
        bool result =
            this_conf->AreEquivalent(opts, that_conf.get(), &mismatch_opt);
        if (!result) {
          *mismatch = name + "." + mismatch_opt;
        }
        return result;
      }}},
};

static std::unordered_map<std::string, OptionTypeInfo> cs_input_type_info = {
    {"column_family",
     OptionTypeInfo::Struct(
         "column_family", &cfd_type_info,
         offsetof(struct CompactionServiceInput, column_family),
         OptionVerificationType::kNormal, OptionTypeFlags::kNone)},
    {"db_options",
     {offsetof(struct CompactionServiceInput, db_options),
      OptionType::kConfigurable, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone,
      [](const ConfigOptions& opts, const std::string& /*name*/,
         const std::string& value, void* addr) {
        auto options = static_cast<DBOptions*>(addr);
        return GetDBOptionsFromString(opts, DBOptions(), value, options);
      },
      [](const ConfigOptions& opts, const std::string& /*name*/,
         const void* addr, std::string* value) {
        const auto options = static_cast<const DBOptions*>(addr);
        std::string result;
        auto status = GetStringFromDBOptions(opts, *options, &result);
        *value = "{" + result + "}";
        return status;
      },
      [](const ConfigOptions& opts, const std::string& name, const void* addr1,
         const void* addr2, std::string* mismatch) {
        const auto this_one = static_cast<const DBOptions*>(addr1);
        const auto that_one = static_cast<const DBOptions*>(addr2);
        auto this_conf = DBOptionsAsConfigurable(*this_one);
        auto that_conf = DBOptionsAsConfigurable(*that_one);
        std::string mismatch_opt;
        bool result =
            this_conf->AreEquivalent(opts, that_conf.get(), &mismatch_opt);
        if (!result) {
          *mismatch = name + "." + mismatch_opt;
        }
        return result;
      }}},
    {"snapshots", OptionTypeInfo::Vector<uint64_t>(
                      offsetof(struct CompactionServiceInput, snapshots),
                      OptionVerificationType::kNormal, OptionTypeFlags::kNone,
                      {0, OptionType::kUInt64T})},
    {"input_files", OptionTypeInfo::Vector<std::string>(
                        offsetof(struct CompactionServiceInput, input_files),
                        OptionVerificationType::kNormal, OptionTypeFlags::kNone,
                        {0, OptionType::kEncodedString})},
    {"output_level",
     {offsetof(struct CompactionServiceInput, output_level), OptionType::kInt,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
    {"db_id",
     {offsetof(struct CompactionServiceInput, db_id),
      OptionType::kEncodedString}},
    {"has_begin",
     {offsetof(struct CompactionServiceInput, has_begin), OptionType::kBoolean,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
    {"begin",
     {offsetof(struct CompactionServiceInput, begin),
      OptionType::kEncodedString, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone}},
    {"has_end",
     {offsetof(struct CompactionServiceInput, has_end), OptionType::kBoolean,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
    {"end",
     {offsetof(struct CompactionServiceInput, end), OptionType::kEncodedString,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
};

static std::unordered_map<std::string, OptionTypeInfo>
    cs_output_file_type_info = {
        {"file_name",
         {offsetof(struct CompactionServiceOutputFile, file_name),
          OptionType::kEncodedString, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"smallest_seqno",
         {offsetof(struct CompactionServiceOutputFile, smallest_seqno),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"largest_seqno",
         {offsetof(struct CompactionServiceOutputFile, largest_seqno),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"smallest_internal_key",
         {offsetof(struct CompactionServiceOutputFile, smallest_internal_key),
          OptionType::kEncodedString, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"largest_internal_key",
         {offsetof(struct CompactionServiceOutputFile, largest_internal_key),
          OptionType::kEncodedString, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"oldest_ancester_time",
         {offsetof(struct CompactionServiceOutputFile, oldest_ancester_time),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"file_creation_time",
         {offsetof(struct CompactionServiceOutputFile, file_creation_time),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"epoch_number",
         {offsetof(struct CompactionServiceOutputFile, epoch_number),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"paranoid_hash",
         {offsetof(struct CompactionServiceOutputFile, paranoid_hash),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"marked_for_compaction",
         {offsetof(struct CompactionServiceOutputFile, marked_for_compaction),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"unique_id",
         OptionTypeInfo::Array<uint64_t, 2>(
             offsetof(struct CompactionServiceOutputFile, unique_id),
             OptionVerificationType::kNormal, OptionTypeFlags::kNone,
             {0, OptionType::kUInt64T})},
};

static std::unordered_map<std::string, OptionTypeInfo>
    compaction_job_stats_type_info = {
        {"elapsed_micros",
         {offsetof(struct CompactionJobStats, elapsed_micros),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"cpu_micros",
         {offsetof(struct CompactionJobStats, cpu_micros), OptionType::kUInt64T,
          OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
        {"num_input_records",
         {offsetof(struct CompactionJobStats, num_input_records),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_blobs_read",
         {offsetof(struct CompactionJobStats, num_blobs_read),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_input_files",
         {offsetof(struct CompactionJobStats, num_input_files),
          OptionType::kSizeT, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_input_files_at_output_level",
         {offsetof(struct CompactionJobStats, num_input_files_at_output_level),
          OptionType::kSizeT, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_output_records",
         {offsetof(struct CompactionJobStats, num_output_records),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_output_files",
         {offsetof(struct CompactionJobStats, num_output_files),
          OptionType::kSizeT, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_output_files_blob",
         {offsetof(struct CompactionJobStats, num_output_files_blob),
          OptionType::kSizeT, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"is_full_compaction",
         {offsetof(struct CompactionJobStats, is_full_compaction),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"is_manual_compaction",
         {offsetof(struct CompactionJobStats, is_manual_compaction),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"total_input_bytes",
         {offsetof(struct CompactionJobStats, total_input_bytes),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"total_blob_bytes_read",
         {offsetof(struct CompactionJobStats, total_blob_bytes_read),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"total_output_bytes",
         {offsetof(struct CompactionJobStats, total_output_bytes),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"total_output_bytes_blob",
         {offsetof(struct CompactionJobStats, total_output_bytes_blob),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_records_replaced",
         {offsetof(struct CompactionJobStats, num_records_replaced),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"total_input_raw_key_bytes",
         {offsetof(struct CompactionJobStats, total_input_raw_key_bytes),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"total_input_raw_value_bytes",
         {offsetof(struct CompactionJobStats, total_input_raw_value_bytes),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_input_deletion_records",
         {offsetof(struct CompactionJobStats, num_input_deletion_records),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_expired_deletion_records",
         {offsetof(struct CompactionJobStats, num_expired_deletion_records),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_corrupt_keys",
         {offsetof(struct CompactionJobStats, num_corrupt_keys),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"file_write_nanos",
         {offsetof(struct CompactionJobStats, file_write_nanos),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"file_range_sync_nanos",
         {offsetof(struct CompactionJobStats, file_range_sync_nanos),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"file_fsync_nanos",
         {offsetof(struct CompactionJobStats, file_fsync_nanos),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"file_prepare_write_nanos",
         {offsetof(struct CompactionJobStats, file_prepare_write_nanos),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"smallest_output_key_prefix",
         {offsetof(struct CompactionJobStats, smallest_output_key_prefix),
          OptionType::kEncodedString, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"largest_output_key_prefix",
         {offsetof(struct CompactionJobStats, largest_output_key_prefix),
          OptionType::kEncodedString, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_single_del_fallthru",
         {offsetof(struct CompactionJobStats, num_single_del_fallthru),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_single_del_mismatch",
         {offsetof(struct CompactionJobStats, num_single_del_mismatch),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
};

namespace {
// this is a helper struct to serialize and deserialize class Status, because
// Status's members are not public.
struct StatusSerializationAdapter {
  uint8_t code;
  uint8_t subcode;
  uint8_t severity;
  std::string message;

  StatusSerializationAdapter() = default;
  explicit StatusSerializationAdapter(const Status& s) {
    code = s.code();
    subcode = s.subcode();
    severity = s.severity();
    auto msg = s.getState();
    message = msg ? msg : "";
  }

  Status GetStatus() const {
    return Status{static_cast<Status::Code>(code),
                  static_cast<Status::SubCode>(subcode),
                  static_cast<Status::Severity>(severity), message};
  }
};
}  // namespace

static std::unordered_map<std::string, OptionTypeInfo>
    status_adapter_type_info = {
        {"code",
         {offsetof(struct StatusSerializationAdapter, code),
          OptionType::kUInt8T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"subcode",
         {offsetof(struct StatusSerializationAdapter, subcode),
          OptionType::kUInt8T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"severity",
         {offsetof(struct StatusSerializationAdapter, severity),
          OptionType::kUInt8T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"message",
         {offsetof(struct StatusSerializationAdapter, message),
          OptionType::kEncodedString, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
};

static std::unordered_map<std::string, OptionTypeInfo> cs_result_type_info = {
    {"status",
     {offsetof(struct CompactionServiceResult, status),
      OptionType::kCustomizable, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone,
      [](const ConfigOptions& opts, const std::string& /*name*/,
         const std::string& value, void* addr) {
        auto status_obj = static_cast<Status*>(addr);
        StatusSerializationAdapter adapter;
        Status s = OptionTypeInfo::ParseType(
            opts, value, status_adapter_type_info, &adapter);
        *status_obj = adapter.GetStatus();
        return s;
      },
      [](const ConfigOptions& opts, const std::string& /*name*/,
         const void* addr, std::string* value) {
        const auto status_obj = static_cast<const Status*>(addr);
        StatusSerializationAdapter adapter(*status_obj);
        std::string result;
        Status s = OptionTypeInfo::SerializeType(opts, status_adapter_type_info,
                                                 &adapter, &result);
        *value = "{" + result + "}";
        return s;
      },
      [](const ConfigOptions& opts, const std::string& /*name*/,
         const void* addr1, const void* addr2, std::string* mismatch) {
        const auto status1 = static_cast<const Status*>(addr1);
        const auto status2 = static_cast<const Status*>(addr2);

        StatusSerializationAdapter adatper1(*status1);
        StatusSerializationAdapter adapter2(*status2);
        return OptionTypeInfo::TypesAreEqual(opts, status_adapter_type_info,
                                             &adatper1, &adapter2, mismatch);
      }}},
    {"output_files",
     OptionTypeInfo::Vector<CompactionServiceOutputFile>(
         offsetof(struct CompactionServiceResult, output_files),
         OptionVerificationType::kNormal, OptionTypeFlags::kNone,
         OptionTypeInfo::Struct("output_files", &cs_output_file_type_info, 0,
                                OptionVerificationType::kNormal,
                                OptionTypeFlags::kNone))},
    {"output_level",
     {offsetof(struct CompactionServiceResult, output_level), OptionType::kInt,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
    {"output_path",
     {offsetof(struct CompactionServiceResult, output_path),
      OptionType::kEncodedString, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone}},
    {"num_output_records",
     {offsetof(struct CompactionServiceResult, num_output_records),
      OptionType::kUInt64T, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone}},
    {"total_bytes",
     {offsetof(struct CompactionServiceResult, total_bytes),
      OptionType::kUInt64T, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone}},
    {"bytes_read",
     {offsetof(struct CompactionServiceResult, bytes_read),
      OptionType::kUInt64T, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone}},
    {"bytes_written",
     {offsetof(struct CompactionServiceResult, bytes_written),
      OptionType::kUInt64T, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone}},
    {"stats", OptionTypeInfo::Struct(
                  "stats", &compaction_job_stats_type_info,
                  offsetof(struct CompactionServiceResult, stats),
                  OptionVerificationType::kNormal, OptionTypeFlags::kNone)},
};

Status CompactionServiceInput::Read(const std::string& data_str,
                                    CompactionServiceInput* obj) {
  if (data_str.size() <= sizeof(BinaryFormatVersion)) {
    return Status::InvalidArgument("Invalid CompactionServiceInput string");
  }
  auto format_version = DecodeFixed32(data_str.data());
  if (format_version == kOptionsString) {
    ConfigOptions cf;
    cf.invoke_prepare_options = false;
    cf.ignore_unknown_options = true;
    return OptionTypeInfo::ParseType(
        cf, data_str.substr(sizeof(BinaryFormatVersion)), cs_input_type_info,
        obj);
  } else {
    return Status::NotSupported(
        "Compaction Service Input data version not supported: " +
        std::to_string(format_version));
  }
}

Status CompactionServiceInput::Write(std::string* output) {
  char buf[sizeof(BinaryFormatVersion)];
  EncodeFixed32(buf, kOptionsString);
  output->append(buf, sizeof(BinaryFormatVersion));
  ConfigOptions cf;
  cf.invoke_prepare_options = false;
  return OptionTypeInfo::SerializeType(cf, cs_input_type_info, this, output);
}

Status CompactionServiceResult::Read(const std::string& data_str,
                                     CompactionServiceResult* obj) {
  if (data_str.size() <= sizeof(BinaryFormatVersion)) {
    return Status::InvalidArgument("Invalid CompactionServiceResult string");
  }
  auto format_version = DecodeFixed32(data_str.data());
  if (format_version == kOptionsString) {
    ConfigOptions cf;
    cf.invoke_prepare_options = false;
    cf.ignore_unknown_options = true;
    return OptionTypeInfo::ParseType(
        cf, data_str.substr(sizeof(BinaryFormatVersion)), cs_result_type_info,
        obj);
  } else {
    return Status::NotSupported(
        "Compaction Service Result data version not supported: " +
        std::to_string(format_version));
  }
}

Status CompactionServiceResult::Write(std::string* output) {
  char buf[sizeof(BinaryFormatVersion)];
  EncodeFixed32(buf, kOptionsString);
  output->append(buf, sizeof(BinaryFormatVersion));
  ConfigOptions cf;
  cf.invoke_prepare_options = false;
  return OptionTypeInfo::SerializeType(cf, cs_result_type_info, this, output);
}

#ifndef NDEBUG
bool CompactionServiceResult::TEST_Equals(CompactionServiceResult* other) {
  std::string mismatch;
  return TEST_Equals(other, &mismatch);
}

bool CompactionServiceResult::TEST_Equals(CompactionServiceResult* other,
                                          std::string* mismatch) {
  ConfigOptions cf;
  cf.invoke_prepare_options = false;
  return OptionTypeInfo::TypesAreEqual(cf, cs_result_type_info, this, other,
                                       mismatch);
}

bool CompactionServiceInput::TEST_Equals(CompactionServiceInput* other) {
  std::string mismatch;
  return TEST_Equals(other, &mismatch);
}

bool CompactionServiceInput::TEST_Equals(CompactionServiceInput* other,
                                         std::string* mismatch) {
  ConfigOptions cf;
  cf.invoke_prepare_options = false;
  return OptionTypeInfo::TypesAreEqual(cf, cs_input_type_info, this, other,
                                       mismatch);
}
#endif  // NDEBUG
}  // namespace ROCKSDB_NAMESPACE

