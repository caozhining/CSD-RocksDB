/**
 * Copyright (C) 2019-2021 Xilinx, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may
 * not use this file except in compliance with the License. A copy of the
 * License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

#include "vadd.h"
#include <fstream>
#include <iostream>
#include <string>
#include <cstring>
#include <stdlib.h>

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

#include "ap_int.h"
#include "host_process.h"
#include "krnl_host.h"

#include <CL/cl2.hpp>
#include <CL/cl_ext_xilinx.h>
#include <thread>
#include <cerrno>

#define OCL_CHECK(error, call)                                                                   \
    call;                                                                                        \
    if (error != CL_SUCCESS)                                                                     \
    {                                                                                            \
        printf("%s:%d Error calling " #call ", error code is: %d\n", __FILE__, __LINE__, error); \
        exit(EXIT_FAILURE);                                                                      \
    }

static const int DATA_SIZE = 4;

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

void cl_ns_print_ms(cl_ulong ns, std::string str)
{
    std::cout << str << " time: " << ns / 1000000.0 << " ms\n";
}

void check_pwrite(ssize_t write_size)
{
  if (write_size == -1) {
    std::cerr << "Error in pwrite: " << strerror(errno) << std::endl;
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

// 对齐到4k
#define ALIGN_TO_4K(x) (((x) + 4095) & ~4095)
#define CSD_ALIGN_TO_4K(x) (((x) + 4095) & ~4095)

std::string compaction_csd_gen_sst_file_size_policy = "kCompactionCSDSSTavg";
int compaction_output_level = 2;

std::string CompactionKernelPath = "/home/yjr/projects/compaction_varkey/build_dir.hw.xilinx_u2_gen3x4_xdma_gc_2_202110_1/compaction_k32v1024_20250606.xclbin";
long unsigned int compaction_on_csd_threads = 1;
std::vector<uint32_t> Compaction_accelerator_id;

cl::Device Compaction_accelerator_device[4];
cl::Context Compaction_csd_context[4];
cl::CommandQueue Compaction_csd_queue[4];
cl::Kernel Compaction_accelerator_kernel[4];
// cl::Buffer input_sst_buffer[4];
cl::Buffer input_metadata_buffer[4];
cl::Buffer output_datablock_buffer[4];
cl::Buffer output_indexblock_buffer[4];
cl::Buffer output_metadata_buffer[4];

void prepare_opencl(std::string &CompactionKernelPath)
{
    std::vector<cl::Device> devices;
      cl_int err;
      std::vector<cl::Platform> platforms;
      bool found_device = false;

      // traversing all Platforms To find Xilinx Platform and targeted
      // Device in Xilinx Platform
      cl::Platform::get(&platforms);
      for (size_t i = 0; (i < platforms.size()) & (found_device == false); i++) {
          cl::Platform platform = platforms[i];
          std::string platformName = platform.getInfo<CL_PLATFORM_NAME>();
          if (platformName == "Xilinx") {
              devices.clear();
              platform.getDevices(CL_DEVICE_TYPE_ACCELERATOR, &devices);
              if (devices.size()) {
                  found_device = true;
                  break;
              }
          }
      }
      if (found_device == false) {
          std::cout << "Error: Unable to find Target Device " << std::endl;
      }


      // std::cout << "INFO: Reading " << xclbinFilename << std::endl;
      FILE* fp;
      if ((fp = fopen(CompactionKernelPath.c_str(), "r")) == nullptr) {
          printf("ERROR: %s xclbin not available please build\n", CompactionKernelPath.c_str());
        
      }
      // Load xclbin
      std::cout << "Loading: '" << CompactionKernelPath << "'\n";
      std::ifstream bin_file(CompactionKernelPath, std::ifstream::binary);
      bin_file.seekg(0, bin_file.end);
      unsigned nb = bin_file.tellg();
      bin_file.seekg(0, bin_file.beg);
      char* buf = new char[nb];
      bin_file.read(buf, nb);
      bin_file.close();

      // assert(Compaction_accelerator_id<=devices.size());

      // Creating Program from Binary File
      cl::Program::Binaries bins;
      bins.push_back({buf, nb});


      std::cout << "device size: " << devices.size() << std::endl;

      for (uint32_t i = 0 ; i < Compaction_accelerator_id.size(); i++){
        Compaction_accelerator_device[i] = devices[Compaction_accelerator_id[i]];
        Compaction_csd_context[i] = cl::Context(Compaction_accelerator_device[i], nullptr, nullptr, nullptr, &err);
        Compaction_csd_queue[i] = cl::CommandQueue(Compaction_csd_context[i], Compaction_accelerator_device[i], CL_QUEUE_PROFILING_ENABLE, &err);
        std::cout << "Trying to program device[" << Compaction_accelerator_id[i] << "]: " << Compaction_accelerator_device[i].getInfo<CL_DEVICE_NAME>() << std::endl;
        cl::Program program(Compaction_csd_context[i], {Compaction_accelerator_device[i]}, bins, nullptr, &err);
        if (err != CL_SUCCESS) {
          std::cout << "Failed to program device[" << Compaction_accelerator_id[i] << "] with xclbin file!\n";
        } else {
            std::cout << "Device[" << Compaction_accelerator_id[i] << "]: program successful!\n";
            Compaction_accelerator_kernel[i] = cl::Kernel(program, "krnl_vadd", &err);
        }

        input_metadata_buffer[i]=cl::Buffer(Compaction_csd_context[i], CL_MEM_READ_WRITE, sizeof(uint64_t) * 15, NULL, &err);

        cl_mem_ext_ptr_t sst_output_p2p_ext;
        sst_output_p2p_ext = {XCL_MEM_EXT_P2P_BUFFER, nullptr, 0};
        output_datablock_buffer[i]=cl::Buffer(Compaction_csd_context[i], CL_MEM_READ_WRITE | CL_MEM_EXT_PTR_XILINX, sizeof(char) * (450 * 4 * 1024 *1024), &sst_output_p2p_ext);
        cl_mem_ext_ptr_t sst_output_index_p2p_ext;
        sst_output_index_p2p_ext = {XCL_MEM_EXT_P2P_BUFFER, nullptr, 0};
        output_indexblock_buffer[i]=cl::Buffer(Compaction_csd_context[i], CL_MEM_READ_WRITE | CL_MEM_EXT_PTR_XILINX, sizeof(char) * (8 * 1024 *1024), &sst_output_index_p2p_ext);

        // output_datablock_buffer=cl::Buffer(Compaction_csd_context[i], CL_MEM_READ_WRITE , sizeof(char) * (768 * 1024 *1024), NULL, &err);
        // output_indexblock_buffer=cl::Buffer(Compaction_csd_context[i], CL_MEM_READ_WRITE, sizeof(char) * (8 * 1024 *1024), NULL, &err);
        output_metadata_buffer[i]=cl::Buffer(Compaction_csd_context[i], CL_MEM_READ_ONLY, sizeof(uint64_t) * (128*4 + 20), NULL, &err);

        Compaction_accelerator_kernel[i].setArg(4, input_metadata_buffer[i]);
        Compaction_accelerator_kernel[i].setArg(5, output_datablock_buffer[i]);
        Compaction_accelerator_kernel[i].setArg(6, output_indexblock_buffer[i]);
        Compaction_accelerator_kernel[i].setArg(7, output_metadata_buffer[i]);
      }

}
 
void compaction(int iter)
{
    std::stringstream ss;
  ss << "-----------compaction all times-----------\n";
  all_time = 0;
  std::chrono::high_resolution_clock::time_point prev_time = std::chrono::high_resolution_clock::now();
  auto time = std::chrono::duration_cast<std::chrono::milliseconds>(prev_time.time_since_epoch());
  std::cout << time.count() << " ms at inner start\n";
  
  std::cout<<"Run on CSD\n";

  chrono_print_time(prev_time, "program device", ss);

  cl_int err;
  uint32_t now_acc = 0;

  cl::Context context = Compaction_csd_context[now_acc];
  cl::CommandQueue q = Compaction_csd_queue[now_acc];
  cl::Kernel Compaction_kernel = Compaction_accelerator_kernel[now_acc];

  // std::fstream filestream[4];
  int fd[4];
  uint64_t file_size_host[4]={0,0,0,0};
  uint64_t file_buffer_size[4]={4096,4096,4096,4096};
  uint64_t file_input_entries[4]={0,0,0,0};
  uint64_t gen_sst_file_limit=0;

  //  char *file_names[119] = {
  //      "/home/yjr/projects/compaction_varkey/compaction_test/001549.sst","/home/yjr/projects/compaction_varkey/compaction_test/000992.sst","/home/yjr/projects/compaction_varkey/compaction_test/001545.sst","/home/yjr/projects/compaction_varkey/compaction_test/001559.sst","/home/yjr/projects/compaction_varkey/compaction_test/001338.sst","/home/yjr/projects/compaction_varkey/compaction_test/000961.sst","/home/yjr/projects/compaction_varkey/compaction_test/001564.sst","/home/yjr/projects/compaction_varkey/compaction_test/001577.sst","/home/yjr/projects/compaction_varkey/compaction_test/001523.sst","/home/yjr/projects/compaction_varkey/compaction_test/001527.sst","/home/yjr/projects/compaction_varkey/compaction_test/001464.sst","/home/yjr/projects/compaction_varkey/compaction_test/001482.sst","/home/yjr/projects/compaction_varkey/compaction_test/001441.sst","/home/yjr/projects/compaction_varkey/compaction_test/001063.sst","/home/yjr/projects/compaction_varkey/compaction_test/001298.sst","/home/yjr/projects/compaction_varkey/compaction_test/001360.sst","/home/yjr/projects/compaction_varkey/compaction_test/001519.sst","/home/yjr/projects/compaction_varkey/compaction_test/000843.sst","/home/yjr/projects/compaction_varkey/compaction_test/001484.sst","/home/yjr/projects/compaction_varkey/compaction_test/001080.sst","/home/yjr/projects/compaction_varkey/compaction_test/000831.sst","/home/yjr/projects/compaction_varkey/compaction_test/001179.sst","/home/yjr/projects/compaction_varkey/compaction_test/001538.sst","/home/yjr/projects/compaction_varkey/compaction_test/000870.sst","/home/yjr/projects/compaction_varkey/compaction_test/001604.sst","/home/yjr/projects/compaction_varkey/compaction_test/001272.sst","/home/yjr/projects/compaction_varkey/compaction_test/001444.sst","/home/yjr/projects/compaction_varkey/compaction_test/001463.sst","/home/yjr/projects/compaction_varkey/compaction_test/001532.sst","/home/yjr/projects/compaction_varkey/compaction_test/001511.sst","/home/yjr/projects/compaction_varkey/compaction_test/001539.sst","/home/yjr/projects/compaction_varkey/compaction_test/000665.sst","/home/yjr/projects/compaction_varkey/compaction_test/001382.sst","/home/yjr/projects/compaction_varkey/compaction_test/001501.sst","/home/yjr/projects/compaction_varkey/compaction_test/001321.sst","/home/yjr/projects/compaction_varkey/compaction_test/001560.sst","/home/yjr/projects/compaction_varkey/compaction_test/001478.sst","/home/yjr/projects/compaction_varkey/compaction_test/000809.sst","/home/yjr/projects/compaction_varkey/compaction_test/000968.sst","/home/yjr/projects/compaction_varkey/compaction_test/001429.sst","/home/yjr/projects/compaction_varkey/compaction_test/001426.sst","/home/yjr/projects/compaction_varkey/compaction_test/001580.sst","/home/yjr/projects/compaction_varkey/compaction_test/001281.sst","/home/yjr/projects/compaction_varkey/compaction_test/001563.sst","/home/yjr/projects/compaction_varkey/compaction_test/001535.sst","/home/yjr/projects/compaction_varkey/compaction_test/001418.sst","/home/yjr/projects/compaction_varkey/compaction_test/001566.sst","/home/yjr/projects/compaction_varkey/compaction_test/001391.sst","/home/yjr/projects/compaction_varkey/compaction_test/001473.sst","/home/yjr/projects/compaction_varkey/compaction_test/001572.sst","/home/yjr/projects/compaction_varkey/compaction_test/001437.sst","/home/yjr/projects/compaction_varkey/compaction_test/001556.sst","/home/yjr/projects/compaction_varkey/compaction_test/001524.sst","/home/yjr/projects/compaction_varkey/compaction_test/000795.sst","/home/yjr/projects/compaction_varkey/compaction_test/000810.sst","/home/yjr/projects/compaction_varkey/compaction_test/001490.sst","/home/yjr/projects/compaction_varkey/compaction_test/001570.sst","/home/yjr/projects/compaction_varkey/compaction_test/001465.sst","/home/yjr/projects/compaction_varkey/compaction_test/001327.sst","/home/yjr/projects/compaction_varkey/compaction_test/001520.sst","/home/yjr/projects/compaction_varkey/compaction_test/001450.sst","/home/yjr/projects/compaction_varkey/compaction_test/001471.sst","/home/yjr/projects/compaction_varkey/compaction_test/001446.sst","/home/yjr/projects/compaction_varkey/compaction_test/001494.sst","/home/yjr/projects/compaction_varkey/compaction_test/001472.sst","/home/yjr/projects/compaction_varkey/compaction_test/001369.sst","/home/yjr/projects/compaction_varkey/compaction_test/001586.sst","/home/yjr/projects/compaction_varkey/compaction_test/001015.sst","/home/yjr/projects/compaction_varkey/compaction_test/001568.sst","/home/yjr/projects/compaction_varkey/compaction_test/001197.sst","/home/yjr/projects/compaction_varkey/compaction_test/001496.sst","/home/yjr/projects/compaction_varkey/compaction_test/001375.sst","/home/yjr/projects/compaction_varkey/compaction_test/001502.sst","/home/yjr/projects/compaction_varkey/compaction_test/001505.sst","/home/yjr/projects/compaction_varkey/compaction_test/001554.sst","/home/yjr/projects/compaction_varkey/compaction_test/001189.sst","/home/yjr/projects/compaction_varkey/compaction_test/000723.sst","/home/yjr/projects/compaction_varkey/compaction_test/001291.sst","/home/yjr/projects/compaction_varkey/compaction_test/001529.sst","/home/yjr/projects/compaction_varkey/compaction_test/000991.sst","/home/yjr/projects/compaction_varkey/compaction_test/001598.sst","/home/yjr/projects/compaction_varkey/compaction_test/001449.sst","/home/yjr/projects/compaction_varkey/compaction_test/001485.sst","/home/yjr/projects/compaction_varkey/compaction_test/001561.sst","/home/yjr/projects/compaction_varkey/compaction_test/001552.sst","/home/yjr/projects/compaction_varkey/compaction_test/001408.sst","/home/yjr/projects/compaction_varkey/compaction_test/001528.sst","/home/yjr/projects/compaction_varkey/compaction_test/001433.sst","/home/yjr/projects/compaction_varkey/compaction_test/001401.sst","/home/yjr/projects/compaction_varkey/compaction_test/001506.sst","/home/yjr/projects/compaction_varkey/compaction_test/001423.sst","/home/yjr/projects/compaction_varkey/compaction_test/001076.sst","/home/yjr/projects/compaction_varkey/compaction_test/001516.sst","/home/yjr/projects/compaction_varkey/compaction_test/001458.sst","/home/yjr/projects/compaction_varkey/compaction_test/001540.sst","/home/yjr/projects/compaction_varkey/compaction_test/001384.sst","/home/yjr/projects/compaction_varkey/compaction_test/001168.sst","/home/yjr/projects/compaction_varkey/compaction_test/000884.sst","/home/yjr/projects/compaction_varkey/compaction_test/001209.sst","/home/yjr/projects/compaction_varkey/compaction_test/001110.sst","/home/yjr/projects/compaction_varkey/compaction_test/001569.sst","/home/yjr/projects/compaction_varkey/compaction_test/001100.sst","/home/yjr/projects/compaction_varkey/compaction_test/000735.sst","/home/yjr/projects/compaction_varkey/compaction_test/001432.sst","/home/yjr/projects/compaction_varkey/compaction_test/001445.sst","/home/yjr/projects/compaction_varkey/compaction_test/000471.sst","/home/yjr/projects/compaction_varkey/compaction_test/001092.sst","/home/yjr/projects/compaction_varkey/compaction_test/001218.sst","/home/yjr/projects/compaction_varkey/compaction_test/001123.sst","/home/yjr/projects/compaction_varkey/compaction_test/001562.sst","/home/yjr/projects/compaction_varkey/compaction_test/001592.sst","/home/yjr/projects/compaction_varkey/compaction_test/001546.sst","/home/yjr/projects/compaction_varkey/compaction_test/001558.sst","/home/yjr/projects/compaction_varkey/compaction_test/001309.sst","/home/yjr/projects/compaction_varkey/compaction_test/000788.sst","/home/yjr/projects/compaction_varkey/compaction_test/000979.sst","/home/yjr/projects/compaction_varkey/compaction_test/001555.sst","/home/yjr/projects/compaction_varkey/compaction_test/001454.sst","/home/yjr/projects/compaction_varkey/compaction_test/001264.sst"
  //    };
  //  int kvsums[119] = {160813, 223164, 104349, 111097, 253721, 1, 160122, 196065, 18462, 87704, 253721, 52849, 145542, 253721, 253721, 253721, 253721, 253721, 84745, 253721, 238921, 253721, 43994, 169716, 62005, 143333, 32917, 33358, 231939, 253721, 226112, 40493, 35567, 64631, 253721, 80941, 95848, 22247, 253721, 82811, 253721, 61992, 253721, 38269, 154982, 253721, 253721, 253721, 72074, 62005, 88649, 182563, 75265, 71622, 218789, 253721, 9591, 74808, 137264, 1, 89164, 44755, 88745, 79895, 253721, 253721, 62009, 145264, 62015, 253721, 76857, 73173, 149705, 67356, 62000, 203874, 253721, 139318, 152184, 13395, 62017, 53884, 38416, 45250, 183352, 180771, 57928, 101329, 141971, 87613, 253721, 123951, 58827, 253721, 253721, 105624, 253721, 216600, 253721, 107419, 253721, 253721, 2439, 29401, 164817, 36194, 158166, 26199, 2, 79921, 62007, 53071, 62003, 120939, 253721, 253721, 1089676, 48641, 253721};
  //  // int kvsums[4] = {1000, 1000, 1000, 1000};
  //  int input_file_num = 119;

  //  char *file_names[4] = {
  //   "/home/yjr/projects/compaction_varkey/compaction_test/001555.sst",
  //   "/home/yjr/projects/compaction_varkey/compaction_test/001568.sst",
  //   "/home/yjr/projects/compaction_varkey/compaction_test/001478.sst",
  //   "/home/yjr/projects/compaction_varkey/compaction_test/001464.sst"
  //  };
  //  int kvsums[4] = {1089676, 62015, 95848, 253721};
  //  int input_file_num = 4;

  // char *file_names[4] = {
  //   "/home/yjr/projects/compaction_varkey/compaction_test/001549.sst",
  //   "/home/yjr/projects/compaction_varkey/compaction_test/001568.sst",
  //   "/home/yjr/projects/compaction_varkey/compaction_test/001478.sst",
  //   "/home/yjr/projects/compaction_varkey/compaction_test/001464.sst"
  //   };
  // int kvsums[4] = {160813, 62015, 95848, 253721};
  // int input_file_num = 4;

  char *file_names[4] = { // key32 value1024
    "/home/yjr/projects/compaction_varkey/testdb/000376.sst",
    "/home/yjr/projects/compaction_varkey/testdb/000334.sst",
    "",
    ""
  };
  int kvsums[4] = {59067, 95357, 0, 0};
  int input_file_num = 2;

  struct stat file_stats[4] = {0};
      
  int file_count=0;
  for (int t = 0 ; t < 4; t++) {
  // srand(std::time(NULL) + t);
  //  int i = rand() % input_file_num;
  int i = t;
    std::cout << "file name: " << file_names[i] << "\n";
    std::cout << "entries: " << kvsums[i] << "\n";
    fd[file_count] = open(file_names[i], O_RDWR | O_DIRECT);
    std::cout<<"fd : " <<fd[file_count]<<"\n";

    file_input_entries[file_count] = kvsums[i];

    fstat(fd[file_count], &file_stats[file_count]);
    file_size_host[file_count]=file_stats[file_count].st_size;

    std::cout<<"file size : "<<file_size_host[file_count]<<"\n";

    file_buffer_size[file_count]=ALIGN_TO_4K(file_size_host[file_count]);

    file_count++;
  }

  gen_sst_file_limit = file_size_host[0] + file_size_host[1] + file_size_host[2] + file_size_host[3] + 5200 * 4;

  gen_sst_file_limit = ALIGN_TO_4K(gen_sst_file_limit);

  chrono_print_time(prev_time, "prepare file", ss);

  uint64_t sst_buffer_size = 1024 * 1024 * 768;
//  uint64_t sst_buffer_size = 1600000000;
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

  cl::Buffer host_data   = input_metadata_buffer[now_acc];
  cl::Buffer sst_buffer  = output_datablock_buffer[now_acc];
  cl::Buffer index_block_buffer = output_indexblock_buffer[now_acc];
  cl::Buffer output_data = output_metadata_buffer[now_acc];

  chrono_print_time(prev_time, "opencl", ss);

  // int narg = 0;
  Compaction_kernel.setArg(0, file0_device);
  Compaction_kernel.setArg(1, file1_device);
  Compaction_kernel.setArg(2, file2_device);
  Compaction_kernel.setArg(3, file3_device);

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

  device_output_data = (uint64_t *)q.enqueueMapBuffer(output_data, CL_TRUE, CL_MAP_READ, 0, sizeof(uint64_t) * (PPS_KERNEL_SIZE + 20), NULL, NULL, &err);

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

  uint64_t chunksize = 16777216;
  ssize_t read_size;
  for(int x=0; x<file_count; x++)
  {
    //  std::cout<<"--------------------\n";
      for (uint64_t j = 0; j < file_size_host[x] / chunksize; j++)
      {
          read_size = pread(fd[x], file_buffer[x] + chunksize * j, chunksize, j*chunksize);
        //  std::cout << "sst read " << j << ": " << read_size << " bytes\n";
      }
      if(file_size_host[x] % chunksize)
      {
          uint64_t j = file_size_host[x] / chunksize;
          // uint64_t read_length=file_size_host[x] % chunksize - 4;
          uint64_t read_length=file_size_host[x] % chunksize;
          read_size = pread(fd[x], file_buffer[x] + chunksize * j, CSD_ALIGN_TO_4K(read_length), j*chunksize);
        //  std::cout << "sst read " << j << ": " << read_size << " bytes\n";
      }
      close(fd[x]);
  }
//  std::cout<<"--------------------\n";

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
  uint64_t offset_index[MAX_OUTPUT_FILE_NUM], offset_pps[MAX_OUTPUT_FILE_NUM], offset_sst[MAX_OUTPUT_FILE_NUM];
  for (uint64_t i = 0; i < MAX_OUTPUT_FILE_NUM; i++){
      // i * 总大小 / 输入文件数 * 输出文件数
#ifndef CSD_KEY_16
      offset_sst[i] = CSD_ALIGN_TO_4K(offset[MAX_INPUT_FILE_NUM] / MAX_OUTPUT_FILE_NUM) * i;
      // offset_sst[i] = CSD_ALIGN_TO_4K(i*(offset[CSD_MAX_INPUT_FILE_NUM] / CSD_MAX_OUTPUT_FILE_NUM));
#else
      offset_sst[i] = i * (offset[CSD_MAX_INPUT_FILE_NUM] / CSD_MAX_OUTPUT_FILE_NUM);
#endif
      // std::cout<<"offset_sst " <<offset_sst[i]<<"\n";
  }
  for (uint64_t i = 0; i < MAX_OUTPUT_FILE_NUM; i++){
      offset_index[i] = i * (index_block_buffer_size / MAX_OUTPUT_FILE_NUM);
  }
  for (uint64_t i = 0; i < MAX_OUTPUT_FILE_NUM; i++){
      offset_pps[i] = i * PPS_KERNEL_SINGEL_SIZE;
  }

  // compaction service(subcompactionservice)


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

  static uint64_t test_largest_write_size = 0;
  static uint64_t test_largest_index_write_size = 0;

  for (int i = 0; i < MAX_OUTPUT_FILE_NUM; i++)
  {
    std::cout << "==============================================" << std::endl;
    uint64_t file_size;
    uint64_t sstlength, indexlength;
    sstlength=device_output_data[PPS_KERNEL_SIZE+i];
    indexlength=device_output_data[PPS_KERNEL_SIZE+MAX_OUTPUT_FILE_NUM+i];
    uint64_t entries = device_output_data[PPS_ENTRIES_OFF + offset_pps[i]];
    std::cout << "sstlength:" << sstlength << "\n";
    std::cout << "indexlength:" << indexlength << "\n";
    std::cout << "entries:" << entries << "\n";

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
    uint64_t file_id = i;

    std::cout<<"outputfile: "<<file_id<<"\n";

    std::string tgt_file = "/mnt/smartssd3/testdb/iter_" + std::to_string(iter) + "_output_sst_" + std::to_string(file_id) + ".sst";

    uint64_t index_block_offset = CSD_ALIGN_TO_4K(sstlength);
    uint64_t index_block_write_size = indexlength;
    uint64_t meta_data_offset = index_block_write_size + index_block_offset;
    pp_index = 0;
    metaindexblock_index = 0;
 
    std::string db_id_ = "embedded415-System-Product-Name";
    std::string db_session_id_ = "W5V0QY60E5FEWEPACUB1";
    TableProperties table_properties(db_id_,db_session_id_,file_id, indexlength-5);
    table_properties.putProperties(properties, pp_index, metaindexblock, metaindexblock_index, meta_data_offset, device_output_data + offset_pps[i]);
 
    // build footer
    // metaindexblock_offset:indexlength实际上是indexblock的index变量，所以需要加上hash长度5；pp_index同理
    putFooter(index_block_offset, indexlength-5, meta_data_offset + pp_index + 5, metaindexblock_index, footer_buffer);

    memcpy((index_block_result+offset_index[i]+index_block_write_size), properties, pp_index + 5);
    memcpy((index_block_result+offset_index[i]+index_block_write_size) + pp_index + 5, metaindexblock, metaindexblock_index + 5);
    memcpy((index_block_result+offset_index[i]+index_block_write_size) + pp_index + 5 + metaindexblock_index + 5, footer_buffer, 53);

    uint64_t otherlength = pp_index + 5 + metaindexblock_index + 5 + 53;
    index_block_write_size = CSD_ALIGN_TO_4K(index_block_write_size + otherlength);

    file_size = meta_data_offset + otherlength;

    std::cout<<"output act file size "<<file_size<<"\n";


    // write sst
    // int nvmeFd = open(tgt_file.c_str(), O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    int nvmeFd;
    do {
      nvmeFd = open(tgt_file.c_str(), O_RDWR | O_CREAT | O_TRUNC | O_DIRECT, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    } while (nvmeFd < 0 && errno == EINTR);
    if (nvmeFd == -1)
    {
      std::cerr << "nvme file open failed: " << std::strerror(errno) << std::endl;
      assert(0);
    }

    std::cout << "-----------------------------------" << std::endl;
    std::cout << "sst length: " << index_block_offset << "\n";

    write_size = pwrite(nvmeFd, sst_result + (offset_sst[i]), index_block_offset, 0);
    std::cout << "dram offset "<< (offset_sst[i])<<" end offset "<< offset_sst[i] + index_block_offset <<"\n" ;
    std::cout << "sst write: " << write_size << " bytes\n";
    assert(write_size>0);

    std::cout << "-----------------------------------" << std::endl;
    std::cout << "index length: " << index_block_write_size << "\n";
    
    write_size = pwrite(nvmeFd, index_block_result + (offset_index[i]), index_block_write_size, index_block_offset);
    std::cout << "dram offset "<< (offset_index[i])<<" end offset "<< offset_index[i] + index_block_write_size <<"\n" ;
    assert((offset_index[i] + index_block_write_size)<(8*1024*1024));
    std::cout << "index write: " << write_size << " bytes\n";
    if (!(write_size > 0))
    {
      std::cout << "ERROR: write size = " << write_size << std::endl;
    }
    assert(write_size>0);

    if (ftruncate(nvmeFd, file_size) != 0) {
      int err = errno;
      fprintf(stderr, "Error: ftruncate failed, errno=%d (%s)\n", err, strerror(err));
      assert(0);
    }

    close(nvmeFd);
    
    char smallest_key_buffer[KEY_LENGTH], largest_key_buffer[KEY_LENGTH];
    uint64_t smallest_key_length, largest_key_length;

    smallest_key_length=(device_output_data + offset_pps[i])[PPS_SMALLESTKEY_LENGTH_OFF];
    for(uint32_t j = 0; j < smallest_key_length / 8; j++)  // 128字节存在uint64_t，128/8=16，每个循环处理一个uint64_t
    {
      for (uint32_t k = 0; k < 8; k++)  // uint64_t共8字节
      {
        smallest_key_buffer[j*8+k] = (char)((((device_output_data + offset_pps[i])[PPS_SMALLESTKEY_OFF+j]) >> (k*8)) & 0xFF);
      }
    }
    largest_key_length=(device_output_data + offset_pps[i])[PPS_LARGESTKEY_LENGTH_OFF];
    for(uint32_t j = 0; j < largest_key_length / 8; j++)  // 128字节存在uint64_t，128/8=16，每个循环处理一个uint64_t
    {
      for (uint32_t k = 0; k < 8; k++)  // uint64_t共8字节
      {
        largest_key_buffer[j*8+k] = (char)((((device_output_data + offset_pps[i])[PPS_LARGESTKEY_OFF+j]) >> (k*8)) & 0xFF);
      }
    }

    printf("smallest key length: %ld\n", smallest_key_length);
    for(uint32_t j = 0; j < smallest_key_length / 8; j++)
    {
      for (uint32_t k = 0; k < 8; k++)  // uint64_t共8字节
      {
        printf("%02x ", (uint32_t)smallest_key_buffer[j*8+k] & 0xFF);
      }
    }
    printf("\n");
    printf("largest key length: %ld\n", largest_key_length);
    for(uint32_t j = 0; j < largest_key_length / 8; j++)
    {
      for (uint32_t k = 0; k < 8; k++)  // uint64_t共8字节
      {
        printf("%02x ", (uint32_t)largest_key_buffer[j*8+k] & 0xFF);
      }
    }
    printf("\n");
  }

  chrono_print_time(prev_time, "write file", ss);

  q.enqueueUnmapMemObject(file0_device, file_buffer[0]);
  q.enqueueUnmapMemObject(file1_device, file_buffer[1]);
  q.enqueueUnmapMemObject(file2_device, file_buffer[2]);
  q.enqueueUnmapMemObject(file3_device, file_buffer[3]);
  q.enqueueUnmapMemObject(host_data, host_data_buffer);

  q.enqueueUnmapMemObject(sst_buffer, sst_result);
  q.enqueueUnmapMemObject(index_block_buffer, index_block_result);
  q.enqueueUnmapMemObject(output_data, device_output_data);
  q.finish();

  std::cout<<"csd compaction finish\n";
  // exit(0);

  chrono_print_time(prev_time, "tail handle", ss);
  ss << "all_time: " << all_time / 1000000.0  << " ms\n";
  ss << "----------------------------------------------\n";

  std::cout << ss.str().c_str();

  auto time2 = std::chrono::duration_cast<std::chrono::milliseconds>(prev_time.time_since_epoch());
  std::cout << time2.count() << " ms at inner end\n";


}
 
int main(int argc, char *argv[])
{
  Compaction_accelerator_id.resize(4);
  Compaction_accelerator_id = {1,0,2,3};

    std::chrono::high_resolution_clock::time_point taskstart = std::chrono::high_resolution_clock::now();

    if (argc != 2)
  {
      std::cout << "Usage: " << argv[0] << " <xclbin>" << std::endl;
      return EXIT_FAILURE;
  }

  std::string xclbinFilename = argv[1];
    prepare_opencl(xclbinFilename);

    std::cout << "1\n";

    
    std::chrono::high_resolution_clock::time_point finddeviceend = std::chrono::high_resolution_clock::now();

    cl_ulong sum_compaction_time=0;

    for(int i=0;i<1;i++)
    {
        std::cout<<"-----------------------------------------------------------\n";
        std::cout<<"-----------------------------------------------------------\n";
        std::cout<<"=== Iteration\t"<<std::to_string(i)<<"\t======================\n";
        std::cout<<"-----------------------------------------------------------\n";

        std::chrono::high_resolution_clock::time_point compactionstart = std::chrono::high_resolution_clock::now();
        auto time = std::chrono::duration_cast<std::chrono::milliseconds>(compactionstart.time_since_epoch());
        std::cout << time.count() << " ms at outer start\n";
        compaction(i);
        std::chrono::high_resolution_clock::time_point compactionsend = std::chrono::high_resolution_clock::now();
        auto time2 = std::chrono::duration_cast<std::chrono::milliseconds>(compactionsend.time_since_epoch());
        std::cout << time2.count() << " ms at outer end\n";
        cl_ulong compactionTime = std::chrono::duration_cast<std::chrono::nanoseconds>(compactionsend - compactionstart).count();
        sum_compaction_time+=compactionTime;
        std::cout<<"-----------------------------------------------------------\n";
        std::cout<<"=== Iteration\t"<<std::to_string(i)<<"\t time: "<<compactionTime<<"ns ("<<compactionTime/1000000<<"ms)\n";
        std::cout<<"-----------------------------------------------------------\n";
        std::cout<<"-----------------------------------------------------------\n";
    }

    std::chrono::high_resolution_clock::time_point taskend = std::chrono::high_resolution_clock::now();
    cl_ulong taskTime = std::chrono::duration_cast<std::chrono::nanoseconds>(taskend - taskstart).count();
    cl_ulong task2Time = std::chrono::duration_cast<std::chrono::nanoseconds>(finddeviceend-taskstart).count();
    // cl_ulong initTime = std::chrono::duration_cast<std::chrono::nanoseconds>(initend - initstart).count();

    std::cout<<"\n";
    std::cout<<"= = = = = = = = = = = =\n";
    // std::cout<<"Task time: "<<taskTime<<" ns\n";
    // std::cout<<"Find device time: "<<task2Time<<" ns\n";
    // std::cout<<"Init properties time: "<<initTime<<" ns\n";
    // std::cout<<"Average compaction time: "<<sum_compaction_time/10<<" ns\n";
    cl_ns_print_ms(taskTime, "Task");
    cl_ns_print_ms(task2Time, "Find device");
    // cl_ns_print_ms(initTime, "Init properties");
    cl_ns_print_ms(sum_compaction_time/10, "Average compaction");
    std::cout<<"= = = = = = = = = = = =\n";
    std::cout<<"\n";

    return 0;
}
 