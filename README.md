# CSD-RocksDB

This repository provides our implementation of **RocksDB accelerated with Samsung SmartSSD**, where we offload **compaction computation** from the host CPU to the **Computational Storage Device (CSD)**. By leveraging the near-data processing capability of SmartSSD, our design significantly improves the overall system throughput.

> **Hardware Requirement**: At least one Samsung SmartSSD device is required to run this system.

The repository is organized into three main components:

- **Accelerator kernel**: FPGA kernels designed for CSD offloading, optimized for different key-value lengths.
- **Experiment result**: Scripts and logs for reproducing the experiments reported in our paper.
- **rocksdb-9.0.0**: A modified version of RocksDB extended with CSD-based compaction support.

## Get Started

### 1. Environment Setup

Before using this repository, both **software** and **hardware** environments need to be configured. Please refer to the official [Samsung SmartSSD User Guide](https://docs.amd.com/v/u/en-US/ug1382-smartssd-csd) for detailed installation steps.

- **Software Environment**:
   Follow the guide to install the **Xilinx Runtime (XRT)** environment on the host system.
- **Hardware Environment**:
   When adding a SmartSSD to the system, follow the hardware installation steps in the official documentation.

------

### 2. SmartSSD Installation & Programming

After physically installing the SmartSSD, the device must be programmed with the base image before use.

1. Identify the SmartSSD device’s **Bus Device Function (BDF)** using:

   ```
   lspci | grep -i xilinx
   ```

   Example output:

   ```
   76:00.0 PCI bridge: Xilinx Corporation Device 9134
   77:00.0 PCI bridge: Xilinx Corporation Device 9234
   77:01.0 PCI bridge: Xilinx Corporation Device 9434
   79:00.0 Processing accelerators: Xilinx Corporation Device 6987
   79:00.1 Processing accelerators: Xilinx Corporation Device 6988
   ```

2. Program the SmartSSD with the base image:

   ```
   sudo /opt/xilinx/xrt/bin/xbmgmt program --base \
     --image /opt/xilinx/firmware/u2/gen3x4-xdma-gc/base/partition.xsabin \
     /opt/xilinx/firmware/u2/gen3x4-xdma-gc/base/partition.xsabin \
     --flash-type spi --device <BDF>
   ```

   Example:

   ```
   sudo /opt/xilinx/xrt/bin/xbmgmt program --base \
     --image /opt/xilinx/firmware/u2/gen3x4-xdma-gc/base/partition.xsabin \
     /opt/xilinx/firmware/u2/gen3x4-xdma-gc/base/partition.xsabin \
     --flash-type spi --device 0000:79:00.0
   ```

   > ⚠️ **Important**: After programming, you must **power off the host completely** (shut down and disconnect power). Restart the system to ensure the SmartSSD finishes flashing.

3. Verify the installation with:

   ```
   xbutil examine
   ```

If this step is successful, the SmartSSD is ready for use.

### 3. Accelerator Kernel Compilation (Optional)

This step is **optional**. If you already have pre-compiled kernels, you can skip to the next section.

1. **Install Xilinx Vitis HLS**
    Please follow the [official installation guide](https://www.amd.com/en/products/software/adaptive-socs-and-fpgas/vitis/vitis-hls.html).

   > In our experiments, we used **Vitis HLS v2022.2**.

2. **Locate Accelerator Kernels**
    The accelerator kernels are located in the **`Accelerator kernel/`** directory. We provide multiple kernel implementations optimized for different key-value lengths. Select the kernel version that matches your workload.

3. **Run Software Emulation**
    To quickly validate functionality at the software level:

   ```
   make run TARGET=sw_emu PLATFORM=xilinx_u2_gen3x4_xdma_gc_2_202110_1
   ```

4. **Run Hardware Emulation**
    For hardware-level validation (⚠️ much slower, recommend using smaller test datasets):

   ```
   make run TARGET=hw_emu PLATFORM=xilinx_u2_gen3x4_xdma_gc_2_202110_1
   ```

5. **Compile Hardware Kernel**
    Finally, compile the hardware kernel for deployment:

   ```
   make run TARGET=hw PLATFORM=xilinx_u2_gen3x4_xdma_gc_2_202110_1
   ```

   The generated kernel binary will be stored under:

   ```
   build_dir.hw.xilinx_u2_gen3x4_xdma_gc_2_202110_1/
   ```

### 4. Using CSD-Enabled RocksDB

Our repository provides an **extended version of RocksDB** with support for **CSD-offloaded compaction**. The modified source code is located under the **`rocksdb-9.0.0/`** directory.

#### 4.1 Compilation

The compilation process remains the same as the original RocksDB. For example:

```
make all -j8
```

For detailed instructions, please refer to the [official RocksDB documentation](https://github.com/facebook/rocksdb).

------

#### 4.2 New Options for CSD Compaction

We added several new configuration options under **`advanced_options`** to control CSD-based compaction.

##### (1) Select Compaction Device

A new enum `CompactionDevice` specifies whether compaction runs on CPU or CSD:

```
enum CompactionDevice : char {
  kCompactionOnCPU = 0x0,  // default: CPU compaction
  kCompactionOnCSD = 0x1,  // CSD-offloaded compaction
};
```

Usage example:

```
open_options_.compaction_device = kCompactionOnCSD;
```

By default, RocksDB performs compaction on the CPU. To enable CSD offloading, explicitly set the option to `kCompactionOnCSD`.

------

##### (2) Specify Accelerator Kernel Path

You must specify the path to the compiled CSD accelerator kernel (`.xclbin`):

```
open_options_.CompactionKernelPath = "/home/usr/Compaction_kernel_path/compaction_k32v1024.xclbin";
```

------

##### (3) Configure Accelerator Devices

Define the number of accelerators and assign IDs for each CSD device.

- First, resize the buffer:

  ```
  open_options_.Compaction_accelerator_id.resize(acc_num);
  ```

- Then, provide device IDs:

  ```
  open_options_.Compaction_accelerator_id = {1, 0, 2, 3};
  ```

> For details on accelerator ID mapping, please refer to **Appendix A: CSD–Accelerator Mapping**.

------

##### (4) Configure CSD Scheduling Policy

We define scheduling strategies via `CompactionCSDPolicy`:

```
enum CompactionCSDPolicy : char {
  kCompactionLessThan4 = 0x0,   // policy for <4 files
  kCompactionCSDArray  = 0x1,   // array scheduling
  kCompactionCSDArrayScheduleOff = 0x2, // array scheduling disabled
};
```

Usage example:

```
open_options_.compaction_csd_policy = kCompactionCSDArray;
```

------

##### (5) Configure SSTable File Size Policy

We provide different strategies to control the output file size when CSD generates SSTables, defined in `CompactionCSDGenSSTfileSizePolicy`:

```
enum CompactionCSDGenSSTfileSizePolicy : char {
  kCompactionCSDSSTavg      = 0x0, // output = input size average
  KCompactionCSDSSTabove64  = 0x1, // output = 64MB
  KCompactionCSDSSTlayer    = 0x2, // output = 64MB * level
  kCompactionCSDSSTwtosmall = 0x3, // output ≈ max(input avg, 64MB) to avoid tiny SSTs
};
```

Usage example:

```
open_options_.compaction_csd_gen_sst_file_size_policy = KCompactionCSDSSTlayer;
```

This option provides **initial tuning strategies**. You may further customize it according to your workload and RocksDB configuration. Future releases will offer extended support for dynamic integration with RocksDB’s configuration.

#### 4.3 Multi-CSD Deployment

When using **multiple CSD devices**, you must configure **`cf_paths`** to assign one path per CSD. Each column family (CF) can then be mapped to a different SmartSSD.

Example configuration:

```
open_options_.cf_paths = {
  "/mnt/csd0/db/",
  "/mnt/csd1/db/",
  "/mnt/csd2/db/",
  "/mnt/csd3/db/"
};
```

Here, each directory corresponds to a separate CSD device. Make sure the number of paths matches the number of CSDs you intend to use.