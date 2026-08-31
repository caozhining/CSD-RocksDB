# CSD-CoKV

[![IEEE ICDE 2026](https://img.shields.io/badge/IEEE%20ICDE-2026-00629B.svg)](https://ieeexplore.ieee.org/document/11629364) [![DOI](https://img.shields.io/badge/DOI-10.1109%2FICDE65706.2026.00016-blue.svg)](https://doi.org/10.1109/ICDE65706.2026.00016)

This repository contains the research prototype and experimental artifacts for our IEEE ICDE 2026 paper:

> **CSD-CoKV: Host-CSD Collaborative Offloading for High-Performance LSM-Tree Based KV Stores**<br> Zhining Cao _et al._<br> _2026 IEEE 42nd International Conference on Data Engineering (ICDE)_, pp. 113–126.

[IEEE Xplore](https://ieeexplore.ieee.org/document/11629364) · [DOI](https://doi.org/10.1109/ICDE65706.2026.00016) · [Author Preprint](https://caozhining.github.io/files/ICDE26_CSD_CoKV.pdf)

## Overview

CSD-CoKV is a hardware-software co-designed extension of RocksDB 9.0.0 that executes LSM-tree compaction collaboratively across the host CPU and an array of Samsung SmartSSD computational storage devices (CSDs).

Rather than blindly offloading every compaction job, CSD-CoKV introduces:

- a **semantic-aware offloader** that selects compaction jobs suitable for CSD execution;
- a lightweight, high-throughput **FPGA compaction kernel** deployed inside each SmartSSD; and
- a **CSD-friendly data partitioning scheme** that balances data placement and compaction workloads across a CSD array.

Compared with the host-only baseline evaluated in the paper, CSD-CoKV improves write throughput by up to **2.39×** and compaction throughput by up to **3.28×**.

> **Hardware requirement:** Running the CSD-offloading path requires at least one Samsung SmartSSD. The experiments in the paper use up to four SmartSSDs.

## Repository Structure

- [`Accelerator kernel/`](Accelerator%20kernel/) contains FPGA compaction kernel source code and precompiled `.xclbin` files. The provided configurations use 24-byte keys and value sizes of 256 B, 512 B, 1 KiB, and 2 KiB.
- [`rocksdb-9.0.0/`](rocksdb-9.0.0/) contains RocksDB 9.0.0 extended with CSD-aware compaction scheduling, accelerator control, and multi-CSD data placement.
- [`Experiment result/`](Experiment%20result/) contains experiment scripts, processed measurements, system-monitoring outputs, and raw logs used in the paper.

## Evaluation Environment

The experiments reported in the paper were conducted in the following environment. Other versions or platform images may require changes to the build configuration or host-side code.

| Component             | Configuration                    |
| --------------------- | -------------------------------- |
| Host CPU              | Intel Core i9-10980XE @ 3.00 GHz |
| Host memory           | 64 GB DDR4-3200                  |
| Operating system      | Ubuntu 20.04.6 LTS               |
| Linux kernel          | 5.15.0-107-generic               |
| RocksDB               | 9.0.0                            |
| Computational storage | 1–4 Samsung SmartSSDs            |
| CSD FPGA              | Xilinx Kintex UltraScale+ KU15P  |
| CSD DRAM              | 4 GB per SmartSSD                |
| CSD interface         | PCIe 3.0 ×4                      |
| NVMe expansion switch | PLX8747                          |
| XRT                   | 2.14.354                         |
| OpenCL                | 2.1.0                            |
| Vitis HLS             | 2022.2                           |

## Getting Started

### 1. Configure the SmartSSD Environment

Install and configure the hardware and software stack described in the [Samsung SmartSSD User Guide](https://docs.amd.com/v/u/en-US/ug1382-smartssd-csd). In particular, install the Xilinx Runtime (XRT) on the host before building or running CSD-CoKV.

### 2. Install and Program the SmartSSD

After physically installing the SmartSSD, program the device with the base image before use.

1. Identify the SmartSSD Bus Device Function (BDF):

   ```bash
   lspci | grep -i xilinx
   ```

   Example output:

   ```text
   76:00.0 PCI bridge: Xilinx Corporation Device 9134
   77:00.0 PCI bridge: Xilinx Corporation Device 9234
   77:01.0 PCI bridge: Xilinx Corporation Device 9434
   79:00.0 Processing accelerators: Xilinx Corporation Device 6987
   79:00.1 Processing accelerators: Xilinx Corporation Device 6988
   ```

2. Program the SmartSSD with the base image. Replace `<BDF>` with the BDF of the accelerator function on your system:

   ```bash
   sudo /opt/xilinx/xrt/bin/xbmgmt program --base \
     --image /opt/xilinx/firmware/u2/gen3x4-xdma-gc/base/partition.xsabin \
     /opt/xilinx/firmware/u2/gen3x4-xdma-gc/base/partition.xsabin \
     --flash-type spi --device <BDF>
   ```

   Example:

   ```bash
   sudo /opt/xilinx/xrt/bin/xbmgmt program --base \
     --image /opt/xilinx/firmware/u2/gen3x4-xdma-gc/base/partition.xsabin \
     /opt/xilinx/firmware/u2/gen3x4-xdma-gc/base/partition.xsabin \
     --flash-type spi --device 0000:79:00.0
   ```

   > [!IMPORTANT] After programming, power off the host completely and disconnect its power. Then restart the system so that the SmartSSD can finish flashing.

3. Verify the installation:

   ```bash
   xbutil examine
   ```

### 3. Build the Accelerator Kernel (Optional)

Precompiled kernels are included in the corresponding subdirectories under [`Accelerator kernel/`](Accelerator%20kernel/). Skip this step if a provided kernel matches your key/value configuration, SmartSSD platform image, and toolchain version.

To build a kernel from source, first install Vitis HLS. The kernels used in the paper were built with **Vitis HLS 2022.2**. A complete Vitis Makefile project is currently provided in `Accelerator kernel/Key24Value256_kernel/`; the other kernel directories provide their source code and precompiled binaries but may require adapting this build project.

Enter the provided build project:

```bash
cd "Accelerator kernel/Key24Value256_kernel"
```

Run software emulation with:

```bash
make run \
  TARGET=sw_emu \
  PLATFORM=xilinx_u2_gen3x4_xdma_gc_2_202110_1
```

Run hardware emulation with:

```bash
make run \
  TARGET=hw_emu \
  PLATFORM=xilinx_u2_gen3x4_xdma_gc_2_202110_1
```

Hardware emulation is substantially slower than software emulation; use a small test dataset when validating the design.

Build the hardware kernel with:

```bash
make build \
  TARGET=hw \
  PLATFORM=xilinx_u2_gen3x4_xdma_gc_2_202110_1
```

The generated kernel binary is placed under:

```text
build_dir.hw.xilinx_u2_gen3x4_xdma_gc_2_202110_1/
```

> [!NOTE] An `.xclbin` is tied to its platform shell and toolchain. Rebuild the kernel when using an incompatible SmartSSD platform image or Vitis version.

### 4. Build CSD-Enabled RocksDB

The modified RocksDB source is located under [`rocksdb-9.0.0/`](rocksdb-9.0.0/). Build it in the same way as upstream RocksDB:

```bash
cd rocksdb-9.0.0
make all -j8
```

For build dependencies and other platform-specific information, refer to the [upstream RocksDB documentation](https://github.com/facebook/rocksdb).

## CSD-CoKV Configuration

CSD-CoKV adds the following configuration options to RocksDB's advanced options.

### Select the Compaction Device

`CompactionDevice` determines whether a compaction job runs on the host CPU or a CSD:

```cpp
enum CompactionDevice : char {
  kCompactionOnCPU = 0x0,  // default: CPU compaction
  kCompactionOnCSD = 0x1,  // CSD-offloaded compaction
};
```

Enable CSD execution with:

```cpp
open_options_.compaction_device = kCompactionOnCSD;
```

### Specify the Accelerator Kernel

Set the path to a compatible `.xclbin` file:

```cpp
open_options_.CompactionKernelPath =
    "/path/to/compaction_k32v1024.xclbin";
```

### Configure Accelerator Devices

Resize the accelerator ID buffer and provide the XRT device IDs assigned to the SmartSSDs:

```cpp
open_options_.Compaction_accelerator_id.resize(acc_num);
open_options_.Compaction_accelerator_id = {1, 0, 2, 3};
```

Device enumeration and the relationship between NVMe devices and accelerator IDs are system-specific. Inspect the local device order with `xbutil examine` and verify the mapping before running CSD-CoKV.

### Configure the CSD Scheduling Policy

The available scheduling policies are represented by `CompactionCSDPolicy`:

```cpp
enum CompactionCSDPolicy : char {
  kCompactionLessThan4 = 0x0,
  kCompactionCSDArray = 0x1,
  kCompactionCSDArrayScheduleOff = 0x2,
};
```

For example:

```cpp
open_options_.compaction_csd_policy = kCompactionCSDArray;
```

### Configure the Output SSTable Size Policy

CSD-CoKV provides the following strategies for selecting the size of SSTables generated by a CSD:

```cpp
enum CompactionCSDGenSSTfileSizePolicy : char {
  kCompactionCSDSSTavg      = 0x0, // output = average input size
  KCompactionCSDSSTabove64  = 0x1, // output = 64 MiB
  KCompactionCSDSSTlayer    = 0x2, // output = 64 MiB * level
  kCompactionCSDSSTwtosmall = 0x3, // avoid undersized SSTables
};
```

For example:

```cpp
open_options_.compaction_csd_gen_sst_file_size_policy =
    KCompactionCSDSSTlayer;
```

These interfaces are part of a research prototype and may require source-level adaptation for RocksDB versions or workloads that differ from the evaluated setup.

### Configure a Multi-CSD Deployment

For multiple CSDs, assign one column-family path per device:

```cpp
open_options_.cf_paths = {
    "/mnt/csd0/db/",
    "/mnt/csd1/db/",
    "/mnt/csd2/db/",
    "/mnt/csd3/db/",
};
```

Each directory must reside on the intended SmartSSD. Ensure that the number and order of paths match the configured CSD deployment.

## Reproducing the Experiments

The [`Experiment result/`](Experiment%20result/) directory contains the scripts and recorded outputs used for the paper's overall-performance, sensitivity, scalability, YCSB, comparison, and overhead experiments.

> [!CAUTION] The experiment scripts were originally written for the machines used in the paper evaluation. Some scripts contain machine-specific absolute paths and cleanup commands that remove existing database directories. Review and update every path, mount point, device ID, kernel path, output path, and cleanup command before running a script. Do not run the scripts unchanged on a machine containing valuable data.

The raw experiment logs are large. If you are interested only in the source code, use a partial or sparse Git clone rather than downloading every result file.

## Known Limitations

- The CSD execution path requires Samsung SmartSSD hardware and a compatible XRT/Vitis software stack.
- The precompiled kernels support the key/value configurations evaluated in the paper and are not general-purpose variable-length kernels.
- Only `Key24Value256_kernel/` currently includes a complete standalone Vitis Makefile project; rebuilding other kernel configurations requires adapting that project.
- The experiment scripts retain environment-specific paths from the original evaluation platform and are not currently one-command reproduction scripts.
- This repository is a research prototype based on RocksDB 9.0.0; it is not a drop-in extension for newer RocksDB releases.

## Citation

If you use CSD-CoKV in your research, please cite the following paper:

```bibtex
@INPROCEEDINGS{11629364,
  author={Cao, Zhining and Zhang, Kai and Yang, Jinrun and Li, Hui and
          Su, Nan and Wei, Qian and Ma, Shikun and Chen, Zehao and
          Yin, Junbo and Zhang, Haijun and Shen, Zhaoyan},
  booktitle={2026 IEEE 42nd International Conference on Data Engineering (ICDE)},
  title={CSD-CoKV: Host-CSD Collaborative Offloading for High-Performance
         LSM-Tree Based KV Stores},
  year={2026},
  pages={113--126},
  doi={10.1109/ICDE65706.2026.00016}
}
```

## Licensing

This repository contains original CSD-CoKV code together with third-party components, including RocksDB, Xilinx-derived build and host code, YCSB-cpp, and HdrHistogram. Third-party components remain subject to their respective licenses and copyright notices. A root-level license and a consolidated third-party notice should be added only after the copyright ownership and redistribution terms for the original code, experimental data, and precompiled FPGA binaries have been confirmed.
