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
   0000:6d:00.0
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