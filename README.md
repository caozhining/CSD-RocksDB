# CSD-RocksDB

This repository provides our implementation of **RocksDB accelerated with Samsung SmartSSD**, where we offload **compaction computation** from the host CPU to the **Computational Storage Device (CSD)**. By leveraging the near-data processing capability of SmartSSD, our design significantly improves the overall system throughput.

> **Hardware Requirement**: At least one Samsung SmartSSD device is required to run this system.

The repository is organized into three main components:

- **Accelerator kernel**: FPGA kernels designed for CSD offloading, optimized for different key-value lengths.
- **Experiment result**: Scripts and logs for reproducing the experiments reported in our paper.
- **rocksdb-9.0.0**: A modified version of RocksDB extended with CSD-based compaction support.