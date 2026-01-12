---
title: Store 端 Shelf 写入模式
order: 6
---

### 概述

- 和传统的WAL+MemTable不同， Shelf模式下没有MemTable，数据成功写入WAL即可返回
- 在WAL累积到一定大小后，将WAL直接转换为TSSP数据文件，而不是从MemTable刷盘
- [配置项明细](../reference/configurations.md#datashelf-mode)
- 内核版本 1.4.0+

### 异步索引

- 数据写入过程中，索引的创建效率总是不尽如人意，在Shelf模式下，我们将索引的创建从同步流程修改为异步流程
- 整体上看并没有提升数据写入性能，但可以有效减少索引创建带来的写入抖动问题

### 去掉MemTable

- MemTable中是按Record结构化存储的数据，内存占用较多，GC压力较大
- 在Shelf模式下，我们去掉了MemTable，同时改进WAL的存储结构，使WAL支持查询
- 优点：
  - 减少碎片内存，降低GC开销
  - 按偏移量读文件，减少读写锁冲突
- 缺点：
  - 读性能相对较差，后续可能会通过HOT-WAL来加速WAL数据的查询
