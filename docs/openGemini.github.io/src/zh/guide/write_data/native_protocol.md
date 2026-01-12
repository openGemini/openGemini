---
title: 原生数据写入协议
order: 3
---

### 概述

- 早期版本openGemini的数据写入接口沿用了InfluxDB的行协议
- 由于InfluxDB的行协议是文本协议，在数据组装和解析上的性能相对较差，逐渐成为端到端数据写入的瓶颈
- 基于此推出了openGemini原生写入协议，提升端到端写入性能
- [配置项明细](../reference/configurations.md#record-write)
- 使用最新版本openGemini客户端进行对接 [opengemini-client-go](https://github.com/openGemini/opengemini-client-go)
- 内核版本 1.4.0+
