---
title: Release notes(v1.1)
order: 3
---

## v1.1.0rc1[2023-08-31]

### 特性

- [跨平台](#跨平台)
- [数据订阅](#数据订阅)
- [查询管理](#查询管理)
- [高基数引擎列协议](#高基数引擎列协议)
- [新增语法](#新增语法)
#### 数据订阅
支持数据订阅功能，[点击参考](../features/subscription.md)，可用于数据同步、数据分析等场景
#### 查询管理
新增查询语句 ```SHOW QUERIES```, 可查看数据库中正在执行的所有查询语句，包括database,duration,status,host等信息。通过查看duration，了解查询语句的执行时间，可通过命令```KILL QUERY xx ```终止查询语句的执行 [#347](https://github.com/openGemini/openGemini/pull/347)
#### 跨平台
新增对Windows和MacOS操作系统的支持，MacOS支持x86和ARM64两个平台，Windows仅限x86，[点击下载](https://github.com/openGemini/openGemini/releases/tag/v1.1.0-rc1)
#### 高基数引擎列协议
高基数存储引擎引入了[Apache Arrow Flight RPC](https://arrow.apache.org/docs/format/Flight.html), 以进一步提升数据写入效率。
#### 新增语法
新增```SET CONFIG```语法，用于动态修改内核配置参数
### 性能优化
- ts-store进程重启时，仅加载时间上最近的少数Shard(分片)，其余Shard元数据信息在首次读写请求来到时加载，该功能通过配置启用。
- 高基数引擎查询Limit算子逻辑优化
- 高基数引擎多字段排序和写流程优化
- 高基数引擎底层Fragment(最小数据单元)均衡分布优化
- 新增gzipWritePool缓存池和打开文件句柄Cache
- 其他缓存池处理逻辑优化
- Golang GC优化
- 代码逻辑优化，包括
	- 删除对于option的重复encode/decode
	- 冗余代码删除
	- 调整聚合算子下推规则
	- ts-store层新增部分查询模板
- 查询数据流程优化
- first, last查询优化，减少磁盘I/O
### Bug修复
- 修复Indexscantransform算子DAG构建失败未关闭Curosr
- 修复异常场景下，DAG资源的释放Bug
- 修复分组查询时，分组时间不合理的极端情况下出现OOM的情况，比如 SELECT mean(*) ... Group by time(1ns)
- 修复创建文本索引关键字不支持大写'TEXT'的问题
- 修复表名称包含冒号的问题
