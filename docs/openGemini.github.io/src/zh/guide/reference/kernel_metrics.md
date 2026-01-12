---
title: 内核监控指标说明
order: 8
---
本章节内容主要介绍openGemini自带的Kernel监控指标项，如果使用ts-monitor采集内核指标，会额外采集节点的内存利用率、cpu利用率、进程状态信息等，见[system](#system-节点指标)。

一般建议用户关注如下数据表内的指标项：
- Cluster_metric
- Httpd
- IO
- Measurement_metric
- RunTime
- System

建议内核开发者或在对openGemini非常熟悉的核心用户可以关注其他表内容，可以通过指标窥探到openGemini实际运行情况，帮助理解和性能参数优化。比如Record表，统计了内存池的使用情况，如果出现大量大内存申请的情况，可以适当考虑将内存池空间调大。

## Grafana看板查询语句示例

### CPU利用率
```
SELECT sum("usage")/count("usage") FROM (SELECT last("usage") as "usage" FROM (SELECT mean("CpuUsage") as "usage" FROM system WHERE time < xxx AND time > xxx GROUP BY time(1m),"host" FILL(null)) GROUP BY "host")
```
### 内存利用率
```
SELECT sum("use")/sum("total") FROM (SELECT last("MemInUse") as "use", last("MemSize") as "total" FROM system WHERE time < xxx AND time > xxx GROUP BY "host")
```

### 磁盘平均使用率
```
SELECT sum("use")/sum("total") FROM (SELECT last("DiskUsed") as "use", last("DiskSize") as "total" FROM system WHERE time < xxx AND time > xxx GROUP BY "host")
```
### 集群写流量趋势（万metrics/s)
```
SELECT difference("write")/1000/60 as cluster_write_qps FROM (SELECT sum("write") as "write" FROM (SELECT last("fieldsWritten") AS "write" FROM httpd WHERE time > xxx AND time < xxx GROUP BY time(1m),* FILL(linear)) GROUP BY time(1m) FILL(linear) LIMIT 718 offset 1
```
### 每个TS-SQL的写带宽(MB/s)
```
SELECT difference("writeReqBytes")/1024/1024/10 AS "mean_writeReqBytes" FROM httpd WHERE time > xxx AND time < xxx GROUP BY hostname
```
这里```difference("writeReqBytes")/1024/1024/10```,采样周期时10s，最后除以10是计算每秒的数据

### 每个TS-SQL的写时延(ns)
```
SELECT difference("writeReqDurationNs")/difference("writeReq") AS "Duration" FROM httpd WHERE time > xxx AND time < xxx GROUP BY "hostname" FILL(null)
```
### 每个TS-STORE的写时延(ns)
```
SELECT difference("writeStorageDurationNs")/difference("WriteRowsBatch") AS "Duration" FROM performance WHERE time > xxx AND time < xxx GROUP BY "hostname" FILL(null)
```

### 每个TS-SQL的写QPS
```
SELECT difference("pointsWrittenOK")/10 FROM httpd WHERE time > xxx AND time < xxx GROUP BY "hostname" FILL(null)
```
采样周期时10s，最后除以10是计算每秒的数据

### 集群的查询QPS
```
SELECT difference("qr")/10 AS "QPS" FROM (SELECT sum(queryReq) AS "qr" FROM httpd WHERE time > xxx AND time < xxx GROUP BY time(10s))
```
采样周期时10s，最后除以10是计算每秒的数据

### 集群的查询时延
```
SELECT difference("duration")/difference("qr")/1000000 AS "latency" FROM (SELECT sum(queryReqDurationNs) AS "duration", sum(queryReq) AS "qr" FROM httpd WHERE time > xxx AND time < xxx GROUP BY time(2m))
```

### 磁盘写时延
```
SELECT mean(writeDuration)/mean(writeOkCount) FROM io WHERE time > xxx AND time < xxx GROUP BY(10s), hostname
```

## Cluster_metric
|指标名称 |数据类型|说明|
|:-----|-----|------|
|DBCount|float|集群中创建的DB总数|
|MstCount|float|集群中所所有DB中表的总数|

## Compact(LSM-Tree文件合并相关指标)
|指标名称 |数据类型|说明|
|:-----|-----|------|
|Action|string|level/full,表示“level compact“ 还是 “full compact"|
|App|string|组件名|
|hostname|组件Ip地址和端口|
|level|表示Compact发生的level，分[0-7]|
|measurement|Compact发生的表|
|shard_id|Compact发生的shard|
|Active|float|当前运行的Compact任务数量|
|CompactedFileCount|float|单次任务，Compact后的文件数量|
|CompactedFileSize|float|单次任务，Compact后的文件总大小|
|Duration|float|单次Compact任务耗时|
|Errors|float|Compact错误次数|
|MaxMemoryUsed|float|Compact最大内存消耗|
|OriginalFileCount|float|单次任务，Compact的源文件数量|
|OriginalFileSize|float|单次任务，Compact的源文件总大小|
|Ratio|float|单次任务，Compact的压缩率|
|RecordPoolGetTotal|float|统计内存池分配Record对象的频次|
|RecordPoolHitTotal|float|Record对象池累计命中次数，用于衡量内存复用效果)

## DownSample(多级将采样相关指标）
|指标名称 |数据类型|说明|
|:-----|:-----|:------|
|app|string|组件名|
|hostname|string|组件IP地址和端口|
|Active|float|当前运行的将采样任务数量|
|Errors|float|将采样任务运行过程中产生的错误数量|

## Engine(存储引擎相关指标)
|指标名称 |数据类型|说明|
|:-----|:-----|:------|
|app|string|组件名|
|hostname|string|组件IP地址和端口|
|CloseDurations|float|存储引擎关闭耗时|
|CloseErrors|float|存储引擎关闭过程出现错误的次数|
|DelIndexCount|float|存储引擎删除索引的总次数|
|DelIndexDuration|float|存储引擎删除索引的总耗时|
|DelIndexErr|float|存储引擎删除索引过程出现错误的次数|
|DelShardCount|float|存储引擎删除Shard的总次数|
|DelShardDuration|float|存储引擎删除Shard的总耗时|
|DelShardErr|float|存储引擎删除Shard过程出现错误的次数|
|DropDatabaseCount|float|存储引擎删除DB的总次数|
|DropDatabaseDurations|float|存储引擎删除DB的总耗时|
|DropDatabaseErrs|float|存储引擎删除DB过程出现错误的总次数|
|DropMstCount|float|存储引擎删除表的总次数|
|DropMstDurations|float|存储引擎删除表的总耗时|
|DropMstErrs|float|存储引擎删除表过程出现错误的总次数|
|DropRPCount|float|存储引擎删除RP的总次数|
|DropRPDurations|float|存储引擎删除RP的总耗时|
|DropRPErrs|float|存储引擎删除RP过程出现错误的总次数|
|OpenDurations|float|存储引擎启动的总耗时|
|OpenErrors|float|存储引擎启动过程中出现的错误次数|

## Errno(错误信息)
|指标名称 |数据类型|说明|
|:-----|:-----|:------|
|app|string|组件名|
|hostname|string|组件IP地址和端口|
|errno|string|错误码|
|module|string|功能模块编码|
|value|float|错误次数|

## Executor(查询引擎相关指标)
|指标名称 |数据类型|说明|
|:-----|:-----|:------|
|app|string|组件名|
|hostname|string|组件IP地址和端口|
|agg_rows_count|float|暂未启用|
|agg_rows_last|float|暂未启用
|agg_rows_sum|float|暂未启用|
|column_length_count|float|暂未启用|
|column_length_last|float|暂未启用|
|column_length_sum|float|暂未启用|
|column_width_count|float|暂未启用|
|column_width_last|float|暂未启用|
|column_width_sum|float|暂未启用|
|dag_edge_count|float|暂未启用|
|dag_edge_last|float|暂未启用|
|dag_edge_sum|float|暂未启用|
|dag_vertex_count|float|暂未启用|
|dag_vertex_lsat|float|暂未启用|
|dag_vertex_sum|float|暂未启用|
|exec_abort_count|float|查询计划的执行器被终止的总次数|
|exec_abort_last|float|无意义，忽略|
|exec_abort_sum|float|无意义，忽略|
|exec_failed_count|float|查询计划的执行器执行失败的总次数|
|exec_failed_last|float|无意义，忽略|
|exec_failed_sum|float|无意义，忽略|
|exec_run_time_count|float|查询计划的执行器执行总次数|
|exec_run_time_last|float|最近一次执行时延|
|exec_run_time_sum|float|查询计划的执行器执行总时长|
|exec_scheduled_count|float|查询计划的执行器调度总次数|
|exec_scheduled_last|float|无意义，忽略|
|exec_scheduled_sum|float|无意义，忽略|
|exec_timeout_count|float|查询计划的执行器超时总次数|
|exec_timeout_last|float|无意义，忽略|
|exec_timeout_sum|float|无意义，忽略|
|exec_wait_time_count|float|查询计划的执行器申请内存资源总次数|
|exec_wait_time_last|float|最近一次申请内存资源的等待时延(ns)|
|exec_wait_time_sum|float|查询计划的执行器申请内存资源是总时延(ns)|
|fill_rows_count|float|暂未启用|
|fill_rows_last|float|暂未启用|
|fill_rows_sum|float|暂未启用|
|filter_rows_count|float|暂未启用|
|filter_rows_last|float|暂未启用|
|filter_rows_sum|float|暂未启用|
|goroutine_count|float|查询计划的执行器执行总次数|
|goroutine_last|float|最近一次查询计划执行时分配的goroutine数量|
|goroutine_sum|float|总的goroutine数量|
|limit_rows_count|float|暂未启用|
|limit_rows_last|float|暂未启用|
|limit_rows_sum|float|暂未启用|
|materialized_rows_count|float|暂未启用|
|materialized_rows_last|float|暂未启用|
|materialized_rows_sum|float|暂未启用|
|memory_count|float|查询计划的执行器执行总次数|
|memory_last|float|最近一次查询计划执行时使用内存大小|
|memory_sum|float|查询计划的执行器累计使用内存大小|
|merge_rows_count|float|暂未启用|
|merge_rows_last|float|暂未启用|
|merge_rows_sum|float|暂未启用|
|sink_length_count|float|暂未启用|
|sink_length_last|float|暂未启用|
|sink_length_sum|float|暂未启用|
|sink_rows_count|float|总次数|
|sink_rows_last|float|最近一次执行器执行时出口数据行数|
|sink_rows_sum|float|执行器执行时出口数据总行数|
|sink_width_count|float|总次数|
|sink_width_last|float|最近一次执行器执行时出口数据列宽|
|sink_width_sum|float|执行器执行时出口数据总列宽|
|source_length_count|float|暂未启用|
|source_length_last|float|暂未启用|
|source_length_sum|float|暂未启用|
|source_rows_count|float|总次数|
|source_rows_last|float|最近一次执行器执行时入口数据行数|
|source_rows_sum|float|执行器执行时入口数据总行数|
|source_width_count|float|总次数|
|source_width_last|float|最近一次执行器执行时入口数据列宽|
|source_width_sum|float|执行器执行时入口数据总列宽|
|trans_abort_count|float|暂未启用|
|trans_abort_last|float|暂未启用|
|trans_abort_sum|float|暂未启用|
|trans_failed_abort_count|float|暂未启用|
|trans_failed_abort_last|float|暂未启用|
|trans_failed_abort_sum|float|暂未启用|
|trans_failed_count|float|暂未启用|
|trans_failed_last|float|暂未启用|
|trans_failed_sum|float|暂未启用|
|trans_run_time_count|float|暂未启用|
|trans_run_time_last|float|暂未启用|
|trans_run_time_sum|float|暂未启用|
|trans_wait_time_count|float|暂未启用|
|trans_wait_time_last|float|暂未启用|
|trans_wait_time_sum|float|暂未启用|

## FileStat(存储文件信息)
|指标名称 |数据类型|说明|
|:-----|:-----|:------|
|app|string|组件名|
|hostname|string|组件IP地址和端口|
|database|string|数据库名|
|id|string|？？
|measurement|string|表名称|
|path|string|数据文件路径|
|retentionPolicy|string|数据库对应的数据保留策略名称|
|FileCount|float|表的数据文件数量|
|FileSize|float|表的数据文件大小|

## FileStat_Level(存储文件信息,按Compact Level维度统计)
|指标名称 |数据类型|说明|
|:-----|:-----|:------|
|app|string|组件名|
|hostname|string|组件IP地址和端口|
|database|string|数据库名|
|level|string|数据文件Level|
|FileCount|float|数据库中，当前Level的数据文件总数|
|FileSize|float|数据库中，当前Level的数据文件总大小|

## Httpd(对外相关统计信息)
|指标名称 |数据类型|说明|
|:-----|:-----|:------|
|app|string|组件名|
|hostname|string|组件IP地址和端口|
|WriteMapRowsDurationNs|float||
|WriteStreamRoutineDurationNs|float||
|WriteUpdateIndexDurationNs|float|暂未启用|
|authFail|float|用户认证失败次数|
|clientError|float|服务器返回4xx错误次数|
|connectionNum|float|客户端连接数量|
|fieldsWritten|float|写入的field列数|
|pingReq|float|收到的ping请求数|
|pointsWrittenDropped|float|批量写入时，部分写失败的数据总条数|
|pointsWrittenFail|float|写失败的数据条数(全部失败)|
|pointsWrittenOK|float|写成功的数据条数|
|query400ErrorStmtCount|float|查询时，服务器400错误的次数|
|queryErrorStmtCount|float|查询失败的查询语句数量|
|queryReq|float|接收到客户端查询请求数量|
|queryReqActive|float|当前活跃的查询请求数量|
|queryReqDurationNs|float|累计查询时延(ns)|
|queryRespBytes|float|查询返回的数据总量(按Byte计算)|
|queryStmtCount|float|查询语句总数|
|recoveredPanics|float|处理程序Panic的总次数|
|req|float|累计接收客户端请求数（含读和写）|
|reqActive|float|当前活跃请求数|
|reqDurationNs|float|所有请求的累计响应时延(ns)|
|scheduleUnmarshalDns|float|数据反序列化累计耗时|
|serverError|float|服务器返回5xx错误累计次数|
|statusReq|float| '/status'接口接收到的请求数|
|write400ErrReq|float|找不到数据库、权限不足等问题导致的错误累计次数|
|write500ErrReq|float|写数据出现服务器内部错误的累计次数|
|writeCreateMstDurationNs|float|写时创表过程的时延(ns)|
|writeCreateSgDurationNs|float|写时创建ShardGroup过程的时延|
|writeReq|float|服务器接收到写请求累计次数|
|writeReqActive|float|当前活跃的写请求数量|
|writeReqDurationNs|float|写数据累计时延(ns)|
|writeReqBytes|float|写入成功的总数据量(按Byte计算)|
|writeReqBytesIn|float|总的写数据量(含写失败和写成功)|
|writeReqParseDurationNs|float|解析写入数据格式的累计时延|
|writeStoresDurationNs|float|数据写存储引擎的累计时延|
|writeUnmarshalSkDurationNs|float|暂未启用|
|writeUpdateSchemaDurationNs|float|更新表元数据(如新增Field)过程产生的累计时延(ns)|

## IO
|指标名称 |数据类型|说明|
|:-----|:-----|:------|
|app|string|组件名|
|hostname|string|组件IP地址和端口|
|readActiveBytes|float|暂未启用|
|readActiveCount|float|暂未启用|
|readCacheCount|float|暂未启用|
|readCacheMem|float|存储引擎读文件时使用的block cache大小|
|readCacheRatio|float|block cache命中率|
|readDuration|float|读文件的IO累计时延|
|readOkBytes|float|读文件IO成功读取的累计数据量(按Byte计算)|
|readOkCount|float|读文件IO成功的次数|
|readTotalBytes|float|读IO总的数据量(按Byte计算)|
|readTotalCount|float|读IO总的次数|
|snapshotSize|float|快照文件大小(累计,按Byte计算)
|snapshotCount|float|快照次数|
|syncActiveCount|float|暂未启用|
|syncDuration|float|累计刷盘File.Sync()时延(ns)|
|syncOKCount|float|成功刷盘的次数|
|syncTotalCount|float|总的刷盘次数|
|writeActiveBytes|float|暂未启用|
|writeActiveCount|float|暂未启用|
|writeDuration|float|累计写IO时延(File.Write())|
|writeOkBytes|float|写文件IO成功写入的累计数据量(按Byte计算)|
|writeOkCount|float|写IO次数|
|writeTotalBytes|float|写IO总数据量(按Byte计算)|
|writeTotalCount|float|写IO总次数|

## Measurement_metric
|指标名称 |数据类型|说明|
|:-----|:-----|:------|
|database|string|数据库名|
|measurement|string|表名|
|seriesCount|float|时间线数量|

## Merge(文件合并相关指标)
|指标名称 |数据类型|说明|
|:-----|:-----|:------|
|app|string|组件名|
|hostname|string|组件IP地址和端口|
|Measurement|string|表名|
|ShardId|string|分片ID|
|Active|float|当前运行中的乱序数据合并任务数|
|CurrentOutOfOrderFile|float|暂未启用|
|Duration|float|暂未启用|
|Errors|float|乱序合并时出现错误累计次数|
|MergeFileCount|float|单次任务，乱序合并后的文件总数|
|MergeFileSize|float|单次任务，乱序合并后的文件总大小|
|OrderFileCount|float|单次任务，有序文件总数|
|OrderFileSize|float|单次任务，有序文件总大小|
|SkipTotal|float|单次任务，不满足乱序合并条件，跳过合并的次数|

## Meta(元数据管理相关指标)
|指标名称 |数据类型|说明|
|:-----|:-----|:------|
|app|string|组件名|
|hostname|string|组件IP地址和端口|
|Host|string|store节点地址|
|NodeID|string|store节点ID|
|LTime|float|ts-meta管理的ts-store用的逻辑时钟，确保seriesID递增|
|LeaderSwitchTotal|float|leader节点故障导致切主的累计次数|
|SnapshotDataSize|float|将当前系统状态(如节点信息等)写入快照的数据大小|
|SnapshotTotal|float|保存系统状态的次数(通过快照保存）|
|SnapshotUnmarshalDuration|float|ts-sql获取meta全量元数据时，meta向sql回复的快照数据反序列化时延(ns)|
|Status|float|meta节点状态，1(健康), 4(故障)|
|storeApplyTotal|float|集群管理类、DDL类命令需要在ts-meta上创建元数据，会调用Apply()方法存储数据，这里统计总次数|

## MetaRaft(ts-meta上Raft相关指标)
|指标名称 |数据类型|说明|
|:-----|:-----|:------|
|app|string|组件名|
|hostname|string|组件IP地址和端口|
|NodeID|string|ts-meta节点ID|
|Status|float|节点角色，0(follower), 1(cadidate), 2(leader)
|foo|float|无意义，后期版本将删除|

## metadata(故障接管相关指标，共享存储支持）

## performance(存储数据时的各项指标）
|指标名称 |数据类型|说明|
|:-----|:-----|:------|
|app|string|组件名|
|hostname|string|组件IP地址和端口|
|FlushOrderRowsCount|float|下盘的有序数据总行数|
|FlushRowsCount|float|下盘的数据总行数(含有序和乱序)|
|FlushSnapshotCount|float|存储引擎缓存数据刷盘次数|
|FlushSnapshotDurationNs|float|缓存刷盘的累计时延(ns)，包括申请资源、缓存数据排序、刷盘、资源释放、删除WAL日志等过程|
|FlushUnOrderRowsCount|float|下盘的乱序数据总行数|
|SnapshotFlushChunksNs|float|特指FlushChunks方法刷盘的累计时延(ns)|
|SnapshotHandleChunksNs|float|无意义，后期版本将删除|
|SnapshotSortChunksNs|float|特指刷盘前排序过程的累计时延(ns)|
|WriteActiveRequests|float|ts-store节点上, 当前正在写入的任务数|
|WriteAddSidRowCountNs|float|暂未启用|
|WriteCreateShardNs|float|ts-store节点上，创建新的分片(shard)累计时延(ns)|
|WriteFieldsCount|float|ts-store节点上，写入的指标列(Field)的总数|
|WriteGetMstInfoNs|float|ts-sotre节点上，写入数据时查询表信息的累计时延|
|WriteGetTokenDurationNs|float|ts-sotre节点上，写入数据时，流控逻辑中等待资源分配的累计时延(ns)|
|WriteIndexDurationNs|float|ts-store节点上，写入数据时创建索引的累计时延(ns)|
|WriteMstInfoNs|float|ts-store节点上，写入缓存的累计时延(特指在write_rows方法中的写时延)|
|WriteReqErrors|float|ts-store节点上，写数据出错的总次数|
|WriteRowsBatch|float|ts-store节点上，批量写入的次数，衡量数据写入频次|
|WriteRowsCount|float|ts-store节点上，数据写入总的行数|
|WriteRowsDurationNs|float|ts-store节点上，写入缓存的累计时延(粒度比WriteMstInfoNs更粗)|
|WriteShardKeyIdxNs|float|ts-store节点上，创建ShardKeyIndex的总时延|
|WriteSortIndexDurationNs|float|ts-store节点上，写索引数据之前索引数据排序的总时延(ns)|
|WriteStorageDurationNs|float|ts-store节点上，整个写入流程的总时延|
|WriteUnmarshalNs|float|ts-store节点接收到数据后，反序列化所需时延(累计)|
|WriteWalDurationNs|float|写WAL累计时延(ns)|

## record(内存池相关指标)
openGemini内部大量使用内存池，根据作用范围可分为：
- AggPool
- CircularRecordPool
- FileCursorPool
- IntervalRecordPool
- SequenceAggPool
- SeriesPool
- TsmMergePool
- TsspSequencePool

每一种Pool采集Abort(累计值)/Get(累计值)/ReUse(累计值)/InUse(当前值)四类指标，分别表示对象申请内存空间过大，不再放回内存池，直接释放的次数；申请内存次数；内存复用的次数；正在使用的内存对象个数。可用于内核参数调优的依据。

|指标名称 |数据类型|说明|
|:-----|:-----|:------|
|app|string|组件名|
|hostname|string|组件IP地址和端口|
|AggPoolAbort|float|内存直接释放的次数|
|AggPoolGet|float|申请内存次数|
|AggPoolGetReUse|float|内存复用的次数|
|AggPoolInUse|float|正在使用内存的对象个数|
|CircularRecordPoolAbort|float|内存直接释放的次数|
|CircularRecordPoolGet|float|申请内存次数|
|CircularRecordPoolGetReUse|float|内存复用的次数|
|CircularRecordPoolInUse|float|正在使用内存的对象个数|
|FileCursorPoolAbort|float|内存直接释放的次数|
|FileCursorPoolGet|float|申请内存次数|
|FileCursorPoolGetReUse|float|内存复用的次数|
|FileCursorPoolInUse|float|正在使用内存的对象个数|
|IntervalRecordPoolAbort|float|内存直接释放的次数|
|IntervalRecordPoolGet|float|申请内存次数|
|IntervalRecordPoolGetReUse|float|内存复用的次数|
|IntervalRecordPoolInUse|float|正在使用内存的对象个数|
|SequenceAggPoolAbort|float|内存直接释放的次数|
|SequenceAggPoolGet|float|申请内存次数|
|SequenceAggPoolGetReUse|float|内存复用的次数|
|SequenceAggPoolInUse|float|正在使用内存的对象个数|
|SeriesPoolAbort|float|内存直接释放的次数|
|SeriesPoolGet|float|申请内存次数|
|SeriesPoolGetReUse|float|内存复用的次数|
|SeriesPoolInUse|float|正在使用内存的对象个数|
|TsmMergePoolAbort|float|内存直接释放的次数|
|TsmMergePoolGet|float|申请内存次数|
|TsmMergePoolGetReUse|float|内存复用的次数|
|TsmMergePoolInUse|float|正在使用内存的对象个数|
|TsspSequencePoolAbort|float|内存直接释放的次数|
|TsspSequencePoolGet|float|申请内存次数|
|TsspSequencePoolGetReUse|float|内存复用的次数|
|TsspSequencePoolInUse|float|正在使用内存的对象个数|

## RunTime(运行时内存指标)
|指标名称 |数据类型|说明|
|:-----|:-----|:------|
|app|string|组件名|
|hostname|string|组件IP地址和端口|
|Alloc|float|已分配但未释放的内存大小|
|CpuUsage|float|cpu利用率|
|Frees|float|累计释放堆对象的内存大小|
|HeapAlloc|float|同Alloc|
|HeapIdle|float|空闲(未使用)Span内存大小，衡量内存碎片程度|
|HeapInUse|float|使用中的内存（最大值）
|HeapObjects|float|累计分配的堆对象总数|
|HeapReleased|float|返还给OS的物理内存大小|
|HeapSys|float|从OS为堆申请的内存大小|
|Lookups|float|运行时执行的指针查找数|
|Mallocs|float|分配堆对象的累计数量|
|NumGC|float|完成的GC数量|
|NumGoroutine|float|产生的Goroutine数量|
|PauseTotalNs|float|从程序开始时累计暂停时长(ns)|
|Sys|float|从OS申请的内存大小|
|TotalAlloc|float|累计为对象分配的内存大小，衡量内存累计使用量|

## spdy(组件间连接复用相关指标)
|指标名称 |数据类型|说明|
|:-----|:-----|:------|
|app|string|组件名|
|hostname|string|组件IP地址和端口|
|link|string|连接方向"sql2meta","sql2store"|
|remote_addr|string|对端IP地址|
|connTotal|float|全部连接数(内部)|
|successCreateSessionTotal|float|成功创建的连接会话数量|

## stream(流计算相关指标)
|指标名称 |数据类型|说明|
|:-----|:-----|:------|
|StreamFilter|float|流式聚合中条件过滤的次数|
|StreamFilterNum|float|流式聚合中条件过滤处理的数据行数|
|StreamIn|float|流式聚合中，写管道次数|
|StreamInNum|float|流式聚合中，写入管道的总数据行数|

## Stream_window(流式窗口聚合指标)
|指标名称 |数据类型|说明|
|:-----|:-----|:------|
|app|string|组件名|
|hostname|string|组件IP地址和端口|
|window|string|流式聚合的窗口|

## System(节点指标)
|指标名称 |数据类型|说明|
|:-----|:-----|:------|
|host|string|组件IP地址和端口|
|AuxDiskSize|float|辅助磁盘容量，一般将WAL文件存储在不同的磁盘上，提高数据的安全性，见ts-monitor.conf aux-disk-path配置项|
|AuxDiskUsage|float|辅助磁盘空间使用率|
|AuxDiskUsed|float|辅助磁盘已使用容量|
|CpuNum|float|节点CPU核数|
|CpuUsage|float|节点CPU利用率|
|DiskSize|float|节点主磁盘容量|
|DiskUsage|float|节点主磁盘空间使用率|
|DiskUsed|float|节点主磁盘已使用磁盘容量|
|IndexUsed|float|索引数据使用磁盘容量|
|MemCacheBuff|float|内存的缓存和缓冲区大小|
|MemInUse|float|已使用的内存大小|
|MemSize|float|节点内存容量|
|MemUsage|float|节点内存使用率|
|MetaPid|float|ts-meta的进程ID|
|MetaStatus|float|meta进程状态，1(runing) 0(killed)|
|SqlPid|float|ts-sql的进程ID|
|SqlStatus|float|sql进程状态，1(runing) 0(killed)|
|StorePid|float|ts-store进程ID|
|StoreStatus|float|store进程状态，1(runing) 0(killed)|
|Uptime|float|节点运行时长(s)|
