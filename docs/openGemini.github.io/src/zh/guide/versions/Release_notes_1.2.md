---
title: Release notes(v1.2)
order: 4
---

:::tip

社区版本下载：https://github.com/openGemini/openGemini/releases

Gitee版本下载：https://gitee.com/opengemini/Releases/releases

:::

## 内核优化

1. **IoT典型场景性能优化**

   本次版本针对IoT场景的写入和典型查询场景进行了优化，通过TSBS工具的测试结果可以看出，openGemini全面超越InfluxDB，最高提升了50倍。相比其他时序数据库，openGemini也是具有领先优势。

   具体性能数据参考:

   https://docs.opengemini.org/zh/guide/introduction/performance.html

2. **schema数据压缩**

   ```go
   type Record struct {
   	*RecMeta
   	ColVals []ColVal
   	Schema  Schemas
   }

   type Schemas []Field

   type Field struct {
   	Type int
   	Name string
   }
   ```

   openGemini将行数据协议转化为Record结构进行存储，Schema保存每一列FIELD的数据类型和名称，并会随着数据一起保存在磁盘上，当表的FIELD数量很多，比如1000列或者更多时，加之时间线数量很大的情况下，这里的Schema数据会变得很大，在tssp文件中的占比可能超过80%。

   可以通过配置文件开启schema数据压缩（默认关闭），但需要注意的是，该功能会对查询性能有一定的影响，因为查询过程中要对schema进行数据解压

   ```toml
     [data]
     ## Compressing ChunkMeta in TSSP Files. 0: not compressed(default); 1: use snappy
     # chunk-meta-compress-mode = 0
   ```

   优化效果：500万时间线 1000+列模型下，开启压缩功能后（Snappy算法），写性能提升 2.5+倍，平均刷盘时延减少68%，level 0 文件大小减少 70%

   **最佳实践：**针对列比较多的情况，尽可能用短字符串给FIELD命名。再考虑开启schema数据压缩。

   > 该优化优先级高于 "tssp 临时文件磁盘占用优化"，开启该优化后，临时文件的优化将自动失效

3. **优化仅一行数据情况下的数据编码**

   在一些业务中，时间线很多，但是每次写入时，同一时间线仅一条数据，这使得在 level 0 的文件中，一条时间线仅一条数据，本身没有办法进行压缩，还需要存储很多额外的信息，占用了大量的磁盘空间。 本次优化是专门针对上述场景，减少level 0文件大小，达到提升compaction、Merge和部分查询的性能的目的。

   优化效果：250万时间线，优化前文件大小 987MB， 优化后 733MB，存储空间减少 25%。

4. **优化全空值或没有空值列的存储**

   在openGemini中，数据的压缩存储是以segment为粒度（1000行），可能存在 segment为全空值（`ColVal.Len == ColVal.NilCount`）或没有空值（`ColVal.NilCount == 0`），此时可以不存储bitmap相关数据，减少磁盘占用。

   优化效果：3万时间线，2400万行数据，没有空值的情况下，优化后存储空间减少20%

5. **tssp 临时文件磁盘占用优化**

   数据写入openGemini后，在数据刷盘过程中，文件元数据会先保存到一个临时文件，在所有数据刷盘完成后，将临时文件追加到数据文件中，**然后删除该临时文件**，当写入数据量非常大时，临时文件数据也会频繁刷盘，占一部分磁盘I/O，本次优化是对临时文件进行一个压缩处理，减少磁盘I/O。该优化对刷盘，compaction，merge 等操作有效。

   可通过修改配置项 `data.temporary-index-compress-mode` 来开启，该优化方法是用CPU开销（解压）换取磁盘I/O，在选择压缩算法时需要考虑实际情况。

6. **`SHOW TAG KEYS`支持条件过滤**

   用法如下：

   ```
   > select * from cpu
   name: cpu
   time                cpu  host        mem
   ----                ---  ----        ---
   1710209706991211420 0.8  192.168.0.1 0.3
   1710209718880801483 0.65 192.168.0.2 0.42
   1710209735839331535 0.77 192.168.0.3 0.46

   > show tag keys from cpu where host="192.168.0.1"
   name: cpu
   tagKey
   ------
   host
   ```

7. **流式聚合（stream）用法和性能优化**

   在用法上，之前使用该功能时，必须是tags & time 一起分组，当前版本支持仅按time分组。例如

   ```sql
   # 之前版本用法
   > CREATE STREAM ... ON SELECT ... FROM ... GROUP BY time(1m),"cpu","host" delay 20s
   # 现在可以仅使用time分组
   > CREATE STREAM ... ON SELECT ... FROM ... GROUP BY time(1m) delay 20s
   ```

   在性能上，聚合效率较之前版本有数倍提升。

8. **新增监控项**（IOReadMetaCounts、IOReadMetaSize、IOReadDataCounts、IOReadDataSize）

   分别表示从数据文件中**读取元数据的次数和大小**和**读取数据的次数和大小**。有了这些基本数据，通过简单的处理就可以知道在某段时间内查询的数据量大小，以此作为性能优化或根因分析的依据。比如在定位查询问题时，如果查询时延比较大，此时可以观察该指标，了解openGemini存储引擎从文件读取数据的情况，以此判断是否由于计算数据量太多导致，从而优化查询语句。

9. **新增监控项**（IOFrontIndexReadOkCount，IOFrontIndexReadOkBytes，IOFrontIndexReadDuration ）

   在openGemini中，磁盘I/O包括业务数据读写I/O, 索引读写I/O，Compaction读写I/O，乱序合并读写I/O等，这里新增的3个监控项均为索引读写I/O的指标，分别是索引数据读取次数/字节数/时延。

   当性能瓶颈发生在磁盘I/O时，通过调取相应细分I/O数据，确定何种类型的I/O流量占比较大，可以进行针对性优化。

10. **新增监控项**

    以下为本次优化新增的关于ts-store上查询请求执行相关的监控项，用于洞察ts-store上的查询执行情况

    ```toml
    # ts-store处理的查询请求数
    storeQueryReq
    # ts-store的查询时延
    storeQueryReqDurationNs
    # 当前ts-store上正在执行的查询请求数
    storeQueryReqActive
    # ts-store上反序列化查询请求的时延
    UnmarshalQueryTimeTotal
    # ts-store上查询请求获取shard资源的时延。
    # 这里有流控，ts-store执行查询请求的并发数是一定的，如果查询并发比较大，就会出现排队情况，这里的时延就会比较大。
    GetShardResourceTimeTotal
    # 以下两项分别是ts-store上TAG构建扫描索引计划的时延和扫描数据的时延
    IndexScanDagBuildTimeTotal
    IndexScanDagRunTimeTotal
    # ts-store上查询请求查找索引的时延
    IndexScanRunTimeTotal
    # ts-store上查询请求涉及的shard个数
    QueryShardNumTotal
    # ts-store上查询请求扫描的时间线数量
    IndexScanSeriesNumTotal
    # 以下两项是查询请求读取数据的监控项
    ChunkReaderDagBuildTimeTotal
    ChunkReaderDagRunTimeTotal
    ```

11. 调整配置项`read-page-size`，默认32KB，最大可配64KB，对提升查询性能有帮助，但会增大磁盘I/O带宽，二者需要平衡。

    ```toml
    [data]
    # read-page-size set pageSize of read from file of datablock, default is "32kb", valid setting is "1kb"/"4kb"/"8kb"/"16kb"/"32kb"/"64kb"/"variable"
    # read-page-size = "32kb"
    ```

## Bug修复

1. 修复了一些异常场景下节点Panic的问题
2. 修复了批量删除表时，ts-store出现删除表失败的问题
3. 修复了一些内核中锁的问题
4. 修复了一些内核处理空值的问题
5. 修复了一些内存泄露的问题

## 安全

1. 修复漏洞：CVE-2022-41723，通过升级golang.org/x/net的版本到v0.7.0
2. 修复漏洞：CVE-2023-39325，通过升级golang.org/x/net的版本到v0.17.0
3. 修复漏洞：CVE-2023-47248，通过升级pyarrow的版本到v14.0.1
