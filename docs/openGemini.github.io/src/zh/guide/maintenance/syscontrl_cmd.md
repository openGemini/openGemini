---
title: 系统命令
order: 6
---

本章节主要介绍openGemini内置的运维命令。

## store类

**1. 数据刷新（DataFlush）**

作用：内存中的数据强制下盘

参数：mod=flush

例：所有节点内存上的数据强制下盘

  ```bash
  curl -i -XPOST 'http://127.0.0.1:8086/debug/ctrl?mod=flush'
  ```

**2. 压缩（compactionEn）**

作用：设置shard是否开启compaction

  - allshards=true

     设置所有的分片的compaction

  - allshards=false

     设置指定的分片的compaction

参数：mod=compen&switchon=true/false&allshards=true/false&shid=[number]

例：设置shard开启compaction

  ```bash
  curl -i -XPOST 'http://127.0.0.1:8086/debug/ctrl?mod=compen&switchon=true&allshards=true'
  ```

**3. 压缩合并（compmerge）**

作用：设置shard是否开启merge(乱序合并)

  - allshards=true

     设置所有的分片的merge

  - allshards=false

     设置指定的分片的merge

参数：mod=merge&switchon=true/false&allshards=true/false&shard_id=[number]

例：设置所d=4的shard开启merge

  ```bash
  curl -i -XPOST 'http://127.0.0.1:8086/debug/ctrl?mod=merge&switchon=true&allshards=false&shard_id=4'
  ```

**4. 快照（snapshot）**

作用：设置快照的时间间隔

参数：mod=snapshot&duration=[time duration]

例：设置快照的时间间隔为30分钟

  ```bash
  curl -i -XPOST 'http://127.0.0.1:8086/debug/ctrl?mod=snapshot&duration=30m'
  ```

**5. 故障点（Failpoint）**

作用：启用/禁用故障点

参数：mod=fallpoint&switchon=true/false&point=[]&term=[]

例：

  - 禁用故障点xxx

  ```bash
  curl -i -XPOST 'http://127.0.0.1:8086/debug/ctrl?mod=fallpoint&switch=false&point=xxx'
  ```

  - 带有term参数启用故障点xxx

  ```bash
  curl -i -XPOST 'http://127.0.0.1:8086/debug/ctrl?mod=fallpoint&switch=true&point=xxx&term=xxx'
  ```

**6. 降采样顺序（DownSampleInOrder）**

作用：设置降采样遍历顺序（顺序或逆序寻找符合条件的DownSamplePolicy）

参数：mod=downsample_in_order&order=true/false（true顺序，false逆序）

例：设置为顺序

  ```bash
  curl -i -XPOST 'http://127.0.0.1:8086/debug/ctrl?mod=downsample_in_order&order=true'
  ```

**7. 节点验证（verifyNode）**

作用：设置是否验证节点状态

参数：mod=verifynode&switchon=true/false

例：禁用验证节点状态

  ```bash
  curl -i -XPOST 'http://127.0.0.1:8086/debug/ctrl?mod=verifynode&switchon=false'
  ```

**8. 只读操作（ReadOnly）**

作用：设置engine读写权限是否是只读的

参数：mod=readonly&switchon=true/false&allnodes=y

例：

  - 向所有节点发设置engine权限只读

  ```
  curl -i -XPOST 'http://127.0.0.1:8086/debug/ctrl?mod=readonly&switchon=true&allnodes=y'
  ```

  - 向指定的host发设置engine权限只读

  ```bash
  curl -i -XPOST 'http://127.0.0.1:8086/debug/ctrl?mod=readonly&switchon=true&host=127.0.0.1
  ```

## sql类

如下命令将发送给所有节点

**1. chunk_reader并发控制(ChunkReaderParallel)**

作用：设置最大chunk_reader并发的数量

参数：mod=chunk_reader_parallel&limit=[number]  （number>=0）

例：设置最大chunk_reader并发的数量为4

  ```bash
  curl -i -XPOST 'http://127.0.0.1:8086/debug/ctrl?mod=chunk_reader_parallel&limit=4'
  ```

**2. 二叉树合并策略（BinaryTreeMerge）**

作用：设置查询时是否启用二叉树合并策略

参数：mod=binary_tree_merge&enabled=1/0

例：设置为查询时使用二叉树合并策略

  ```bash
  curl -i -XPOST 'http://127.0.0.1:8086/debug/ctrl?mod=binary_tree_merge&enabled=1'
  ```

**3. 打印逻辑计划（PrintLogicalPlan）**

作用：设置是否打印逻辑计划

参数：mod=print_logical_plan&enabled=1/0

例：设置为查询时打印出逻辑计划

  ```bash
  curl -i -XPOST 'http://127.0.0.1:8086/debug/ctrl?mod=print_logical_plan&enabled=1'
  ```

**4. 滑动窗口推送（SlidingWindowPushUp）**

作用：设置在支持聚合下推优化计划和shema没有子查询的情况下是否启用的是滑动窗口推送

参数：mod=sliding_window_push_up&enabled=1/0

例：在支持聚合下推优化计划和shema没有子查询的情况下**启用**的是滑动窗口推送

  ```bash
  curl -i -XPOST 'http://127.0.0.1:8086/debug/ctrl?mod=sliding_window_push_up&enabled=1'
  ```

**5. 日志行（LogRows）**

作用：设置打印数据的规则，用于排查是否丢数据

参数：mod=log_rows&switchon=true&rules=主节点标记,key1=value1,key2=value2.....

例：设置为新的日志行规则主节点标记为mst，标签信息为tk1=tv1

  ```bash
  curl -i -XPOST 'http://127.0.0.1:8086/debug/ctrl?mod=log_rows&switchon=true&rules=mst,tk1=tv1'
  ```

**6. 强制广播查询（ForceBroadcastQuery）**

作用：设置进行强制广播查询开关

  - 开：针对所有分片
  - 关：针对计算得到的分片

参数：mod=force_broadcast_query&enabled=1/0

例：强制广播查询

  ```bash
  curl -i -XPOST 'http://127.0.0.1:8086/debug/ctrl?mod=force_broadcast_query&enabled=1'
  ```

**7. 强制时间过滤（TimeFilterProtection）**

作用：设置查询操作是否必须有时间过滤

参数：mod=time_filter_protection&enabled=true/false

例：设置为查询操作必须时间过滤

  ```bash
  curl -i -XPOST 'http://127.0.0.1:8086/debug/ctrl?mod=time_filter_protection&enabled=true'
  ```

## meta类

**1. 切换Leader（SwitchLeader）**

作用：切换Leader

> 注：Leader执行该命令能生效，即切换Leader，Follower执行该命令会返回Leader地址

参数：无

例：

```bash
curl -i -XPOST 'http://127.0.0.1:8091/leadershiptransfer'
```

**2. 查看节点信息（ShowNodeInfo）**
作用：查看节点的信息

参数：witch=raft-stat

例：

```bash
curl -s -GET http://127.0.0.1:8091/debug?witch=raft-stat -H 'all:y' | python -mjson.tool
```
