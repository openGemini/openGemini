---
title: Configuration file
order: 1
---

openGemini.conf 配置项解释，openGemini.single.conf亦可参考本内容

## [common]

common是ts-sql、ts-store、ts-meta公共的配置项。

**meta-join** <Badge text="必填" type="danger" />

- 类型: `[]string`
- 默认值: `无`

- SQL / STOR和META的RPC通信地址。比如：`["127.0.0.1:8092", "127.0.0.2:8092", "127.0.0.3:8092"]`

**ha-enable**

- 类型: `bool`
- 默认值: `false`

- 基于共享存储的HA开关，目前仅支持共享存储可设置为true。
- 生产环境不建议开启。

**executor-memory-size-limit**

- 类型: `string | toml.Size`
- 默认值: `0`

- 执行内存大小限制，比如 `256GB`，`0`表示不限制。

**executor-memory-wait-time**

- 类型: `string | toml.Duration`
- 默认值: `0s`

- 执行内存等待时间，比如 `120s`，`0s`表示不限制。

**pprof-enabled**

- 类型: `bool`
- 默认值: `false`

- 是否开启go pprof服务，监控内存、CPU、goroutine等信息。

**cpu-num**<Badge text="建议" type="tip" />

- 类型: `int`
- 默认值: `0`

- 可使用的cpu核心数，`0`表示自动获取，docker环境中建议手动设置。

**memory-size**

- 类型: `string | toml.Size`
- 默认值: `0`

- 可使用的内存大小，比如：`256GB`，`0`表示不限制。

**ignore-empty-tag**

- 类型: ` bool`
- 默认值: `false`

- 是否忽略空tag。

**report-enable**

- 类型: ` bool`
- 默认值: `true`

- 是否上报遥测数据到openGemini服务器。

**global-dict-files**

- type: `array`
- default: `[]`

- Global dictionary: When encoding information such as Record and ChunkMeta, the field names are replaced with dictionary numbers through the dictionary.
- It can improve data compression rate and data decoding efficiency.
- When adding a new dictionary, only new files can be appended, and existing dictionary files cannot be modified
- the order of dictionary files remains unchanged.

## [meta]

meta是ts-meta专属配置。

**bind-address** <Badge text="必填" type="danger" />

- 类型: `string`
- 默认值: `无`

- meta提供服务的地址，比如：`127.0.0.1:8088`。

**http-bind-address** <Badge text="必填" type="danger" />

- 类型: `string`
- 默认值: `无`

- meta提供HTTP服务的地址，比如：`127.0.0.1:8091`。可外部访问。

**rpc-bind-address** <Badge text="必填" type="danger" />

- 类型: `string`
- 默认值: `无`

- meta提供RPC服务的地址，比如：`127.0.0.1:8092`。仅内部通信使用。

**dir** <Badge text="必填" type="danger" />

- 类型: `string`
- 默认值: `无`

- meta数据保存目录。

**expand-shards-enable**

- 类型: `bool`
- 默认值: `false`

- 是否扩大shards。

**retention-autocreate**

- 类型: `bool`
- 默认值: `true`

- 是否自动创建retention policy。

**election-timeout**

- 类型: `string | toml.Duration`

- 默认值: `1s`
- 选主超时时间。

**heartbeat-timeout**

- 类型: `string | toml.Duration`
- 默认值: `1s`
- 心跳超时时间。

**leader-lease-timeout**

- 类型: `string | toml.Duration`
- 默认值: 60s`
- leader租赁超时时间。

**commit-timeout**

- 类型: `string | toml.Duration`
- 默认值: `50ms`
- 事件提交超时时间。

**cluster-tracing**

- 类型: ` bool`
- 默认值: `true`

- 是否记录trace日志。

**logging-enabled**

- deprecated

**lease-duration**

- 类型: `string | toml.Duration`
- 默认值: `1m`
- 租赁期限。

**meta-version**

- 类型: `int`
- 默认值: `2`
- meta版本。

**split-row-threshold**

- 类型: `int`
- 默认值: `10000`
- row最大行数分裂阈值。

**imbalance-factor**

- 类型: `float`
- 默认值: `0.3`
- 不平衡因子。

**auth-enabled**

- 类型: ` bool`
- 默认值: `false`

- 是否开启鉴权。

**https-enabled**

- 类型: ` bool`
- 默认值: `false`

- 是否开启HTTPS。

**https-certificate**

- 类型: ` string`
- 默认值: `""`

- 开启HTTPS后，证书路径。

**https-private-key**

- 类型: ` string`
- 默认值: `""`

- 开启HTTPS后，私钥路径。

**ptnum-pernode**

- 类型: ` int`
- 默认值: `1`

- 每个store节点的PT的数量。

## [coordinator]
ts-sql的配置，用于和ts-store通信相关。

**write-timeout**

- 类型: `string | toml.Duration`
- 默认值: `120s`
- 数据写入超时时间。

**shard-writer-timeout**

- 类型: `string | toml.Duration`
- 默认值: `30s`
- 数据写入shard内超时时间。

**shard-mapper-timeout**

- 类型: `string | toml.Duration`
- 默认值: `10s`
- 数据打散到指定shard的超时时间。

**shard-tier**

- 类型: `string`
- 默认值: `warm`
- 数据形态，可选值：`warm`, `hot`。
- `hot`模式会比较耗内存。

**rp-limit**

- 类型: `int`
- 默认值: `100`
- 所有database，可以创建的retention policy的上限。

**force-broadcast-query**

- 类型: ` bool`
- 默认值: `false`

- 是否强制使用广播到所有节点进行查询。

**time-range-limit**

- 类型: ` []string`
- 默认值: `["0s", "0s"]`
- 限制写的时间范围。比如：`["72h", "48h"]`，表示仅支持写入**3天前到2天后之间**的数据。如果是默认值，则表示不限制。

## [http]
ts-sql专属配置。

**bind-address** <Badge text="必填" type="danger" />

- 类型: `string`
- 默认值: `无`

- sql提供服务的地址，比如：`127.0.0.1:8086`。

**auth-enabled** <Badge text="建议" type="tip" />

- 类型: `bool`
- 默认值: `false`
- 是否开启身份校验功能。


**weakpwd-path**

- 类型: `string`
- 默认值: `/tmp/openGemini/weakpasswd.properties`
- 此路径是常见弱密码的文件路径，[这里](https://github.com/openGemini/openGemini/blob/main/config/weakpasswd.properties)提供了常用的弱密码，仅供参考。

**pprof-enabled**

- 类型: `bool`
- 默认值: `false`

- 是否开启go pprof服务，监控内存、CPU、goroutine等信息。

**max-connection-limit**

- 类型: `int`
- 默认值: `0`
- ts-sql的最大连接数限制。`0`表示不限制。

**max-concurrent-write-limit**

- 类型: `int`
- 默认值: `0`
- 最大写并发限制。`0`表示不限制。

**max-enqueued-write-limit**

- 类型: `int`
- 默认值: `0`
- 最大写队列数限制。`0`表示不限制。

**enqueued-write-timeout**

- 类型: `string | toml.Duration`
- 默认值: `30s`
- 写队列中，等待超时时间。

**max-concurrent-query-limit**

- 类型: `int`
- 默认值: `0`
- 最大读并发限制。`0`表示不限制。

**max-enqueued-query-limit**

- 类型: `int`
- 默认值: `0`
- 最大读队列数限制。`0`表示不限制。

**enqueued-query-timeout**

- 类型: `string | toml.Duration`
- 默认值: `5m`
- 读队列中，等待超时时间。

**chunk-reader-parallel**

- 类型: `int`
- 默认值: `0`
- 单个查询，并发度数量。`0`表示不限制。

**max-body-size**

- 类型: `int`
- 默认值: `25e6`
- 写入的数据body体最大限制，单位byte。`0`表示不限制。

**https-enabled** <Badge text="建议" type="tip" />

- 类型: `bool`
- 默认值: `false`
- 是否开启HTTPS。

**https-certificate**

- 类型: ` string`
- 默认值: `""`

- 开启HTTPS后，证书路径。

**https-private-key**

- 类型: ` string`
- 默认值: `""`

- 开启HTTPS后，私钥路径。

## [data]

ts-store专属配置。

**store-ingest-addr** <Badge text="必填" type="danger" />

- 类型: `string`
- 默认值: `无`

- 数据接入的RPC地址，比如：`127.0.0.1:8400`。

**store-select-addr** <Badge text="必填" type="danger" />

- 类型: `string`
- 默认值: `无`

- 数据查询的RPC地址，比如：`127.0.0.1:8401`。

**store-data-dir** <Badge text="必填" type="danger" />

- 类型: `string`
- 默认值: `无`

- 主要数据存储目录。

**store-wal-dir** <Badge text="必填" type="danger" />

- 类型: `string`
- 默认值: `无`
- 写前日志WAL存储目录。

:::  tip

建议和**store-data-dir** 不同硬盘，防止IO带宽不足。

:::

**store-meta-dir**

::: warning

deprecated

:::

**wal-enabled**

- 类型: `bool`
- 默认值: `true`
- 是否开启WAL功能。

**wal-sync-interval**

- 类型: `string | toml.Duration`
- 默认值: `100ms`
- WAL定时刷盘时间周期。

**wal-replay-parallel**

- 类型: `bool`
- 默认值: `false`
- 是否并发回放WAL。

**wal-replay-async**

- 类型: `bool`
- 默认值: `false`
- 是否异步回放WAL。

**write-cold-duration**

- 类型: `string | toml.Duration`
- 默认值: `5s`
- 数据写冷（下盘）周期。

**shard-mutable-size-limit**

- 类型: `string | toml.Size`
- 默认值: [memorySize](#memory-size)/256，并强制介于 **8MB - 1GB** 之间。
- 单个shard中数据占用内存大小限制。

**node-mutable-size-limit**

- 类型: `string | toml.Size`
- 默认值: [memory-size](#memory-size)/16，并强制介于 **32MB - 16GB** 之间。
- 节点中数据占用内存大小限制。

**max-write-hang-time**

- 类型: `string | toml.Duration`
- 默认值: `15s`
- 数据hang住，最大时间。

**enable-mmap-read**

- 类型: `bool`
- 默认值: `false`
- 是否开启mmap。

::: danger

目前建议不要开启，会占用比较多的内存。

:::

**read-cache-limit**

- 类型: `int`
- 默认值: `0`
- 读缓存大小限制，单位byte。

**write-concurrent-limit**

- 类型: `int`
- 默认值: `0`
- 写并发数限制。

**open-shard-limit**

- 类型: `int`
- 默认值: `0`
- ts-store启动同时打开shard的数量限制。`0`表示不限制。

**readonly**

- 类型: `bool`
- 默认值: `false`
- ts-store只读功能是否打开。

**downsample-write-drop**

- 类型: `bool`
- 默认值: `false`
- 是否支持将采样丢点，仅在ts-store只读功能打开情况下可用。

**max-wait-resource-time**

- 类型: `string | toml.Duration`
- 默认值: `0s`
- TODO

**max-series-parallelism-num**

- 类型: `int`
- 默认值: `0`
- 时间线并发度最大限制。等于 0 将使用默认值; 小于 0 表示不限制。

**max-shards-parallelism-num**

- 类型: `int`
- 默认值: `0`
- shard并发度最大限制。等于 0 将使用默认值; 小于 0 表示不限制。

**chunk-reader-threshold**

- 类型: `int`
- 默认值: `0`
- chunk reader阈值。`0`表述不限制。

**min-chunk-reader-concurrency**

- 类型: `int`
- 默认值: `0`
- 最小chunk reader并发度。`0`表述不限制。

**min-shards-concurrency**

- 类型: `int`
- 默认值: `0`
- 最小shard并发度。`0`表述不限制。

**max-downsample-task-concurrency**

- 类型: `int`
- 默认值: `0`
- 最大将采样任务并发度。`0`表述不限制。

**max-series-per-database**

- 类型: `int`
- 默认值: `0`
- 限制每个database中时间线数量，`0`表述不限制

## [data.compact]

**max-concurrent-compactions**

- 类型: `int`
- 默认值: [cpu-num](#cpu-num)数，并介于 **2-32**之间。
- Maximum number of concurrent Level Compaction tasks. The value 0 indicates that the default value is used.

**compact-full-write-cold-duration**

- 类型: `string | toml.Duration`
- 默认值: `1h`
- shard内，超过该配置时间范围没有新数据写入，将对该shard执行 full compaction

**max-full-compactions**

- 类型: `int`
- 默认值: `1`
- Maximum number of concurrent Full Compaction tasks. Between 1 and 32.

**compact-throughput**

- 类型: `string | toml.Size`
- 默认值: `80m`
- compact吞吐量。

**compact-throughput-burst**

- 类型: `string | toml.Size`
- 默认值: `90m`
- compact吞吐量突发。

**compact-recovery**

- 类型: `bool`
- 默认值: `false`
- Compaction任务失败后自动恢复功能的开关，开启后可以防止进程Panic。

**snapshot-throughput**

- 类型: `string | toml.Size`
- 默认值: `64m`
- 打快照吞吐量。

**snapshot-throughput-burst**

- 类型: `string | toml.Size`
- 默认值: `64m`
- 打快照突发吞吐量。

**compaction-method**
- Type: `int`
- Default: `1`
- Compaction Method. 0 Auto; 1 stream mode; 2 normal

**max-compaction-level**
- Type: `int`
- Default: `0`
- Upper limit of the compaction level. 0: not limited

## [data.ops-monitor]

该项为云服务配置，可忽略

## [data.hot-mod]

**enabled = false**

- type: `bool`
- default: `false`
- If this flag is set to true, the newly flushed file will be read into the memory

**memory-allowed-percent = 5**

- type: `int`
- default: `30`
- Allowed percent of system memory hot mode cache may occupy. default 30

**duration = "1h"**

- type: `toml.Duration`
- default: `1h`
- The default value is 0, indicating that the time range is not limited

**time-window = "60s"**

- type: `toml.Duration`
- default: `60s`
- When the memory usage reaches the threshold or hot data expires, hot data is converted to warm data in batches.
- Calculate the time window based on the maximum file time.
- Select the earliest time window and change the files in the window from hot to warn.


## [retention]

retention policy（保留策略）配置。

**enabled = true**

- 类型: `bool`
- 默认值: `true`
- 是否开启。


**check-interval = "30m"**

- 类型: `string | toml.Duration`
- 默认值: `30m`
- 检测周期。

## [downsample]

多级将采样配置。

**enable = true**

- 类型: `bool`
- 默认值: `true`
- 是否开启。


**check-interval = "30m"**

- 类型: `string | toml.Duration`
- 默认值: `30m`
- 检测周期。

## [logging]

日志全局配置。

**format**

- 类型: `string`
- 默认值: `auto`
- 输出格式样式。

**level**

- 类型: `string`
- 默认值: `info`
- 日志记录的级别。可选值：`debug`, `info`, `warn`, `error`, `panic`等

**path** <Badge text="必填" type="danger" />

- 类型: `string`
- 默认值: 无
- 日志输出目录。

**max-size**

- 类型: `string | toml.Size`
- 默认值: `64m`
- 日志单个文件最大大小。

**max-num**

- 类型: `int`
- 默认值: `16`
- 日志最多保存的文件数。

**max-age**

- 类型: `int`
- 默认值: `16`
- 日志最多保存的周期，单位天。

**compress-enabled**

- 类型: `bool`
- 默认值: `true`
- 是否压缩。

## [tls]

tsl全局配置。

**min-version**

- 类型: `string`
- 默认值: 无
- tls最小版本。建议："TLS1.2"

**ciphers**

- 类型: `[]string`
- 默认值: 无
- ciphers。建议:
```toml
ciphers = [
  "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
  "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
  "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
  "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384",
]
```

## [monitor]

ts-sql, ts-store, ts-meta 自身监控相关配置。

**pushers**

- 类型: `string`
- 默认值: `""`
- 监控指标推送方式，可选值：`http`, `file`。

**store-enabled**

- 类型: `bool`
- 默认值: `false`
- 是否开启monitor数据存储。

**store-database**

- 类型: `string`
- 默认值: `_internal`
- 监控指标保存到database名称。仅在`pushers="http"`可用。

**store-interval**

- 类型: `string | toml.Duration`
- 默认值: `10s`
- 数据保存周期。

**store-path**

- 类型: `string`
- 默认值: 无
- 监控指标保存路径和文件名称模板。仅在`pushers="file"`可用。比如：`/tmp/openGemini/metric/metric.data`

**compress**

- 类型: `bool`
- 默认值: `false`
- 监控指标保存到文件是否压缩。仅在`pushers="file"`可用。

::: tip

目前不建议打开

:::

**http-endpoint**

- 类型: `string`
- 默认值: 无
- 监控指标通过http发送到的endpoint。仅在`pushers="http"`可用。比如：`127.0.0.1:8086`

**username**

- 类型: `string`
- 默认值: 空
- 监控指标通过http发送到的endpoint的用户名。仅在`pushers="http"`可用。

**password**

- 类型: `string`
- 默认值: 空
- 监控指标通过http发送到的endpoint的密码。仅在`pushers="http"`可用。

## [gossip]

ts-store，ts-meta的gossip配置，用于检测集群各节点是否健康。

**enabled**

- 类型: `bool`
- 默认值: `集群:true，单机:false`
- 是否开启gossip。

**log-enabled**

- 类型: `bool`
- 默认值: `true`
- 是否开启gossip日志输出。

**bind-address**

- 类型: `string`
- 默认值: 无
- gossip绑定的地址。比如: `127.0.0.1`

::: danger

集群必填

::::

**store-bind-port**

- 类型: `int`
- 默认值: `8011`
- ts-store端的gossip，绑定的端口。比如：`8011`

**meta-bind-port**

- 类型: `int`
- 默认值: `8010`
- ts-meta端的gossip，绑定的端口。比如：`8010`

**prob-interval**

- 类型: `string | toml.Duration`
- 默认值: `1s`
- 探测周期。

**suspicion-mult**

- 类型: `int`
- 默认值: `4`
- 怀疑多个。

**members**

- 类型: `[]string`
- 默认值: `无`

- META的gossip地址。比如：`["127.0.0.1:8010", "127.0.0.2:8010", "127.0.0.3:8010"]`

::: danger

集群必填

::::

## [spdy]

连接复用专用配置，一般不需要修改

## [castor]

时序数据异常检测专用配置。

enabled = false
pyworker-addr = ["127.0.0.1:6666"]
connect-pool-size = 30
result-wait-timeout = 10  # unit: second

## [castor.detect]

algorithm=
['BatchDIFFERENTIATEAD','DIFFERENTIATEAD','IncrementalAD','ThresholdAD','ValueChangeAD']

config_filename = ['detect_base']

## [castor.fit_detect]

algorithm=
['BatchDIFFERENTIATEAD','DIFFERENTIATEAD','IncrementalAD','ThresholdAD','ValueChangeAD']

config_filename = ['detect_base']

## [sherlock]

sherlock，自动导出pprof文件功能相关配置。
sherlock-enable = false
collect-interval = "10s"
cpu-max-limit = 95
dump-path = "/tmp"

## [sherlock.cpu]

enable = false
min = 30
diff = 25
abs = 70
cool-down = "10m"

## [sherlock.memory]

enable = false
min = 25
diff = 25
abs = 80
cool-down = "10m"

## [sherlock.goroutine]

enable = false
min = 10000
diff = 20
abs = 20000
max = 100000
cool-down = "30m"

## [shelf-mode]

Configuration items related to the write process in shelf mode

**enabled = false**

- type: `bool`
- default: `false`
- Set this parameter to true to enable the shelf write mode

**reliability-level = 2**

- type: `int`
- default: `2`
- Reliability requirements for data writing
- 1: Low reliability. Data may be lost due to process faults.
- 2: Medium reliability. Data may be lost due to container or VM faults. (default)
- 3: High reliability. Data may be lost when a storage medium is faulty.

**max-wal-file-size = "256m"**
- type: `toml.Size`
- default: `256m`
- Maximum size of a WAL file on the disk.
- If the size of a WAL file exceeds the value, a new WAL file is used.

**max-wal-duration = "300s"**
- type: `toml.Duration`
- default: `300s`
- Indicates the hold time of a WAL.
- If the hold time of a WAL exceeds the configured value, a new WAL is forcibly switched even if the hold time of a WAL does not reach the value specified by `max-wal-file-size`.

**wal-compress-mode = 1**

- type: `int`
- default: `1`
- WAL data compression algorithm. 0 no compression; 1 LZ4; 2 Snappy

**concurrent = 0**

- type: `int`
- default: `0`
- number of background write threads. default value is CPUNum/2

**series-hash-factor = 1**

- type: `int`
- default: `1`
- by default, the table is grouped based on the hash value of the measurement name
- If this parameter is set to a value greater than 1, secondary grouping is performed based on the hash value of the series key

**tssp-convert-concurrent = 0**

- type: `int`
- default: `0`
- Max number of concurrent WAL files to be converted to SSP files.
- default value is the same as Concurrent
