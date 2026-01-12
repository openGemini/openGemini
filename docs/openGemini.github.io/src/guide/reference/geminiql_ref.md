---
title: Key words
order: 6
---

## 文法介绍
go-yacc

## 系统保留关键字(Keywords)

```
ALL          ALTER         ANY          AS           ASC         BEGIN
BY           CREATE        CONTINUOUS   DATABASE     DATABASES   DOWNSAMPLE
DOWNSAMPLES  DEFAULT       DELETE       DELAY        DESC        DESTINATIONS
DIAGNOSTICS  DISTINCT      DROP         DURATION     END         EVERY
EXPLAIN      FIELD         FOR          FROM         GRANT       GRANTS
GROUP        GROUPS        IN           INF          INSERT      INTO
KEY           KEYS         KILL         LIMIT        SHOW        MEASUREMENT
MEASUREMENTS  NAME         OFFSET       ON           ORDER       PASSWORD
POLICY        POLICIES     PRIVILEGES   QUERIES      QUERY       READ
REPLICATION   RESAMPLE     RETENTION    REVOKE       SAMPLEINTERVAL
SELECT        SERIES       SET          SHARD        SHARDS      SLIMIT
SOFFSET       STATS        STREAM       STREAMS      SUBSCRIPTION
SUBSCRIPTIONS TAG          TIMEINTERVAL TO           USER        USERS
VALUES        WHERE        WITH         WRITE
```


如果您使用GeminiQL关键字作为标识符，您需要将每个查询中的标识符用双引号括起来。

关键字`time`是一个特例。`time`可以是一个连续查询名字、数据库名字、measurement的名字、保留策略名字、和用户名。在这些情况下，不需要在查询中用双引号将`time`括起来。
`time`不能是field key或tag key；openGemini拒绝写入将`time`作为field key或tag key的数据，对于这种数据写入，openGemini会返回错误。
