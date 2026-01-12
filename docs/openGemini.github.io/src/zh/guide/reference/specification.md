---
title: 硬件推荐配置
order: 1
---
为了帮助您更好使用openGemini,我们给到如下推荐配置，主要以时间线数量为参考标准。
|规格|时间线上限|
|---|---------|
|4U8G|300万|
|4U16G|600万|
|8U16G|600万|
|8U32G|1250万|
|16U32G|1250万|
|16U64G|2500万|
|32U64G|2500万|
|32U128G|5000万|
|64U256G|1亿|

**<font size=5 color=green>时间线解释</font>**

---

**时序数据是什么？** 是指按时间顺序记录的、具有时间戳的数据点。
**时间线是什么？** 时间线是对时序数据的建模，一条时间线的数据指同一数据源产生的一系列数据点的集合。
比如
```sql
mst,host=“192.168.0.1” cpu=0.82 1566000000
mst,host=“192.168.0.1” cpu=0.62 1567000000
mst,host=“192.168.0.1” cpu=0.32 1568000000
```
这里的数据源指host为“192.168.0.1"的这台机器。
openGemini支持InfluxDB的Line Protocol，时间线由两部分组成（时间线Key和时间线Value），时间线Key由tags（数据属性)决定，唯一标识一条时间线，换句话说，每一种tags的组合就是一条时间线。
举个例子：
有如下6条数据，表名为mst，Tag分别是region,host
```sql
mst,region="Shanghai",host=“192.168.0.1” cpu=0.82 1566000000
mst,region="Guangdong",host=“192.168.0.1” cpu=0.72 1566000000
mst,region="Shanghai",host=“192.168.0.2” cpu=0.82 1566000000
mst,region="Shanghai",host=“192.168.0.2” cpu=0.72 15676000000
mst,region="Guangdong",host=“192.168.0.3” cpu=0.72 1566000000
mst,region="Guangdong",host=“192.168.0.2” cpu=0.72 1566000000
```
存在5条时间线，分别是
```sql
region="Shanghai",host=“192.168.0.1”
region="Shanghai",host=“192.168.0.2”
region="Guangdong",host=“192.168.0.1”
region="Guangdong",host=“192.168.0.2”
region="Guangdong",host=“192.168.0.3”
```
参考[查看时间线数量](../schema/schema.md#show-series-cardinality)
