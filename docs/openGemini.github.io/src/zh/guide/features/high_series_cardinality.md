---
title: 高基数存储引擎
order: 6
---

在数据库中，基数是指数据库的特定列或字段中包含的唯一值的数量。时间序列数据往往包含描述该数据的元数据（习惯称为“TAG”）。通常，主要时间序列数据或元数据会被索引，以提高查询性能，以便您可以快速找到与之匹配的所有值。时间序列数据集的基数通常由每个单独索引列的基数的交叉乘积定义。如果有多个索引列，每个列都有大量唯一值，那么交叉乘积的基数可能会变得非常大。这就是软件开发人员在谈论具有“高基数”的时间序列数据集时通常的意思。

高基数问题直接表现为索引膨胀，内存资源占用高，查询性能下降。该问题是所有时序数据库都会面临的一个难题，openGemini高基数存储引擎提供了该问题的一个解决方案。本文主要介绍高基数存储引擎的配置和使用。

## 配置

高基数存储引擎支持InfluxDB Line Protocol协议（简称行协议）和Apache Arrow Flight协议（简称列协议）写入，从实际测试效果来看，列协议写入性能更优。

列协议写入配置如下：

```
[http]
  flight-address = "{{addr}}:8087" // 8087为列协议写入端口
  flight-enabled = false           // 开启列协议，集群配置文件openGemini.conf中默认关闭，单机版中默认开启
  flight-ch-factor = 2             // 列协议缓存方法系数，可调节列协议性能，默认即可。单机版中该配置项被隐藏
  flight-auth-enabled = false      // 列协议鉴权开关，默认关闭
```

此外，增加了一个隐藏配置项

```
[data]
  snapshot-table-number = 1   //数据排序刷盘的并发数，默认值为1，最大为8
```

当写入流量非常大的情形下，可以添加该配置项，将并发数适当调大，提高数据下盘效率。

## 创建表

高基数存储引擎下创建表，参考[创建表文档](../schema/measurement.md)

## 查询分区键

```
> SHOW SHARDKEY FROM rtt
shard_key  type ShardGroup
---------  ---- ----------
[deviceIp] hash 1
```

`SHOW SHARDKEY`仅对使用了高基数存储引擎的表有效

## 查询排序健

```sql
> SHOW SORTKEY FROM rtt
sort_key
--------
[deviceIp campus time]
```

`SHOW SORTKEY`仅对使用了高基数存储引擎的表有效

## 查询表结构(Schema)

```sql
> SHOW SCHEMA FROM mst0;
shard_key type ShardGroup engine_type primary_key sort_key
--------- ---- ---------- ----------- ----------- --------
[tag1]    hash 1          columnstore [tag1]      [tag1 field1]
```

## 查询

与openGemini默认存储引擎相比，查询语法基本一致，[参考数据查询文档](../query_data/SELECT.html)

:::tip

1. 当前高基数存储引擎支持的聚合算子：count/sum/min/max/mean/first/last/percentile

2. 表达式过滤不支持正则匹配和复合表达式（如a+b>c)
3. 支持嵌套查询

:::

<font color=red>**下面主要列举高基维引擎的查询语法差异之处，主要为SELECT，GROUP BY，ORDER BY三个方面**</font>

### Sample数据

```sql
# 创建数据库db0
CREATE DATABASE db0

# 创建表mst0
USE db0
CREATE MEASUREMENT mst0 (country tag,  "name" tag, age int64,  height float64,  address string, alive bool) WITH  ENGINETYPE = columnstore  PRIMARYKEY time,country,"address" SORTKEY time,country,"address",age,height,"name"

# 原始数据查询
> SELECT * FROM mst0
name: mst0
time                address   age alive country    height name
----                -------   --- ----- -------    ------ ----
1629129600000000000 shenzhen  12  true  "china"    70     "azhu"
1629129601000000000 shanghai  20  false "american" 80     "alan"
1629129602000000000 beijin    3   true  "germany"  90     "alang"
1629129603000000000 guangzhou 30  false "japan"    121    "ahui"
1629129604000000000 chengdu   35  true  "canada"   138    "aqiu"
1629129605000000000 wuhan     48  true  "china"    149    "agang"
1629129606000000000 wuhan     52  true  "american" 153    "agan"
1629129607000000000 anhui     28  false "germany"  163    "alin"
1629129608000000000 xian      32  true  "japan"    173    "ali"
1629129609000000000 hangzhou  60  false "canada"   180    "ali"
1629129610000000000 nanjin    102 true  "canada"   191    "ahuang"
1629129611000000000 zhengzhou 123 false "china"    203    "ayin"
```

### SELECT

语法

```sql
SELECT COLUMN_CLAUSES FROM_CLAUSE
```

COLUMN_CLAUSES可支持字段TAG或FIELD的明细与聚合查询。

**差异对比**

|                                                      | 高基数存储引擎 | 默认存储引擎 |
| ---------------------------------------------------- | -------------- | ------------ |
| SELECT *country* FROM *mst0*     //*country*为TAG    | 支持           | 不支持       |
| SELECT *age* FROM *mst0*    //age为FIELD             | 支持           | 支持         |
| SELECT *country*, *age* FROM *mst0*                  | 支持           | 支持         |
| SELECT count(*country*) FROM mst0   //*country*为TAG | 支持           | 不支持       |
| SELECT count(*age*) FROM mst0                        | 支持           | 支持         |

### GROUP BY

语法

```sql
SELECT COLUMN_CLAUSES FROM_CLAUSE [WHERE_CLAUSE] GROUP BY [* | <tag_key>[,<field_key]]
```

GROUP BY可支持**字符串**字段TAG或FIELD的明细与聚合查询。

**差异对比**

|                                                              | 高基数存储引擎 | 默认存储引擎 |
| ------------------------------------------------------------ | -------------- | ------------ |
| SELECT "name" FROM mst0 GROUP BY country   //“name”为TAG     | 支持           | 不支持       |
| SELECT mean(height) FROM mst0 GROUP BY country  //country为TAG | 支持           | 支持         |
| SELECT mean(height) FROM mst0 GROUP BY address   //address为FIELD | 支持           | 不支持       |
| SELECT "name" FROM mst0 GROUP BY address   //“name”为TAG，address为FIELD | 支持           | 不支持       |

### ORDER BY

语法

```
SELECT COLUMN_CLAUSES FROM_CLAUSE [WHERE_CLAUSE] [GROUP_BY_CLAUSE] ORDER BY COLUMN_CLAUSES [ASC|DESC]
```

openGemini高基维引擎默认不对查询结果进行排序，若要求返回结果有序，可使用ORDER BY排序，支持对TIME、TAG、FIELD或聚合结果等进行排序。

ORDER BY默认升序ASC，可按照排序字段分别指定升序ASC或降序DESC。

**差异对比**

|                                                              | 高基数存储引擎 | 默认存储引擎 |
| ------------------------------------------------------------ | -------------- | ------------ |
| SELECT mean(height) as avg_height <br/>FROM mst0 <br/>WHERE time >=1629129600000000000 AND time <=1629129611000000000 <br/>GROUP BY time(5s), country <br/>FILL(none) <br/>ORDER BY country, avg_height, time | 支持           | 不支持       |
| SELECT mean(height) as avg_height <br/>FROM mst0 <br/>WHERE time >=1629129600000000000 AND time <=1629129611000000000 <br/>GROUP BY time(5s),country <br/>FILL(none) <br/>ORDER BY country DESC, avg_height DESC, time ASC | 支持           | 不支持       |

## 数据写入

### 行协议

行协议写入参考[openGemini数据行协议写入](../write_data/insert_line_protocol.md)

### 列协议

列协议写入参考[openGemini数据列协议写入](../write_data/insert_column_protocol.md)
