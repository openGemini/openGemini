---
title: 聚合函数
order: 1
---

本章主要介绍如下函数：
| 聚合函数 | 说明 |
| --- | --- |
| [COUNT()](#count) | 统计数据总量 |
| [**COUNT(time)**](#count-time) | 按时间列统计数据 |
| [MEAN()](#mean) | 平均值 |
| [SUM ()](#sum) | 求和 |
| [MODE()](#mode) | 最频繁值|
| [STDDEV()](#stddev) | 标准差 |
| [MEDIAN()](#median) | 中位数 |
| [SPREAD()](#spread) | 跨度值 |
| [DISTINCT()](#distinct) | 去重 |
| [**IRATE()**](#irate) | ( \|A<sub>n</sub> - A<sub>n-1</sub>\| ) / ( \|T<sub>n</sub> - T<sub>n-1</sub>\| ) |
| [**RATE()**](#rate) | ( \|A<sub>n</sub>-A<sub>1</sub>\| ) / ( \|T<sub>n</sub>-T<sub>1</sub>\|) |
| [MOVING_AVERAGE()](#moving-average) | 滑动平均 |
| [HOLT_WINTERS()](#holt-winters) | 预测未来N个值 |
| [CUMULATIVE_SUM()](#cumulative-sum) | 累积求和 |
| [DERIVATIVE()](derivative) | 后续值的变化率 |
| [DIFFERENCE()](#difference) | 后续值的差值 |
| [ELAPSED()](#elapsed)	| 后续值的时间差值 |
| [NON_NEGATIVE_DERIVATIVE()](#non-negative-derivative)	|后续值的非负变化率 |
| [NON_NEGATIVE_DIFFERENCE()](#non-negative-difference)	|后续值的非负差值 |

::: tip

openGemini提供的函数兼容InfluxDB的用法，可参考InfluxDB对应的[函数用法](https://docs.influxdata.com/influxdb/v1/query_language/functions)。
:::

## COUNT()

返回给定Field列非空值数量，不支持对Tag列计算, 不支持SLIMIT和SOFFSET。

**语法**

```sql
SELECT COUNT( [ * | <field_key> | /<regular_expression>/ ] ) [INTO_CLAUSE] FROM_CLAUSE [WHERE_CLAUSE] [GROUP_BY_CLAUSE] [ORDER_BY_CLAUSE] [LIMIT_CLAUSE] [OFFSET_CLAUSE]
```
**嵌套语法**

```sql
SELECT COUNT(DISTINCT( [ * | <field_key> | /<regular_expression>/ ] )) [...]
```

支持将`DISTINCT()`函数嵌套在`COUNT()`函数里，统计不同值的数量。

**示例**

- **计算指定field key的field value的数目**

```sql
> SELECT COUNT("water_level") FROM "h2o_feet"
name: h2o_feet
+----------------------+-------+
| time                 | count |
+----------------------+-------+
| 1970-01-01T00:00:00Z | 15258 |
+----------------------+-------+
2 columns, 1 rows in set
```

该查询返回measurement`h2o_feet`中的`water_level`的非空field value的数量。

- **计数measurement中每个field key关联的field value的数量**

```sql
> SELECT COUNT(*) FROM "h2o_feet"
name: h2o_feet
+----------------------+-------------------------+-------------------+
| time                 | count_level description | count_water_level |
+----------------------+-------------------------+-------------------+
| 1970-01-01T00:00:00Z | 15258                   | 15258             |
+----------------------+-------------------------+-------------------+
3 columns, 1 rows in set
```

该查询返回与measurement`h2o_feet`相关联的每个field key的非空field value的数量。`h2o_feet`有两个field keys：`level_description`和`water_level`

- **计算匹配一个正则表达式的每个field key关联的field value的数目**

```sql
> SELECT COUNT(/water/) FROM "h2o_feet"
name: h2o_feet
+----------------------+-------------------+
| time                 | count_water_level |
+----------------------+-------------------+
| 1970-01-01T00:00:00Z | 15258             |
+----------------------+-------------------+
2 columns, 1 rows in set
```

该查询返回measurement`h2o_feet`中包含`water`单词的每个field key的非空字段值的数量。

- **计数包括多个子句的field key的field value的数目**

```sql
> SELECT COUNT("water_level") FROM "h2o_feet" WHERE time >= '2019-08-20T00:12:01Z' AND time <= '2019-08-20T08:00:00Z' GROUP BY time(7m),* fill(1000) LIMIT 4
name: h2o_feet
tags: location=coyote_creek
+----------------------+-------+
| time                 | count |
+----------------------+-------+
| 2019-08-20T00:10:00Z | 1000  |
| 2019-08-20T00:17:00Z | 1     |
| 2019-08-20T00:24:00Z | 2     |
| 2019-08-20T00:31:00Z | 1     |
+----------------------+-------+
2 columns, 4 rows in set
```

该查询返回`water_level`field key中的非空field value的数量。它涵盖`2019-08-20T00:12:01Z`和`2019-08-20T08:00:00Z`之间的`时间段`，并将结果分组为7分钟的时间间隔和每个tag。并用`1000`填充空的时间间隔，并返回4个数据point，表格返回1。

- **计算一个field key的distinct的field value的数量**

```sql
> SELECT COUNT(DISTINCT("level description")) FROM "h2o_feet"
name: h2o_feet
+----------------------+-------+
| time                 | count |
+----------------------+-------+
| 1970-01-01T00:00:00Z | 4     |
+----------------------+-------+
2 columns, 1 rows in set
```

查询返回measurement为`h2o_feet`field key为`level description`的唯一field value的数量。

**`COUNT()`的常见问题>**

- **`COUNT()`和`fill()`**

大多数函数对于没有数据的时间间隔返回`null`值，`fill()`将该`null`值替换为`fill_option`。 `COUNT()`针对没有数据的时间间隔返回`0`，`fill(<fill_option>)`用`fill_option`替换0值。

**示例**

下面的代码块中的第一个查询不包括`fill()`。有些时间段没有数据，因此该时间间隔的值返回为零。第二个查询包括`fill(800000)`; 它将没有数据的间隔中的零替换为`800000`。

```sql
> SELECT COUNT("water_level") FROM "h2o_feet" WHERE time >= '2019-08-20T00:12:01Z' AND time <= '2019-08-20T08:00:00Z' GROUP BY time(7m),*  LIMIT 4
name: h2o_feet
tags: location=coyote_creek
+----------------------+-------+
| time                 | count |
+----------------------+-------+
| 2019-08-20T00:10:00Z | 0     |
| 2019-08-20T00:17:00Z | 1     |
| 2019-08-20T00:24:00Z | 2     |
| 2019-08-20T00:31:00Z | 1     |
+----------------------+-------+
2 columns, 4 rows in set

> SELECT COUNT("water_level") FROM "h2o_feet" WHERE time >= '2019-08-20T00:12:01Z' AND time <= '2019-08-20T08:00:00Z' GROUP BY time(7m),* fill(800000) LIMIT 4
name: h2o_feet
tags: location=coyote_creek
+----------------------+--------+
| time                 | count  |
+----------------------+--------+
| 2019-08-20T00:10:00Z | 800000 |
| 2019-08-20T00:17:00Z | 1      |
| 2019-08-20T00:24:00Z | 2      |
| 2019-08-20T00:31:00Z | 1      |
+----------------------+--------+
2 columns, 4 rows in set
```

## SUM()

返回指定Field列的值的总和，限 int 和 float 类型。不支持对Tag列计算

**语法**

```sql
SELECT SUM( [ * | <field_key> | /<regular_expression>/ ] ) [INTO_CLAUSE] FROM_CLAUSE [WHERE_CLAUSE] [GROUP_BY_CLAUSE] [ORDER_BY_CLAUSE] [LIMIT_CLAUSE] [OFFSET_CLAUSE]
```

`SUM(field_key)`
返回field key对应的field value的总和。

`SUM(/regular_expression/)`
返回与正则表达式匹配的每个field key对应的field value的总和。

`SUM(*)`
返回在measurement中每个field key对应的field value的总和。

**示例**

**计算指定field key对应的field value的总和**

```sql
> SELECT SUM(water_level) FROM h2o_feet
name: h2o_feet
+----------------------+-------------------+
| time                 | sum               |
+----------------------+-------------------+
| 1970-01-01T00:00:00Z | 67774.98933334895 |
+----------------------+-------------------+
2 columns, 1 rows in set
```

该查询返回measurement `h2o_feet`中field key `water_level`对应的field value的总和。

**计算measurement中每个field key对应的field value的总和**

```sql
> SELECT SUM(*) FROM h2o_feet
name: h2o_feet
+----------------------+-------------------+
| time                 | sum_water_level   |
+----------------------+-------------------+
| 1970-01-01T00:00:00Z | 67774.98933334895 |
+----------------------+-------------------+
2 columns, 1 rows in set
```

该查询返回measurement `h2o_feet`中每个存储数值的field key对应的field value的总和。measurement `h2o_feet`中只有一个数值类型的field：`water_level`。

**计算与正则表达式匹配的每个field key对应的field value的总和**

```sql
> SELECT SUM(/water/) FROM h2o_feet
name: h2o_feet
+----------------------+-------------------+
| time                 | sum_water_level   |
+----------------------+-------------------+
| 1970-01-01T00:00:00Z | 67774.98933334895 |
+----------------------+-------------------+
2 columns, 1 rows in set
```

该查询返回measurement `h2o_feet`中每个存储数值并包含单词`water`的field key对应的field value的总和。

**计算指定field key对应的field value的总和并包含多个子句**

```sql
> SELECT SUM(water_level) FROM h2o_feet WHERE time >= '2019-08-20T00:12:01Z' AND time <= '2019-08-20T08:00:00Z' GROUP BY time(7m),* fill(1000) LIMIT 4
name: h2o_feet
tags: location=coyote_creek
+----------------------+--------+
| time                 | sum    |
+----------------------+--------+
| 2019-08-20T00:10:00Z | 1000   |
| 2019-08-20T00:17:00Z | 8.684  |
| 2019-08-20T00:24:00Z | 17.316 |
| 2019-08-20T00:31:00Z | 8.619  |
+----------------------+--------+
2 columns, 4 rows in set
```

该查询返回measurement `h2o_feet`中field key `water_level`对应的field value的总和，它涵盖的时间范围在`2019-08-20T00:12:01Z`和`2019-08-20T08:00:00Z`之间，并将查询结果按7分钟的时间间隔和每个tag进行分组，同时，该查询用`1000`填充没有数据的时间间隔，并将返回的数据point个数限制为4。

## COUNT(time)

返回time列的数量，由于其他列可能出现空值，导致count返回的数量不准确，由于time列不可能存在空值，因此Count(time)则为准确值。

**语法**

```
SELECT COUNT(time) [INTO_CLAUSE] FROM_CLAUSE [WHERE_CLAUSE] [GROUP_BY_CLAUSE] [ORDER_BY_CLAUSE] [LIMIT_CLAUSE] [OFFSET_CLAUSE]
```

**示例**

```sql
> select count(time) from h2o_quality
name: h2o_quality
+------+-------+
| time | count |
+------+-------+
| 0    | 15258 |
+------+-------+

2 columns, 1 rows in set
```

其他语法例子可以参考[COUNT](#count)

## MEAN()

返回指定Field列的算术平均值，不支持对Tag列计算, 不支持SLIMIT和SOFFSET。

**语法**

```sql
SELECT MEAN( [ * | <field_key> | /<regular_expression>/ ] ) [INTO_clause] FROM_clause [WHERE_clause] [GROUP_BY_clause] [ORDER_BY_clause] [LIMIT_clause] [OFFSET_clause]
```

**示例**

**计算所有Field的算术平均值**

```sql
> SELECT MEAN(*) FROM h2o_quality
name: h2o_quality
+------+-------------------+
| time | mean_index        |
+------+-------------------+
| 0    | 49.85705859221392 |
+------+-------------------+
2 columns, 1 rows in set
```

**计算所有满足正则匹配的Field的算术平均值**

```sql
> SELECT MEAN(/water/) FROM "h2o_feet"
name: h2o_feet
+------+-------------------+
| time | mean_water_level  |
+------+-------------------+
| 0    | 4.441931402107023 |
+------+-------------------+
2 columns, 1 rows in set
```

**按location分组求算术平均值**

```sql
> SELECT MEAN(water_level) FROM h2o_feet GROUP BY location
name: h2o_feet
tags: location=coyote_creek
+------+-------------------+
| time | mean              |
+------+-------------------+
| 0    | 5.359142420303915 |
+------+-------------------+
2 columns, 1 rows in set

name: h2o_feet
tags: location=santa_monica
+------+--------------------+
| time | mean               |
+------+--------------------+
| 0    | 3.5307120942458807 |
+------+--------------------+
2 columns, 1 rows in set
```

**指定时间范围，按location和time分组求平均值**

```sql
> SELECT MEAN(water_level) FROM h2o_feet WHERE time > 1565827200000000000 AND time < 1568851200000000000 GROUP BY location,time(7d)
name: h2o_feet
tags: location=coyote_creek
+---------------------+--------------------+
| time                | mean               |
+---------------------+--------------------+
| 1565827200000000000 | 5.2549815615608315 |
| 1566432000000000000 | 5.26997790112619   |
| 1567036800000000000 | 5.272352687407984  |
| 1567641600000000000 | 5.460220592024403  |
| 1568246400000000000 | 5.542803999609522  |
+---------------------+--------------------+
2 columns, 5 rows in set

name: h2o_feet
tags: location=santa_monica
+---------------------+--------------------+
| time                | mean               |
+---------------------+--------------------+
| 1565827200000000000 | 3.3571135810808332 |
| 1566432000000000000 | 3.2962888674321427 |
| 1567036800000000000 | 3.535538707562054  |
| 1567641600000000000 | 3.5668245066446427 |
| 1568246400000000000 | 3.9068693439619184 |
+---------------------+--------------------+
2 columns, 5 rows in set
```

## MODE()

返回Field列中最常见的值，不支持对Tag列计算, 不支持SLIMIT和SOFFSET。

**语法**

```sql
SELECT MODE( [ * | <field_key> | /<regular_expression>/ ] ) [INTO_clause] FROM_clause [WHERE_clause] [GROUP_BY_clause] [ORDER_BY_clause] [LIMIT_clause] [OFFSET_clause]
```

**示例**

**计算指定Field列出现次数最多的值**

```sql
> SELECT MODE(index) FROM h2o_quality
name: h2o_quality
+------+------+
| time | mode |
+------+------+
| 0    | 80   |
+------+------+
2 columns, 1 rows in set
```

::: tip

如果存在两个值出现次数相同，则返回时间戳最早的那个值

:::

**计算每一列出现次数最多的值**

```sql
> SELECT MODE(*) FROM h2o_feet
name: h2o_feet
+------+------------------------+------------------+
| time | mode_level description | mode_water_level |
+------+------------------------+------------------+
| 0    | between 3 and 6 feet   | 2.69             |
+------+------------------------+------------------+
3 columns, 1 rows in set
```

**指定时间范围，按location和time分组求water_level出现次数最多的值。*time(7d)表示数据按7天分组***

```sql
> SELECT MODE(water_level) FROM h2o_feet WHERE time > 1565827200000000000 AND time < 1568851200000000000 GROUP BY location,time(7d)
name: h2o_feet
tags: location=coyote_creek
+---------------------+-------+
| time                | mode  |
+---------------------+-------+
| 1565827200000000000 | 6.713 |
| 1566432000000000000 | 8.386 |
| 1567036800000000000 | 9.728 |
| 1567641600000000000 | 9.098 |
| 1568246400000000000 | 2.831 |
+---------------------+-------+
2 columns, 5 rows in set

name: h2o_feet
tags: location=santa_monica
+---------------------+-------+
| time                | mode  |
+---------------------+-------+
| 1565827200000000000 | 4.8   |
| 1566432000000000000 | 2.979 |
| 1567036800000000000 | 5.328 |
| 1567641600000000000 | 4.101 |
| 1568246400000000000 | 2.034 |
+---------------------+-------+
2 columns, 5 rows in set
```

**指定时间范围和localtion，按time分组求water_level出现次数最多的值。*time(3d)表示数据按3天分组***

```sql
> SELECT MODE(water_level) FROM h2o_feet WHERE time > 1565827200000000000 AND time < 1568851200000000000 and location='santa_monica' GROUP BY time(3d)
name: h2o_feet
+---------------------+-------+
| time                | mode  |
+---------------------+-------+
| 1565827200000000000 | 4.8   |
| 1566086400000000000 | 2.352 |
| 1566345600000000000 | 3.11  |
| 1566604800000000000 | 3.855 |
| 1566864000000000000 | 4.429 |
| 1567123200000000000 | 1.375 |
| 1567382400000000000 | 2.69  |
| 1567641600000000000 | 1.722 |
| 1567900800000000000 | 5.377 |
| 1568160000000000000 | 6.07  |
| 1568419200000000000 | 2.116 |
| 1568678400000000000 | 3.917 |
+---------------------+-------+
2 columns, 12 rows in set
```

## STDDEV()

返回指定Field列的标准差，不支持对Tag列计算，不支持SLIMIT和SOFFSET

**语法**

```sql
SELECT STDDEV( [ * | <field_key> | /<regular_expression>/ ] ) [INTO_clause] FROM_clause [WHERE_clause] [GROUP_BY_clause] [ORDER_BY_clause] [LIMIT_clause] [OFFSET_clause]
```

**示例**

**指定时间范围，计算water_level列的标准差**

```sql
> SELECT STDDEV(water_level) FROM h2o_feet WHERE time > 1565827200000000000 AND time < 1568851200000000000
name: h2o_feet
+---------------------+--------------------+
| time                | stddev             |
+---------------------+--------------------+
| 1565827200000000001 | 2.2791252793622587 |
+---------------------+--------------------+
2 columns, 1 rows in set
```

**指定时间范围，分组计算water_level列的标准差**

```sql
> SELECT STDDEV(water_level) FROM h2o_feet WHERE time > 1565827200000000000 AND time < 1568851200000000000 GROUP BY location
name: h2o_feet
tags: location=coyote_creek
+---------------------+-------------------+
| time                | stddev            |
+---------------------+-------------------+
| 1565827200000000001 | 2.553231100033397 |
+---------------------+-------------------+
2 columns, 1 rows in set

name: h2o_feet
tags: location=santa_monica
+---------------------+--------------------+
| time                | stddev             |
+---------------------+--------------------+
| 1565827200000000001 | 1.4875647953606128 |
+---------------------+--------------------+
2 columns, 1 rows in set
```

**指定时间范围，分组计算water_level列的标准差，从第3条数据开始返回10条数据**

```sql
> SELECT STDDEV(water_level) FROM h2o_feet WHERE time >= '2019-08-12T00:00:00Z' AND time <= '2019-08-19T00:59:00Z' GROUP BY time(12m),* fill(18000) LIMIT 10 OFFSET 2
name: h2o_feet
tags: location=coyote_creek
+------+---------------------+
| time | stddev              |
+------+---------------------+
| 0    | 0.09545941546026109 |
| 0    | 0.0975807358037642  |
| 0    | 0.09050966799184483 |
| 0    | 0.08768124086706051 |
| 0    | 0.09758073580369138 |
| 0    | 0.09263098833540352 |
| 0    | 0.10253048327207068 |
| 0    | 0.09050966799184483 |
| 0    | 0.08768124086714155 |
| 0    | 0.09050966799188409 |
+------+---------------------+
2 columns, 10 rows in set

> SELECT STDDEV(water_level) FROM h2o_feet WHERE location='coyote_creek' AND time >= '2019-08-12T00:00:00Z' AND time <= '2019-08-19T00:59:00Z' GROUP BY time(12m),* fill(18000) LIMIT 10 OFFSET 2
name: h2o_feet
tags: location=coyote_creek
+------+---------------------+
| time | stddev              |
+------+---------------------+
| 0    | 0.09545941546026109 |
| 0    | 0.0975807358037642  |
| 0    | 0.09050966799184483 |
| 0    | 0.08768124086706051 |
| 0    | 0.09758073580369138 |
| 0    | 0.09263098833540352 |
| 0    | 0.10253048327207068 |
| 0    | 0.09050966799184483 |
| 0    | 0.08768124086714155 |
| 0    | 0.09050966799188409 |
+------+---------------------+
2 columns, 10 rows in set
```

## MEDIAN()

返回指定Field列数据的中位数，不支持对Tag列计算，不支持SLIMIT和SOFFSET。

**语法**

```sql
SELECT MEDIAN( [ * | <field_key> | /<regular_expression>/ ] ) [INTO_clause] FROM_clause [WHERE_clause] [GROUP_BY_clause] [ORDER_BY_clause] [LIMIT_clause] [OFFSET_clause]
```

**示例**

**指定时间范围，计算pH列的中位数**

```sql
> SELECT MEDIAN(pH) FROM h2o_pH WHERE time > 1565827200000000000 AND time < 1568851200000000000
name: h2o_pH
+---------------------+--------+
| time                | median |
+---------------------+--------+
| 1565827200000000001 | 7      |
+---------------------+--------+
2 columns, 1 rows in set
```

**指定时间范围，分组计算pH列的中位数**

```sql
> SELECT MEDIAN(pH) FROM h2o_pH WHERE time > 1565827200000000000 AND time < 1568851200000000000 GROUP BY location, time(1d) FILL(700) LIMIT 10
name: h2o_pH
tags: location=coyote_creek
+---------------------+--------+
| time                | median |
+---------------------+--------+
| 1565827200000000000 | 700    |
| 1565913600000000000 | 700    |
| 1566000000000000000 | 7      |
| 1566086400000000000 | 7      |
| 1566172800000000000 | 7      |
| 1566259200000000000 | 7      |
| 1566345600000000000 | 7      |
| 1566432000000000000 | 7      |
| 1566518400000000000 | 7      |
| 1566604800000000000 | 7      |
+---------------------+--------+
2 columns, 10 rows in set
```

## SPREAD()

返回指定Field列最大值和最小值之间的差值，不支持对Tag列计算，不支持SLIMIT和SOFFSET。

**语法**

```sql
SELECT SPREAD( [ * | <field_key> | /<regular_expression>/ ] ) [INTO_clause] FROM_clause [WHERE_clause] [GROUP_BY_clause] [ORDER_BY_clause] [LIMIT_clause] [OFFSET_clause]
```

**示例**

**指定时间范围，计算pH列的跨度**

```sql
> SELECT max(pH),min(pH),SPREAD(pH) FROM h2o_pH WHERE time > 1565827200000000000 AND time < 1568851200000000000
name: h2o_pH
+---------------------+-----+-----+--------+
| time                | max | min | spread |
+---------------------+-----+-----+--------+
| 1565827200000000001 | 8   | 6   | 2      |
+---------------------+-----+-----+--------+
4 columns, 1 rows in set
```

**指定时间范围和Tag，计算pH列的跨度**

```sql
> SELECT max(pH),min(pH),SPREAD(pH) FROM h2o_pH WHERE time > 1565827200000000000 AND time < 1568851200000000000 AND location='santa_monica'
name: h2o_pH
+---------------------+-----+-----+--------+
| time                | max | min | spread |
+---------------------+-----+-----+--------+
| 1565827200000000001 | 8   | 6   | 2      |
+---------------------+-----+-----+--------+
4 columns, 1 rows in set
```

## DISTINCT()

返回对指定Field列去重后的数据集，不支持对Tag列计算，不支持SLIMIT和SOFFSET。

**语法**

```sql
SELECT DISTINCT( [ <field_key> | /<regular_expression>/ ] ) FROM_clause [WHERE_clause] [GROUP_BY_clause] [ORDER_BY_clause] [LIMIT_clause] [OFFSET_clause]
```

**示例**

**指定时间范围，去重pH列数据**

```sql
> SELECT DISTINCT(pH) FROM h2o_pH WHERE time > 1565827200000000000 AND time < 1568851200000000000
name: h2o_pH
+---------------------+----------+
| time                | distinct |
+---------------------+----------+
| 1565827200000000001 | 6        |
| 1565827200000000001 | 7        |
| 1565827200000000001 | 8        |
+---------------------+----------+
2 columns, 3 rows in set
```

**指定时间线（Tag）去重数据**

```sql
> SELECT DISTINCT(pH) FROM h2o_pH WHERE time > '2019-09-09T00:00:00Z' AND time < '2019-09-23T00:00:00Z' AND location='santa_monica'
name: h2o_pH
+---------------------+----------+
| time                | distinct |
+---------------------+----------+
| 1567987200000000001 | 7        |
| 1567987200000000001 | 8        |
| 1567987200000000001 | 6        |
+---------------------+----------+
2 columns, 3 rows in set
```

## RATE()

返回一定时间范围内指定Field列数据的平均变化率/秒，计算公式：( \|A<sub>n</sub>-A<sub>1</sub>| ) / ( \|T<sub>n</sub>-T<sub>1</sub>\|)。不支持对Tag列计算，不支持SLIMIT和SOFFSET。

::: tip

若数据集包含多条时间线数据，则使用其中一条时间线数据来计算，因此在使用时建议给出Tag来指定时间线。

:::

**语法**

```sql
SELECT RATE( [ <field_key> | /<regular_expression>/ ] ) FROM_clause [WHERE_clause] [GROUP_BY_clause] [ORDER_BY_clause] [LIMIT_clause] [OFFSET_clause]
```

**示例**

**给定时间范围，计算温度的平均变化率**

```sql
> SELECT * FROM h2o_temperature WHERE time > '2019-09-12T00:00:00Z' AND time < '2019-09-12T01:00:00Z' AND location='santa_monica'
name: h2o_temperature
+---------------------+---------+--------------+
| time                | degrees | location     |
+---------------------+---------+--------------+
| 1568246760000000000 | 67      | santa_monica |
| 1568247120000000000 | 68      | santa_monica |
| 1568247480000000000 | 64      | santa_monica |
| 1568247840000000000 | 62      | santa_monica |
| 1568248200000000000 | 63      | santa_monica |
| 1568248560000000000 | 70      | santa_monica |
| 1568248920000000000 | 66      | santa_monica |
| 1568249280000000000 | 64      | santa_monica |
| 1568249640000000000 | 60      | santa_monica |
+---------------------+---------+--------------+
3 columns, 9 rows in set

> SELECT rate(degrees) FROM h2o_temperature WHERE time > '2019-09-12T00:00:00Z' AND time < '2019-09-12T01:00:00Z' AND location='santa_monica'
name: h2o_temperature
+---------------------+------------------------+
| time                | rate                   |
+---------------------+------------------------+
| 1568246760000000000 | -0.0024305555555555556 |
+---------------------+------------------------+
2 columns, 1 rows in set
```

**分组计算**

```sql
> SELECT rate(degrees) FROM h2o_temperature WHERE time > '2019-09-12T00:00:00Z' AND time < '2019-09-12T01:00:00Z' GROUP BY location
name: h2o_temperature
tags: location=coyote_creek
+---------------------+-----------------------+
| time                | rate                  |
+---------------------+-----------------------+
| 1568246760000000000 | -0.001388888888888889 |
+---------------------+-----------------------+
2 columns, 1 rows in set

name: h2o_temperature
tags: location=santa_monica
+---------------------+------------------------+
| time                | rate                   |
+---------------------+------------------------+
| 1568246760000000000 | -0.0024305555555555556 |
+---------------------+------------------------+
2 columns, 1 rows in set
```

## IRATE()

返回指定Field列数据的实时变化率/秒，计算公式：( \|A<sub>n</sub>-A<sub>n-1</sub>| ) / ( \|T<sub>n</sub>-T<sub>n-1</sub>\|)。A<sub>n</sub>和A<sub>n-1</sub>分别表示给定时间范围内最新的两条数据。不支持对Tag列计算，不支持SLIMIT和SOFFSET。

::: tip

时间范围条件的常见写法为：now()-5m, 或者 now()-10m

:::

**语法**

```sql
SELECT IRATE( [ <field_key> | /<regular_expression>/ ] ) FROM_clause [WHERE_clause] [GROUP_BY_clause] [ORDER_BY_clause] [LIMIT_clause] [OFFSET_clause]
```

**示例**

**给定时间范围，计算温度的平均变化率**

```sql
> SELECT * FROM h2o_temperature WHERE time > '2019-09-12T00:00:00Z' AND time < '2019-09-12T01:00:00Z' AND location='santa_monica'
name: h2o_temperature
+---------------------+---------+--------------+
| time                | degrees | location     |
+---------------------+---------+--------------+
| 1568246760000000000 | 67      | santa_monica |
| 1568247120000000000 | 68      | santa_monica |
| 1568247480000000000 | 64      | santa_monica |
| 1568247840000000000 | 62      | santa_monica |
| 1568248200000000000 | 63      | santa_monica |
| 1568248560000000000 | 70      | santa_monica |
| 1568248920000000000 | 66      | santa_monica |
| 1568249280000000000 | 64      | santa_monica |
| 1568249640000000000 | 60      | santa_monica |
+---------------------+---------+--------------+
3 columns, 9 rows in set

> SELECT irate(degrees) FROM h2o_temperature WHERE time > '2019-09-12T00:00:00Z' AND time < '2019-09-12T01:00:00Z' AND location='santa_monica'
name: h2o_temperature
+---------------------+-----------------------+
| time                | irate                 |
+---------------------+-----------------------+
| 1568249280000000000 | -0.011111111111111112 |
+---------------------+-----------------------+
2 columns, 1 rows in set
```

**分组计算**

```sql
> SELECT irate(degrees) FROM h2o_temperature WHERE time > '2019-09-12T00:00:00Z' AND time < '2019-09-12T01:00:00Z' GROUP BY location
name: h2o_temperature
tags: location=coyote_creek
+---------------------+-----------------------+
| time                | irate         |
+---------------------+-----------------------+
| 1568249280000000000 | -0.013888888888888888 |
+---------------------+-----------------------+
2 columns, 1 rows in set

name: h2o_temperature
tags: location=santa_monica
+---------------------+-----------------------+
| time                | irate         |
+---------------------+-----------------------+
| 1568249280000000000 | -0.011111111111111112 |
+---------------------+-----------------------+
2 columns, 1 rows in set
```

## MOVING_AVERAGE()

返回指定Field列的滑动窗口平均值, 不支持对Tag列计算，不支持SLIMIT和SOFFSET，不支持按time分组。

**语法**

```sql
SELECT MOVING_AVERAGE( [ * | <field_key> | /<regular_expression>/ ] , <N> ) [INTO_clause] FROM_clause [WHERE_clause] [GROUP_BY_clause] [ORDER_BY_clause] [LIMIT_clause] [OFFSET_clause]
```

**示例**

**计算water_level 字段两个值窗口的滚动平均值**

```sql
> SELECT water_level FROM h2o_feet WHERE time > '2019-09-12T00:00:00Z' AND time < '2019-09-12T01:00:00Z' AND location='santa_monica'
name: h2o_feet
+----------------------+-------------+
| time                 | water_level |
+----------------------+-------------+
| 2019-09-12T00:06:00Z | 2.684       |
| 2019-09-12T00:12:00Z | 2.782       |
| 2019-09-12T00:18:00Z | 2.881       |
| 2019-09-12T00:24:00Z | 2.93        |
| 2019-09-12T00:30:00Z | 2.93        |
| 2019-09-12T00:36:00Z | 3.104       |
| 2019-09-12T00:42:00Z | 3.225       |
| 2019-09-12T00:48:00Z | 3.307       |
| 2019-09-12T00:54:00Z | 3.373       |
+----------------------+-------------+
2 columns, 9 rows in set

> SELECT MOVING_AVERAGE(water_level,2) FROM h2o_feet WHERE time > '2019-09-12T00:00:00Z' AND time < '2019-09-12T01:00:00Z' AND location='santa_monica'
name: h2o_feet
+----------------------+--------------------+
| time                 | moving_average     |
+----------------------+--------------------+
| 2019-09-12T00:12:00Z | 2.733              |
| 2019-09-12T00:18:00Z | 2.8315             |
| 2019-09-12T00:24:00Z | 2.9055             |
| 2019-09-12T00:30:00Z | 2.93               |
| 2019-09-12T00:36:00Z | 3.0170000000000003 |
| 2019-09-12T00:42:00Z | 3.1645000000000003 |
| 2019-09-12T00:48:00Z | 3.266              |
| 2019-09-12T00:54:00Z | 3.34               |
+----------------------+--------------------+
2 columns, 8 rows in set
```

第一个结果 (2.733) 是原始数据中前两点的平均值：(2.684 + 2.782) / 2)。 第二个结果 (2.8315) 是原始数据中后两个点的平均值：(2.881 + 2.93) / 2)。

**按location分组计算滑动窗口平均值**

```sql
> SELECT MOVING_AVERAGE(water_level,2) FROM h2o_feet WHERE time > '2019-09-12T00:00:00Z' AND time < '2019-09-12T01:00:00Z' GROUP BY location
name: h2o_feet
tags: location=coyote_creek
+----------------------+--------------------+
| time                 | moving_average     |
+----------------------+--------------------+
| 2019-09-12T00:12:00Z | 4.5600000000000005 |
| 2019-09-12T00:18:00Z | 4.3995             |
| 2019-09-12T00:24:00Z | 4.2455             |
| 2019-09-12T00:30:00Z | 4.090999999999999  |
| 2019-09-12T00:36:00Z | 3.9319999999999995 |
| 2019-09-12T00:42:00Z | 3.7749999999999995 |
| 2019-09-12T00:48:00Z | 3.6289999999999996 |
| 2019-09-12T00:54:00Z | 3.4875             |
+----------------------+--------------------+
2 columns, 8 rows in set

name: h2o_feet
tags: location=santa_monica
+----------------------+--------------------+
| time                 | moving_average     |
+----------------------+--------------------+
| 2019-09-12T00:12:00Z | 2.733              |
| 2019-09-12T00:18:00Z | 2.8315             |
| 2019-09-12T00:24:00Z | 2.9055             |
| 2019-09-12T00:30:00Z | 2.93               |
| 2019-09-12T00:36:00Z | 3.0170000000000003 |
| 2019-09-12T00:42:00Z | 3.1645000000000003 |
| 2019-09-12T00:48:00Z | 3.266              |
| 2019-09-12T00:54:00Z | 3.34               |
+----------------------+--------------------+
2 columns, 8 rows in set
```

## HOLT_WINTERS()

返回给定Field列N个预测值，HOLT_WINTERS根据预测值与实际值对比来辅助发现异常点，也可以预测未来可能超过设定阈值的大致时间。不支持对Tag列计算，不支持SLIMIT和SOFFSET，不支持HOLT_WINTERS_WITH-FIT。

**语法**

```sql
SELECT HOLT_WINTERS(<function>(<field_key>),<N>,<S>) [INTO_clause] FROM_clause [WHERE_clause] GROUP_BY_clause [ORDER_BY_clause] [LIMIT_clause] [OFFSET_clause]
```

**示例**

**根据原始数据预测未来3小时和5小时的数据**

```sql
> SELECT water_level FROM h2o_feet WHERE time > '2019-09-12T00:00:00Z' AND time < '2019-09-12T03:00:00Z' AND location='santa_monica'
name: h2o_feet
+----------------------+-------------+
| time                 | water_level |
+----------------------+-------------+
| 2019-09-12T00:06:00Z | 2.684       |
| 2019-09-12T00:12:00Z | 2.782       |
| 2019-09-12T00:18:00Z | 2.881       |
| 2019-09-12T00:24:00Z | 2.93        |
| 2019-09-12T00:30:00Z | 2.93        |
| 2019-09-12T00:36:00Z | 3.104       |
| 2019-09-12T00:42:00Z | 3.225       |
| 2019-09-12T00:48:00Z | 3.307       |
| 2019-09-12T00:54:00Z | 3.373       |
| 2019-09-12T01:00:00Z | 3.455       |
| 2019-09-12T01:06:00Z | 3.579       |
| 2019-09-12T01:12:00Z | 3.678       |
| 2019-09-12T01:18:00Z | 3.835       |
| 2019-09-12T01:24:00Z | 3.95        |
| 2019-09-12T01:30:00Z | 4.016       |
| 2019-09-12T01:36:00Z | 4.068       |
| 2019-09-12T01:42:00Z | 4.226       |
| 2019-09-12T01:48:00Z | 4.367       |
| 2019-09-12T01:54:00Z | 4.436       |
| 2019-09-12T02:00:00Z | 4.56        |
| 2019-09-12T02:06:00Z | 4.652       |
| 2019-09-12T02:12:00Z | 4.751       |
| 2019-09-12T02:18:00Z | 4.856       |
| 2019-09-12T02:24:00Z | 4.957       |
| 2019-09-12T02:30:00Z | 5.066       |
| 2019-09-12T02:36:00Z | 5.161       |
| 2019-09-12T02:42:00Z | 5.194       |
| 2019-09-12T02:48:00Z | 5.348       |
| 2019-09-12T02:54:00Z | 5.476       |
+----------------------+-------------+
2 columns, 29 rows in set

> SELECT FIRST(water_level) FROM h2o_feet WHERE time > '2019-09-12T00:00:00Z' AND time < '2019-09-12T03:00:00Z' AND location='santa_monica' GROUP BY time(1h,6m)
name: h2o_feet
+----------------------+-------+
| time                 | first |
+----------------------+-------+
| 2019-09-11T23:06:00Z | <nil> |
| 2019-09-12T00:06:00Z | 2.684 |
| 2019-09-12T01:06:00Z | 3.579 |
| 2019-09-12T02:06:00Z | 4.652 |
+----------------------+-------+
2 columns, 4 rows in set

> SELECT HOLT_WINTERS(FIRST(water_level),3,3) FROM h2o_feet WHERE time > '2019-09-12T00:00:00Z' AND time < '2019-09-12T03:00:00Z' AND location='santa_monica' GROUP BY time(1h,6m)
name: h2o_feet
+----------------------+--------------------+
| time                 | holt_winters       |
+----------------------+--------------------+
| 2019-09-12T03:06:00Z | 6.09448440489647   |
| 2019-09-12T04:06:00Z | 9.9270949466756    |
| 2019-09-12T05:06:00Z | 15.233234117005084 |
+----------------------+--------------------+
2 columns, 3 rows in set

> SELECT HOLT_WINTERS(FIRST(water_level),5,3) FROM h2o_feet WHERE time > '2019-09-12T00:00:00Z' AND time < '2019-09-12T03:00:00Z' AND location='santa_monica' GROUP BY time(1h,6m)
name: h2o_feet
+----------------------+--------------------+
| time                 | holt_winters       |
+----------------------+--------------------+
| 2019-09-12T03:06:00Z | 6.09448440489647   |
| 2019-09-12T04:06:00Z | 9.9270949466756    |
| 2019-09-12T05:06:00Z | 15.233234117005084 |
| 2019-09-12T06:06:00Z | 21.9164342386234   |
| 2019-09-12T07:06:00Z | 39.25458327752615  |
+----------------------+--------------------+
2 columns, 5 rows in set
```

该查询返回5条预测数据，由`(func(Field_Key),N,S)`的N决定，返回数据之间的时间间隔为1小时，是由分组的`time(1h,6m)`决定，6m是时间偏移量，所以每条预测的数据都是整点过后6分钟。

- 时间分组`time(T,offset)`的`T`如何确定？

  对于周期分布的时间序列数据集，一般设置为数据每个波峰和波谷的时间间隔。offset为调整参数。

- `(func(Field_Key),N,S)`中`func`如何确定？

  数据采样方法，可以是FIRST、LAST、MEAN。根据实际的数据分布情况而定。

- `(func(Field_Key),N,S)`中`S`如何确定？

  `S`表示一个周期内数据模式包含的关键数据点个数。比如一个数据周期内，按时间`T`进行分割，每个区域的第一个点（`FIRST`）连线就是数据模式，能整体表现出数据的分布形状，`S`就是模式中点的个数。

参考[InfluxDB‘s HOLT_WINTERS](https://docs.influxdata.com/influxdb/v1/query_language/functions/#holt_winters)

## CUMULATIVE_SUM()

返回指定Field列的累积和，不支持对Tag列计算，不支持SLIMIT和SOFFSET

**语法**

```sql
SELECT CUMULATIVE_SUM( [ * | <field_key> | /<regular_expression>/ ] ) [INTO_clause] FROM_clause [WHERE_clause] [GROUP_BY_clause] [ORDER_BY_clause] [LIMIT_clause] [OFFSET_clause]
```

**示例**

**计算water_level列数据累计和**

```sql
> SELECT water_level FROM h2o_feet WHERE time > '2019-09-12T00:00:00Z' AND time < '2019-09-12T03:00:00Z' AND location='santa_monica' LIMIT 10
name: h2o_feet
+----------------------+-------------+
| time                 | water_level |
+----------------------+-------------+
| 2019-09-12T00:06:00Z | 2.684       |
| 2019-09-12T00:12:00Z | 2.782       |
| 2019-09-12T00:18:00Z | 2.881       |
| 2019-09-12T00:24:00Z | 2.93        |
| 2019-09-12T00:30:00Z | 2.93        |
| 2019-09-12T00:36:00Z | 3.104       |
| 2019-09-12T00:42:00Z | 3.225       |
| 2019-09-12T00:48:00Z | 3.307       |
| 2019-09-12T00:54:00Z | 3.373       |
| 2019-09-12T01:00:00Z | 3.455       |
+----------------------+-------------+
2 columns, 10 rows in set

> SELECT CUMULATIVE_SUM(water_level) FROM h2o_feet WHERE time > '2019-09-12T00:00:00Z' AND time < '2019-09-12T03:00:00Z' AND location='santa_monica' LIMIT 10
name: h2o_feet
+----------------------+--------------------+
| time                 | cumulative_sum     |
+----------------------+--------------------+
| 2019-09-12T00:06:00Z | 2.684              |
| 2019-09-12T00:12:00Z | 5.466              |
| 2019-09-12T00:18:00Z | 8.347              |
| 2019-09-12T00:24:00Z | 11.277             |
| 2019-09-12T00:30:00Z | 14.206999999999999 |
| 2019-09-12T00:36:00Z | 17.311             |
| 2019-09-12T00:42:00Z | 20.536             |
| 2019-09-12T00:48:00Z | 23.843             |
| 2019-09-12T00:54:00Z | 27.216             |
| 2019-09-12T01:00:00Z | 30.671             |
+----------------------+--------------------+
2 columns, 10 rows in set
```

第一个值2.684保持不变，第二个值5.466为原始数据前两个数之和（2.684 + 2.782 = 5.466），第三个值8.347为前三个数之和，或者说第二个值与原始数据的第三个数之和（2.684 + 2.782 + 2.881 = 8.347）。

**高级用法**

先按时间分组求平均值，再计算每个分组的累积和

```sql
> SELECT CUMULATIVE_SUM(mean_water) FROM (SELECT MEAN(water_level) as mean_water FROM h2o_feet WHERE time > '2019-09-12T00:00:00Z' AND time < '2019-09-12T03:00:00Z' AND location='santa_monica' GROUP BY time(12m) LIMIT 10)
```

*以下用法不支持* `CUMULATIVE_SUM(MEAN("water_level"))`

```sql
> SELECT CUMULATIVE_SUM(MEAN("water_level")) FROM h2o_feet WHERE time > '2019-09-12T00:00:00Z' AND time < '2019-09-12T03:00:00Z' AND location='santa_monica' GROUP BY time(12m) LIMIT 10
```

## DERIVATIVE()

返回指定Field列的值之间的连续变化率，不支持对Tag列计算，不支持SLIMIT和SOFFSET。

**语法**

```sql
SELECT DERIVATIVE( [ * | <field_key> | /<regular_expression>/ ] [ , <unit> ] ) [INTO_clause] FROM_clause [WHERE_clause] [GROUP_BY_clause] [ORDER_BY_clause] [LIMIT_clause] [OFFSET_clause]
```

**示例**

**计算water_level的连续变化率**

```sql
> SELECT water_level FROM h2o_feet WHERE time > '2019-09-12T00:00:00Z' AND time < '2019-09-12T01:00:00Z' AND location='santa_monica'
name: h2o_feet
+----------------------+-------------+
| time                 | water_level |
+----------------------+-------------+
| 2019-09-12T00:06:00Z | 2.684       |
| 2019-09-12T00:12:00Z | 2.782       |
| 2019-09-12T00:18:00Z | 2.881       |
| 2019-09-12T00:24:00Z | 2.93        |
| 2019-09-12T00:30:00Z | 2.93        |
| 2019-09-12T00:36:00Z | 3.104       |
| 2019-09-12T00:42:00Z | 3.225       |
| 2019-09-12T00:48:00Z | 3.307       |
| 2019-09-12T00:54:00Z | 3.373       |
+----------------------+-------------+
2 columns, 9 rows in set

> SELECT DERIVATIVE(water_level) FROM h2o_feet WHERE time > '2019-09-12T00:00:00Z' AND time < '2019-09-12T01:00:00Z' AND location='santa_monica'
name: h2o_feet
+----------------------+------------------------+
| time                 | derivative             |
+----------------------+------------------------+
| 2019-09-12T00:12:00Z | 0.0002722222222222218  |
| 2019-09-12T00:18:00Z | 0.0002749999999999993  |
| 2019-09-12T00:24:00Z | 0.00013611111111111216 |
| 2019-09-12T00:30:00Z | 0                      |
| 2019-09-12T00:36:00Z | 0.0004833333333333331  |
| 2019-09-12T00:42:00Z | 0.0003361111111111111  |
| 2019-09-12T00:48:00Z | 0.00022777777777777738 |
| 2019-09-12T00:54:00Z | 0.00018333333333333412 |
+----------------------+------------------------+
2 columns, 8 rows in set
```

计算公式：(A<sub>2</sub> - A<sub>1</sub>）/（T<sub>2</sub> - T<sub>1</sub>)

第一个值`0.0002722222222222218`的为原始数据第二个数与第一个数的差值再除这两个数据之间换算成秒(s)的时间间隔 (2.782 - 2.684) / 360 = 0.0002722222222222218，第二个值`0.0002749999999999993`为原始数据第三个数与第二个数的差值再除这两个数据之间换算成秒(s)的时间间隔 (2.881 - 2.782) / 360 = 0.0002749999999999993.

## DIFFERENCE()

返回指定Field列的连续差值，不支持对Tag列计算，不支持SLIMIT和SOFFSET。

**语法**

```sql
SELECT DIFFERENCE( [ * | <field_key> | /<regular_expression>/ ] ) [INTO_clause] FROM_clause [WHERE_clause] [GROUP_BY_clause] [ORDER_BY_clause] [LIMIT_clause] [OFFSET_clause]
```

**示例**

**计算water_level的连续差值**

```sql
> SELECT water_level FROM h2o_feet WHERE time > '2019-09-12T00:00:00Z' AND time < '2019-09-12T01:00:00Z' AND location='santa_monica'
name: h2o_feet
+----------------------+-------------+
| time                 | water_level |
+----------------------+-------------+
| 2019-09-12T00:06:00Z | 2.684       |
| 2019-09-12T00:12:00Z | 2.782       |
| 2019-09-12T00:18:00Z | 2.881       |
| 2019-09-12T00:24:00Z | 2.93        |
| 2019-09-12T00:30:00Z | 2.93        |
| 2019-09-12T00:36:00Z | 3.104       |
| 2019-09-12T00:42:00Z | 3.225       |
| 2019-09-12T00:48:00Z | 3.307       |
| 2019-09-12T00:54:00Z | 3.373       |
+----------------------+-------------+
2 columns, 9 rows in set

> SELECT DIFFERENCE(water_level) FROM h2o_feet WHERE time > '2019-09-12T00:00:00Z' AND time < '2019-09-12T01:00:00Z' AND location='santa_monica'
name: h2o_feet
+----------------------+---------------------+
| time                 | difference          |
+----------------------+---------------------+
| 2019-09-12T00:12:00Z | 0.09799999999999986 |
| 2019-09-12T00:18:00Z | 0.09899999999999975 |
| 2019-09-12T00:24:00Z | 0.04900000000000038 |
| 2019-09-12T00:30:00Z | 0                   |
| 2019-09-12T00:36:00Z | 0.17399999999999993 |
| 2019-09-12T00:42:00Z | 0.121               |
| 2019-09-12T00:48:00Z | 0.08199999999999985 |
| 2019-09-12T00:54:00Z | 0.06600000000000028 |
+----------------------+---------------------+
2 columns, 8 rows in set
```

第一个值`0.09799999999999986`为原始数据第二个数与第一个数的差值(2.782 - 2.684 = 0.09799999999999986)，第二个值`0.09899999999999975`为原始数据第三个数与第二个数的差值(2.881 - 2.782 = 0.09899999999999975)。

**高级用法**

*不支持如下用法*

```sql
> SELECT DIFFERENCE(<function>( [ * | <field_key> | /<regular_expression>/ ] )) [INTO_clause] FROM_clause [WHERE_clause] GROUP_BY_clause [ORDER_BY_clause] [LIMIT_clause] [OFFSET_clause]
```

**换一种方法可实现**

```sql
> SELECT DIFFERENCE(max_water) FROM (SELECT MAX(water_level) as max_water FROM h2o_feet WHERE time > '2019-09-12T00:00:00Z' AND time < '2019-09-12T01:00:00Z' AND location='santa_monica' GROUP BY time(12m))
name: h2o_feet
+----------------------+---------------------+
| time                 | difference          |
+----------------------+---------------------+
| 2019-09-12T00:12:00Z | 0.19699999999999962 |
| 2019-09-12T00:24:00Z | 0.04900000000000038 |
| 2019-09-12T00:36:00Z | 0.29499999999999993 |
| 2019-09-12T00:48:00Z | 0.14800000000000013 |
+----------------------+---------------------+
2 columns, 4 rows in set
```

## ELAPSED()

返回指定Field列的连续时间戳差值，不支持对Tag列计算，不支持SLIMIT和SOFFSET。

**语法**

```sql
SELECT ELAPSED( [ * | <field_key> | /<regular_expression>/ ] [ , <unit> ] ) [INTO_clause] FROM_clause [WHERE_clause] [GROUP_BY_clause] [ORDER_BY_clause] [LIMIT_clause] [OFFSET_clause]
```

**示例**

**计算water_level列连续的时间戳差值**

```sql
> SELECT water_level FROM h2o_feet WHERE time > '2019-09-12T00:00:00Z' AND time < '2019-09-12T01:00:00Z' AND location='santa_monica'
name: h2o_feet
+----------------------+-------------+
| time                 | water_level |
+----------------------+-------------+
| 2019-09-12T00:06:00Z | 2.684       |
| 2019-09-12T00:12:00Z | 2.782       |
| 2019-09-12T00:18:00Z | 2.881       |
| 2019-09-12T00:24:00Z | 2.93        |
| 2019-09-12T00:30:00Z | 2.93        |
| 2019-09-12T00:36:00Z | 3.104       |
| 2019-09-12T00:42:00Z | 3.225       |
| 2019-09-12T00:48:00Z | 3.307       |
| 2019-09-12T00:54:00Z | 3.373       |
+----------------------+-------------+
2 columns, 9 rows in set

> SELECT ELAPSED(water_level) FROM h2o_feet WHERE time > '2019-09-12T00:00:00Z' AND time < '2019-09-12T01:00:00Z' AND location='santa_monica'
name: h2o_feet
+----------------------+--------------+
| time                 | elapsed      |
+----------------------+--------------+
| 2019-09-12T00:12:00Z | 360000000000 |
| 2019-09-12T00:18:00Z | 360000000000 |
| 2019-09-12T00:24:00Z | 360000000000 |
| 2019-09-12T00:30:00Z | 360000000000 |
| 2019-09-12T00:36:00Z | 360000000000 |
| 2019-09-12T00:42:00Z | 360000000000 |
| 2019-09-12T00:48:00Z | 360000000000 |
| 2019-09-12T00:54:00Z | 360000000000 |
+----------------------+--------------+
2 columns, 8 rows in set
```

## NON_NEGATIVE_DERIVATIVE()

相比[DERIVATIVE()](#derivative)，NON_NEGATIVE_DERIVATIVE 顾名思义仅返回非负值连续变化率，不支持对Tag列计算，不支持SLIMIT和SOFFSET。

**语法**

```sql
SELECT NON_NEGATIVE_DERIVATIVE( [ * | <field_key> | /<regular_expression>/ ] [ , <unit> ] ) [INTO_clause] FROM_clause [WHERE_clause] [GROUP_BY_clause] [ORDER_BY_clause] [LIMIT_clause] [OFFSET_clause]
```

**示例**

**计算degrees字段的非负连续变化率**

```sql
> SELECT * FROM h2o_temperature WHERE time > '2019-08-17T00:00:00Z' AND time < '2019-08-17T01:00:00Z' AND location='santa_monica'
name: h2o_temperature
+----------------------+---------+--------------+
| time                 | degrees | location     |
+----------------------+---------+--------------+
| 2019-08-17T00:06:00Z | 60      | santa_monica |
| 2019-08-17T00:12:00Z | 62      | santa_monica |
| 2019-08-17T00:18:00Z | 62      | santa_monica |
| 2019-08-17T00:24:00Z | 60      | santa_monica |
| 2019-08-17T00:30:00Z | 63      | santa_monica |
| 2019-08-17T00:36:00Z | 64      | santa_monica |
| 2019-08-17T00:42:00Z | 63      | santa_monica |
| 2019-08-17T00:48:00Z | 63      | santa_monica |
| 2019-08-17T00:54:00Z | 61      | santa_monica |
+----------------------+---------+--------------+
3 columns, 9 rows in set

> SELECT NON_NEGATIVE_DERIVATIVE(degrees) FROM h2o_temperature WHERE time > '2019-08-17T00:00:00Z' AND time < '2019-08-17T01:00:00Z' AND location='santa_monica'
name: h2o_temperature
+----------------------+-------------------------+
| time                 | non_negative_derivative |
+----------------------+-------------------------+
| 2019-08-17T00:12:00Z | 0.005555555555555556    |
| 2019-08-17T00:18:00Z | 0                       |
| 2019-08-17T00:30:00Z | 0.008333333333333333    |
| 2019-08-17T00:36:00Z | 0.002777777777777778    |
| 2019-08-17T00:48:00Z | 0                       |
+----------------------+-------------------------+
2 columns, 5 rows in set
```

*不支持如下内嵌函数的用法*

```
SELECT NON_NEGATIVE_DERIVATIVE(<function> ([ * | <field_key> | /<regular_expression>/ ]) [ , <unit> ] ) [INTO_clause] FROM_clause [WHERE_clause] GROUP_BY_clause [ORDER_BY_clause] [LIMIT_clause] [OFFSET_clause]
```

可改为SELECT 子查询实现

```sql
> SELECT NON_NEGATIVE_DERIVATIVE(mean_degrees) FROM (SELECT MEAN(degrees) as mean_degrees FROM h2o_temperature WHERE time > '2019-08-17T00:00:00Z' AND time < '2019-08-17T01:00:00Z' AND location='santa_monica' GROUP BY time(12m))
name: h2o_temperature
+----------------------+-------------------------+
| time                 | non_negative_derivative |
+----------------------+-------------------------+
| 2019-08-17T00:12:00Z | 0.002777777777777778    |
| 2019-08-17T00:36:00Z | 0.002777777777777778    |
+----------------------+-------------------------+
2 columns, 2 rows in set
```

## NON_NEGATIVE_DIFFERENCE()

相比[DIFFERENCE()](#difference)，NON_NEGATIVE_DIFFERENCE 顾名思义仅返回指定Field列的非负值连续差值，不支持对Tag列计算，不支持SLIMIT和SOFFSET。

**语法**

```sql
SELECT NON_NEGATIVE_DIFFERENCE( [ * | <field_key> | /<regular_expression>/ ] ) [INTO_clause] FROM_clause [WHERE_clause] [GROUP_BY_clause] [ORDER_BY_clause] [LIMIT_clause] [OFFSET_clause]
```

**示例**

**计算degrees字段的非负连续差值**

```sql
> SELECT * FROM h2o_temperature WHERE time > '2019-08-17T00:00:00Z' AND time < '2019-08-17T01:00:00Z' AND location='santa_monica'
name: h2o_temperature
+----------------------+---------+--------------+
| time                 | degrees | location     |
+----------------------+---------+--------------+
| 2019-08-17T00:06:00Z | 60      | santa_monica |
| 2019-08-17T00:12:00Z | 62      | santa_monica |
| 2019-08-17T00:18:00Z | 62      | santa_monica |
| 2019-08-17T00:24:00Z | 60      | santa_monica |
| 2019-08-17T00:30:00Z | 63      | santa_monica |
| 2019-08-17T00:36:00Z | 64      | santa_monica |
| 2019-08-17T00:42:00Z | 63      | santa_monica |
| 2019-08-17T00:48:00Z | 63      | santa_monica |
| 2019-08-17T00:54:00Z | 61      | santa_monica |
+----------------------+---------+--------------+
3 columns, 9 rows in set

> SELECT NON_NEGATIVE_DIFFERENCE(degrees) FROM h2o_temperature WHERE time > '2019-08-17T00:00:00Z' AND time < '2019-08-17T01:00:00Z' AND location='santa_monica'
name: h2o_temperature
+----------------------+-------------------------+
| time                 | non_negative_difference |
+----------------------+-------------------------+
| 2019-08-17T00:12:00Z | 2                       |
| 2019-08-17T00:18:00Z | 0                       |
| 2019-08-17T00:30:00Z | 3                       |
| 2019-08-17T00:36:00Z | 1                       |
| 2019-08-17T00:48:00Z | 0                       |
+----------------------+-------------------------+
2 columns, 5 rows in set
```

*不支持如下内嵌函数的用法*，可使用SELECT 子查询替代
