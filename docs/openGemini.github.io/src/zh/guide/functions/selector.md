---
title: 选择器函数
order: 3
---
本章主要介绍如下函数：

| 选择函数 | 说明 |
| --- | --- |
| [FIRST()](#first) | 最晚一条数据 |
| [LAST()](#last) | 最晚一条数据 |
| [MIN()](#min) | 最小值 |
| [MAX()](#max) | 最大值 |
| [TOP()](#top) | 最大N个值 |
|[BOTTOM()](#bottom)| 最小N个值 |
| [PERCENTILE()](#percentile) | 计算百分位数 |
| [SAMPLE()](#sample) | N个随机样本 |
| [**PERCENTILE_OGSKETCH()**](#percentile-ogsketch) | 近似分位数 |

::: tip

openGemini提供的函数兼容InfluxDB的用法，可参考InfluxDB对应的[函数用法](https://docs.influxdata.com/influxdb/v1/query_language/functions)。

时间戳默认显示为整数，希望时间戳显示格式为rfc3339：“YYYY-MM-DD'T'hh:mm:ss.XXX”，在ts-cli中执行命令

```sql
> precision rfc3339
> SELECT FIRST("level description") FROM "h2o_feet"
name: h2o_feet
+----------------------+----------------------+
| time                 | first                |
+----------------------+----------------------+
| 2019-08-17T00:00:00Z | between 6 and 9 feet |
+----------------------+----------------------+
2 columns, 1 rows in set
```

:::


## FIRST()
返回指定Field列最早时间的值，不支持对Tag进行计算，不支持SLIMIT和SOFFSET。

**语法**

```sql
SELECT FIRST(<field_key>)[,<tag_key(s)>|<field_key(s)>] [INTO_CLAUSE] FROM_CLAUSE [WHERE_CLAUSE] [GROUP_BY_CLAUSE] [ORDER_BY_CLAUSE] [LIMIT_CLAUSE] [OFFSET_CLAUSE]
```

**示例**

**选择指定Field对应的具有最早时间戳的值**

```sql
> SELECT FIRST("level description") FROM "h2o_feet"
name: h2o_feet
+---------------------+----------------------+
| time                | first                |
+---------------------+----------------------+
| 1566000000000000000 | between 6 and 9 feet |
+---------------------+----------------------+
2 columns, 1 rows in set
```

该查询返回 `level description`列最早时间戳的值。

**选择表中所有Field列对应最早时间戳的值**

```sql
> SELECT FIRST(*) FROM "h2o_feet"
name: h2o_feet
+------+-------------------------+-------------------+
| time | first_level description | first_water_level |
+------+-------------------------+-------------------+
| 0    | between 6 and 9 feet    | 8.12              |
+------+-------------------------+-------------------+
3 columns, 1 rows in set
```

**选择与正则表达式匹配的每个Field列对应最早时间戳的值**

```sql
> SELECT FIRST(/level/) FROM "h2o_feet"
name: h2o_feet
+----------------------+-------------------------+-------------------+
| time                 | first_level description | first_water_level |
+----------------------+-------------------------+-------------------+
| 1970-01-01T00:00:00Z | between 6 and 9 feet    | 8.12              |
+----------------------+-------------------------+-------------------+
3 columns, 1 rows in set
```

该查询返回表 `h2o_feet`中包含单词`level`的所有Field对应最早时间戳的值。

> FIRST(*)返回的时间为0，因为应用写入的数据未必包含全部的列，换句话说，可能每一列的最早数据的时间是不同的。

**选择指定Field列最早的数据，和及其相关的Tag和Field**

```sql
> SELECT FIRST("level description"),"location","water_level" FROM "h2o_feet"
name: h2o_feet
+----------------------+----------------------+--------------+-------------+
| time                 | first                | location     | water_level |
+----------------------+----------------------+--------------+-------------+
| 2019-08-17T00:00:00Z | between 6 and 9 feet | coyote_creek | 8.12        |
+----------------------+----------------------+--------------+-------------+
4 columns, 1 rows in set
```

**指定时间范围分组查询指定Field列的最早时间的值**

```sql
> SELECT FIRST("water_level") FROM "h2o_feet" WHERE time >= '2019-08-20T00:12:01Z' AND time <= '2019-08-20T08:00:00Z' GROUP BY time(7m),* fill(1000) LIMIT 4
name: h2o_feet
tags: location=coyote_creek
+----------------------+-------+
| time                 | first |
+----------------------+-------+
| 2019-08-20T00:10:00Z | 1000  |
| 2019-08-20T00:17:00Z | 8.684 |
| 2019-08-20T00:24:00Z | 8.661 |
| 2019-08-20T00:31:00Z | 8.619 |
+----------------------+-------+
2 columns, 4 rows in set
```

该查询返回表 `h2o_feet`中 `water_level` 列对应的最早时间戳的值（Field value），指定时间范围为`2019-08-20T00:12:01Z`和`2019-08-20T08:00:00Z`，并将查询结果按7分钟的时间间隔和每个Tag进行分组，同时，若分组为空值，用`1000`填充，并将返回4条结果数据。

## LAST()
返回指定Filed列最新写入的数据。不支持对Tag进行计算，不支持SLIMIT和SOFFSET。

**语法**

```sql
SELECT LAST(<field_key>)[,<tag_key(s)>|<field_keys(s)>] [INTO_CLAUSE] FROM_CLAUSE [WHERE_CLAUSE] [GROUP_BY_CLAUSE] [ORDER_BY_CLAUSE] [LIMIT_CLAUSE] [OFFSET_CLAUSE]
```

**示例**

**选择`level description`列最新写入数据**

```sql
> SELECT LAST("level description") FROM "h2o_feet"
name: h2o_feet
+----------------------+----------------------+
| time                 | last                 |
+----------------------+----------------------+
| 2019-09-17T21:42:00Z | between 3 and 6 feet |
+----------------------+----------------------+
2 columns, 1 rows in set

#对比FIRST()查询最早写入的数据
> SELECT FIRST("level description") FROM "h2o_feet"
name: h2o_feet
+----------------------+----------------------+
| time                 | first                |
+----------------------+----------------------+
| 2019-08-17T00:00:00Z | between 6 and 9 feet |
+----------------------+----------------------+
2 columns, 1 rows in set
```

**查询每列最新写入的数据**

```sql
> SELECT LAST(*) FROM "h2o_feet"
name: h2o_feet
+----------------------+------------------------+------------------+
| time                 | last_level description | last_water_level |
+----------------------+------------------------+------------------+
| 1970-01-01T00:00:00Z | between 3 and 6 feet   | 4.938            |
+----------------------+------------------------+------------------+
3 columns, 1 rows in set
```

> LAST(*)返回的时间为0，因为应用写入的数据未必包含全部的列，换句话说，可能每一列的最新数据的时间是不同的。

**选择指定Field列最新的数据，和及其相关的Tag和Field**

```sql
> SELECT LAST("level description"),"location","water_level" FROM "h2o_feet"
name: h2o_feet
+----------------------+----------------------+--------------+-------------+
| time                 | last                 | location     | water_level |
+----------------------+----------------------+--------------+-------------+
| 2019-09-17T21:42:00Z | between 3 and 6 feet | santa_monica | 4.938       |
+----------------------+----------------------+--------------+-------------+
4 columns, 1 rows in set
```

该查询返回measurement `h2o_feet`中field key `level description`对应的具有最新时间戳的field value，以及相关的tag key `location`和field key `water_level`的值。

**指定时间范围分组查询每一组的最新数据**

```sql
> SELECT LAST("water_level") FROM "h2o_feet" WHERE time >= '2019-08-20T00:12:01Z' AND time <= '2019-08-20T08:00:00Z' GROUP BY time(7m),* fill(1000) LIMIT 4
name: h2o_feet
tags: location=coyote_creek
+----------------------+-------+
| time                 | last  |
+----------------------+-------+
| 2019-08-20T00:10:00Z | 1000  |
| 2019-08-20T00:17:00Z | 8.684 |
| 2019-08-20T00:24:00Z | 8.655 |
| 2019-08-20T00:31:00Z | 8.619 |
+----------------------+-------+
2 columns, 4 rows in set
```

该查询返回表 `h2o_feet`中 `water_level` 列对应的最新的值（Field value），指定时间范围为`2019-08-20T00:12:01Z`和`2019-08-20T08:00:00Z`，并将查询结果按7分钟的时间间隔和每个Tag进行分组，同时，若分组为空值，用`1000`填充，并将返回4条结果数据。

## MAX()
返回指定Field列的最大值，不支持对Tag进行计算，不支持SLIMIT和SOFFSET。

**语法**

```sql
SELECT MAX(<field_key>)[,<tag_key(s)>|<field__key(s)>] [INTO_CLAUSE] FROM_CLAUSE [WHERE_CLAUSE] [GROUP_BY_CLAUSE] [ORDER_BY_CLAUSE] [LIMIT_CLAUSE] [OFFSET_CLAUSE] [SLIMIT_CLAUSE] [SOFFSET_CLAUSE]
```

**示例**

**查询指定Field列的最大值**

```sql
> SELECT MAX("water_level") FROM "h2o_feet"
name: h2o_feet
+----------------------+-------+
| time                 | max   |
+----------------------+-------+
| 2019-08-28T07:24:00Z | 9.964 |
+----------------------+-------+
2 columns, 1 rows in set
```

**查询所有列的最大值**

```sql
> SELECT MAX(*) FROM "h2o_feet"
name: h2o_feet
+----------------------+-----------------+
| time                 | max_water_level |
+----------------------+-----------------+
| 2019-08-28T07:24:00Z | 9.964           |
+----------------------+-----------------+
2 columns, 1 rows in set
```

表h2o_feet共有Field列两个：`water_level`和`level_description`，`MAX`仅支持对数值类型进行计算，因此不会有`level_description`。此外，如果存在数值类型的多列，返回值的`time`则为0，因为每列最大值所对应的时间可能不一致。

**正则表达式使用**

```sql
> SELECT MAX(/level/) FROM "h2o_feet"
name: h2o_feet
+----------------------+-----------------+
| time                 | max_water_level |
+----------------------+-----------------+
| 2019-08-28T07:24:00Z | 9.964           |
+----------------------+-----------------+
2 columns, 1 rows in set
```

该查询返回表 `h2o_feet`中列名（Field Key）包含`level`的所有数值类型的列的最大值。

**查询指定Field列的最大值，以及相关的Tag和Field**

```sql
> SELECT MAX("water_level"),"location","level description" FROM "h2o_feet"
name: h2o_feet
+----------------------+-------+--------------+---------------------------+
| time                 | max   | location     | level description         |
+----------------------+-------+--------------+---------------------------+
| 2019-08-28T07:24:00Z | 9.964 | coyote_creek | at or greater than 9 feet |
+----------------------+-------+--------------+---------------------------+
4 columns, 1 rows in set
```

**指定时间范围，并按时间分组查询每组最大值**

```sql
> SELECT MAX("water_level") FROM "h2o_feet" WHERE time >= '2019-08-20T00:12:01Z' AND time <= '2019-08-20T08:00:00Z' GROUP BY time(7m),* fill(1000) LIMIT 4
name: h2o_feet
tags: location=coyote_creek
+----------------------+-------+
| time                 | max   |
+----------------------+-------+
| 2019-08-20T00:10:00Z | 1000  |
| 2019-08-20T00:17:00Z | 8.684 |
| 2019-08-20T00:24:00Z | 8.661 |
| 2019-08-20T00:31:00Z | 8.619 |
+----------------------+-------+
2 columns, 4 rows in set
```

该查询返回表 `h2o_feet`中 `water_level` 列对应的最大值（Field value），指定时间范围为`2019-08-20T00:12:01Z`和`2019-08-20T08:00:00Z`，并将查询结果按7分钟的时间间隔和每个Tag进行分组，同时，若分组为空值，用`1000`填充，并将返回4条结果数据。

## MIN()

返回指定Field列的最小值。不支持对Tag进行计算，不支持SLIMIT和SOFFSET。

**语法**

```sql
SELECT MIN(<field_key>)[,<tag_key(s)>|<field_key(s)>] [INTO_CLAUSE] FROM_CLAUSE [WHERE_CLAUSE] [GROUP_BY_CLAUSE] [ORDER_BY_CLAUSE] [LIMIT_CLAUSE] [OFFSET_CLAUSE]
```

**示例**

**查询指定Field列的最小值**

```sql
> SELECT MIN("water_level") FROM "h2o_feet"
name: h2o_feet
+----------------------+-------+
| time                 | min   |
+----------------------+-------+
| 2019-08-28T14:30:00Z | -0.61 |
+----------------------+-------+
2 columns, 1 rows in set
```

**查询所有Field列的最小值**

```sql
> SELECT MIN(*) FROM "h2o_feet"
name: h2o_feet
+----------------------+-----------------+
| time                 | min_water_level |
+----------------------+-----------------+
| 2019-08-28T14:30:00Z | -0.61           |
+----------------------+-----------------+
2 columns, 1 rows in set
```

表h2o_feet共有Field列两个：`water_level`和`level_description`，`MIN`仅支持对数值类型进行计算，因此不会有`level_description`。此外，如果存在数值类型的多列，返回值的`time`则为0，因为每列最小值所对应的时间可能不一致。

**正则表达式的使用**

```sql
> SELECT MIN(/level/) FROM "h2o_feet"
name: h2o_feet
+----------------------+-----------------+
| time                 | min_water_level |
+----------------------+-----------------+
| 2019-08-28T14:30:00Z | -0.61           |
+----------------------+-----------------+
2 columns, 1 rows in set
```

该查询返回表 `h2o_feet`中列名（Field Key）包含`level`的所有数值类型的列的最小值。

**查询指定Field列的最小值，以及相关的Tag和Field**

```sql
> SELECT MIN("water_level"),"location","level description" FROM "h2o_feet"
name: h2o_feet
+----------------------+-------+--------------+-------------------+
| time                 | min   | location     | level description |
+----------------------+-------+--------------+-------------------+
| 2019-08-28T14:30:00Z | -0.61 | coyote_creek | below 3 feet      |
+----------------------+-------+--------------+-------------------+
4 columns, 1 rows in set
```

**指定时间范围，并按时间分组查询每组最小值**

```sql
> SELECT MIN("water_level") FROM "h2o_feet" WHERE time >= '2019-08-20T00:12:01Z' AND time <= '2019-08-20T08:00:00Z' GROUP BY time(7m),* fill(1000) LIMIT 4
name: h2o_feet
tags: location=coyote_creek
+----------------------+-------+
| time                 | min   |
+----------------------+-------+
| 2019-08-20T00:10:00Z | 1000  |
| 2019-08-20T00:17:00Z | 8.684 |
| 2019-08-20T00:24:00Z | 8.655 |
| 2019-08-20T00:31:00Z | 8.619 |
+----------------------+-------+
2 columns, 4 rows in set
```

该查询返回表 `h2o_feet`中 `water_level` 列对应的最大值（Field value），指定时间范围为`2019-08-20T00:12:01Z`和`2019-08-20T08:00:00Z`，并将查询结果按7分钟的时间间隔和每个Tag进行分组，同时，若分组为空值，用`1000`填充，并将返回4条结果数据。

## TOP()

返回指定Field列最大的N个值，不支持使用*表示所有列，

**语法**

```sql
SELECT TOP( <field_key>[,<tag_key(s)>],<N> )[,<tag_key(s)>|<field_key(s)>] [INTO_clause] FROM_clause [WHERE_clause] [GROUP_BY_clause] [ORDER_BY_clause] [LIMIT_clause] [OFFSET_clause]
```

**示例**

**查询指定Field列最大的3个值**

```sql
> SELECT TOP("water_level", 3) FROM "h2o_feet"
name: h2o_feet
+----------------------+-------+
| time                 | top   |
+----------------------+-------+
| 2019-08-28T07:18:00Z | 9.957 |
| 2019-08-28T07:24:00Z | 9.964 |
| 2019-08-28T07:30:00Z | 9.954 |
+----------------------+-------+
2 columns, 3 rows in set
```

**查询指定Field列最大的3个值，以及相关的Tag和Field**

```sql
> SELECT TOP("water_level",3), "location", "level description" FROM "h2o_feet"
name: h2o_feet
+----------------------+-------+--------------+---------------------------+
| time                 | top   | location     | level description         |
+----------------------+-------+--------------+---------------------------+
| 2019-08-28T07:18:00Z | 9.957 | coyote_creek | at or greater than 9 feet |
| 2019-08-28T07:24:00Z | 9.964 | coyote_creek | at or greater than 9 feet |
| 2019-08-28T07:30:00Z | 9.954 | coyote_creek | at or greater than 9 feet |
+----------------------+-------+--------------+---------------------------+
4 columns, 3 rows in set
```

**指定时间范围，并按时间分组查询每组最大3个值**

```sql
> SELECT TOP("water_level", 3) FROM "h2o_feet" WHERE time >= '2019-08-20T00:12:01Z' AND time <= '2019-08-20T08:00:00Z' GROUP BY time(1h),* fill(1000) LIMIT 4
name: h2o_feet
tags: location=coyote_creek
+----------------------+-------------+
| time                 | top         |
+----------------------+-------------+
| 2019-08-20T00:18:00Z | 8.684       |
| 2019-08-20T00:24:00Z | 8.661       |
| 2019-08-20T00:30:00Z | 8.655       |
| 2019-08-20T01:00:00Z | 8.415       |
| 2019-08-20T01:06:00Z | 8.34        |
| 2019-08-20T01:12:00Z | 8.258       |
| 2019-08-20T02:00:00Z | 7.382       |
| 2019-08-20T02:06:00Z | 7.274       |
| 2019-08-20T02:12:00Z | 7.156       |
| 2019-08-20T03:00:00Z | 6.273       |
| 2019-08-20T03:06:00Z | 6.126144142 |
| 2019-08-20T03:12:00Z | 6.017       |
+----------------------+-------------+
2 columns, 12 rows in set
```

在分组的情况下，`LIMIT 4` 表示返回4个分组，每个分组包含3个最大值，因此返回总数据量是12条。

## BOTTOM()

返回指定Field列最小的N个值，不支持使用*表示所有列，不支持SLIMIT和SOFFSET。

**语法**

```sql
SELECT BOTTOM( <field_key>[,<tag_key(s)>],<N> )[,<tag_key(s)>|<field_key(s)>] [INTO_clause] FROM_clause [WHERE_clause] [GROUP_BY_clause] [ORDER_BY_clause] [LIMIT_clause] [OFFSET_clause]
```

**示例**

**查询指定Field列最大的3个值**

```sql
> SELECT BOTTOM("water_level", 3) FROM "h2o_feet"
name: h2o_feet
+----------------------+--------+
| time                 | bottom |
+----------------------+--------+
| 2019-08-28T14:30:00Z | -0.61  |
| 2019-08-28T14:36:00Z | -0.591 |
| 2019-08-29T15:18:00Z | -0.594 |
+----------------------+--------+
2 columns, 3 rows in set
```

**查询指定Field列最大的3个值，以及相关的Tag和Field**

```sql
> SELECT BOTTOM("water_level",3), "location", "level description" FROM "h2o_feet"
name: h2o_feet
+----------------------+--------+--------------+-------------------+
| time                 | bottom | location     | level description |
+----------------------+--------+--------------+-------------------+
| 2019-08-28T14:30:00Z | -0.61  | coyote_creek | below 3 feet      |
| 2019-08-28T14:36:00Z | -0.591 | coyote_creek | below 3 feet      |
| 2019-08-29T15:18:00Z | -0.594 | coyote_creek | below 3 feet      |
+----------------------+--------+--------------+-------------------+
4 columns, 3 rows in set
```

**指定时间范围，并按时间分组查询每组最大3个值**

```sql
> SELECT BOTTOM("water_level", 3) FROM "h2o_feet" WHERE time >= '2019-08-20T00:12:01Z' AND time <= '2019-08-20T08:00:00Z' GROUP BY time(1h),* fill(1000) LIMIT 4
name: h2o_feet
tags: location=coyote_creek
+----------------------+--------+
| time                 | bottom |
+----------------------+--------+
| 2019-08-20T00:42:00Z | 8.593  |
| 2019-08-20T00:48:00Z | 8.56   |
| 2019-08-20T00:54:00Z | 8.481  |
| 2019-08-20T01:42:00Z | 7.743  |
| 2019-08-20T01:48:00Z | 7.631  |
| 2019-08-20T01:54:00Z | 7.507  |
| 2019-08-20T02:42:00Z | 6.621  |
| 2019-08-20T02:48:00Z | 6.509  |
| 2019-08-20T02:54:00Z | 6.378  |
| 2019-08-20T03:42:00Z | 5.354  |
| 2019-08-20T03:48:00Z | 5.23   |
| 2019-08-20T03:54:00Z | 5.098  |
+----------------------+--------+
2 columns, 12 rows in set
```

在分组的情况下，`LIMIT 4` 表示返回4个分组，每个分组包含3个最小值，因此返回总数据量是12条。

## PERCENTILE()

返回指定Field列的第N百分位数，N属于[0,100]。百分位数用P加下标表示，如P<sub>30</sub> = 60，表示目标数据中小于60的占比30%。 不支持SLIMIT和SOFFSET。

**语法**

```sql
SELECT PERCENTILE(<field_key>, <N>)[,<tag_key(s)>|<field_key(s)>] [INTO_clause] FROM_clause [WHERE_clause] [GROUP_BY_clause] [ORDER_BY_clause] [LIMIT_clause] [OFFSET_clause]
```

**示例**

**查询指定Field列的P<sub>90</sub>**

```sql
> SELECT PERCENTILE("water_level", 90) FROM "h2o_feet"
name: h2o_feet
+----------------------+------------+
| time                 | percentile |
+----------------------+------------+
| 2019-09-13T19:36:00Z | 7.969      |
+----------------------+------------+
2 columns, 1 rows in set
```

表示`water_level`列90%的数小于`7.969`

**查询指定Field列的P<sub>90</sub>，以及相关的Tag和Field**

```sql
> SELECT PERCENTILE("water_level",90), "location", "level description" FROM "h2o_feet"
name: h2o_feet
+----------------------+------------+--------------+----------------------+
| time                 | percentile | location     | level description    |
+----------------------+------------+--------------+----------------------+
| 2019-09-13T19:36:00Z | 7.969      | coyote_creek | between 6 and 9 feet |
+----------------------+------------+--------------+----------------------+
4 columns, 1 rows in set
```

**指定时间范围，并按时间分组查询每组的P<sub>90</sub>**

```sql
SELECT PERCENTILE("water_level",90), "location", "level description" FROM "h2o_feet" WHERE time >= '2019-08-20T00:12:01Z' AND time <= '2019-08-21T08:00:00Z' GROUP BY time(12h),* fill(1000)
name: h2o_feet
tags: location=coyote_creek
+----------------------+------------+--------------+----------------------+
| time                 | percentile | location     | level description    |
+----------------------+------------+--------------+----------------------+
| 2019-08-20T00:00:00Z | 7.959      | coyote_creek | between 6 and 9 feet |
| 2019-08-20T12:00:00Z | 7.49       | coyote_creek | between 6 and 9 feet |
| 2019-08-21T00:00:00Z | 8.491      | coyote_creek | between 6 and 9 feet |
+----------------------+------------+--------------+----------------------+
4 columns, 3 rows in set

name: h2o_feet
tags: location=santa_monica
+----------------------+------------+--------------+----------------------+
| time                 | percentile | location     | level description    |
+----------------------+------------+--------------+----------------------+
| 2019-08-20T00:00:00Z | 3.704      | santa_monica | between 3 and 6 feet |
| 2019-08-20T12:00:00Z | 4.797      | santa_monica | between 3 and 6 feet |
| 2019-08-21T00:00:00Z | 3.678      | santa_monica | between 3 and 6 feet |
+----------------------+------------+--------------+----------------------+
4 columns, 3 rows in set
```

## SAMPLE()

随机返回指定Field列的N个样本数据。不支持SLIMIT和SOFFSET。

**语法**

```sql
SELECT SAMPLE(<field_key>, <N>)[,<tag_key(s)>|<field_key(s)>] [INTO_clause] FROM_clause [WHERE_clause] [GROUP_BY_clause] [ORDER_BY_clause] [LIMIT_clause] [OFFSET_clause]
```

**示例**

**随机采样指定Field列5个数**

```sql
# 第一次执行
> SELECT SAMPLE("water_level",5) FROM "h2o_feet"
name: h2o_feet
+----------------------+--------+
| time                 | sample |
+----------------------+--------+
| 2019-08-23T06:30:00Z | 1.355  |
| 2019-08-31T11:24:00Z | 8.533  |
| 2019-09-02T04:48:00Z | 2.536  |
| 2019-09-13T07:54:00Z | 8.806  |
| 2019-09-15T10:42:00Z | 2.602  |
+----------------------+--------+
2 columns, 5 rows in set
# 第二次执行
> SELECT SAMPLE("water_level",5) FROM "h2o_feet"
name: h2o_feet
+----------------------+-------------+
| time                 | sample      |
+----------------------+-------------+
| 2019-08-20T06:48:00Z | 2.159       |
| 2019-08-30T03:06:00Z | 4.728       |
| 2019-09-09T21:24:00Z | 7.126144146 |
| 2019-09-10T23:48:00Z | 4.656       |
| 2019-09-14T18:24:00Z | 5.86        |
+----------------------+-------------+
2 columns, 5 rows in set
```

每次都是随机选择，因此返回的结果也不相同。

**返回采样数据相关的Tag和其他Field值**

```sql
> SELECT SAMPLE("water_level",5), "location", "level description" FROM "h2o_feet"
name: h2o_feet
+----------------------+--------+--------------+----------------------+
| time                 | sample | location     | level description    |
+----------------------+--------+--------------+----------------------+
| 2019-08-17T21:18:00Z | 4.075  | coyote_creek | below 3 feet         |
| 2019-09-07T09:36:00Z | 2.726  | coyote_creek | below 3 feet         |
| 2019-09-08T14:54:00Z | 3.514  | coyote_creek | between 3 and 6 feet |
| 2019-09-09T09:00:00Z | 0.919  | santa_monica | below 3 feet         |
| 2019-09-13T06:00:00Z | 6.119  | coyote_creek | between 6 and 9 feet |
+----------------------+--------+--------------+----------------------+
4 columns, 5 rows in set
```

## PERCENTILE_OGSKETCH()

返回指定Field列的第N个近似百分位数，N属于[0,100]，近似百分位数顾名思义对百分位数的计算是一个近似值，不是准确值。相比PERCENTILE()计算数速度更快，适合针对大数据集求解包分位数的场景。

语法

```sql
SELECT PERCENTILE_OGSKETCH(<field_key>, <N>) [INTO_clause] FROM_clause [WHERE_clause] [GROUP_BY_clause] [ORDER_BY_clause] [LIMIT_clause] [OFFSET_clause]
```

示例

**查询指定Field列的近似P<sub>90</sub>**

```sql
#准确值
> SELECT PERCENTILE("water_level", 90) FROM "h2o_feet"
name: h2o_feet
+----------------------+------------+
| time                 | percentile |
+----------------------+------------+
| 2019-09-13T19:36:00Z | 7.969      |
+----------------------+------------+
2 columns, 1 rows in set

#近似值
> SELECT PERCENTILE_OGSKETCH("water_level", 90) FROM "h2o_feet"
name: h2o_feet
+----------------------+---------------------+
| time                 | percentile_ogsketch |
+----------------------+---------------------+
| 1970-01-01T00:00:00Z | 7.974716590327904   |
+----------------------+---------------------+
2 columns, 1 rows in set
```
