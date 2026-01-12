---
title: 数据查询
order: 1
---

## 样本数据

开始探索数据之前，为了更好地演示下面的语法，我们先导入一些公开可用数据，请先按照[样本数据](../reference/sample_data.md)文档录入数据。

以下部分中的示例查询均按照上述样本数据进行操作。

## 客户端

在学习查询语句之前，需要使用客户端ts-cli连接openGemini：

```shell
$ ts-cli -database NOAA_water_database
openGemini CLI 0.1.0 (rev-revision)
Please use `quit`, `exit` or `Ctrl-D` to exit this program.
>
```

## SELECT
`SELECT` 语句执行数据检索。 默认情况下，请求的数据将返回给客户端，同时结合 [SELECT INTO]() 可以被转发到不同的表。

### 语法

```sql
SELECT <field_key>[,<field_key>,<tag_key>] FROM <measurement_name>[,<measurement_name>]
```

### SELECT子句

 `SELECT` 子句支持下面的格式:

| 格式                                           | 含义                                                         |
| ---------------------------------------------- | ------------------------------------------------------------ |
| `SELECT *`                                     | 返回所有的tag和field                                         |
| `SELECT "<field_key>"`                         | 返回指定的field                                              |
| `SELECT "<field_key>","<field_key>"`           | 返回多个指定的field                                          |
| `SELECT "<field_key>","<tag_key>"`             | 返回指定的field和tag。如果指定了tag，则必须指定至少一个field |
| `SELECT "<field_key>"::field,"<tag_key>"::tag` | 返回特定field和tag。 ::[field \| tag] 语法指定标识符类型。使用此语法区分具有相同名称的field和tag |

其他支持的功能： [数学表达式](../reference/math.md)、 [聚合算子](../functions/aggregate.md)、 [正则表达式]()

::: tip

SELECT 语句不能同时包含==聚合函数==**和**==非聚合函数、field_key或tag_key==。

:::

### FROM子句

`FROM` 子句指定从以下数据源中读取数据:

- 表
- 子查询

支持的格式：

| 格式                                                         | 含义                                                         |
| ------------------------------------------------------------ | ------------------------------------------------------------ |
| `FROM <measurement_name>`                                    | 从单个表中获取数据                                           |
| `FROM <measurement_name>,<measurement_name>`                 | 从多个表中获取数据                                           |
| `FROM <database_name>.<retention_policy_name>.<measurement_name>` | 从指定的database，指定的retention_policy，指定的表中获取数据 |
| `FROM <database_name>..<measurement_name>`                   | 从指定的database，默认的retention_policy，指定的表中获取数据 |
| `FROM /<regular_expression_measurement>/`                    | 用正则表达式匹配对应的表，获取数据                           |

### 示例
从一个表中查询的所有的field和tags。

```sql
> select * from "h2o_feet"
name: h2o_feet
time                 level description         location     water_level
----                 -----------------         --------     -----------
2019-08-17T00:00:00Z below 3 feet              santa_monica 2.064
2019-08-17T00:00:00Z between 6 and 9 feet      coyote_creek 8.12
2019-08-17T00:06:00Z below 3 feet              santa_monica 2.116
[......]
2019-09-17T21:30:00Z between 3 and 6 feet      santa_monica 5.01
2019-09-17T21:36:00Z between 3 and 6 feet      santa_monica 5.066
2019-09-17T21:42:00Z between 3 and 6 feet      santa_monica 4.938
```

从一个表查询指定field的top 5数据

```sql
> SELECT top("water_level", 5) FROM "h2o_feet"
name: h2o_feet
time                 top
----                 ---
2019-08-28T07:12:00Z 9.938
2019-08-28T07:18:00Z 9.957
2019-08-28T07:24:00Z 9.964
2019-08-28T07:30:00Z 9.954
2019-08-28T07:36:00Z 9.941
```

从一个表中查询最新的一条记录并查询对应tag。

```sql
> SELECT last("water_level"), location FROM "h2o_feet"
name: h2o_feet
time                 last  location
----                 ----  --------
2019-09-17T21:42:00Z 4.938 santa_monica
```

从一个表中查询所有数据并做一些基础运算。

```sql
> SELECT ("water_level" * 4) + 2 FROM "h2o_feet" limit 10
name: h2o_feet
time                 water_level
----                 -----------
2019-08-17T00:00:00Z 10.256
2019-08-17T00:00:00Z 34.48
[......]
2019-09-17T21:36:00Z 22.264
2019-09-17T21:42:00Z 21.752
```


## WHERE

`WHERE`子句根据field、tag、timestamp来过滤数据。

### 语法

```sql
SELECT COLUMN_CLAUSES FROM_CLAUSE WHERE <CONDITION> [(AND|OR) <CONDITION> [...]]
```

<font size=4 color=green>TAGS</font>

```sql
tag_key <operator> ['tag_value']
```

在`WHERE`子句中，请对`tag value`用单引号括起来。如果`tag value`没有使用引号或者使用了双引号，那么不会返回任何查询结果，在大多数情况下，也不会返回错误。

支持的 operator 有：

| 操作符 | 含义   |
| :----: | :----- |
|  `=`   | 等于   |
|  `<>`  | 不等于 |
|  `!=`  | 不等于 |

operator还支持：正则表达式。


<font size=4 color=green>Fields</font>

```sql
field_key <operator> ['string' | boolean | float | integer]
```

`WHERE`子句支持对`field value`进行比较，`field value`可以是字符串、布尔值、浮点数或者整数。

在`WHERE`子句中，请对字符串类型的`field value`用单引号括起来。如果字符串类型的field value没有使用引号或者使用了双引号，那么不会返回任何查询结果，在大多数情况下，也不会返回错误。

支持的 operator 有：

| 操作符 | 含义     |
| :----: | :------- |
|  `=`   | 等于     |
|  `<>`  | 不等于   |
|  `!=`  | 不等于   |
|  `>`   | 大于     |
|  `>=`  | 大于等于 |
|  `<`   | 小于     |
|  `<=`  | 小于等于 |

operator还支持：算术运算和正则表达式。

<font size=4 color=green>Timestamp</font>

对于大多数`SELECT`语句，默认的时间范围是全部时间范围。对于带`GROUP BY time()`子句的`SELECT`语句，默认的时间范围是从**时间最小的数据的时间**`到`now()`。

### 示例

- **查询field value满足一定条件的数据**

```sql
> SELECT * FROM "h2o_feet" WHERE "water_level" > 8
name: h2o_feet
+---------------------+---------------------------+--------------+-------------+
| time                | level description         | location     | water_level |
+---------------------+---------------------------+--------------+-------------+
| 1566000000000000000 | between 6 and 9 feet      | coyote_creek | 8.12        |
| 1566000360000000000 | between 6 and 9 feet      | coyote_creek | 8.005       |
| [......]                                                                     |
| 1568679120000000000 | between 6 and 9 feet      | coyote_creek | 8.189       |
| 1568679480000000000 | between 6 and 9 feet      | coyote_creek | 8.084       |
+---------------------+---------------------------+--------------+-------------+
4 columns, 1503 rows in set
```

该查询返回`h2o_feet`中的数据，这些数据满足条件：field key `water_level`的值大于8。

- **查询field value和tag value都满足一定条件的数据**

```sql
> SELECT "water_level" FROM "h2o_feet" WHERE "location" <> 'santa_monica' AND (water_level < -0.57 OR water_level > 9.95)
name: h2o_feet
+---------------------+-------------+
| time                | water_level |
+---------------------+-------------+
| 1566976680000000000 | 9.957       |
| 1566977040000000000 | 9.964       |
| 1566977400000000000 | 9.954       |
| 1567002240000000000 | -0.587      |
| 1567002600000000000 | -0.61       |
| 1567002960000000000 | -0.591      |
| 1567091880000000000 | -0.594      |
| 1567092240000000000 | -0.571      |
+---------------------+-------------+
2 columns, 8 rows in set
```

- **查询timestamp满足一定条件的数据**

```sql
> SELECT * FROM "h2o_feet" WHERE time > now() - 7d
Elapsed: 1.062851ms
```

没有满足过去7天内的数据。

## GROUP BY

`GROUP BY`子句按用户指定的tag或者时间区间对查询结果进行分组。

`GROUP BY`子句按以下方式对查询结果进行分组：

- 一个或多个指定的`tags`
- 指定的时间间隔

::: warning

不能使用`GROUP BY`对`fields`进行分组

:::

### 语法

<font size=4 color=green>GROUP BY TAGS</font>
按标签分组
```sql
SELECT COLUMN_CLAUSES FROM_CLAUSE [WHERE_CLAUSE] GROUP BY [* | <tag_key>[,<tag_key]]
```

<font size=4 color=green>GROUP BY TIME INTERVALS</font>
`GROUP BY time()`按用户指定的时间间隔对查询结果进行分组，time和tag可以一起分组。

```sql
SELECT COLUMN_CLAUSES FROM_CLAUSE [WHERE_CLAUSE] GROUP BY time(<time_interval>),[tag_key] [fill(<fill_option>)]
```

<font size=4 color=green>time(time_interval)</font>

`GROUP BY time()`子句中的`time_interval`（时间间隔）是一个持续时间（duration），决定了openGemini按多大的时间间隔将查询结果进行分组。例如，当`time_interval`为`5m`时，那么在`WHERE`子句中指定的时间范围内，将查询结果按5分钟进行分组。

<font size=4 color=green>fill()</font>

`fill(<fill_option>)`是可选的，它会改变不含数据的时间间隔的返回值。


### 示例

- **按单个tag对查询结果进行分组**

```sql
> SELECT MEAN("water_level") FROM "h2o_feet" GROUP BY "location"
name: h2o_feet
tags: location=coyote_creek
+------+--------------------+
| time | mean               |
+------+--------------------+
| 0    | 5.3591424203039155 |
+------+--------------------+
2 columns, 1 rows in set

name: h2o_feet
tags: location=santa_monica
+------+--------------------+
| time | mean               |
+------+--------------------+
| 0    | 3.5307120942458803 |
+------+--------------------+
2 columns, 1 rows in set
```


- **将查询结果按12分钟的时间间隔进行分组**

```sql
> SELECT COUNT("water_level") FROM "h2o_feet" WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY time(12m)
name: h2o_feet
+---------------------+-------+
| time                | count |
+---------------------+-------+
| 1566086400000000000 | 4     |
| 1566087120000000000 | 4     |
| 1566087840000000000 | 4     |
+---------------------+-------+
2 columns, 3 rows in set
```


## ORDER BY

openGemini 默认按递增的时间顺序返回结果。第一个返回的数据点，其时间戳是最早的，而最后一个返回的数据点，其时间戳是最新的。`ORDER BY time DESC`将默认的时间顺序调转，使得openGemini首先返回有最新时间戳的数据点，也就是说，按递减的时间顺序返回结果。

`ORDER BY` 子句仅支持对time排序。

### 语法

```sql
SELECT COLUMN_CLAUSES FROM_CLAUSE [WHERE_CLAUSE] [GROUP_BY_CLAUSE] ORDER BY time [ASC|DESC]
```

### 示例

- **首先返回最新的点**

```sql
> SELECT "water_level","location" FROM "h2o_feet" WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' ORDER BY time DESC
name: h2o_feet
+---------------------+-------------+--------------+
| time                | water_level | location     |
+---------------------+-------------+--------------+
| 1566088200000000000 | 2.267       | santa_monica |
| 1566088200000000000 | 8.012       | coyote_creek |
| 1566087840000000000 | 2.264       | santa_monica |
| 1566087840000000000 | 8.13        | coyote_creek |
| 1566087480000000000 | 2.329       | santa_monica |
| 1566087480000000000 | 8.225       | coyote_creek |
| 1566087120000000000 | 2.343       | santa_monica |
| 1566087120000000000 | 8.32        | coyote_creek |
| 1566086760000000000 | 2.379       | santa_monica |
| 1566086760000000000 | 8.419       | coyote_creek |
| 1566086400000000000 | 2.352       | santa_monica |
| 1566086400000000000 | 8.504       | coyote_creek |
+---------------------+-------------+--------------+
3 columns, 12 rows in set
```

- **首先返回最新的点并且包含`GROUP BY time()`子句**

```sql
> SELECT COUNT("water_level") FROM "h2o_feet" WHERE time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:30:00Z' GROUP BY time(12m) ORDER BY time DESC
name: h2o_feet
+---------------------+-------+
| time                | count |
+---------------------+-------+
| 1566087840000000000 | 4     |
| 1566087120000000000 | 4     |
| 1566086400000000000 | 4     |
+---------------------+-------+
2 columns, 3 rows in set
```

## LIMIT OFFSET

`LIMIT <N>` 子句是限制每个查询返回的数据点个数。LIMIT可以单独使用

`OFFSET <N>`子句表示从查询结果中的第`N`个数据点开始返回

### 语法

```sql
SELECT COLUMN_CLAUSES FROM_CLAUSE [WHERE_CLAUSE] [GROUP_BY_CLAUSE] [ORDER_BY_CLAUSE] LIMIT <N1> OFFSET <N2>
```

`N2`表示从第`N2`个数据点开始返回，返回指定measurement的前`N1`个数据点，不会返回所有时间线的数据。

### 示例

- **同时指定数据点返回的位置和数据量**

```sql
>>> SELECT "water_level","location" FROM "h2o_feet" LIMIT 3 OFFSET 3
name: h2o_feet
+---------------------+-------------+--------------+
| time                | water_level | location     |
+---------------------+-------------+--------------+
| 1566000360000000000 | 2.116       | santa_monica |
| 1566000720000000000 | 7.887       | coyote_creek |
| 1566000720000000000 | 2.028       | santa_monica |
+---------------------+-------------+--------------+
3 columns, 3 rows in set
```

该查询从measurement `h2o_feet`中返回第四、第五和第六个数据点。如果以上查询语句中没有使用`OFFSET 3`，那么查询将返回该measurement的第一、第二和第三个数据点。


## TIMEZONE

`tz()`子句返回指定时区的UTC偏移量。

### 语法

```sql
SELECT COLUMN_CLAUSES FROM_CLAUSE [WHERE_CLAUSE] [GROUP_BY_CLAUSE] [LIMIT_CLAUSE] [OFFSET_CLAUSE] tz('<time_zone>')
```

openGemini 默认以UTC格式存储和返回时间戳。

### 示例

- **返回America/Chicago时区的UTC偏移量**

```sql
>>> SELECT "water_level" FROM "h2o_feet" WHERE "location" = 'santa_monica' AND time >= '2019-08-18T00:00:00Z' AND time <= '2019-08-18T00:18:00Z' tz('America/Chicago')
name: h2o_feet
+---------------------+-------------+
| time                | water_level |
+---------------------+-------------+
| 1566086400000000000 | 2.352       |
| 1566086760000000000 | 2.379       |
| 1566087120000000000 | 2.343       |
| 1566087480000000000 | 2.329       |
+---------------------+-------------+
2 columns, 4 rows in set
```

该查询结果中，时间戳包含了美国/芝加哥（`America/Chicago`）的时区的UTC偏移量（`-05:00`）。
