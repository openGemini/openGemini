---
title: 表结构
order: 4
---
本章主要包括如下内容
- [SHOW TAG KEYS (查看表中所有TAG字段)](#show-tag-keys)
- [SHOW TAG VALUES (查看表中所有TAG字段及其对应的值)](#show-tag-values)
- [SHOW FIELD KEYS (查看表中全部Field字段及其数据类型)](#show-field-keys)
- [SHOW SERIES (查看全部时间线)](#show-series)
- [SHOW SERIES CARDINALITY (查询时间线统计数量)](#show-series-cardinality)
- [SHOW SHARDS (查看数据分片信息)](#show-shards)
- [SHOW SHARD GROUPS(查看分片组信息)](#show-shard-groups)

## SHOW TAG KEYS

查看表中所有TAG字段

### 语法

```sql
SHOW TAG KEYS [ON <database_name>] [FROM_clause] [WHERE <tag_key> <operator> ['<tag_value>' | <regular_expression>]] [LIMIT_clause] [OFFSET_clause]
```

如果查询中没有包含`ON <database_name>`，您必须在CLI中使用`USE <database_name>`指定数据库，或者在openGemini API请求中使用参数`db`指定数据库。

### 示例

- **运行带有`ON`子句的`SHOW TAG KEYS`查询**

:::tabs

@tab ts-cli

```sql
> SHOW TAG KEYS ON "NOAA_water_database"
name: average_temperature
+----------+
| tagKey   |
+----------+
| location |
+----------+
1 columns, 1 rows in set

name: h2o_feet
+----------+
| tagKey   |
+----------+
| location |
+----------+
1 columns, 1 rows in set

name: h2o_pH
+----------+
| tagKey   |
+----------+
| location |
+----------+
1 columns, 1 rows in set

name: h2o_quality
+----------+
| tagKey   |
+----------+
| location |
| randtag  |
+----------+
1 columns, 2 rows in set

name: h2o_temperature
+----------+
| tagKey   |
+----------+
| location |
+----------+
1 columns, 1 rows in set

```

该查询返回数据库`NOAA_water_database`中的所有表的tag key。

@tab HTTP API

```sql
> curl -G "http://localhost:8086/query?pretty=true" --data-urlencode "q=SHOW TAG KEYS on NOAA_water_database"
{
	"results": [{
		"statement_id": 0,
		"series": [{
			"name": "average_temperature",
			"columns": ["tagKey"],
			"values": [
				["location"]
			]
		}, {
			"name": "h2o_feet",
			"columns": ["tagKey"],
			"values": [
				["location"]
			]
		}, {
			"name": "h2o_pH",
			"columns": ["tagKey"],
			"values": [
				["location"]
			]
		}, {
			"name": "h2o_quality",
			"columns": ["tagKey"],
			"values": [
				["location"],
				["randtag"]
			]
		}, {
			"name": "h2o_temperature",
			"columns": ["tagKey"],
			"values": [
				["location"]
			]
		}]
	}]
}
```

:::

- **运行不带有`ON`子句的`SHOW TAG KEYS`查询**

::: tabs

@tab ts-cli

使用`USE <database_name>`指定数据库：

```sql
> use NOAA_water_database
Elapsed: 452ns
> SHOW TAG KEYS
name: average_temperature
+----------+
| tagKey   |
+----------+
| location |
+----------+
1 columns, 1 rows in set

name: h2o_feet
+----------+
| tagKey   |
+----------+
| location |
+----------+
1 columns, 1 rows in set

name: h2o_pH
+----------+
| tagKey   |
+----------+
| location |
+----------+
1 columns, 1 rows in set

name: h2o_quality
+----------+
| tagKey   |
+----------+
| location |
| randtag  |
+----------+
1 columns, 2 rows in set

name: h2o_temperature
+----------+
| tagKey   |
+----------+
| location |
+----------+
1 columns, 1 rows in set
```

@tab HTTP API

使用参数`db`指定数据库

```bash
> curl -G "http://localhost:8086/query?db=NOAA_water_database&pretty=true" --data-urlencode "q=SHOW TAG KEYS"
{
	"results": [{
		"statement_id": 0,
		"series": [{
				"name": "average_temperature",
				"columns": [
					"tagKey"
				],
				"values": [
					[
						"location"
					]
				]
			},
			{
				"name": "h2o_feet",
				"columns": [
					"tagKey"
				],
				"values": [
					[
						"location"
					]
				]
			},
			{
				"name": "h2o_pH",
				"columns": [
					"tagKey"
				],
				"values": [
					[
						"location"
					]
				]
			},
			{
				"name": "h2o_quality",
				"columns": [
					"tagKey"
				],
				"values": [
					[
						"location"
					],
					[
						"randtag"
					]
				]
			},
			{
				"name": "h2o_temperature",
				"columns": [
					"tagKey"
				],
				"values": [
					[
						"location"
					]
				]
			}
		]
	}]
}
```

:::

- **运行带有多个子句的`SHOW TAG KEYS`查询**

```sql
> SHOW TAG KEYS ON "NOAA_water_database" FROM "h2o_quality" LIMIT 1 OFFSET 1
name: h2o_quality
+---------+
| tagKey  |
+---------+
| randtag |
+---------+
1 columns, 1 rows in set
```

该查询返回数据库`NOAA_water_database`中名为`h2o_quality`的measurement里的tag key。`LIMIT`子句将返回的tag key的个数限制为1，`OFFSET`子句将输出结果偏移一个。

- **查看TAG的统计数量**

在某些场景下，仅需要了解TAG数量，不关心具体的TAG，可以使用SHOW TAG KEY CARDINALITY命令，使用方式如下：

```
SHOW TAG KEY CARDINALITY [ON <database_name>] [FROM_CLAUSE]
```
例如：
```sql
> SHOW TAG KEY CARDINALITY
name: average_temperature
+-------+
| count |
+-------+
| 1     |
+-------+
1 columns, 1 rows in set

name: h2o_feet
+-------+
| count |
+-------+
| 1     |
+-------+
1 columns, 1 rows in set

name: h2o_pH
+-------+
| count |
+-------+
| 1     |
+-------+
1 columns, 1 rows in set

name: h2o_quality
+-------+
| count |
+-------+
| 2     |
+-------+
1 columns, 1 rows in set

name: h2o_temperature
+-------+
| count |
+-------+
| 1     |
+-------+
1 columns, 1 rows in set

> SHOW TAG KEY CARDINALITY FROM h2o_temperature
name: h2o_temperature
+-------+
| count |
+-------+
| 1     |
+-------+
1 columns, 1 rows in set
```

- **运行带条件的`SHOW TAG KEYS`查询**

```sql
> SHOW TAG KEYS FROM h2o_quality WHERE location=coyote_creek
name: h2o_quality
+----------+
| tagKey   |
+----------+
| location |
| randtag  |
+----------+
1 columns, 2 rows in set

> SHOW TAG KEYS FROM h2o_quality WHERE location=coyote_creek LIMIT 1
name: h2o_quality
+----------+
| tagKey   |
+----------+
| location |
+----------+
1 columns, 1 rows in set
```

## SHOW TAG VALUES

返回数据库中指定tag key的tag value。

### 语法

```sql
SHOW TAG VALUES [ON <database_name>] [FROM_CLAUSE] WITH KEY [ [<operator> "<tag_key>" | <regular_expression>] | [IN ("<tag_key1>","<tag_key2")]] [WHERE <tag_key> <operator> ['<tag_value>' | <regular_expression>]] [LIMIT_CLAUSE] [OFFSET_CLAUSE]
```

`ON <database_name>`是可选的。如果查询中没有包含`ON <database_name>`，您必须在CLI中使用`USE <database_name>`指定数据库，或者在HTTP API请求中使用参数`db`指定数据库。

`WITH`子句是必须要有的，它支持指定一个tag key、一个正则表达式或多个tag key。

`FROM`子句、`WHERE`子句、`LIMIT`子句和`OFFSET`子句是可选的。`WHERE`子句支持tag比较；在`SHOW TAG VALUES`查询中，field比较是无效的。

`WITH`子句和`WHERE`子句中支持的操作符：

| 操作符 | 含义   |
| ------ | ------ |
| `=`    | 等于   |
| `<>`   | 不等于 |
| `!=`   | 不等于 |
| `=~`   | 匹配   |
| `!~`   | 不匹配 |

请查阅DML章节获得关于[`FROM`子句](../query_data/select.md#select)、[`LIMIT、OFFSET`子句](../query_data/select.md#limit-offset)、和正则表达式的介绍。

### 示例

- **运行带有`ON`子句的`SHOW TAG VALUES`查询**

```sql
> SHOW TAG VALUES ON "NOAA_water_database" WITH KEY = "randtag"

name: h2o_quality
key       value
---       -----
randtag   1
randtag   2
randtag   3
```

该查询返回数据库`NOAA_water_database`中的tag key `randtag`的所有tag value。`SHOW TAG VALUES`将查询结果按measurement的名字进行分组。

- **运行不带有`ON`子句的`SHOW TAG KEYS`查询**

::: tabs

@tab ts-cli

使用`USE <database_name>`指定数据库：

```sql
> USE NOAA_water_database
Using database NOAA_water_database

> SHOW TAG VALUES WITH KEY = "randtag"

name: h2o_quality
key       value
---       -----
randtag   1
randtag   2
randtag   3
```

@tab HTTP API

使用参数`db`指定数据库

```bash
> curl -G "http://localhost:8086/query?db=NOAA_water_database&pretty=true" --data-urlencode 'q=SHOW TAG VALUES WITH KEY = "randtag"'
{
	"results": [{
		"statement_id": 0,
		"series": [{
			"name": "h2o_quality",
			"columns": [
				"key",
				"value"
			],
			"values": [
				[
					"randtag",
					"1"
				],
				[
					"randtag",
					"2"
				],
				[
					"randtag",
					"3"
				]
			]
		}]
	}]
}
```

:::

- **运行带有多个子句的`SHOW TAG VALUES`查询**

```sql
> SHOW TAG VALUES ON "NOAA_water_database" WITH KEY IN ("location","randtag") WHERE "randtag" =~ /./ LIMIT 3

name: h2o_quality
key        value
---        -----
location   coyote_creek
location   santa_monica
randtag	   1
```
该查询从数据库`NOAA_water_database`的所有measurement中返回`location`或`randtag`的tag value，并且返回的数据还需满足条件：`randtag`的tag value不为空。`LIMIT`子句将返回的tag value的个数限制为3。

## SHOW FIELD KEYS
返回field key和field value的数据类型。

### 语法

```sql
SHOW FIELD KEYS [ON <database_name>] [FROM <measurement_name>]
```

`ON <database_name>`是可选的
`FROM`子句是可选的。请参考[`FROM`子句](../query_data/select.md)的介绍。

如果查询中没有包含`ON <database_name>`，您必须在CLI中使用`USE <database_name>`指定数据库，或者在openGemini API请求中使用参数`db`指定数据库。

### 示例

- **运行带有`ON`子句的`SHOW FIELD KEYS`查询**

```sql
> SHOW FIELD KEYS ON "NOAA_water_database"

name: average_temperature
fieldKey            fieldType
--------            ---------
degrees             float

name: h2o_feet
fieldKey            fieldType
--------            ---------
level description   string
water_level         float

name: h2o_pH
fieldKey            fieldType
--------            ---------
pH                  float

name: h2o_quality
fieldKey            fieldType
--------            ---------
index               float

name: h2o_temperature
fieldKey            fieldType
--------            ---------
degrees             float
```

该查询返回数据库`NOAA_water_database`中每个measurement的field key以及对应的field value的数据类型。

- **运行不带有`ON`子句的`SHOW FIELD KEYS`查询**

::: tabs

@tab ts-cli

使用`USE <database_name>`指定数据库：

```sql
> USE NOAA_water_database
Using database NOAA_water_database

> SHOW FIELD KEYS

name: average_temperature
fieldKey            fieldType
--------            ---------
degrees             float

name: h2o_feet
fieldKey            fieldType
--------            ---------
level description   string
water_level         float

name: h2o_pH
fieldKey            fieldType
--------            ---------
pH                  float

name: h2o_quality
fieldKey            fieldType
--------            ---------
index               float

name: h2o_temperature
fieldKey            fieldType
--------            ---------
degrees             float
```

@tab HTTP API

使用参数`db`指定数据库

```bash
> curl -G "http://localhost:8086/query?db=NOAA_water_database&pretty=true" --data-urlencode 'q=SHOW FIELD KEYS'
{
	"results": [{
		"statement_id": 0,
		"series": [{
				"name": "average_temperature",
				"columns": [
					"fieldKey",
					"fieldType"
				],
				"values": [
					[
						"degrees",
						"float"
					]
				]
			},
			{
				"name": "h2o_feet",
				"columns": [
					"fieldKey",
					"fieldType"
				],
				"values": [
					[
						"level description",
						"string"
					],
					[
						"water_level",
						"float"
					]
				]
			},
			{
				"name": "h2o_pH",
				"columns": [
					"fieldKey",
					"fieldType"
				],
				"values": [
					[
						"pH",
						"float"
					]
				]
			},
			{
				"name": "h2o_quality",
				"columns": [
					"fieldKey",
					"fieldType"
				],
				"values": [
					[
						"index",
						"float"
					]
				]
			},
			{
				"name": "h2o_temperature",
				"columns": [
					"fieldKey",
					"fieldType"
				],
				"values": [
					[
						"degrees",
						"float"
					]
				]
			}
		]
	}]
}
```

:::

- **运行带有`FROM`子句的`SHOW FIELD KEYS`查询**

```sql
> SHOW FIELD KEYS ON "NOAA_water_database" FROM "h2o_feet"

name: h2o_feet
fieldKey            fieldType
--------            ---------
level description   string
water_level         float
```

该查询返回数据库`NOAA_water_database`中measurement `h2o_feet`里的fields key以及对应的field value的数据类型。

## SHOW SERIES

返回指定数据库的时间线。

### 语法

```sql
SHOW SERIES [ON <database_name>] [FROM_CLAUSE] [WHERE <tag_key> <operator> [ '<tag_value>' | <regular_expression>]] [LIMIT_CLAUSE] [OFFSET_CLAUSE]
```

`ON <database_name>`是可选的。如果查询中没有包含`ON <database_name>`，您必须在CLI中使用`USE <database_name>`指定数据库，或者在openGemini API请求中使用参数`db`指定数据库。

`WHERE`子句支持`tag`比较；在`SHOW SERIES`查询中，`field`比较是无效的。

`WHERE`子句中支持的操作符：

| 操作符 | 含义   |
| ------ | ------ |
| `=`    | 等于   |
| `<>`   | 不等于 |
| `!=`   | 不等于 |
| `=~`   | 匹配   |
| `!~`   | 不匹配 |

参考[`FROM`子句](../query_data/select.md#select)、[`LIMIT、OFFSET`子句](../query_data/select.md#limit-offset)、和正则表达式的介绍。

### 示例

- **运行带有`ON`子句的`SHOW SERIES`查询**

```sql
>>> SHOW SERIES ON NOAA_water_database
+---------------------------------------------+
| key                                         |
+---------------------------------------------+
| average_temperature,location=coyote_creek   |
| average_temperature,location=santa_monica   |
| h2o_feet,location=coyote_creek              |
| h2o_feet,location=santa_monica              |
| h2o_pH,location=coyote_creek                |
| h2o_pH,location=santa_monica                |
| h2o_quality,location=coyote_creek,randtag=1 |
| h2o_quality,location=coyote_creek,randtag=2 |
| h2o_quality,location=coyote_creek,randtag=3 |
| h2o_quality,location=santa_monica,randtag=1 |
| h2o_quality,location=santa_monica,randtag=2 |
| h2o_quality,location=santa_monica,randtag=3 |
| h2o_temperature,location=coyote_creek       |
| h2o_temperature,location=santa_monica       |
+---------------------------------------------+
1 columns, 14 rows in set
```

该查询的输出类似行协议格式。第一个逗号之前的所有内容是`measurement`的名字。第一个逗号之后的所有内容都是`tag key`或者`tag value`。数据库`NOAA_water_database`有五个不同的`measurement`和14个不同的系列。

- **运行不带有`ON`子句的`SHOW SERIES`查询**

::: tabs

@tab TS-CLI

使用`USE <database_name>`指定数据库：

```bash
> USE NOAA_water_database
Elapsed: 561ns
> SHOW SERIES
+---------------------------------------------+
| key                                         |
+---------------------------------------------+
| average_temperature,location=coyote_creek   |
| average_temperature,location=santa_monica   |
| h2o_feet,location=coyote_creek              |
| h2o_feet,location=santa_monica              |
| h2o_pH,location=coyote_creek                |
| h2o_pH,location=santa_monica                |
| h2o_quality,location=coyote_creek,randtag=1 |
| h2o_quality,location=coyote_creek,randtag=2 |
| h2o_quality,location=coyote_creek,randtag=3 |
| h2o_quality,location=santa_monica,randtag=1 |
| h2o_quality,location=santa_monica,randtag=2 |
| h2o_quality,location=santa_monica,randtag=3 |
| h2o_temperature,location=coyote_creek       |
| h2o_temperature,location=santa_monica       |
+---------------------------------------------+
1 columns, 14 rows in set
```

@tab HTTP API

使用参数`db`指定数据库

```bash
> curl -G "http://localhost:8086/query?db=NOAA_water_database&pretty=true" --data-urlencode "q=SHOW SERIES"
{
	"results": [{
		"statement_id": 0,
		"series": [{
			"columns": [
				"key"
			],
			"values": [
				[
					"average_temperature,location=coyote_creek"
				],
				[
					"average_temperature,location=santa_monica"
				],
				[
					"h2o_feet,location=coyote_creek"
				],
				[
					"h2o_feet,location=santa_monica"
				],
				[
					"h2o_pH,location=coyote_creek"
				],
				[
					"h2o_pH,location=santa_monica"
				],
				[
					"h2o_quality,location=coyote_creek,randtag=1"
				],
				[
					"h2o_quality,location=coyote_creek,randtag=2"
				],
				[
					"h2o_quality,location=coyote_creek,randtag=3"
				],
				[
					"h2o_quality,location=santa_monica,randtag=1"
				],
				[
					"h2o_quality,location=santa_monica,randtag=2"
				],
				[
					"h2o_quality,location=santa_monica,randtag=3"
				],
				[
					"h2o_temperature,location=coyote_creek"
				],
				[
					"h2o_temperature,location=santa_monica"
				]
			]
		}]
	}]
}
```

:::

- **运行带有多个子句的`SHOW SERIES`查询**

```
> SHOW SERIES ON NOAA_water_database FROM "h2o_quality" WHERE "location" = 'coyote_creek' LIMIT 2
+---------------------------------------------+
| key                                         |
+---------------------------------------------+
| h2o_quality,location=coyote_creek,randtag=1 |
| h2o_quality,location=coyote_creek,randtag=2 |
+---------------------------------------------+
1 columns, 2 rows in set
```

该查询返回数据库`NOAA_water_database`中，与measurement `h2o_quality`和tag `location = coyote_creek`相关联的所有系列。`LIMIT`子句将返回的系列个数限制为2。

::: danger

`SHOW SERIES`返回表中所有时间线，对内存资源占用较大，慎用！

如需使用，一定要使用条件过滤

:::

## SHOW SERIES CARDINALITY
返回指定数据库的时间线数量，结果按时间分组

```
SHOW SERIES CARDINALITY [ON <database_name>] [FROM_clause]
```

### 示例

```sql
> SHOW SERIES CARDINALITY ON NOAA_water_database
+----------------------+----------------------+-------+
| startTime            | endTime              | count |
+----------------------+----------------------+-------+
| 2019-08-12T00:00:00Z | 2019-08-19T00:00:00Z | 14    |
+----------------------+----------------------+-------+
3 columns, 1 rows in set

+----------------------+----------------------+-------+
| startTime            | endTime              | count |
+----------------------+----------------------+-------+
| 2019-08-19T00:00:00Z | 2019-08-26T00:00:00Z | 14    |
+----------------------+----------------------+-------+
3 columns, 1 rows in set

+----------------------+----------------------+-------+
| startTime            | endTime              | count |
+----------------------+----------------------+-------+
| 2019-08-26T00:00:00Z | 2019-09-02T00:00:00Z | 14    |
+----------------------+----------------------+-------+
3 columns, 1 rows in set

+----------------------+----------------------+-------+
| startTime            | endTime              | count |
+----------------------+----------------------+-------+
| 2019-09-02T00:00:00Z | 2019-09-09T00:00:00Z | 14    |
+----------------------+----------------------+-------+
3 columns, 1 rows in set

+----------------------+----------------------+-------+
| startTime            | endTime              | count |
+----------------------+----------------------+-------+
| 2019-09-09T00:00:00Z | 2019-09-16T00:00:00Z | 14    |
+----------------------+----------------------+-------+
3 columns, 1 rows in set

+----------------------+----------------------+-------+
| startTime            | endTime              | count |
+----------------------+----------------------+-------+
| 2019-09-16T00:00:00Z | 2019-09-23T00:00:00Z | 14    |
+----------------------+----------------------+-------+
3 columns, 1 rows in set

> SHOW SERIES CARDINALITY ON NOAA_water_database FROM h2o_quality
+----------------------+----------------------+-------+
| startTime            | endTime              | count |
+----------------------+----------------------+-------+
| 2019-08-12T00:00:00Z | 2019-08-19T00:00:00Z | 6     |
+----------------------+----------------------+-------+
3 columns, 1 rows in set

+----------------------+----------------------+-------+
| startTime            | endTime              | count |
+----------------------+----------------------+-------+
| 2019-08-19T00:00:00Z | 2019-08-26T00:00:00Z | 6     |
+----------------------+----------------------+-------+
3 columns, 1 rows in set

+----------------------+----------------------+-------+
| startTime            | endTime              | count |
+----------------------+----------------------+-------+
| 2019-08-26T00:00:00Z | 2019-09-02T00:00:00Z | 6     |
+----------------------+----------------------+-------+
3 columns, 1 rows in set

+----------------------+----------------------+-------+
| startTime            | endTime              | count |
+----------------------+----------------------+-------+
| 2019-09-02T00:00:00Z | 2019-09-09T00:00:00Z | 6     |
+----------------------+----------------------+-------+
3 columns, 1 rows in set

+----------------------+----------------------+-------+
| startTime            | endTime              | count |
+----------------------+----------------------+-------+
| 2019-09-09T00:00:00Z | 2019-09-16T00:00:00Z | 6     |
+----------------------+----------------------+-------+
3 columns, 1 rows in set

+----------------------+----------------------+-------+
| startTime            | endTime              | count |
+----------------------+----------------------+-------+
| 2019-09-16T00:00:00Z | 2019-09-23T00:00:00Z | 6     |
+----------------------+----------------------+-------+
3 columns, 1 rows in set
```

## SHOW SHARDS
返回指定数据库的分片信息

### 示例

```sql
> SHOW SHARDS
name: NOAA_water_database
+----+---------------------+------------------+-------------+----------------------+----------------------+----------------------+--------+------+------------------+
| id | database            | retention_policy | shard_group | start_time           | end_time             | expiry_time          | owners | tier | downSample_level |
+----+---------------------+------------------+-------------+----------------------+----------------------+----------------------+--------+------+------------------+
| 19 | NOAA_water_database | autogen          | 19          | 2019-08-12T00:00:00Z | 2019-08-19T00:00:00Z | 2019-08-19T00:00:00Z | 2      | warm | 0                |
| 20 | NOAA_water_database | autogen          | 20          | 2019-08-19T00:00:00Z | 2019-08-26T00:00:00Z | 2019-08-26T00:00:00Z | 2      | warm | 0                |
| 21 | NOAA_water_database | autogen          | 21          | 2019-08-26T00:00:00Z | 2019-09-02T00:00:00Z | 2019-09-02T00:00:00Z | 2      | warm | 0                |
| 18 | NOAA_water_database | autogen          | 18          | 2019-09-02T00:00:00Z | 2019-09-09T00:00:00Z | 2019-09-09T00:00:00Z | 2      | warm | 0                |
| 22 | NOAA_water_database | autogen          | 22          | 2019-09-09T00:00:00Z | 2019-09-16T00:00:00Z | 2019-09-16T00:00:00Z | 2      | warm | 0                |
| 23 | NOAA_water_database | autogen          | 23          | 2019-09-16T00:00:00Z | 2019-09-23T00:00:00Z | 2019-09-23T00:00:00Z | 2      | warm | 0                |
+----+---------------------+------------------+-------------+----------------------+----------------------+----------------------+--------+------+------------------+
10 columns, 6 rows in set
```

每一条数据表示一个数据库的分片信息，每个分片包含所使用的数据保留策略、分片起始时间等。相关阅读 [数据保留策略](./retention_policy.md)

## SHOW SHARD GROUPS
返回指定数据库的分片组信息

### 示例

```sql
> SHOW SHARD GROUPS
name: shard groups
+----+---------------------+------------------+----------------------+----------------------+----------------------+
| id | database            | retention_policy | start_time           | end_time             | expiry_time          |
+----+---------------------+------------------+----------------------+----------------------+----------------------+
| 19 | NOAA_water_database | autogen          | 2019-08-12T00:00:00Z | 2019-08-19T00:00:00Z | 2019-08-19T00:00:00Z |
| 20 | NOAA_water_database | autogen          | 2019-08-19T00:00:00Z | 2019-08-26T00:00:00Z | 2019-08-26T00:00:00Z |
| 21 | NOAA_water_database | autogen          | 2019-08-26T00:00:00Z | 2019-09-02T00:00:00Z | 2019-09-02T00:00:00Z |
| 18 | NOAA_water_database | autogen          | 2019-09-02T00:00:00Z | 2019-09-09T00:00:00Z | 2019-09-09T00:00:00Z |
| 22 | NOAA_water_database | autogen          | 2019-09-09T00:00:00Z | 2019-09-16T00:00:00Z | 2019-09-16T00:00:00Z |
| 23 | NOAA_water_database | autogen          | 2019-09-16T00:00:00Z | 2019-09-23T00:00:00Z | 2019-09-23T00:00:00Z |
+----+---------------------+------------------+----------------------+----------------------+----------------------+
6 columns, 6 rows in set
```

这里有6个分片组，结合`SHOW SHARDS`命令可以看出，每个分片组包含一个分片(SHARD)。openGemini单机默认初始化一个分片(SHARD)，如果是三节点的集群，则初始化为三个分片（每个节点一个）。当一个SHARD GROUP到期后，系统会创建新的SHARD GROUP，并分配新的分片 (SHARD)。相关阅读[SHARD GROUP DURATION](./retention_policy.md#shard-duration)
