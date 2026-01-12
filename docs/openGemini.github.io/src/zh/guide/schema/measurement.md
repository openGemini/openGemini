---
title: 表操作
order: 2
---

## CREATE MEASUREMENT(创建表)

openGemini在写数据时支持自动创建表，但如下三种情况，需要提前创建表

### 指定分区键

openGemini中数据默认按照时间线进行hash分区打散，但某些场景下，业务频繁使用某个或者某几个TAG进行数据检索，采用时间线hash分区的方式让这部分TAG的数据分散到了不同的节点，造成查询扇出度比较大。
如果可以按照这部分频繁使用的TAG对数据进行分区，这样相同TAG值的数据会集中存储在同一个节点之上，从而减少查询扇出度，提升数据检索效率。

- **指定一个TAG（如location）对数据进行打散**
```sql
CREATE MEASUREMENT mst WITH SHARDKEY location
```
- **指定多个TAG（如location，region）作为SHARDKEY**
```sql
CREATE MEASUREMENT mst WITH SHARDKEY location, region
```

### 文本检索

文本检索指根据文本内容，如关键字、短语等对文本集合进行检索、过滤等。openGemini支持文本检索，比如对日志进行关键字检索，可返回包含关键字的所有日志数据。
当你在使用该功能时，需要预先创建表，创建表的目的其实是为了指定要在哪些Field字段上创建全文索引，但有个前提，这些Field字段必须是String数据类型。

创建名为mst的表，并指定在description和error_message两个字段上创建全文索引。

```sql
> CREATE MEASUREMENT mst WITH INDEXTYPE text INDEXLIST description, error_message
```
创建名为mst的表，并指定在description和error_message两个字段上创建全文索引, 同时设置mst根据location对数据进行分区打散

```sql
> CREATE MEASUREMENT mst WITH INDEXTYPE text INDEXLIST description, error_message SHARDKEY location
```
::: tip

仅会在INDEXLIST指定的字段descriptio和error_message创建全文索引，若在其他Field中检索关键字，可能会比较慢

在字段descriptio和error_message支持精确匹配，短语匹配和模糊匹配三种，相关语法示例参考[文本检索](../features/logs.md)

不建议在TAG上创建文本索引，可能出现不可预见的问题

:::

### 使用高基数存储引擎

基于高基数存储引擎的其他读写操作，参考[文档](../features/high_series_cardinality.md)

#### 语法

```
CREATE MEASUREMENT $mst_name ($columnlists)
WITH ENGINETYPE = COLUMNSTORE
[SHARDKEY $shardkeylist]
[TYPE HASH|RANGE]
[PRIMARYKEY $primarykeylist]
[SORTKEY $sortkeylist]
[PROPERTY $key="$value",...]
```

关键字说明如下(上述出现的大写单词均为关键字，使用时不区分大小写) ：

- **$mst_name**为创建的表名，实际使用时用具体名称替换。不支持包含 `,:;/\` 等特殊字符，如需包含其他特殊字符，需要使用双引号包含 mst_name

  ```sql
  CREATE MEASUREMENT ":mst0" (tag1 TAG, field1 INT64 FIELD, field2 BOOL, field3 STRING, field4 FLOAT64)
  ```



- **$columnlists**用于定义 Schema，包含在括号内，需要显式指定，暂不支持对 Schema 进行变更

  ```
  (tag1 TAG, field1 INT64 FIELD, field2 BOOL, field3 STRING, field4 FLOAT64)
  ```

  columnlists 指定了每一列的列名、数据类型以及列属性，其中

  - TAG不需要指定数据类型，默认为 `STRING`
  - 列属性可选值为 `TAG` 或者 `FIELD`，未指定列属性时，默认为 `FIELD`
  - 数据类型仅支持 `FLOAT64`, `INT64`, `BOOL`, `STRING`
  - 默认带 `time` 列，不需要包含在 columnlists 中

- **ENGINETYPE**关键字必须显示指定存储引擎类型为COLUMNSTORE（表示使用高基数引擎）

- **SHARDKEY**关键字指定存储引擎按给定的一个或多个字段进行数据分区打散，默认按全部TAG KEYS进行分区打散

- **TYPE**关键字表示打散方式，分为HASH和RANGE两种。默认为HASH

- **PRIMARYKEY**关键字指定索引列，可以是一个或者多个字段，意味着存储引擎会在这些字段之上创建索引。

- **SORTKEY**指定存储引擎内部的数据排序方式，time表示按时间排序，也可以换为其他的字段。

- **PROPERTY** 指定额外的属性

| 属性名称   | 类型     | 默认值    | 描述                |
|--------|--------|--------|-------------------|
| unique | bool   | `false`  | 是否将主键+排序键+时间作为唯一键 |
| primaryKeyType | string | `normal` | 指定主键类型，可选 `cluster` |

- **unique** 不是一个强约束，仅在数据刷盘，数据合并时，根据`主键+排序键+时间`进行去重，文件之间不会去重，只做文件内去重， 在最终 full compact 后达成最终唯一

PRIMARYKEY和SORTKEY二者关系是，**`PRIMARYKEY` 需为 `SORTKEY` 的左前缀，否则报错，**如果只配置了其中一个，则二者保持一致。

举个例子帮助大家更好理解PRIMARYKEY和SORTKEY。假如一个应用需要监控某服务的出入流量，该服务的属性包括REGION、AZ、POD、SERVICE_ID，若要观测某个服务的出入流量，查询条件应该定位到具体云服务`REGION=xxx,AZ=xxx,POD=xxx,SERVICE_ID=xxx`. 为了加速查询效率，通常需要创建索引。传统倒排索引在高基数场景近似稠密索引，索引开销较大，同时对于数据过滤几乎没有效果。高基数存储引擎放弃传统倒排索引的思路，创建了稀疏索引，前提条件是数据在底层需要是已排好序的。因此，**创建索引需要先给数据排序。**具体做法是把SORTKEY设为REGION，AZ，POD，SERVICE_ID，使得内部数据按REGION、AZ、POD、SERVICE_ID的层级排好序，这时就可以在已排好序的数据的基础上创建PRIMARYKEY指定的数据稀疏索引了。

#### 示例

```sql
> CREATE DATABASE testdb
> USE testdb
> CREATE MEASUREMENT rtt (deviceIp STRING, deviceName STRING, campus STRING, rtt INT64) WITH ENGINETYPE = COLUMNSTORE SHARDKEY deviceIp PRIMARYKEY deviceIp,campus SORTKEY deviceIp,campus,time
> SHOW SCHEMA FROM rtt
shard_key  type ShardGroup engine_type primary_key       sort_key
---------  ---- ---------- ----------- -----------       --------
[deviceIp] hash 1          columnstore [deviceIp campus] [deviceIp campus time]
```

创建表名为rtt的数据表，该表使用高基数存储引擎，表结构包含4个字段，分别是deviceIp（string 类型），deviceName（string类型），campus（string类型）和rtt（整型），数据按照deviceIp分区打散，数据按deviceIp，campus，time层级排序，并在deviceIp字段上创建索引。

::: tip

openGemini高基数存储引擎具备非常高读写性能，我们欢迎感兴趣的开发者参与进来，一起完善功能。

:::


## SHOW MEASUREMENTS(查看表)

返回指定数据库的measurement。

### 语法

```sql
SHOW MEASUREMENTS [ON <database_name>] [WITH MEASUREMENT <operator> ['<measurement_name>' | <regular_expression>]]
```
`ON <database_name>`是可选项。如果查询中没有包含`ON <database_name>`，您必须在CLI中使用`USE <database_name>`指定数据库，或者在openGemini API请求中使用参数`db`指定数据库。

`WITH`子句，`WHERE`子句，`LIMIT`子句和`OFFSET`子句是可选的。`WHERE`子句支持tag比较；在`SHOW MEASUREMENTS`查询中，field比较是无效的。

`WHERE`子句中支持的操作符：

| 操作符 | 含义   |
| ------ | ------ |
| `=`    | 等于   |
| `<>`   | 不等于 |
| `!=`   | 不等于 |
| `=~`   | 匹配   |
| `!~`   | 不匹配 |

请查阅DML章节获得关于[`FROM`子句](../query_data/select.md#select)、[`LIMIT、OFFSET`子句](../query_data/select.md#limit-offset)、和正则表达式的介绍。

### 示例

- **运行带有`ON`子句的`SHOW MEASUREMENTS`查询**

```sql
> SHOW MEASUREMENTS ON NOAA_water_database
name: measurements
+---------------------+
| name                |
+---------------------+
| average_temperature |
| h2o_feet            |
| h2o_pH              |
| h2o_quality         |
| h2o_temperature     |
+---------------------+
1 columns, 5 rows in set
```

该查询返回数据库`NOAA_water_database`中的measurement。数据库`NOAA_water_database`有五个measurement：`average_temperature`、`h2o_feet`、`h2o_pH`、`h2o_quality`和`h2o_temperature`。

- **运行不带有`ON`子句的`SHOW MEASUREMENTS`查询**

::: tabs

@tab CLI

使用`USE <database_name>`指定数据库：

```bash
> USE NOAA_water_database
Elapsed: 781ns
> SHOW MEASUREMENTS
name: measurements
+---------------------+
| name                |
+---------------------+
| average_temperature |
| h2o_feet            |
| h2o_pH              |
| h2o_quality         |
| h2o_temperature     |
+---------------------+
1 columns, 5 rows in set
```

@tab API

使用参数`db`指定数据库

```bash
> curl -G "http://localhost:8086/query?db=NOAA_water_database&pretty=true" --data-urlencode "q=SHOW MEASUREMENTS"
{
    "results": [
        {
            "statement_id": 0,
            "series": [
                {
                    "name": "measurements",
                    "columns": [
                        "name"
                    ],
                    "values": [
                        [
                            "average_temperature"
                        ],
                        [
                            "h2o_feet"
                        ],
                        [
                            "h2o_pH"
                        ],
                        [
                            "h2o_quality"
                        ],
                        [
                            "h2o_temperature"
                        ]
                    ]
                }
            ]
        }
    ]
}
```

:::

- **运行带有多个子句的`SHOW MEASUREMENTS`查询**

```sql
> SHOW MEASUREMENTS ON NOAA_water_database WITH MEASUREMENT =~ /h2o.*/
name: measurements
+-----------------+
| name            |
+-----------------+
| h2o_feet        |
| h2o_pH          |
| h2o_quality     |
| h2o_temperature |
+-----------------+
1 columns, 4 rows in set
```

该查询返回数据库`NOAA_water_database`中名字以`h2o`开头的measurement。

- **查看表数量**
```sql
> SHOW MEASUREMENTS CARDINALITY
TODO

> SHOW MEASUREMENTS CARDINALITY ON NOAA_water_database
TODO
```

## DROP MEASUREMENT(删除表)

使用`DROP MEASUREMENT`删除measurement

`DROP MEASUREMENT`从指定的measurement中删除所有数据和series，并删除measurement。

### 语法

```sql
DROP MEASUREMENT <measurement_name>
```

**<font size=5 color=green>示例</font>**

---

删除名称为`h2o_feet`的measurement

```sql
> DROP MEASUREMENT "h2o_feet"
```

::: warning

1. `DROP MEASUREMENT`会删除measurement中的所有数据点和series。
2. 成功执行`DROP MEASUREMENT`不返回任何结果

:::
