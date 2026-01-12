---
title: InfluxQL与标准SQL的对比
order: 11
---

本文档主要介绍InfluxQL和标准SQL的语法对比，有两个目的：

1. 了解openGemini与标准SQL的区别
2. 指导后续新增语法/函数

本文从7个方面来对比InfluxQL和标准SQL
* [Data Model and Terminology](#Data-Model-and-Terminology)
* [Time-Series Focus](#Time-Series-Focus)
* [Joins](#Joins)
* [DML](#Data-Modification--DML-)
* [Functions](#Functions)
* [Schema Exploration](#Schema-Exploration)
* [Syntax](#Syntax)

## Data Model and Terminology

### 标准SQL：

- 数据被组织成具有行（Row）和列（Column）的表格（Table）。
- 主键（Primary Key）和外键（Foreign Key）定义了表之间的关系。
- Schema是预定义的，允许增加、修改、删除列（通过ALTER TABLE等语句）
- 常用术语：Database, Table, Row, Column，Primary Key，Foreign Key，Index，View，Store Procedure，Trigger，Transaction

### InfluxQL:

- 数据被组织成Measurement（类似Table），存储到Measurement的数据被称之为Point，每个数据点（Point，类似数据行）都有一个时间戳。
- 数据点固定分为4部分，分别是表名，标签（Tags）（数据元数据，如设备名），字段（Fields）（实际值，如温度、CPU使用率），时间
- Schemaless，自动根据写入数据增加列，不支持按列删除和修改列数据类型
- 常用术语：Database, Measurement, Point, Field, Tag, Series, Retention Policy

### 举例

```sql
# 标准SQL创建表
CREATE TABLE sensor_data (
    id INT PRIMARY KEY,
    sensor_id VARCHAR(50),
    location VARCHAR(50),
    temperature FLOAT,
    humidity FLOAT,
    timestamp DATETIME,
    FOREIGN KEY (sensor_id) REFERENCES tab (id)
);
INSERT INTO sensor_data VALUES (1, 's001', 'U13', 33, 12, 1750647476)

# InfluxQL
# 通常数据写入时自动创建表，无需提前定义表结构
INSERT sensor_data,sensor_id='s001',location='U13' temperature=33,humidity=12 1750647476

# 如下情况，也可以手动创建表（openGemini扩展语法）
# 1. 指定分区键
CREATE MEASUREMENT sensor_data WITH SHARDKEY sensor_id, location
# 2. 创建文本索引
CREATE MEASUREMENT sensor_data WITH INDEXTYPE text INDEXLIST description, error_message SHARDKEY location
```

## Time-Series Focus

### 标准SQL

虽然现代SQL数据库可以很好地处理时间序列数据，但时间被当做普通的一列，基于时间的聚合或窗口函数通常是标准的，并不总是像 InfluxQL 那样有针对性的进行优化。

### InfluxQL

现代时序数据库openGemini是专门针对时序业务（异常检测、数据统计等）的典型查询场景进行专门优化的垂直领域数据库。
典型查询场景都有：

1. 单设备+时间范围查询
2. 单设备+时间范围查询+聚合计算
3. 多设备+时间范围+分组查询
4. 多设备+时间范围+分组+聚合计算

这些查询场景有一个特点，就是查询都带有时间范围。所以openGemini的数据格式（数据按时间排序）、数据分片（数据按时间范围分片）等关键功能设计都充分考虑时间这个属性。时间相关的聚合或窗口函数（比如计算平均值、最大最小值、求和等），会根据时间范围，快速检索到目标数据，在openGemini中，这些函数都做了优化（值是预先计算好存储在数据文件中）。语法上，InfluxQL更加简洁，更易懂。

**流式计算**：
流式计算，顾名思义，就是对数据流进行实时处理，openGemini内核实现了一个轻量级的流式计算框架，可以对写入数据进行实时数据聚合，这是SQL数据库不存在的特性。在流式计算中，首先会计算一个滑动的时间窗，只有落入这个时间窗内的数据才会被计算。


### 举例

查询2025年1月1日到3日期间每小时的平均温度

```sql
# 标准SQL写法
SELECT
  DATE_TRUNC('hour', timestamp) AS hour_of_day,
  AVG(temperature) AS average_temperature
FROM temperature_data
WHERE timestamp >= '2025-01-01 00:00:00' AND timestamp < '2025-01-03 00:00:00'
GROUP BY hour_of_day
ORDER BY hour_of_day

# InfluxQL写法
SELECT
	MEAN(temperature) AS average_temperature
FROM temperature_data
WHERE time >= '2025-01-01 00:00:00' AND time < '2025-01-03 00:00:00'
GROUP BY time(1h)
```

## Joins

### 标准SQL

支持各种类型的 JOIN 操作（INNER、LEFT、RIGHT、FULL、CROSS），这是关系数据库的基石，根据公共列将多个表中的数据组合在一起。

### InfluxQL

openGemini兼容InfluxQL 1.x并且做了一些SQL扩展，[支持Join能力](https://clouddevops.huawei.com/domains/47159/wiki/3/WIKI2025051301671)（INNER, FULL, LEFT [OUTER] JION, RIGHT [OUTER] JION).
与标准SQL JOIN不同的是:

1. 标准SQL的JOIN会产生2个时间列，在openGemini里面只有一个时间列（如果两个表时间不同，JOIN后会产生2条数据，其他字段都相同，仅时间不同）
2. JOIN ON的条件字段，目前必须要在SELECT 或者 GROUP BY中出现。比如 SELECT t1.a, t1.b, t.c FROM t1 FULL JOIN t2 ON t1.d=t2.d 则不能执行。
3. JOIN ON的条件，如果是Field字段，当前openGemini只支持String类型。
4. openGemini暂不支持多表JOIN（也就是大于2个表的JOIN）。

### 举例

```
#标准SQL语法
SELECT_clause
FROM
	<left_join_items> [INNER | LEFT [OUTER] | RIGHT [OUTER] | FULL [OUTER]] JOIN <right_join_items>
ON <join_condition>
[WHERE_clause]
[GROUP_BY_clause]
[HAVING_clause]
[ORDER_BY_clause]

# 示例
SELECT
    sd.timestamp,
    sd.temperature,
    sm.manufacturer,
    sm.model
FROM
    sensor_data sd
JOIN
    sensor_metadata sm ON sd.sensor_id = sm.sensor_id
WHERE
    sd.time > now() - INTERVAL '1 day';

# InfluxQL（openGemini）
SELECT
    sd.sensor_id,
    sd.timestamp,
    sd.temperature,
    sm.manufacturer,
    sm.model
FROM
	(SELECT *  FROM sensor_data) as sd
FULL JOIN
	(SELECT * FROM sensor_metadata) as sm ON (sd.sensor_id = sm.sensor_id)
WHERE
    sd.time > now() - 1d;
# 其他
SELECT * FROM table1 as t1 INNER JOIN table2 as t2 ON t1.tk=t2.tk GROUP BY tk
SELECT * FROM table1 LEFT OUTER JOIN table2 ON table1.tk=table2.tk GROUP BY tk
SELECT * FROM table1 RIGHT OUTER JOIN table2 ON table1.tk=table2.tk GROUP BY tk
```

## Data Modification (DML)

### 标准SQL

DML包括：增（INSERT）删（DELETE）改（UPDATE）查（SELECT）以及EXPLAIN ANALYZE和TRUNCATE

### InfluxQL

InfluxQL包括数据写入（INSERT）和查询（SELECT）和EXPLAIN ANALYZE，不支持删除、修改和TRUNCATE。
INSERT语法和标准SQL语法不同，因为InfluxQL写入采用的自有数据协议 `InfluxDB Line Protocol`

### 举例

```
# 标准SQL
1. INSERT INTO table_name VALUES (value1,value2,value3,...)
2. DELETE FROM table_name WHERE condition
3. UPDATE table_name SET column1 = value1, column2 = value2, ... WHERE condition
4. SELECT 字段列表，[函数] FROM 表|嵌套子查询 【WHERE 条件】 【ORDER BY 排序字段 ASC|DESC】【GROUP BY 分组字段列表】 【HAVING 筛选分组条件】【LIMIT 返回数据量】
5. EXPLAIN ANALYZE SELECT STATEMENT

# InfluxQL
1. INSERT table_name, tag_key1=tag_value1,tag_key2=tag_value2 field_key1=value1, field_key2=value2 [timestamp]
2. SELECT语法基本上与标准SQL一致，Having子句除外（InfluxQL不支持）。在使用上仍然存在一些小的差异：
    - SELECT子句中必须带有至少一个field字段，不能只带tag查询。
    - SELECT子句中即便没有查询time字段，也会扫描返回time字段，所有字段按照time对齐。
    - SELECT子句中Selector函数可以带多个字段，非selector函数待字段，查询报错。
3. EXPLAIN ANALYZE SELECT STATEMENT （标准SQL用法一致）
4. 数据删除在开发计划中（兼容InfluxDB用法，这里也存在与标准SQL语法差异）
```

## Clauses
与标准SQl相比，InfluxQL的如下子句存在差异：
- FROM子句中支持多表查询，默认UNION ALL BY NAME语义，而非JOIN语义。
- GROUP_BY子句只支持对tag字段分组，而无法对field分组。
- ORDER_BY子句只支持对time排序，而不支持对其他字段排序。
- LIMIT子句默认按照分组与时间排序来返回数据，相当于SQL ORDER BY XX LIMIT XXX。
- 不支持SLIMT

## Functions

### 标准SQL

具有大量内置函数（字符串、数值、日期/时间、聚合、窗口函数、JSON 函数等），并允许用户定义函数（UDF）、存储过程、触发器和自定义运算符。

### InfluxQL

可以把InfluxQL的函数看着是标准SQL的子集，缺少许多常见的字符串函数，`InfluxQL主要关注时序业务场景中需要用到的函数`。比如

1. 聚合函数：MEAN(), SUM(), MIN(), MAX(), COUNT(), MEDIAN(),DISTINCT()等
2. 选择器函数：`FIRST(), LAST(), TOP(), BOTTOM(), PERCENTILE(), SAMPLE()`等, 绝大部分标准SQL中都不存在，是InfluxDB根据业务场景新增的时序数据库特有的函数。
3. 转换函数：ABS(), ACOS(), EXP(),FLOOR(), LN(), LOG()等，多为数学函数。`这些函数虽然与标准SQL函数相同，但输入参数存在差异，函数输入必须是字段名，不能传递常量单独使用`
4. 数据预测函数：HOLT_WINTERS() `InfluxDB新增，标准SQL不存在`

openGemini在此基础上扩展了一部分函数

- STR() 字符串中是否包含指定字符 `标准SQL中，instr()负责相同功能`
- STRLEN() 计算字符串长度 `标准SQL中，lenght()负责相同功能`
- SUBSTR() 取子字符串 `与标准SQL函数一致`
- CASTOR() 异常检测函数 `新增，标准SQL不存在`
- REGR\_SLOPE() 计算线性回归斜率，是扩展了标准的函数，跟它同名但是函数参数不一样，`新增，标准SQL不存在`
- AD\_RMSE\_EXT 异常检测，扩展RMSE算法。`新增，标准SQL不存在`
- COMPARE() 同环比。`新增，标准SQL不存在`

### 举例

```
# 标准SQL
SELECT
    timestamp,
    temperature,
    AVG(temperature) OVER (ORDER BY timestamp ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS moving_avg
FROM
    sensor_data
WHERE
    timestamp >= now() - INTERVAL '1 day'

# InfluxQL
SELECT
    MOVING_AVERAGE(temperature, 3)
FROM
    sensor_data
WHERE
    time >= now() - 1d
```

## Schema Exploration

这部分主要描述标准SQL和InfluxQL是如何查询表结构的。

### 标准SQL

标准SQL没有定义专门的查询表结构的SQL语句，规定数据库系统要支持标准的INFORMATION_SCHEMA以及对应的TABLES等系统表（或系统视图）。用户可以采用标准的SELECT语句来查询这些系统表，获取表结构等信息。MySQL等数据库有专门的SHOW DATABASES、SHOW TABLES等语句来查询库、表信息。

查看表：SHOW tables
查看表结构（有哪些字段/每个字段什么类型）：DESCRIBE TABLE <table\_name>

### InfluxQL

查看DB: SHOW DATABASES
查看表：SHOW MEASUREMENTS
查看表结构：SHOW TAG KEYS, SHOW FIELD KEYS

openGemini 在此基础上扩展了一条命令：`show measurements detail with measurement = cpu`, 整合了show tag keys和show filed keys, 以及索引、分区键等信息

## Syntax

### 标识符

SQL中定义了很多保留标识符（语法关键字），例如CREATE、SELECT等SQL语法的关键字，openGemini也不例外，不过二者的保留标识符不完全一致，openGemini在原来InfluxQL基础上扩展很多关键字。
| 类型     | Keywords                                                     |
| -------- | ------------------------------------------------------------ |
| InfluxQL | `ALL`, `ALTER`, `ANY`, `AS`, `ASC`, `AND`, `BEGIN`, `BY`,  `BOOLEAN`, `CREATE`, `CONTINUOUS`, `DATABASE`, `DATABASES`, `DEFAULT`, `DELETE`, `DESC`, `DESTINATIONS`, `DIAGNOSTICS`, `DISTINCT`, `DROP`, `DURATION`, `DEFAULT`, `END`, `EVERY`, `EXPLAIN`, `FIELD`, `FLOAT`,  `FOR`, `FROM`, `FILL`, `GRANT`, `GRANTS`, `GROUP`, `GROUPS`, `IN`, `INF`, `INSERT`, `INTO`,  `INT`, `INTEGER`, `KEY`, `KEYS`, `KILL`, `LIMIT`, `SHOW`, **`MEASUREMENT`**, **`MEASUREMENTS`**, `NAME`, `OFFSET`, `ON`, `OR`, `ORDER`, `PASSWORD`, **`POLICY`**, **`POLICIES`**, **`PRIVILEGES`**, `QUERIES`, `QUERY`, `READ`, `REPLICATION`, `RESAMPLE`, `RETENTION`, `REVOKE`, `SELECT`,**`SERIES`**, `SET`, **`SHARD`**, **`SHARDS`**, `SLIMIT`, `SOFFSET`, `STATS`, **`SUBSCRIPTION`** , `SUBSCRIPTIONS`, **`TAG`**, `TO`, `USER`, `USERS`, `VALUES`, `WHERE`, `WITH`, `WRITE`,`TIME`<br>openGemini扩展：`STREAM`, `STREAMS`, `DELAY`, `HISTOGRAM`, `TTl`, `RETURN`, `EDGE`, `NODE`, `GRAH`, `JOIN`, `LEFT`, `RIGHT`, `INNER`, `FULL`, `OUTER`,`UNION`, `INDEXES`, `IDNEXLIST`, `INDEXTYPE`,`DOWNSAMPLE`, `DOWNSAMPLES`, `DETAIL`, `SHARDMERGE`, `EXCEPT`, `SHARDKEY`,`SORTKEY`, `PRIMARYKEY`, `MATCH`, `MATCHPHRASE`, `IPINRANGE`, `CLUSTER`, `CARDINALITY`, `ENGINETYPE`, `COLUMNSTORE`, `TSSTORE`, `SNAPSHOT`, `HOT`, `WARM`, `ATTRIBUTE`, `REPLICAS`, `DETAIL`, `COMPACT`, `AUTO`, `COMPACT`, `HINT` |
| 标准SQL  | `ADD`, `ALL`, `ALTER`, `ANY`, `AS`, `ASC`, `AND`, `BEGIN`, `BETWEEN`, `BIGINT`, `BLOB`, `BOOLEAN`, `CASE`, `CHAR`, `CHECK`, `COMMIT`, `CONSTRAINT`, `CREATE`, `DATABASE`, `DATE`, `DATETIME`, `DECIMAL`, `DEFAULT`, `DELETE`, `DESC`, `DESCRIBE`, `DISTINCT`, `DOUBLE`, `DROP`, `ELSE`, `END`, `EXISTS`, `EXPLAIN`, `FLOAT`, `FOREIGNKEY`, `FROM`, `FULL`, `FUNCTION`, `GRANT`, `GROUPBY`, `HAVING`, `IN`, `INDEX`, `INNER`, `INSERT`, `INT`, `INTEGER`, `INTERNAL`, `INTO`, `IS`, `IS NULL`, `JOIN`, `LEFT`, `LIKE`, `LIMIT`, `LIMIT`, `NOT`, `NOT NULL`, `NUMERIC`, `OFFSET`, `ON`, `OR`, `ORDER BY`, `PRIMARYKEY`, `PROCEDURE`, `REAL`, `REFERENCES`, `REVOKE`, `RIGHT`, `ROLLBACK`, `SCHEMA`, `SELECT`, `SET`, `SMALLINT`, `SOME`, `TABLE`, `TEXT`, `THEN`, `TIME`, `TIMESTAMP`, `TRIGGER`, `TRUNCATE`, `UNION`, `UNIQUE`, `UPDATE`, `USE`, `VALUES`, `VARCHAR`, `VIEW`, `WHEN`, `WHERE`, `WITH` |

[InfluxQL Keywords 参考](https://docs.influxdata.com/influxdb/v1/query_language/spec/#keywords)

用户创建库、表等数据库对象时采用的库名、表名也属于标识符，它们的命名需要满足特定的规则。
在SQL中可以用英文双引号括起来，例如："Databases of Databases"；如果名字中带有英文双引号，则需要用它自己转义（也就是写两遍），例如："Book ""SQL3 Guide""'s Chapters".
SQL 库名和表名通常不能包含空格和特殊字符，且通常不能以数字开头。具体的限制可能因数据库系统而异，但通常遵循以下规则：
SQL一般限制：
- 空格:不能包含空格。
- 特殊字符:不能包含除下划线(_) 之外的特殊字符，如! @ # $ % ^ & * ( ) + = { } | \ : " ; ' < > , . ? / 等。
- 数字:不能以数字开头。
- 保留字:不能使用数据库的保留字，例如SELECT, FROM, WHERE 等。

在InfluxQL中，库、表、TAG、FIELD、保留策略（Retention Policy）等名称：
- 如果标识符中不包含特殊字符、空格和内核关键字，可以不需要双引号括起来，如果有用到，则需要用双引号括起来，例如 "Databases&", "Database s"；
- 如果名字中带有英文双引号，则需要用'\'转义, 例如 "Data\"base"；
- 不带引号的标识符必须以大写或小写或"_"下划线开头；
- 不带引号的标识符只能包含ASCII字母、十进制数字和下划线"_"。
- `,:;./\`这些字符不能用到库名中，即使双引号括起来也不允许。
- `,;/\`这些字符不能用到表名中，即使双引号括起来也不允许。

### 字符串常量

标准SQL：SQL中定义的字符串常量可以用英文单引号括起来，例如：'A tale of two cities'；如果字符串常量中带有英文单引号，则需要用它自己转义（也就是写两遍），例如：'Xi''an, Shaanxi'。
InfluxQL：InfluxQL的字符串常量同样用英文单引号括起来，例如：'A tale of two cities'; 不同的是，如果字符串常量中带英文单引号，转义用斜线 `\'`,而不是用它自己, 例如：'Xi\'an, Shaanxi'。

### 日期时间常量

**标准SQL**

使用标准的日期/时间函数和间隔语法，比如
日期格式：`'2025-06-10 09:00:00'`
时间间隔：间隔5分钟 `INTERVAL 5 minute`, 间隔5天 `INTERVAL 5 days`
时间常量：`current_time(), current_date(), current_timestamp, now()`

**InfluxQL**

日期格式：rfc3339格式`2015-09-18T21:24:00Z`或者类rfc3339格式 `2025-06-10 09:00:00`
时间间隔：1小时时间范围`time(1h)`，1天 `1d`, 1秒 `1s`，1周 `1w`, 相比标准SQL要简洁一些
时间常量：now()

**举例**

```
# 标准SQL
SELECT
    *
FROM
    sensor_data
WHERE
    timestamp >= now() - INTERVAL '1 hour'

# InfluxQL
# 在InfluxQL中，time是系统内置关键字，时间列名称固定为time，因此遇到time的地方，特指时间列。
SELECT
   *
FROM
   sensor_data
WHERE
   time >= now() - 1h
```
