---
title: 连续查询
order: 6
---

## CREATE CONTINUOUS QUERY

### 基本语法

```sql
CREATE CONTINUOUS QUERY <cq_name> ON <database_name>
BEGIN
  <cq_query>
END
```

### 基本语法描述

`cq_name`连续查询的名称。

`database_name`连续查询所在数据库的名称

`cq_query`连续查询中的查询语句。需要一个函数，一个`INTO`子句和一个`GROUP BY time() `子句：

```sql
SELECT <function[s]> INTO <destination_measurement> FROM <measurement> [WHERE <stuff>] GROUP BY time(<interval>)[,<tag_key[s]>]
```

::: warning

在`WHERE`子句中，`cq_query`不需要时间范围。 openGemini在执行CQ时自动生成`cq_query`的时间范围。`cq_query`的`WHERE`子句中的任何用户指定的时间范围将被系统忽略。

:::

#### 运行时间点以及覆盖的时间范围

CQ对实时数据进行操作。他们使用本地服务器的时间戳，`GROUP BY time()`间隔和openGemini的预设时间边界来确定何时执行以及查询中涵盖的时间范围。

CQs以与`cq_query`的`GROUP BY time()`间隔相同的间隔执行，并且它们在openGemini的预设时间边界开始时运行。如果`GROUP BY time()`间隔为1小时，则CQ每小时开始执行一次。

当CQ执行时，它对于`now()`和`now()`减去`GROUP BY time()`间隔的时间范围运行单个查询。 如果`GROUP BY time()`间隔为1小时，当前时间为17:00，查询的时间范围为16:00至16:59999999999。

### 基本语法示例

以下示例使用数据库`transportation`中的示例数据。measurement `bus_data`数据存储有关公共汽车乘客数量和投诉数量的15分钟数据：

```sql
name: bus_data
--------------
time                   passengers   complaints
2016-08-28T07:00:00Z   5            9
2016-08-28T07:15:00Z   8            9
2016-08-28T07:30:00Z   8            9
2016-08-28T07:45:00Z   7            9
2016-08-28T08:00:00Z   8            9
2016-08-28T08:15:00Z   15           7
2016-08-28T08:30:00Z   15           7
2016-08-28T08:45:00Z   17           7
2016-08-28T09:00:00Z   20           7
```

#### 自动采样数据

使用简单的CQ自动从单个字段中降采样数据，并将结果写入同一数据库中的另一个measurement。

```sql
CREATE CONTINUOUS QUERY "cq_basic" ON "transportation"
BEGIN
  SELECT mean("passengers") INTO "average_passengers" FROM "bus_data" GROUP BY time(1h)
END
```

`cq_basic`从`bus_data`中计算乘客的平均小时数，并将结果存储在数据库`transportation`中的`average_passengers`中。

`cq_basic`以一小时的间隔执行，与`GROUP BY time()`间隔相同的间隔。 每个小时，`cq_basic`运行一个单一的查询，覆盖了`now()`和`now()`减去`GROUP BY time()`间隔之间的时间范围，即`now()`和`now()`之前的一个小时之间的时间范围。

下面是2016年8月28日上午的日志输出：

```sql
>
在8点时，cq_basic执行时间范围为time >= '7:00' AND time <'08：00'的查询。
cq_basic向average_passengers写入一个点：
>
    name: average_passengers
    ------------------------
    time                   mean
    2016-08-28T07:00:00Z   7
>
在9点时，cq_basic执行时间范围为time >= '8:00' AND time <'09：00'的查询。
cq_basic向average_passengers写入一个点：
>
    name: average_passengers
    ------------------------
    time                   mean
    2016-08-28T08:00:00Z   13.75
```

结果如下：

```sql
> SELECT * FROM "average_passengers"
name: average_passengers
------------------------
time                   mean
2016-08-28T07:00:00Z   7
2016-08-28T08:00:00Z   13.75
```

#### 自动采样数据到另一个保留策略里

从默认的的保留策略里面采样数据到完全指定的目标measurement中：

```sql
CREATE CONTINUOUS QUERY "cq_basic_rp" ON "transportation"
BEGIN
  SELECT mean("passengers") INTO "transportation"."three_weeks"."average_passengers" FROM "bus_data" GROUP BY time(1h)
END
```

`cq_basic_rp`从`bus_data`中计算乘客的平均小时数，并将结果存储在数据库`tansportation`的RP为`three_weeks`的measurement`average_passengers`中。

`cq_basic_rp`以一小时的间隔执行，与`GROUP BY time()`间隔相同的间隔。每个小时，`cq_basic_rp`运行一个单一的查询，覆盖了`now()`和`now()`减去`GROUP BY time()`间隔之间的时间段，即`now()`和`now()`之前的一个小时之间的时间范围。

下面是2016年8月28日上午的日志输出：

```sql
>
在8:00cq_basic_rp执行时间范围为time >='7:00' AND time <'8:00'的查询。
cq_basic_rp向RP为three_weeks的measurement average_passengers写入一个点：
>
    name: average_passengers
    ------------------------
    time                   mean
    2016-08-28T07:00:00Z   7
>
在9:00cq_basic_rp执行时间范围为time >='8:00' AND time <'9:00'的查询。
cq_basic_rp向RP为three_weeks的measurementaverage_passengers写入一个点：
>
    name: average_passengers
    ------------------------
    time                   mean
    2016-08-28T08:00:00Z   13.75
```

结果如下：

```sql
> SELECT * FROM "transportation"."three_weeks"."average_passengers"
name: average_passengers
------------------------
time                   mean
2016-08-28T07:00:00Z   7
2016-08-28T08:00:00Z   13.75
```

`cq_basic_rp`使用CQ和保留策略自动降低样本数据，并将这些采样数据保留在不同的时间长度上。

#### 自动采样数据并配置CQ的时间边界

使用`GROUP BY time()`子句的偏移间隔来改变CQ的默认执行时间和呈现的时间边界：

```sql
CREATE CONTINUOUS QUERY "cq_basic_offset" ON "transportation"
BEGIN
  SELECT mean("passengers") INTO "average_passengers" FROM "bus_data" GROUP BY time(1h,15m)
END
```

`cq_basic_offset`从`bus_data`中计算乘客的平均小时数，并将结果存储在`average_passengers`中。

`cq_basic_offset`以一小时的间隔执行，与`GROUP BY time()`间隔相同的间隔。15分钟偏移间隔迫使CQ在默认执行时间后15分钟执行; `cq_basic_offset`在8:15而不是8:00执行。

每个小时，`cq_basic_offset`运行一个单一的查询，覆盖了`now()`和`now()`减去`GROUP BY time()`间隔之间的时间段，即`now()`和`now()`之前的一个小时之间的时间范围。 15分钟偏移间隔在CQ的`WHERE`子句中向前移动生成的预设时间边界; `cq_basic_offset`在7:15和8：14.999999999而不是7:00和7：59.999999999之间进行查询。

下面是2016年8月28日上午的日志输出：

```sql
>
在8:15cq_basic_offset执行时间范围time> ='7:15'AND time <'8:15'的查询。
cq_basic_offset向average_passengers写入一个数据点：
>
    name: average_passengers
    ------------------------
    time                   mean
    2016-08-28T07:15:00Z   7.75
>
在9:15cq_basic_offset执行时间范围time> ='8:15'AND time <'9:15'的查询。
cq_basic_offset向average_passengers写入一个数据点：
>
    name: average_passengers
    ------------------------
    time                   mean
    2016-08-28T08:15:00Z   16.75
```

结果如下：

```sql
> SELECT * FROM "average_passengers"
name: average_passengers
------------------------
time                   mean
2016-08-28T07:15:00Z   7.75
2016-08-28T08:15:00Z   16.75
```

请注意，时间戳为7:15和8:15而不是7:00和8:00。

### 基本语法的常见问题

#### 无数据处理时间间隔

如果没有数据落在该时间范围内，则CQ不会在时间间隔内写入任何结果。请注意，基本语法不支持使用`fill()`更改不含数据的间隔报告的值。如果基本语法包括了`fill()`，则会忽略`fill()`。一个解决办法是使用下面的高级语法。

#### 重新采样以前的时间间隔

基本的CQ运行一个查询，覆盖了`now()`和`now()`减去`GROUP BY time()`间隔之间的时间段。有关如何配置查询的时间范围，请参阅高级语法。

#### 旧数据的回填结果

CQ对实时数据进行操作，即具有相对于`now()`发生的时间戳的数据。使用基本的`INTO`查询来回填具有较旧时间戳的数据的结果。

#### CQ结果中缺少tag

默认情况下，所有`INTO`查询将源measurement中的任何tag转换为目标measurement中的field。

在CQ中包含`GROUP BY *`，以保留目标measurement中的tag。

### 高级语法

```sql
CREATE CONTINUOUS QUERY <cq_name> ON <database_name>
RESAMPLE EVERY <interval> FOR <interval>
BEGIN
  <cq_query>
END
```

### 高级语法描述

`cq_name`，`database_name`和`cq_query`查看基本语法描述。

#### 运行时间点以及覆盖的时间范围

CQs对实时数据进行操作。使用高级语法，CQ使用本地服务器的时间戳以及`RESAMPLE`子句中的信息和openGemini的预设时间边界来确定执行时间和查询中涵盖的时间范围。

CQs以与`RESAMPLE`子句中的`EVERY`间隔相同的间隔执行，并且它们在openGemini的预设时间边界开始时运行。如果`EVERY`间隔是两个小时，openGemini将在每两小时的开始执行CQ。

当CQ执行时，它运行一个单一的查询，在`now()`和`now()`减去`RESAMPLE`子句中的`FOR`间隔之间的时间范围。如果`FOR`间隔为两个小时，当前时间为17:00，查询的时间间隔为15:00至16:59999999999。

`EVERY`间隔和`FOR`间隔都接受时间字符串。`RESAMPLE`子句适用于同时配置`EVERY`和`FOR`,或者是其中之一。如果没有提供`EVERY`间隔或`FOR`间隔，则CQ默认为相关为基本语法。

### 高级语法示例

示例数据如下：

```sql
name: bus_data
--------------
time                   passengers
2016-08-28T06:30:00Z   2
2016-08-28T06:45:00Z   4
2016-08-28T07:00:00Z   5
2016-08-28T07:15:00Z   8
2016-08-28T07:30:00Z   8
2016-08-28T07:45:00Z   7
2016-08-28T08:00:00Z   8
2016-08-28T08:15:00Z   15
2016-08-28T08:30:00Z   15
2016-08-28T08:45:00Z   17
2016-08-28T09:00:00Z   20
```

#### 配置执行间隔

在`RESAMPLE`中使用`EVERY`来指明CQ的执行间隔。

```sql
CREATE CONTINUOUS QUERY "cq_advanced_every" ON "transportation"
RESAMPLE EVERY 30m
BEGIN
  SELECT mean("passengers") INTO "average_passengers" FROM "bus_data" GROUP BY time(1h)
END
```

`cq_advanced_every`从`bus_data`中计算`passengers`的一小时平均值，并将结果存储在数据库`transportation`中的`average_passengers`中。

`cq_advanced_every`以30分钟的间隔执行，间隔与`EVERY`间隔相同。每30分钟，`cq_advanced_every`运行一个查询，覆盖当前时间段的时间范围，即与`now()`交叉的一小时时间段。

下面是2016年8月28日上午的日志输出：

```sql
>
在8:00cq_basic_every执行时间范围time> ='7:00'AND time <'8:00'的查询。
cq_basic_every向average_passengers写入一个数据点：
>
    name: average_passengers
    ------------------------
    time                   mean
    2016-08-28T07:00:00Z   7
>
在8:30cq_basic_every执行时间范围time> ='8:00'AND time <'9:00'的查询。
cq_basic_every向average_passengers写入一个数据点：
>
    name: average_passengers
    ------------------------
    time                   mean
    2016-08-28T08:00:00Z   12.6667
>
在9:00cq_basic_every执行时间范围time> ='8:00'AND time <'9:00'的查询。
cq_basic_every向average_passengers写入一个数据点：
>
    name: average_passengers
    ------------------------
    time                   mean
    2016-08-28T08:00:00Z   13.75
```

结果如下：

```sql
> SELECT * FROM "average_passengers"
name: average_passengers
------------------------
time                   mean
2016-08-28T07:00:00Z   7
2016-08-28T08:00:00Z   13.75
```

请注意，`cq_advanced_every`计算8:00时间间隔的结果两次。第一次，它运行在8:30，计算每个可用数据点在8:00和9:00（8,15和15）之间的平均值。 第二次，它运行在9:00，计算每个可用数据点在8:00和9:00（8,15,15和17）之间的平均值。由于openGemini处理重复点的方式，TODO：开发验证中。

#### 配置CQ的重采样时间范围

在`RESAMPLE`中使用`FOR`来指明CQ的时间间隔的长度。

```sql
CREATE CONTINUOUS QUERY "cq_advanced_for" ON "transportation"
RESAMPLE FOR 1h
BEGIN
  SELECT mean("passengers") INTO "average_passengers" FROM "bus_data" GROUP BY time(30m)
END
```

`cq_advanced_for`从`bus_data`中计算`passengers`的30分钟平均值，并将结果存储在数据库`transportation`中的`average_passengers`中。

`cq_advanced_for`以30分钟的间隔执行，间隔与`GROUP BY time()`间隔相同。每30分钟，`cq_advanced_for`运行一个查询，覆盖时间段为`now()`和`now()`减去`FOR`中的间隔，即是`now()`和`now()`之前的一个小时之间的时间范围。

下面是2016年8月28日上午的日志输出：

```sql
>
在8:00cq_advanced_for执行时间范围time> ='7:00'AND time <'8:00'的查询。
cq_advanced_for向average_passengers写入两个数据点
>
    name: average_passengers
    ------------------------
    time                   mean
    2016-08-28T07:00:00Z   6.5
    2016-08-28T07:30:00Z   7.5
>
在8:30cq_advanced_for执行时间范围time> ='7:30'AND time <'8:30'的查询。
cq_advanced_for向average_passengers写入两个数据点：
>
    name: average_passengers
    ------------------------
    time                   mean
    2016-08-28T07:30:00Z   7.5
    2016-08-28T08:00:00Z   11.5
>
在9:00cq_advanced_for执行时间范围time> ='8:00'AND time <'9:00'的查询。
cq_advanced_for向average_passengers写入两个数据点：
>
    name: average_passengers
    ------------------------
    time                   mean
    2016-08-28T08:00:00Z   11.5
    2016-08-28T08:30:00Z   16
```

请注意，`cq_advanced_for`会计算每次间隔两次的结果。CQ在8:00和8:30计算7:30的平均值，在8:30和9:00计算8:00的平均值。

结果如下：

```sql
> SELECT * FROM "average_passengers"
name: average_passengers
------------------------
time                   mean
2016-08-28T07:00:00Z   6.5
2016-08-28T07:30:00Z   7.5
2016-08-28T08:00:00Z   11.5
2016-08-28T08:30:00Z   16
```

#### 配置执行间隔和CQ时间范围

在`RESAMPLE`子句中使用`EVERY`和`FOR`来指定CQ的执行间隔和CQ的时间范围长度。

```sql
CREATE CONTINUOUS QUERY "cq_advanced_every_for" ON "transportation"
RESAMPLE EVERY 1h FOR 90m
BEGIN
  SELECT mean("passengers") INTO "average_passengers" FROM "bus_data" GROUP BY time(30m)
END
```

`cq_advanced_every_for`从`bus_data`中计算`passengers`的30分钟平均值，并将结果存储在数据库`transportation`中的`average_passengers`中。

`cq_advanced_every_for`以1小时的间隔执行，间隔与`EVERY`间隔相同。每1小时，`cq_advanced_every_for`运行一个查询，覆盖时间段为`now()`和`now()`减去`FOR`中的间隔，即是`now()`和`now()`之前的90分钟之间的时间范围。

下面是2016年8月28日上午的日志输出：

```sql
>
在8:00cq_advanced_every_for执行时间范围time>='6:30'AND time <'8:00'的查询。
cq_advanced_every_for向average_passengers写三个数据点：
>
    name: average_passengers
    ------------------------
    time                   mean
    2016-08-28T06:30:00Z   3
    2016-08-28T07:00:00Z   6.5
    2016-08-28T07:30:00Z   7.5
>
在9:00cq_advanced_every_for执行时间范围time> ='7:30'AND time <'9:00'的查询。
cq_advanced_every_for向average_passengers写入三个数据点：
>
    name: average_passengers
    ------------------------
    time                   mean
    2016-08-28T07:30:00Z   7.5
    2016-08-28T08:00:00Z   11.5
    2016-08-28T08:30:00Z   16
```

请注意，`cq_advanced_every_for`会计算每次间隔两次的结果。CQ在8:00和9:00计算7:30的平均值。

结果如下：

```sql
> SELECT * FROM "average_passengers"
name: average_passengers
------------------------
time                   mean
2016-08-28T06:30:00Z   3
2016-08-28T07:00:00Z   6.5
2016-08-28T07:30:00Z   7.5
2016-08-28T08:00:00Z   11.5
2016-08-28T08:30:00Z   16
```

#### 配置CQ的时间范围并填充空值

使用`FOR`间隔和`fill()`来更改不含数据的时间间隔值。请注意，至少有一个数据点必须在`fill()`运行的`FOR`间隔内。 如果没有数据落在`FOR`间隔内，则CQ不会将任何点写入目标measurement。

```sql
CREATE CONTINUOUS QUERY "cq_advanced_for_fill" ON "transportation"
RESAMPLE FOR 2h
BEGIN
  SELECT mean("passengers") INTO "average_passengers" FROM "bus_data" GROUP BY time(1h) fill(1000)
END
```

`cq_advanced_for_fill`从`bus_data`中计算`passengers`的1小时的平均值，并将结果存储在数据库`transportation`中的`average_passengers`中。并会在没有结果的时间间隔里写入值`1000`。

`cq_advanced_for_fill`以1小时的间隔执行，间隔与`GROUP BY time()`间隔相同。每1小时，`cq_advanced_for_fill`运行一个查询，覆盖时间段为`now()`和`now()`减去`FOR`中的间隔，即是`now()`和`now()`之前的两小时之间的时间范围。

下面是2016年8月28日上午的日志输出：

```sql
>
在6:00cq_advanced_for_fill执行时间范围time>='4:00'AND time <'6:00'的查询。 cq_advanced_for_fill向average_passengers不写入任何数据点，因为在那个时间范围bus_data没有数据：
>
在7:00cq_advanced_for_fill执行时间范围time>='5:00'AND time <'7:00'的查询。 cq_advanced_for_fill向average_passengers写入两个数据点：
>
    name: average_passengers
    ------------------------
    time                   mean
    2016-08-28T05:00:00Z   1000          <------ fill(1000)
    2016-08-28T06:00:00Z   3             <------ average of 2 and 4
>
[...]
>
在11:00cq_advanced_for_fill执行时间范围time> ='9:00'AND time <'11:00'的查询。 cq_advanced_for_fill向average_passengers写入两个数据点：
>
    name: average_passengers
    ------------------------
    2016-08-28T09:00:00Z   20            <------ average of 20
    2016-08-28T10:00:00Z   1000          <------ fill(1000)
>
```

在**12:00**`cq_advanced_for_fill`执行时间范围`time>='10:00'AND time <'12:00'`的查询。 `cq_advanced_for_fill`向`average_passengers`不写入任何数据点，因为在那个时间范围`bus_data`没有数据.

结果如下：

```sql
> SELECT * FROM "average_passengers"
name: average_passengers
------------------------
time                   mean
2016-08-28T05:00:00Z   1000
2016-08-28T06:00:00Z   3
2016-08-28T07:00:00Z   7
2016-08-28T08:00:00Z   13.75
2016-08-28T09:00:00Z   20
2016-08-28T10:00:00Z   1000
```

::: warning

如果前一个值在查询时间之外，则`fill(previous)`不会在时间间隔里填充数据。

:::

### 高级语法的常见问题

#### 如果`EVERY`间隔大于`GROUP BY time()`的间隔

如果`EVERY`间隔大于`GROUP BY time()`间隔，则CQ以与`EVERY`间隔相同的间隔执行，并运行一个单个查询，该查询涵盖`now()`和`now()`减去`EVERY`间隔之间的时间范围(不是在`now()`和`now()`减去`GROUP BY time()`间隔之间）。

例如，如果`GROUP BY time()`间隔为5m，并且`EVERY`间隔为10m，则CQ每10分钟执行一次。每10分钟，CQ运行一个查询，覆盖`now()`和`now()`减去`EVERY`间隔之间的时间段，即`now()`到`now()`之前十分钟之间的时间范围。

此行为是故意的，并防止CQ在执行时间之间丢失数据。



## DROP CONTINUOUS QUERY

### 语法

从一个指定的database删除CQ：

```sql
DROP CONTINUOUS QUERY <cq_name> ON <database_name>
```

`DROP CONTINUOUS QUERY`返回一个空的结果。

### 示例

从数据库`telegraf`中删除`idle_hands`这个CQ：

```sql
> DROP CONTINUOUS QUERY "idle_hands" ON "telegraf"`
```



## SHOW CONTINUOUS QUERIES

### 语法

列出openGemini实例上的所有CQ：

```sql
SHOW CONTINUOUS QUERIES
```

`SHOW CONTINUOUS QUERIES`按照database分组。

### 示例

下面展示了`test1`和`test2`的CQ：

```sql
> SHOW CONTINUOUS QUERIES
name: test1
+-------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| name  | query                                                                                                                                                                                   |
+-------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| cq1_1 | CREATE CONTINUOUS QUERY cq1_1 ON test1 RESAMPLE EVERY 1h FOR 90m BEGIN SELECT mean(passengers) INTO test1.autogen.average_passengers FROM test1.autogen.bus_data GROUP BY time(30m) END |
+-------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
2 columns, 1 rows in set

name: test2
+-------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| name  | query                                                                                                                                                                              |
+-------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| cq2_1 | CREATE CONTINUOUS QUERY cq2_1 ON test2 RESAMPLE EVERY 1h FOR 30m BEGIN SELECT min(passengers) INTO test2.autogen.min_passengers FROM test2.autogen.bus_data GROUP BY time(15m) END |
+-------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
2 columns, 1 rows in set
```
