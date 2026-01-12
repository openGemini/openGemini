---
title: 字符串函数
order: 4
---

# 字符串函数

| 字符串函数 | 说明|
| --- | --- |
| [STR()](#str) | 字符串中是否包含指定字符 |
| [STRLEN()](#strlen) | 字符串长度 |
| [SUBSTR()](#substr) | 取子串 |

示例数据

```sql
> SELECT * FROM water
name: water
+-----------------------------+-----------------------+--------------+-------------+
| time                        | description           | location     | water_level |
+-----------------------------+-----------------------+--------------+-------------+
| 2024-01-13T14:12:41.841223Z | below 3 feet          | santa_monica | 2.064       |
| 2024-01-13T14:13:22.249027Z | upper 10 feet         | santa_monica | 3.064       |
| 2024-01-13T14:14:20.800698Z | between 6 and 9 feet  | coyote_creek | 3.064       |
| 2024-01-13T14:14:59.929578Z | highest 100 feet      | coyote_creek | 10          |
| 2024-01-13T14:15:35.698398Z | between 3 and 10 feet | coyote_creek | 3.42        |
+-----------------------------+-----------------------+--------------+-------------+
4 columns, 5 rows in set
```

## STR()

返回指定Field列是否包含给定模式串, Field必须属于字符类型。不支持按`time`分组，因为`STR()`不属于聚合函数。

**语法**

```sql
SELECT STR( [ * | <field_key> | /<regular_expression>/ ], <pattern> ) [INTO_CLAUSE] FROM_CLAUSE [WHERE_CLAUSE] [GROUP_BY_CLAUSE] [ORDER_BY_CLAUSE] [LIMIT_CLAUSE] [OFFSET_CLAUSE]
```

`<pattern>`为指定的字符串模式串。模式串需要使用**单引号**

**示例**

**查询`description`列哪些包含` ’below‘` 关键字**

```sql
> SELECT STR("description", 'below') FROM "water"
name: water
+-----------------------------+-------+
| time                        | str   |
+-----------------------------+-------+
| 2024-01-13T14:12:41.841223Z | true  |
| 2024-01-13T14:13:22.249027Z | false |
| 2024-01-13T14:14:20.800698Z | false |
| 2024-01-13T14:14:59.929578Z | false |
| 2024-01-13T14:15:35.698398Z | false |
+-----------------------------+-------+
2 columns, 5 rows in set
```

只有第一条数据`below 3 feet`包含 'below', 返回 true，其余为false

**分组查询**

```sql
> SELECT STR(/descr/, 'below') FROM "water" GROUP BY location
name: water
tags: location=coyote_creek
+-----------------------------+-----------------+
| time                        | str_description |
+-----------------------------+-----------------+
| 2024-01-13T14:14:20.800698Z | false           |
| 2024-01-13T14:14:59.929578Z | false           |
| 2024-01-13T14:15:35.698398Z | false           |
+-----------------------------+-----------------+
2 columns, 3 rows in set

name: water
tags: location=santa_monica
+-----------------------------+-----------------+
| time                        | str_description |
+-----------------------------+-----------------+
| 2024-01-13T14:12:41.841223Z | true            |
| 2024-01-13T14:13:22.249027Z | false           |
+-----------------------------+-----------------+
2 columns, 2 rows in set
```

## STRLEN()

返回指定Field列的字符串长度, Field必须属于字符类型。不支持按`time`分组，因为`STRLEN()`不属于聚合函数。

**语法**

```sql
SELECT STRLEN( [ * | <field_key> | /<regular_expression>/ ]) [INTO_CLAUSE] FROM_CLAUSE [WHERE_CLAUSE] [GROUP_BY_CLAUSE] [ORDER_BY_CLAUSE] [LIMIT_CLAUSE] [OFFSET_CLAUSE]
```

**示例**

**查询`description`列哪些包含` ’below‘` 关键字**

```sql
> SELECT STRLEN("description") FROM "water"
name: water
+-----------------------------+--------------------+
| time                        | strlen_description |
+-----------------------------+--------------------+
| 2024-01-13T14:12:41.841223Z | 12                 |
| 2024-01-13T14:13:22.249027Z | 13                 |
| 2024-01-13T14:14:20.800698Z | 20                 |
| 2024-01-13T14:14:59.929578Z | 16                 |
| 2024-01-13T14:15:35.698398Z | 21                 |
+-----------------------------+--------------------+
2 columns, 5 rows in set
```

**分组查询**

```sql
> SELECT STRLEN(/descr/) FROM "water" GROUP BY location
name: water
tags: location=coyote_creek
+-----------------------------+--------------------+
| time                        | strlen_description |
+-----------------------------+--------------------+
| 2024-01-13T14:14:20.800698Z | 20                 |
| 2024-01-13T14:14:59.929578Z | 16                 |
| 2024-01-13T14:15:35.698398Z | 21                 |
+-----------------------------+--------------------+
2 columns, 3 rows in set

name: water
tags: location=santa_monica
+-----------------------------+--------------------+
| time                        | strlen_description |
+-----------------------------+--------------------+
| 2024-01-13T14:12:41.841223Z | 12                 |
| 2024-01-13T14:13:22.249027Z | 13                 |
+-----------------------------+--------------------+
2 columns, 2 rows in set
```

## SUBSTR()

提取指定Field列的子串， Field必须属于字符类型。不支持按`time`分组，因为`SUBSTR()`不属于聚合函数。

**语法**

```sql
SELECT SUBSTR( [ * | <field_key> | /<regular_expression>/ ]，start_index，end_index) [INTO_CLAUSE] FROM_CLAUSE [WHERE_CLAUSE] [GROUP_BY_CLAUSE] [ORDER_BY_CLAUSE] [LIMIT_CLAUSE] [OFFSET_CLAUSE]
```

`<start_index>`, `<end_index>` 表示需要提取的子串起始位置。

**示例**

**提取`description`列所有值的前3个字符**

```sql
> SELECT SUBSTR("description", 1, 3) FROM "water"
name: water
+-----------------------------+--------+
| time                        | substr |
+-----------------------------+--------+
| 2024-01-13T14:12:41.841223Z | elo    |
| 2024-01-13T14:13:22.249027Z | ppe    |
| 2024-01-13T14:14:20.800698Z | etw    |
| 2024-01-13T14:14:59.929578Z | igh    |
| 2024-01-13T14:15:35.698398Z | etw    |
+-----------------------------+--------+
2 columns, 5 rows in set
```

##
