---
title: 查询管理
order: 10
---

查询管理主要用于查看内核正在执行的查询语句，以及在必要时可以杀掉特定的正在执行的查询语句。

## SHOW QUERIES

```
> SHOW QUERIES
+-------+---------------------------------+---------------------+----------+---------+----------------+
| qid   | query                           | database            | duration | status  | host           |
+-------+---------------------------------+---------------------+----------+---------+----------------+
| 1     | SELECT * FROM h2o_feet LIMIT 10 | NOAA_water_database | 1m30s    | running | 127.0.0.1:8400 |
| 2     | SELECT * FROM h2o_feet LIMIT 78 | NOAA_water_database | 1m22s    | killed  | 127.0.0.1:8404 |
6 columns, 2 rows in set
```

- **qid** 表示查询语句在内部的编号
- **query**表示当前正在执行的查询语句
- **database**表示目标数据库
- **duration**表示查询语句已经运行的时长
- **status**表示查询语句当前的执行状态，running表示正在运行，killed表示已被杀死
- **host**表示查询语句执行的节点IP

## KILL QUERY

```
> KILL QUERY 2  // “2”表示qid
```

执行成功时，没有消息返回，可再次通过`SHOW QUERIES`查看是否对应的query已被杀死
