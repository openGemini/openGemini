---
title: 数据库操作
order: 1
---

## CREATE DATABASE (创建数据库)

### 语法

```sql
CREATE DATABASE <database_name> [WITH [DURATION <duration>] [REPLICATION <n>] [SHARD DURATION <duration>] [INDEX DURATION <duration>] [NAME <retention-policy-name>]]
```
`CREATE DATABASE`需要数据库名称。

`WITH` ，`DURATION`，`REPLICATION`，`SHARD DURATION`，`INDEX DURATION`，`NAME` 子句以及创建与数据库相关联的单个保留策略是可选项。
如果未在`WITH`之后指定子句，则会默认创建名称为`autogen`的保留策略。

成功的`CREATE DATABASE`查询不返回任何结果。

如果创建一个已经存在的数据库，openGemini 不执行任何操作，但也不会返回错误。

### 示例

- **创建数据库**

```sql
> CREATE DATABASE "NOAA_water_database"
```

该查询创建一个名为 `NOAA_water_database`的数据库。

默认情况下，openGemini还会创建默认的保留策略`autogen`并与数据库`NOAA_water_database`进行关联。

- **创建数据库指定保留策略**

```sql
> CREATE DATABASE "NOAA_water_database" WITH DURATION 3d REPLICATION 1 SHARD DURATION 1h INDEX DURATION 7h NAME "rp3d"
```

该操作创建一个名称为`NOAA_water_database`的数据库。还为`NOAA_water_database`创建一个保留策略，名称为`rp3d`，其`DURATION`为3d，复制因子为1，分片组持续时间为1h，索引组持续时间为7h。

- **特例**

使用[tag array](../features/tag_array.md)功能时，创建数据库的语句如下：

```sql
> create database NOAA_water_database tag attribute array
```

## SHOW DATABASES (查看数据库)

返回实例上所有数据库的列表。

### 语法

```sql
SHOW DATABASES
```

### 示例

- **运行 `SHOW DATABASES` 查询语句**

```sql
> SHOW DATABASES
name: databases
+---------------------+
| name                |
+---------------------+
| NOAA_water_database |
+---------------------+
1 columns, 1 rows in set
```

该查询以表格格式返回数据库名称，这个实例有一个数据库：`NOAA_water_database`。

## DROP DATABASE (删除数据库)

`DROP DATABASE`删除数据库，并删除与之关联的所有数据，包括measurement、series、连续查询和保留策略。

### 语法

```sql
DROP DATABASE <database_name>
```
### 示例

删除数据库`NOAA_water_database`：
```sql
> DROP DATABASE "NOAA_water_database"
```

成功的`DROP DATABASE`命令不返回任何结果。如果删除不存在的数据库，openGemini也不会返回错误。
