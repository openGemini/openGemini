---
title: 数据保留策略
order: 3
---
## CREATE RETENTION POLICY(创建数据保留策略)

### 语法
```sql
CREATE RETENTION POLICY <retention_policy_name> ON <database_name> DURATION <duration> REPLICATION <n> [SHARD DURATION <duration>] [INDEX DURATION <duration>] [DEFAULT]
```

#### DURATION

`DURATION`子句确定openGemini将数据保留多长时间。 保留策略的最短持续时间为一小时，最长持续时间为无限。

#### REPLICATION

`REPLICATION`子句确定每个数据点在集群中存储了多少个独立副本，目前仅支持`1`副本。

#### SHARD DURATION

- 可选项， `SHARD DURATION` 子句确定分片组的时间范围。
- 默认情况下，分片组的持续时间由保留策略的`DURATION`确定：

| 保留策略期限              | 分片组持续时间 |
| ------------------------- | -------------- |
| < 2 days                  | 1 hour         |
| >= 2 days and <= 6 months | 1 day          |
| > 6 months                | 7 days         |

最小允许的 `SHARD GROUP DURATION` 为`1h`.
如果 `创建保留策略` 查询试图将 `SHARD GROUP DURATION` 设置为小于 `1h` 且大于 `0s`, openGemini 会自动的将 `SHARD GROUP DURATION` 设置为 `1h`.
如果 `CREATE RETENTION POLICY` 查询试图将 `SHARD GROUP DURATION` 设置为你 `0s`, openGemini 会根据上面列出的默认自动设置`SHARD GROUP DURATION`

#### INDEX DURATION

- 可选项，`INDEX DURATION` 子句确定索引组的时间范围。

#### DEFAULT

将新的保留策略设置为数据库的默认保留策略。此设置是可选项。

### 示例

- **创建保留策略**

```sql
CREATE RETENTION POLICY "one_day_only" ON "NOAA_water_database" DURATION 1d REPLICATION 1
```
该查询为数据库`NOAA_water_database`创建了一个名为`one_day_only`的保留策略，该策略的期限为`1d`，复制因子为`1`。

- **创建默认保留策略**

```sql
CREATE RETENTION POLICY "one_day_only" ON "NOAA_water_database" DURATION 24h REPLICATION 1 DEFAULT
```

该查询创建与上例相同的保留策略，但是将其设置为数据库的默认保留策略。

- **创建数据不过期的保留策略**

```sql
CREATE RETENTION POLICY "never_expire" ON "NOAA_water_database" DURATION 0s REPLICATION 1
```

该查询为数据库`NOAA_water_database`创建了一个名为`never_expire`的保留策略，该策略的下的数据是不会过期的。

::: tip
成功的`CREATE RETENTION POLICY`查询不返回任何结果。

如果尝试创建与现有策略相同的保留策略，则openGemini不会返回错误。
如果尝试创建与现有保留策略相同名称的保留策略，但属性不同，则openGemini将返回错误。

请参阅 [数据库操作](./database)
:::

## SHOW RETENTION POLICIES(查看数据保留策略)

返回指定数据库的**保留策略**列表。

### 语法

```sql
SHOW RETENTION POLICIES [ON <database_name>]
```

`ON <database_name>`是可选项。如果查询中没有包含`ON <database_name>`，您必须在CLI中使用`USE <database_name>`指定数据库，或者在openGemini API请求中使用参数`db`指定数据库。

### 示例

- **运行带有`ON`子句的`SHOW RETENTION POLICIES`查询**

```sql
> SHOW RETENTION POLICIES ON NOAA_water_database
+---------+----------+--------------------+--------------+---------------+----------------+----------+---------+
| name    | duration | shardGroupDuration | hot duration | warm duration | index duration | replicaN | default |
+---------+----------+--------------------+--------------+---------------+----------------+----------+---------+
| autogen | 0s       | 168h0m0s           | 0s           | 0s            | 168h0m0s       | 1        | true    |
+---------+----------+--------------------+--------------+---------------+----------------+----------+---------+
8 columns, 1 rows in set
```

该查询以表格的形式返回数据库`NOAA_water_database`中所有的保留策略。这个数据库有一个名为`autogen`的保留策略，该保留策略具有无限的持续时间，为期7天的shard group持续时间，复制系数为1，并且它是这个数据库的默认(`DEFAULT`)保留策略。

- **运行不带有`ON`子句的`SHOW RETENTION POLICIES`查询**

::: tabs

@tab ts-cli

使用`USE <database_name>`指定数据库

```bash
> use NOAA_water_database
> SHOW RETENTION POLICIES
+---------+----------+--------------------+--------------+---------------+----------------+----------+---------+
| name    | duration | shardGroupDuration | hot duration | warm duration | index duration | replicaN | default |
+---------+----------+--------------------+--------------+---------------+----------------+----------+---------+
| autogen | 0s       | 168h0m0s           | 0s           | 0s            | 168h0m0s       | 1        | true    |
+---------+----------+--------------------+--------------+---------------+----------------+----------+---------+
8 columns, 1 rows in set
```

@tab HTTP API

使用参数`db`指定数据库

```bash
> curl -G "http://localhost:8086/query?db=NOAA_water_database&pretty=true" --data-urlencode "q=SHOW RETENTION POLICIES"

{
    "results": [
        {
            "statement_id": 0,
            "series": [
                {
                    "columns": [
                        "name",
                        "duration",
                        "shardGroupDuration",
                        "hot duration",
                        "warm duration",
                        "index duration",
                        "replicaN",
                        "default"
                    ],
                    "values": [
                        [
                            "autogen",
                            "0s",
                            "168h0m0s",
                            "0s",
                            "0s",
                            "168h0m0s",
                            1,
                            true
                        ]
                    ]
                }
            ]
        }
    ]
}
```

:::

## ALTER RETENTION POLICY(修改数据保留策略)

### 语法

`ALTER RETENTION POLICY`语法如下，必须声明至少一个保留策略属性`DURATION`，`REPLICATION`，`SHARD DURATION`或`DEFAULT`：

```sql
ALTER RETENTION POLICY <retention_policy_name> ON <database_name> DURATION <duration> REPLICATION <n> SHARD DURATION <duration> DEFAULT
```

::: warning

复制因子 `REPLICATION <n>` 仅支持 1

:::

### 示例

首先，以2d的`DURATION`创建保留策略`what_is_time`：

```sql
CREATE RETENTION POLICY "what_is_time" ON "NOAA_water_database" DURATION 2d REPLICATION 1
```

修改`what_is_time`以使其具有三周的`DURATION`，两个小时的分片组持续时间，并使其成为`NOAA_water_database`的`DEFAULT`保留策略。

```sql
ALTER RETENTION POLICY "what_is_time" ON "NOAA_water_database" DURATION 3w SHARD DURATION 2h DEFAULT
```
在最后一个示例中，` what_is_time`保留其原始复制因子`1`。

成功的`ALTER RETENTION POLICY`查询不返回任何结果。

## DROP RETENTION POLICY(删除数据保留策略)

::: danger

删除保留策略将永久删除使用该保留策略的所有measurement和数据

:::

### 语法

```sql
DROP RETENTION POLICY <retention_policy_name> ON <database_name>
```

### 示例
在`NOAA_water_database`数据库中删除保留策略`what_is_time`：

```sql
> DROP RETENTION POLICY "what_is_time" ON "NOAA_water_database"
```

成功执行`DROP RETENTION POLICY`不返回任何结果。

::: tip

如果尝试删除不存在的保留策略，openGemini也不会返回错误。

:::
