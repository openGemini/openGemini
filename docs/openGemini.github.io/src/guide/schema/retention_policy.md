---
title: Retention policy
order: 3
---
## CREATE RETENTION POLICY
### Syntax
```sql
CREATE RETENTION POLICY <retention_policy_name> ON <database_name> DURATION <duration> REPLICATION <n> [SHARD DURATION <duration>] [INDEX DURATION <duration>] [DEFAULT]
```

#### DURATION

`DURATION` determines how long openGemini keeps data. A retention policy has a minimum duration of one hour and a maximum duration of infinite.

#### REPLICATION

`REPLICATION` determines how many independent replicas of each data point are stored in the cluster, currently only `1` replica is supported.

#### SHARD DURATION

- Optional, `SHARD DURATION` sets the time range of the shard group
- By default, the data expiration time is determined by the `DURATION` of the retention policy：

| DURATION                  | SHARD(GROUP) DURATION |
| ------------------------- | --------------------- |
| < 2 days                  | 1 hour                |
| >= 2 days and <= 6 months | 1 day                 |
| > 6 months                | 7 days                |

`SHARD GROUP DURATION` minimum is`1h`.

If the `Create retention policy` query tries to set `SHARD GROUP DURATION` to be less than `1h` and greater than `0s`, openGemini will automatically set `SHARD GROUP DURATION` to `1h`.

If the `CREATE RETENTION POLICY` query tries to set `SHARD GROUP DURATION` to your `0s`, openGemini will automatically set `SHARD GROUP DURATION` according to the defaults listed above.

#### INDEX DURATION

- Optional, `INDEX DURATION` sets the time range of the index group

#### DEFAULT

Set the new retention policy as the default retention policy for the database. This setting is optional.

### Examples

- **Creating a retention policy**

```sql
CREATE RETENTION POLICY "one_day_only" ON "NOAA_water_database" DURATION 1d REPLICATION 1
```
This query creates a retention policy named `one_day_only` for the database `NOAA_water_database` with a duration of `1d` and a replication factor of `1`.

- **Creating a default retention policy**

```sql
CREATE RETENTION POLICY "one_day_only" ON "NOAA_water_database" DURATION 24h REPLICATION 1 DEFAULT
```

This query creates the same retention policy as the above example, but sets it as the default retention policy for the database.

- **Create a retention policy that does not expire**

```sql
CREATE RETENTION POLICY "never_expire" ON "NOAA_water_database" DURATION 0s REPLICATION 1
```

This query creates a retention policy called `never_expire` for the database `NOAA_water_database`, the data under this policy will not expire.

::: tip

A successful `CREATE RETENTION POLICY` query does not return any results.

If an attempt is made to create a retention policy with the same name as an existing policy, openGemini will not return an error.
If an attempt is made to create a retention policy with the same name as an existing retention policy, but with different attributes, openGemini will return an error.

**related entries** [CREATE DATABASE](./database)

:::

## SHOW RETENTION POLICIES

Returns a list of **reservation policies** for the specified database.

### Syntax

```sql
SHOW RETENTION POLICIES [ON <database_name>]
```
`ON <database_name>` is optional. If the query does not contain `ON <database_name>`, you must specify the database in the CLI using `USE <database_name>` or in the openGemini API request using the parameter `db`.

### Examples

#### `SHOW RETENTION POLICIES` with the `ON` clause

```sql
> SHOW RETENTION POLICIES ON NOAA_water_database
+---------+----------+--------------------+--------------+---------------+----------------+----------+---------+
| name    | duration | shardGroupDuration | hot duration | warm duration | index duration | replicaN | default |
+---------+----------+--------------------+--------------+---------------+----------------+----------+---------+
| autogen | 0s       | 168h0m0s           | 0s           | 0s            | 168h0m0s       | 1        | true    |
+---------+----------+--------------------+--------------+---------------+----------------+----------+---------+
8 columns, 1 rows in set
```

This query returns all the retention policies in the database `NOAA_water_database` in tabular form. This database has a retention policy named `autogen` that has unlimited duration, a shard group duration of 7 days, a replication factor of 1, and it is the default (`DEFAULT`) retention policy for this database.

#### `SHOW RETENTION POLICIES` without the `ON` clause

::: tabs

@tab ts-cli

Specify the database using `USE <database_name>`

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

Use the parameter `db` to specify the database

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

## ALTER RETENTION POLICY

### Syntax

The `ALTER RETENTION POLICY` query syntax is as follows and must declare at least one reservation policy attribute `DURATION`, `REPLICATION`, `SHARD DURATION` or `DEFAULT`:

```sql
ALTER RETENTION POLICY <retention_policy_name> ON <database_name> DURATION <duration> REPLICATION <n> SHARD DURATION <duration> DEFAULT
```

::: warning warning

`REPLICATION <n>` only support 1
:::

### Examples

First, create the retention policy `what_is_time` with the 2d `DURATION`:

```sql
CREATE RETENTION POLICY "what_is_time" ON "NOAA_water_database" DURATION 2d REPLICATION 1
```

Modify `what_is_time` to have three weeks of `DURATION`, two hours of slice group duration, and make it a `DEFAULT` retention policy for `NOAA_water_database`.

```sql
ALTER RETENTION POLICY "what_is_time" ON "NOAA_water_database" DURATION 3w SHARD DURATION 2h DEFAULT
```
In the last example, ` what_is_time` retains its original replication factor `1`.

There does not return any results when `ALTER RETENTION POLICY` execute successful.

## DROP RETENTION POLICY

::: danger danger

Deleting a retention policy will permanently delete all measurements and data using that retention policy

:::

### Syntax

```sql
DROP RETENTION POLICY <retention_policy_name> ON <database_name>
```

### Examples
Delete the retention policy `what_is_time` in the `NOAA_water_database` database:

```sql
DROP RETENTION POLICY "what_is_time" ON "NOAA_water_database"
```

There does not return any results when `DROP RETENTION POLICY` execute successful.

::: tip

If an attempt is made to delete a non-existent retention policy, openGemini will not return an error.

:::
