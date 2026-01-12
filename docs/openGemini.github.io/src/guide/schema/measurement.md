---
title: Manage meausurments
order: 2
---

## CREATE MEASUREMENT

openGemini supports automatic table creation when writing data, but in the following three situations, tables need to be created in advance.

### Specify a tag as partition key

By default, data in openGemini is partition by hash base on time series. However, in some scenarios, users frequently use one or more tags for data query. the hash method distributes the data to different nodes. AS a result, the query fanout is large.
If the data can be partitioned according to these frequently used TAGs, then the data with the same TAG value will be stored on the same node, thereby reducing query fan-out and improving data query efficiency.

#### Specify a TAG (such as location) to break up the data
```sql
CREATE MEASUREMENT mst WITH SHARDKEY location
```
#### Specify multiple TAGs (such as location, region) as SHARD KEY
```sql
CREATE MEASUREMENT mst WITH SHARDKEY location，region
```

### TEXT SEARCH

Text retrieval refers to searching and filtering base on keywords or phrases in text data sets. openGemini supports text retrieval, such as keyword retrieval of logs, which can return all log data containing keywords.

You need to create a measurement before use it. The purpose of creating a measurement is to specify which Field fields to create a full-text index on, but there is a premise that these fields must be 'string' data type

```sql
> CREATE MEASUREMENT mst WITH INDEXTYPE text INDEXLIST description, error_message
```

Create a measurement named mst and specify to create a full-text index on the two fields ```description ```and ```error_message```.

```sql
> CREATE MEASUREMENT mst WITH INDEXTYPE text INDEXLIST description, error_message SHARDKEY location
```

Create a measurement named mst, and specify to create a full-text index on the ```description``` and ```error_message``` fields, and set ```location``` as partition key
::: tip

Full-text indexes will only be created on the fields ```description``` and ```error_message``` specified by PRIMARYKEY. If you search keywords in other Fields, it may be slower

It supports exact matching, phrase matching and fuzzy matching on fields ```description``` and ```error_message```

**related entries** [Text Search](../features/logs.md)

It is not recommended to create a text index on a TAG, and unforeseeable problems may occur.

:::

### USE A HIGH SERIES CARDINALITY STORAGE ENGINE(HSCE)

The traditional time series database has the problem of index expansion due to the large time series, and openGemini's HSCE solves this problem. When we use it, we need to specify the engine type 'columnstore' when creating a measurement. The default storage engine is not HSCE.
```sql
> CREATE MEASUREMENT mst (location string field default "", direction string field default "", rtt int field default 0, time int field default 0,) WITH ENGINETYPE = columnstore SHARDKEY location TYPE hash PRIMARYKEY location, direction SORTKEY time
```
Create a measurement named mst with four fields ```location```, ```direction```, ```rtt```, ```time, ```, and specify the data type and default value respectively. For example, location is a string type, and the default value is an empty string.

#### ENGINETYPE
Required, must be columnstore
#### SHARDKEY
Required, specify ```location``` as partition key
#### TYPE
Required, There are two ways to break up data: hash and range
#### PRIMARYKEY
Required, The primary key is ```location``` and ```direction```, which means that the storage engine will create indexes on these two fields.
#### SORTKEY
Required, specify the data sorting method inside the storage engine. ```time``` means sorting by time, and can also be changed to ```rtt``` or ```direction```, or even other fields in the table.

When creating measurement, you need to pay attention to:

1. Must specify all field names, data types, TAG or ordinary field and the default value in case of missing values.

2. If ```SHARDKEY``` is not specified, all data will be written to one data node

::: tip

The traditional inverted index is similar to a dense index with high series cardinality. The index take a lot of memory space, and the query efficiency is low, and it has little effect on data filtering. The openGemini high series cardinality storage engine improves data query efficiency by building sparse indexes.

For the problem of high series cardinality, openGemini has found a solution, but many functions of openGemini on the new storage engine are not yet perfect and cannot be used in a production environment. For example, it does not support aggregation operators, and the syntax for creating measurement still needs further streamlining, and some exceptions have not been handled yet.

Welcome to participate and improve the functions together.

:::


## SHOW MEASUREMENTS

View the measurements created in the database

```sql
SHOW MEASUREMENTS [ON <database_name>] [WITH MEASUREMENT <operator> ['<measurement_name>' | <regular_expression>]]
```
`ON <database_name>`is optional。If ```ON <database_name>``` is not included in the query, you must specify the database with ```USE <database_name>``` in the CLI before, or use the parameter `db` in the openGemini API request.

The `WITH` clause, `WHERE` clause, `LIMIT` clause and `OFFSET` clause are optional. The `WHERE` clause supports tag comparison; in `SHOW MEASUREMENTS` queries, field comparison is invalid.

The operators in `WHERE` clause are:

| Operators | Description   |
| ------ | ------ |
| `=`    | equal   |
| `<>`   | not equal |
| `!=`   | not equal |
| `=~`   | match   |
| `!~`   | not match |

**relate entries** [`FROM`clause](../query_data/select.md#select)、[`LIMIT、OFFSET`clause](../query_data/select.md#limit-offset)

### Examples

#### `SHOW MEASUREMENTS` with an `ON` clause

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
The database `NOAA_water_database` has five measurements: `average_temperature`, `h2o_feet`, `h2o_pH`, `h2o_quality` and `h2o_temperature`.

#### `SHOW MEASUREMENTS` without the `ON` clause

::: tabs

@tab CLI

use command `USE <database_name>` specified database：

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

Use the parameter `db` to specify the database

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

#### `SHOW MEASUREMENTS` with multiple clauses

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

Return measurements whose names start with `h2o` in the database `NOAA_water_database`.

#### View the number of measurements
```sql
> SHOW MEASUREMENTS CARDINALITY
TODO

> SHOW MEASUREMENTS CARDINALITY ON NOAA_water_database
TODO
```

## DROP MEASUREMENT

use command `DROP MEASUREMENT` to delete measurement.

Deleting a measurement will delete all data and indexes.

```sql
DROP MEASUREMENT <measurement_name>
```

### Examples

Delete the measurement `h2o_feet`

```sql
> DROP MEASUREMENT "h2o_feet"
```

::: warning

There has no results return when the command 'DROP MEASUREMENT' excute success.

:::

## ALTER MEASUREMENT
##TODO
