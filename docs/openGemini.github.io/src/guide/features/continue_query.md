---
title: Continue query
order: 6
---

## CREATE CONTINUOUS QUERY

### Basic Syntax

```sql
CREATE CONTINUOUS QUERY <cq_name> ON <database_name>
BEGIN
  <cq_query>
END
```

### Basic Syntax Description

`cq_name` name of the continuous query.

`database_name` name of the database where the continuous query is located.

`cq_query` query statement in continuous query. This statement requires a function, an `INTO` clause and a `GROUP BY time()` clause:

```sql
SELECT <function[s]> INTO <destination_measurement> FROM <measurement> [WHERE <stuff>] GROUP BY time(<interval>)[,<tag_key[s]>]
```

::: warning

`cq_query` does not require a time range in the `WHERE` clause. openGemini automatically generates the time range of `cq_query` when executing CQ. Any user-specified time ranges in the `WHERE` clause of `cq_query` will be ignored by the databse.

:::

#### When CQ Executes and What Timeframe it Covers

CQ operates on real-time data. They use the local server's timestamp, the `GROUP BY time()` interval and openGemini's preset time boundaries to determine when to execute and the time range covered in the query.

CQs are executed at the same interval as `cq_query`'s `GROUP BY time()` interval, and they run at the start of openGemini's preset time boundaries. If the `GROUP BY time()` interval is 1 hour, the CQ starts executing every hour.

When CQ executes, it runs a single query for time ranges between `now()` and `now()` minus the `GROUP BY time()` interval. If the `GROUP BY time()` interval is 1 hour and the current time is 17:00, the query time range is from 16:00 to 16:59999999999.

### Basic Syntax Examples

The following examples use sample data from the database `transportation`. The measurement `bus_data` data stores 15-minute data on the number of bus passengers and the number of complaints:

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

#### Automatically Sample Data

Use simple CQ to automatically downsample data from a single field and write the results to another measurement in the same database.

```sql
CREATE CONTINUOUS QUERY "cq_basic" ON "transportation"
BEGIN
  SELECT mean("passengers") INTO "average_passengers" FROM "bus_data" GROUP BY time(1h)
END
```

`cq_basic` calculates the average hours of passengers from `bus_data` and stores the result in `average_passengers` in the database `transportation`.

`cq_basic` is executed at one-hour intervals, the same interval as `GROUP BY time()`. Every hour, `cq_basic` runs a single query covering the time range between `now()` and `now()` minus the `GROUP BY time()` interval, that is, `now()` and the time range between the hour before `now()`.

The following is the log on the morning of August 28, 2016:

```sql
>
"At 8 o'clock, cq_basic executes the query with time range time >= '7:00' AND time < '08:00'.
cq_basic writes a point to average_passengers:"
>
    name: average_passengers
    ------------------------
    time                   mean
    2016-08-28T07:00:00Z   7
>
"At 9 o'clock, cq_basic executes the query with time range time >= '8:00' AND time < '09:00'.
cq_basic writes a point to average_passengers:"
>
    name: average_passengers
    ------------------------
    time                   mean
    2016-08-28T08:00:00Z   13.75
```

The result is as follows:

```sql
> SELECT * FROM "average_passengers"
name: average_passengers
------------------------
time                   mean
2016-08-28T07:00:00Z   7
2016-08-28T08:00:00Z   13.75
```

#### Automatically Sample Data into Another Retention Policy

Sample data from the default retention policy to a fully specified target measurement:

```sql
CREATE CONTINUOUS QUERY "cq_basic_rp" ON "transportation"
BEGIN
  SELECT mean("passengers") INTO "transportation"."three_weeks"."average_passengers" FROM "bus_data" GROUP BY time(1h)
END
```

`cq_basic_rp` calculates the average hours of passengers from `bus_data`, and stores the result in the measurement `average_passengers` whose RP is `three_weeks` in the database `tansportation`.

`cq_basic_rp` is executed at one-hour intervals, the same interval as `GROUP BY time()`. Every hour, `cq_basic_rp` runs a single query covering the time period between `now()` and `now()` minus the `GROUP BY time()` interval, that is, `now()` and the time range between the hour before `now()`.

The following is the log output on the morning of August 28, 2016:

```sql
>
"Execute the query with time >='7:00' AND time <'8:00' at 8:00cq_basic_rp.
cq_basic_rp writes a point to the measurement average_passengers whose RP is three_weeks:"
>
    name: average_passengers
    ------------------------
    time                   mean
    2016-08-28T07:00:00Z   7
>
"At 9:00cq_basic_rp execute the query whose time range is time >='8:00' AND time <'9:00'.
cq_basic_rp writes a point to the measurementaverage_passengers whose RP is three_weeks:"
>
    name: average_passengers
    ------------------------
    time                   mean
    2016-08-28T08:00:00Z   13.75
```

The result is as follows:

```sql
> SELECT * FROM "transportation"."three_weeks"."average_passengers"
name: average_passengers
------------------------
time                   mean
2016-08-28T07:00:00Z   7
2016-08-28T08:00:00Z   13.75
```

`cq_basic_rp` uses CQ and retention policies to automatically downsample data and keep those samples for varying lengths of time.

#### Automatically Sample Data and Configure Time Boundaries for CQ

Use the offset interval of the `GROUP BY time()` clause to change the default execution time of CQ and the time boundary of rendering:

```sql
CREATE CONTINUOUS QUERY "cq_basic_offset" ON "transportation"
BEGIN
  SELECT mean("passengers") INTO "average_passengers" FROM "bus_data" GROUP BY time(1h,15m)
END
```

`cq_basic_offset` calculates the average hours of passengers from `bus_data` and stores the result in `average_passengers`.

`cq_basic_offset` is executed at one-hour intervals, the same interval as `GROUP BY time()`. A 15 minute offset interval forces the CQ to execute 15 minutes after the default execution time; `cq_basic_offset` executes at 8:15 instead of 8:00.

Every hour, `cq_basic_offset` runs a single query covering the time period between `now()` and `now()` minus the `GROUP BY time()` interval, that is, `now()` and the time range between the hour before `now()`. The 15-minute offset interval shifts the generated preset time boundaries forward in the CQ's `WHERE` clause; `cq_basic_offset` queries between 7:15 and 8:14.999999999 instead of 7:00 and 7:59.999999999.

```sql
>
"Execute the query with time range time >= '7:15' AND time < '8:15' at 8:15cq_basic_offset.
cq_basic_offset writes a data point to average_passengers:"
>
    name: average_passengers
    ------------------------
    time                   mean
    2016-08-28T07:15:00Z   7.75
>
"Execute the query with time range time >= '8:15' AND time <'9:15' at 9:15cq_basic_offset.
cq_basic_offset writes a data point to average_passengers:"
>
    name: average_passengers
    ------------------------
    time                   mean
    2016-08-28T08:15:00Z   16.75
```

The result is as follows:

```sql
> SELECT * FROM "average_passengers"
name: average_passengers
------------------------
time                   mean
2016-08-28T07:15:00Z   7.75
2016-08-28T08:15:00Z   16.75
```

Note that the timestamps are 7:15 and 8:15 instead of 7:00 and 8:00.

### Common Problems about Basic Synax

#### No Data Processing Time Interval

If no data falls within that time range, CQ will not write any results for the time interval. Note that the base syntax does not support using `fill()` to change the value reported for intervals with no data. `fill()` is ignored if the base syntax includes `fill()`. A workaround is to use the advanced syntax below.

#### Resample the Previous Time Interval

Basic CQ runs a query that covers the time period between `now()` and `now()` minus the `GROUP BY time()` interval. See Advanced Syntax for how to configure the time range for queries.

#### Backfill Result of Old Data

CQs operate on real-time data, i.e. data with timestamps that occur relative to `now()`. Use basic `INTO` queries to backfill results with data with older timestamps.

#### The Tag is Missing in the CQ Result

By default, all INTO queries convert any tags in the source measurement to fields in the target measurement.

Include `GROUP BY *` in the CQ to preserve the tags in the target measurement.

### Advanced Syntax

```sql
CREATE CONTINUOUS QUERY <cq_name> ON <database_name>
RESAMPLE EVERY <interval> FOR <interval>
BEGIN
  <cq_query>
END
```

### Advanced Syntax Description

About `cq_name`，`database_name` and `cq_query`, please see the basic syntax description.

#### When CQ Executes and What Timeframe it Covers

CQs operate on real-time data. Using an advanced syntax, CQ uses the local server's timestamp along with information in the `RESAMPLE` clause and openGemini's preset time boundaries to determine the execution time and time range covered in the query.

CQs are executed at the same interval as the `EVERY` interval in the `RESAMPLE` clause, and they run at the start of openGemini's preset time boundaries. If `EVERY` interval is two hours, openGemini will execute CQ at the beginning of every two hours.

When CQ executes, it runs a single query with the time range between `now()` and `now()` minus the `FOR` interval in the `RESAMPLE` clause. If the `FOR` interval is two hours and the current time is 17:00, the time interval of the query is from 15:00 to 16:59999999999.

Both the `EVERY` interval and the `FOR` interval accept time strings. The `RESAMPLE` clause is suitable for configuring both `EVERY` and `FOR`, or one of them. If no `EVERY` interval or `FOR` interval is provided, CQ defaults to relative as the base syntax.

###  Advanced Syntax Examples

Sample data is as follows:

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

#### Configure the Execution Interval

Use `EVERY` in `RESAMPLE` to indicate the execution interval of CQ.

```sql
CREATE CONTINUOUS QUERY "cq_advanced_every" ON "transportation"
RESAMPLE EVERY 30m
BEGIN
  SELECT mean("passengers") INTO "average_passengers" FROM "bus_data" GROUP BY time(1h)
END
```

`cq_advanced_every` calculates the hourly average of `passengers` from `bus_data` and stores the result in `average_passengers` in the database `transportation`.

`cq_advanced_every` is executed at 30-minute intervals, the same interval as `EVERY`. Every 30 minutes, `cq_advanced_every` runs a query covering the time range of the current time period, which is the one-hour time period intersected by `now()`.

The following is the log output on the morning of August 28, 2016:

```sql
>
"Execute the query with time range time >= '7:00' AND time < '8:00' at 8:00cq_basic_every.
cq_basic_every writes a data point to average_passengers:"
>
    name: average_passengers
    ------------------------
    time                   mean
    2016-08-28T07:00:00Z   7
>
"Execute the query with time range time >= '8:00' AND time < '9:00' at 8:30cq_basic_every.
cq_basic_every writes a data point to average_passengers:"
>
    name: average_passengers
    ------------------------
    time                   mean
    2016-08-28T08:00:00Z   12.6667
>
"Execute the query with time range time >= '8:00' AND time < '9:00' at 9:00cq_basic_every.
cq_basic_every writes a data point to average_passengers:"
>
    name: average_passengers
    ------------------------
    time                   mean
    2016-08-28T08:00:00Z   13.75
```

The result is as follows:

```sql
> SELECT * FROM "average_passengers"
name: average_passengers
------------------------
time                   mean
2016-08-28T07:00:00Z   7
2016-08-28T08:00:00Z   13.75
```

Note that `cq_advanced_every` computes the result twice for the 8:00 time interval. The first time, it runs at 8:30, calculating the average for each available data point between 8:00 and 9:00 (8, 15, and 15). The second time, it runs at 9:00, calculating the average for each available data point between 8:00 and 9:00 (8, 15, 15, and 17). Due to the way openGemini handles duplicate points, TODO: pending verification.

#### Configure the Resampling Time Range of CQ

Use `FOR` in `RESAMPLE` to specify the length of the CQ interval.

```sql
CREATE CONTINUOUS QUERY "cq_advanced_for" ON "transportation"
RESAMPLE FOR 1h
BEGIN
  SELECT mean("passengers") INTO "average_passengers" FROM "bus_data" GROUP BY time(30m)
END
```

`cq_advanced_for` calculates the 30-minute average of `passengers` from `bus_data` and stores the result in `average_passengers` in the database `transportation`.

`cq_advanced_for` is executed at 30-minute intervals, the same interval as `GROUP BY time()`. Every 30 minutes, `cq_advanced_for` runs a query covering the time period `now()` and `now()` minus the interval in `FOR`, which is `now()` and `now()` before A time range between one hour.

The following is the log output on the morning of August 28, 2016:

```sql
>
"At 8:00cq_advanced_for executes the query with time range time >= '7:00' AND time <'8:00'.
cq_advanced_for writes two data points to average_passengers"
>
    name: average_passengers
    ------------------------
    time                   mean
    2016-08-28T07:00:00Z   6.5
    2016-08-28T07:30:00Z   7.5
>
"At 8:30cq_advanced_for executes the query with time range time >= '7:30' AND time <'8:30'.
cq_advanced_for writes two data points to average_passengers:"
>
    name: average_passengers
    ------------------------
    time                   mean
    2016-08-28T07:30:00Z   7.5
    2016-08-28T08:00:00Z   11.5
>
"At 9:00cq_advanced_for executes the query with time range time>='8:00' AND time <'9:00'.
cq_advanced_for writes two data points to average_passengers:"
>
    name: average_passengers
    ------------------------
    time                   mean
    2016-08-28T08:00:00Z   11.5
    2016-08-28T08:30:00Z   16
```

Note that `cq_advanced_for` calculates the result twice for each interval. CQ calculates the average of 7:30 at 8:00 and 8:30, and the average of 8:00 at 8:30 and 9:00.

The result is as follows:

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

#### Configure Execution Interval and CQ Time Range

Use `EVERY` and `FOR` in the `RESAMPLE` clause to specify the execution interval of the CQ and the length of the time range of the CQ.

```sql
CREATE CONTINUOUS QUERY "cq_advanced_every_for" ON "transportation"
RESAMPLE EVERY 1h FOR 90m
BEGIN
  SELECT mean("passengers") INTO "average_passengers" FROM "bus_data" GROUP BY time(30m)
END
```

`cq_advanced_every_for` calculates the 30-minute average of `passengers` from `bus_data` and stores the result in `average_passengers` in the database `transportation`.

`cq_advanced_every_for` is executed at 1-hour intervals, which are the same as `EVERY` intervals. Every 1 hour, `cq_advanced_every_for` runs a query covering the time period `now()` and `now()` minus the interval in `FOR`, that is, `now()` and `now()` before Time range between 90 minutes.

The following is the log output on the morning of August 28, 2016:

```sql
>
"At 8:00cq_advanced_every_for execute the query with time range time>='6:30' AND time <'8:00'.
cq_advanced_every_for writes three data points to average_passengers:"
>
    name: average_passengers
    ------------------------
    time                   mean
    2016-08-28T06:30:00Z   3
    2016-08-28T07:00:00Z   6.5
    2016-08-28T07:30:00Z   7.5
>
"At 9:00cq_advanced_every_for execute the query with time range time >= '7:30' AND time <'9:00'.
cq_advanced_every_for writes three data points to average_passengers:"
>
    name: average_passengers
    ------------------------
    time                   mean
    2016-08-28T07:30:00Z   7.5
    2016-08-28T08:00:00Z   11.5
    2016-08-28T08:30:00Z   16
```

Note that `cq_advanced_every_for` calculates the result twice for each interval. CQ calculates an average of 7:30 at 8:00 and 9:00.

The result is as follows:

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

#### Configure the Time Range of CQ and Fill in Empty Values

Use `FOR` intervals and `fill()` to change interval values without data. Note that at least one data point must be within the FOR interval for fill() to run. If no data falls within the `FOR` interval, CQ will not write any points to the target measurement.

```sql
CREATE CONTINUOUS QUERY "cq_advanced_for_fill" ON "transportation"
RESAMPLE FOR 2h
BEGIN
  SELECT mean("passengers") INTO "average_passengers" FROM "bus_data" GROUP BY time(1h) fill(1000)
END
```

`cq_advanced_for_fill` calculates the 1-hour average of `passengers` from `bus_data`, and stores the result in `average_passengers` in the database `transportation`. and will write the value `1000` during the time interval with no result.

`cq_advanced_for_fill` is executed at 1-hour intervals, the same interval as `GROUP BY time()`. Every 1 hour, `cq_advanced_for_fill` runs a query covering the time period `now()` and `now()` minus the interval in `FOR`, that is, `now()` and `now()` before A time range between two hours.

The following is the log output on the morning of August 28, 2016:

```sql
>
"At 6:00cq_advanced_for_fill executes the query with time range time>='4:00' AND time <'6:00'. cq_advanced_for_fill does not write any data points to average_passengers because bus_data has no data in that time range:"
>
"At 7:00cq_advanced_for_fill executes the query with time range time>='5:00' AND time <'7:00'. cq_advanced_for_fill writes two data points to average_passengers:"
>
    name: average_passengers
    ------------------------
    time                   mean
    2016-08-28T05:00:00Z   1000          <------ fill(1000)
    2016-08-28T06:00:00Z   3             <------ average of 2 and 4
>
[...]
>
"At 11:00cq_advanced_for_fill executes the query for the time range time >= '9:00' AND time <'11:00'. cq_advanced_for_fill writes two data points to average_passengers:"
>
    name: average_passengers
    ------------------------
    2016-08-28T09:00:00Z   20            <------ average of 20
    2016-08-28T10:00:00Z   1000          <------ fill(1000)
>
```

At **12:00**  `cq_advanced_for_fill` executes the query for the time range `time>='10:00'AND time <'12:00'`. `cq_advanced_for_fill` does not write any data points to `average_passengers` because there is no data in `bus_data` for that time range.

The result is as follows:

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

`fill(previous)` will not fill the interval with data if the previous value was outside the query time.

:::

### Common Problems about Advanced Synax

#### If `EVERY` Interval is Greater than `GROUP BY time()` Interval

If the `EVERY` interval is greater than the `GROUP BY time()` interval, the CQ is executed at the same interval as the `EVERY` interval and runs a single query that covers `now()` and `now()` minus The time range between `EVERY` intervals (not between `now()` and `now()` minus `GROUP BY time()` intervals).

For example, if the `GROUP BY time()` interval is 5m, and the `EVERY` interval is 10m, then the CQ is executed every 10 minutes. Every 10 minutes, CQ runs a query covering the period between `now()` and `now()` minus the `EVERY` interval, i.e. between `now()` and ten minutes before `now()` time range.

This behavior is intentional and prevents CQ from losing data between execution times.



## DROP CONTINUOUS QUERY

### Syntax

Delete CQ from a specified database:

```sql
DROP CONTINUOUS QUERY <cq_name> ON <database_name>
```

`DROP CONTINUOUS QUERY` returns an empty result.

### Examples

Delete the CQ `idle_hands` from the database `telegraf`:

```sql
> DROP CONTINUOUS QUERY "idle_hands" ON "telegraf"`
```



## SHOW CONTINUOUS QUERIES

### Syntax

List all CQs on an openGemini instance:

```sql
SHOW CONTINUOUS QUERIES
```

`SHOW CONTINUOUS QUERIES` group by database.

### Examples

The following shows the CQs on database `test1` and database `test2`:

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
