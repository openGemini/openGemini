---
title: Stream aggregate
order: 2
---

When the amount of data increases and the timeline distribution, especially aggregation synchronization, drops by several orders of magnitude, it is recommended to use streaming computing for pre-aggregation. Streaming computing has the advantages of fast calculation, small network overhead, pure memory, and no impact on the underlying layer. Continuous query applicable reference scenario documents[Continue Query](./continue_query.html)

![Stream computing demonstration diagram](../../../static/img/guide/features/stream_1.jpg)

The overall process is shown in the figure above. If the user writes data and is specified as ```stream```, copy a copy of the data and follow the ```stream``` process. The stream calculation on the ts-store side is currently divided into two modules,``` Stream``` and ```window```. The whole is a five-level ```pipeline```. The ```Stream``` module is responsible for task management, data access and preliminary filtering. The ```Window``` module is responsible for time window filtering, calculation, and data downloading.

## CREATE STREAM

You can add a stream calculation to the timeline in the following way to achieve pre-aggregation. After adding stream calculation, data can be written and aggregated at the same time.

``` sql
CREATE STREAM test INTO db0.autogen.cpu1 ON SELECT sum("usage_user") AS "sum_usage_user" FROM "telegraf"."autogen"."cpu" group by time(1m),"cpu","host" delay 20s
```
The meaning of this statement is to create a streaming aggregation task "test" and calculate the data written to "telegraf"."autogen"."cpu" (db is Telegraf, RP is autogen, table is cpu), and the result Written into table cpu1, the aggregation method is sum("usage_user"). Among them, delay 20s represents the tolerated time delay. If the data does not arrive within the current time window 1m + 20s due to network interruption, the data will be discarded and will no longer be calculated in the current time window. New other data will be Calculated within the next time window.

## VIEW STREAM

You can use this command to view all stream calculations in the database:

```sql
SHOW STREAMS
```

## DELETE STREAM

This command can be used to delete stream calculations in the database:
```sql
DROP STREAM <name of the stream computing>
```
