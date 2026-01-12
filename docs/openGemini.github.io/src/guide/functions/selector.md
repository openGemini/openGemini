---
title: Selectors
order: 3
---
This chapter mainly introduces the following functions:

| selector functions | descriptions |
| --- | --- |
| FIRST() | Returns the field value with the oldest timestamp |
| LAST() | Returns the field value with the most recent timestamp |
| MIN() | Returns the lowest field value |
| MAX() | Returns the greatest field value |
| TOP() | Returns the greatest N field values |
|BOTTOM()| Returns the smallest N field values |
| PERCENTILE() | Returns the Nth percentile field value |
| SAMPLE() | Returns a random sample of N field values |
|  SPREAD() | Returns the difference between the minimum and maximum field values |
| **PERCENTILE_OGSKETCH()** | Returns the Nth percentile(approximate) field value |

::: tip

The bold part in the list is the unique method of openGemini. Other methods are compatible with the usage of InfluxDB. [You can refer to the corresponding function usage of InfluxDB](https://docs.influxdata.com/influxdb/v1.8/query_language/functions/#content).

The community is working hard to complete the relevant documents.
:::
