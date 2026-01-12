---
title: Aggregates
order: 1
---

This chapter mainly introduces the following functions:
| aggregate function | description |
| --- | --- |
| COUNT() | Returns the number of non-null field values |
| **COUNT(time)** | the total amount of data by time column |
| MEAN() | Returns the arithmetic average of field values |
| SUM () | Returns the sum of field values. |
| MODE()| Returns the most frequent value in a list of field values|
| STDDEV() | Returns the standard deviation of field values |
| MEDIAN() | Returns the middle value from a sorted list of field values |
| SPREAD() | Returns the difference between the minimum and maximum field values |
| DISTINCT() | Returns the list of unique field values |
| **IRATE()** | ( \|An - An-1\| )/( \|Tn-1 - Tn\| ) |
| **RATE()**| ( \|A1-A2\| )/( \|T1-T2\|) |
| MOVING_AVERAGE() | Returns the rolling average across a window of subsequent field values |
| HOLT_WINTERS() | Returns N number of predicted field values|
| CUMULATIVE_SUM() | Returns the running total of subsequent field values |
| DERIVATIVE() | Returns the rate of change between subsequent field values |
| DIFFERENCE() | Returns the result of subtraction between subsequent field values |
| ELAPSED()	| Returns the difference between subsequent field value’s timestamps |
| NON_NEGATIVE_DERIVATIVE()	|Returns the non-negative rate of change between subsequent field values |
| NON_NEGATIVE_DIFFERENCE()	|Returns the non-negative result of subtraction between subsequent field values |

::: tip

The bold part in the list is the unique method of openGemini. Other methods are compatible with the usage of InfluxDB. [You can refer to the corresponding function usage of InfluxDB](https://docs.influxdata.com/influxdb/v1.8/query_language/functions/#content).

The community is working hard to complete the relevant documents.
:::

## COUNT(time)

## IRATE

## RATE
