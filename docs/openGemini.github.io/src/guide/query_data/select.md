---
title: Explore data
order: 1
---

## Sample data

Before we start exploring the data, to better demonstrate the syntax below, let's import publicly available data from the National Oceanic and Atmospheric Administration's (NOAA) Center for Operational Oceanic Products and Services, first follow the document [Sample data](../reference/sample_data.md) to import data.

## Clients

Before learning the query statement, you need to use the client ts-cli to connect to openGemini:

```shell
~$ ts-cli -database NOAA_water_database
openGemini CLI 0.1.0 (rev-revision)
Please use `quit`, `exit` or `Ctrl-D` to exit this program.
>
```
## Syntax
openGemini is compatible with InfluxQL. For detail InfluxQL’s SELECT statement and useful query syntax for exploring your data, see [InfluxQL’s SELECT statement](https://docs.influxdata.com/influxdb/v1.8/query_language/explore-data/#the-where-clause)
