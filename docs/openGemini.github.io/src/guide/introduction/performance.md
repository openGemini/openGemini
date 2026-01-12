---
title: Database Performance
order: 3
---

openGemini is an open source time series database system, and it is the common wish of the majority of developers that the community will develop better and better. As one of the most critical indicators of the database, performance largely determines how far the community can go in the future. On the other hand, databases have become an important part of enterprise information construction, and database performance is one of the key factors in enterprise information construction. The performance of the database directly affects the business processes and efficiency of the enterprise. Therefore, optimizing the performance of the database is very important. Because of this, the community has always regarded performance optimization as a long-term task for the community.

## Testing Scenarios
|Item|Description|
|---|---|
| Test Tool | TSBS (https://github.com/timescale/tsbs)              |
| Test Case| devops |
| Data Model | 1 DB,1 measurement,10 tags,10 fields                               |
| Data Size| 300,000 time series; 2.592 billion points, original data size (gzip compression) 88G |
| Stand-alone or Cluster| <font color=red>stand-alone</font>                                        |
| Machine Specifications| 32U128G                                               |

> Format use "InfluxDB", and other tools also use tsbs_load_influx,tsbs_run_queries_influx etc.

## Write Performance

| Database                | Concurrencies | Writing Speed（rows/sec） | Disk Data Size |
| --------------------- | ------ | ----------------------------------------- | -------- |
| openGemini v1.1.0-rc1 | 32     | 392,915.45                                | 15GB     |
| InfluxDB 2.x (OSS)      | 32     | 87,196.63                                 | 14GB     |
| Other TSDB(OSS）              | 32     | 361,871.25                                | 20GB     |

> 1 row = 10tag + 10field
> 1 row = 10 metrics

## Comparison by Self

| Cases                  | Concurrencies | v1.1.0-rc1(Average Delay,unit:ms) | v1.0.1 (Average Delay,unit:ms) |
| --------------------- | ------ | ---------------------- | ------------------- |
| single-groupby-1-1-12 | 32     | 7.17                   | 17.07               |
| single-groupby-1-1-1  | 32     | 3.12                   | 6.67                |
| single-groupby-1-8-1  | 32     | 7.3                    | 13.35               |
| single-groupby-5-1-12 | 32     | 13.35                  | 36.37               |
| single-groupby-5-1-1  | 32     | 4.55                   | 12.41               |
| single-groupby-5-8-1  | 32     | 10.1                   | 23.01               |
| cpu-max-all-1         | 32     | 6.89                   | 19.54               |
| cpu-max-all-8         | 32     | 16.88                  | 40.01               |
| double-groupby-1      | 8      | 24,719                 | 30,408.05           |
| double-groupby-5      | 8      | 51,478.43              | 67,023.02           |
| double-groupby-all    | 8      | 77,594.52              | 85,673.76           |
| lastpoint             | 8      | 54,672.35              | 64,107.8            |
| groupby-orderby-limit | 2      | 9,225.74               | 116,212.15          |
| high-cpu-1            | 32     | 19.99                  | 37.65               |
| high-cpu-all          | 1      | 35,201.1               | 109,319.78          |

## Comparison by Other TSDB

| Cases                  | Concurrencies | openGemini v1.1.0-rc1 | InfluxDB 2.x OSS | Others TSDB(OSS)   |
| --------------------- | ---- | --------------------- | ---------------- | ---------- |
| single-groupby-1-1-12 | 32   | 7.17                  | 24.89            | 14.18      |
| single-groupby-1-1-1  | 32   | 3.12                  | 4.28             | 10.72      |
| single-groupby-1-8-1  | 32   | 7.3                   | 12.17            | 80.05      |
| single-groupby-5-1-12 | 32   | 13.35                 | 101.63           | 19.75      |
| single-groupby-5-1-1  | 32   | 4.55                  | 12.04            | 13.67      |
| single-groupby-5-8-1  | 32   | 10.1                  | 48.6             | 109.96     |
| cpu-max-all-1         | 32   | 6.89                  | 13.86            | 17.84      |
| cpu-max-all-8         | 32   | 16.88                 | 91.8             | 213.23     |
| double-groupby-1      | 8    | 24,719                | 243,356.06       | 141,016.09 |
| double-groupby-5      | 8    | 51,478.43             | 122,317.54       | 205,317.78 |
| double-groupby-all    | 8    | 77,594.52             | OOM              | >5min      |
| lastpoint             | 8    | 54,672.35             | OOM              | 826,563.36 |
| groupby-orderby-limit | 2    | 9,225.74              | OOM              | 503,419.97 |
| high-cpu-1            | 32   | 19.99                 | 23.41            | 19.78      |
| high-cpu-all          | 1    | 35,201.1              | OOM              | 查询失败   |
