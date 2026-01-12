---
title: One-click deployment (Recommended)
order: 2
---

## Cluster deployment

Please refer to the [Gemix Usage Guide]() to deploy the openGemini cluster.

## Topological (For reference)

topology information

| component | number | machine specs                                                | IP                              |
| :-------- | :----- | :----------------------------------------------------------- | :------------------------------ |
| ts-meta   | 3      | 4 VCore 8 GiB 100 GiB                                        | 10.0.1.11, 10.0.1.12, 10.0.1.13 |
| ts-sql    | 3      | 16 VCore 32 GiB 100 GiB                                      | 10.0.1.11, 10.0.1.12, 10.0.1.13 |
| ts-store  | 3      | 16 VCore 32 GiB 2 TiB (NVMe SSD) ，try to achieve better disk performance | 10.0.1.11, 10.0.1.12, 10.0.1.13 |

## Topology file

Refer to the [configuration example]().
