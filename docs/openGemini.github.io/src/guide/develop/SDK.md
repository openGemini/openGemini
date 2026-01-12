---
title: Drivers
order: 1
---

## Compatible with InfluxDB SDK

openemini is compatible with InfluxDB, so you can use the InfluxDB SDK to develop openGemini applications. You can click the link below for the installation and usage, which can be found in the github repository.
- [C/C++](https://github.com/openGemini/openGemini/blob/main)
- [Java](https://github.com/influxdata/influxdb-java)
- [Java(support connecting more than one ts-sql)](https://github.com/xiangyu5632/influxdb-java)
- [JavaScript](https://github.com/node-influx/node-influx)
- [Python](https://github.com/influxdata/influxdb-python)
- [GO](https://github.com/influxdata/influxdb1-client)
- [Ruby](https://github.com/influxdata/influxdb-ruby)
- [PHP](https://github.com/influxdata/influxdb-php)

## openGemini SDK

The SDK of openGemini is under development, and Python, Java, and Go will be developed first, and it is planned to be completed in August-September

### The features of openGemini's SDK
- Load balancing

Support connect multiple ts-sql, and automatic load balancing

- Client cache

Support the client to cache some query results to improve query efficiency

- Retry on failure

When the client encounters network problems or other failures, causing query or writing failures, try to re-establish the connection

- Support Arrow protocol

The client supports Arrow protocol writing, and the writing performance will be improved by 300%

- Other optimizations

For example, batchsize is automatically adjusted

:::tip
These features may be available in multiple versions
:::
