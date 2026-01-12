---
title: About openGemini
order: 1
---
## Introduction

OpenGemini is a global open-source cloud-native distributed time series database designed and developed by [HUAWEI CLOUD Database Innovation Lab ](https://www.huaweicloud.com/lab/clouddb/home.html). It provides standalone and distributed versions with excellent read and write performance and efficient data analysis capabilities. Supports mainstream development languages and multi-form deployment (such as cloud, Docker, and physical machine), integrates storage and analysis, and is easy to expand. It is dedicated to efficiently storing and analyzing massive time series data in IoT and O&M monitoring scenarios to further reduce enterprise operation and O&M costs and improve product quality and production efficiency.

## Five Core Features

### High Performance

Store and query data quickly and efficiently with automatic partitioning, LSM-based storage techniques, and better data process engineering

### High Scalability

Adopting MPP architecture, support distributed cluster deployment and can be flexibly expanded as business grows to meet higher load requirements.

### High Cardinality

A new high cardinality storage engine solves problems such as excessive index memory usage and low read and write performance

### Integrated storage and analysis

The built-in AI data analysis platform provides real-time anomaly detection capabilities for time series data and implements closed-loop management from data storage to data analysis.

### Observability

All observability-data such as metrics, logs, and traces are stored in openGemini, simplifying the storage and analysis

### Low Storage Costs

Column-based storage is used to provide efficient data compression algorithm. With the same data volume, the storage cost is only 1/20 of that of relational databases and 1/10 of that of NoSQL.

## Typical Application Scenarios

### Monitor（Observability）

| <span style="display:inline-block;width:80pt">categorize</span> | Application Examples                                         |
| ------------------------------------------------------------ | ------------------------------------------------------------ |
| DevOps                                                       | Stores monitoring metrics, traces, and logs of IT infrastructure and Cloud Native applications, such as cloud services, containers, microservices, and physical servers. It implements real-time status monitoring, exception detection and prediction, root cause analysis, alarm reporting, and statistics analysis. |
| Network                                                      | Stores network device and system data, such as uplink and downlink bandwidth, traffic, application API success rate, and access IP address. It implements real-time network performance insight, real-time error alarm, and network attack detection. |



### Internet of Things (IoT)

| <span style="display:inline-block;width:80pt">categorize</span> | Sub-industry                                                 | Application Examples                                         |
| ------------------------------------------------------------ | ------------------------------------------------------------ | ------------------------------------------------------------ |
| Industrial Internet of Things                                | Smart manufacturing, Smart energy, Electricity (water), New energy (photovoltaic and wind power), Smart mines, Agriculture and animal husbandry | Take smart manufacturing as an example. Equipment management: equipment running status monitoring and predictive maintenance; Quality management: fault prediction and analysis, quality tracing, process optimization, and online quality monitoring; Energy management: energy consumption analysis and power consumption monitoring; Others: BI/Report |
| Enterprise IoT                                               | Smart City, Smart Fire Protection, Smart Building, Environmental Monitoring, Smart Logistics, and Smart Campus | Take smart buildings as an example. Device management: device health check (air conditioners, fans, and elevators), device predictive maintenance, and fault demarcation; Monitoring alarms: exception detection and prediction; Energy management: power consumption monitoring, energy consumption analysis and prediction; Environmental monitoring: air quality monitoring, indoor temperature monitoring |
| Consumer Internet of Things                                  | Smart cars, Smart homes, Smart wearables, Shared economy (sharing bicycles, shared cars, and shared electric vehicles), and smart old-age pension | Take smart cars as an example. Vehicle management: real-time vehicle status monitoring and vehicle abnormality detection; People-vehicle interaction: query of running positions, track playback, and driving behavior; Product innovation and optimization: driving behavior analysis, vehicle condition analysis |
