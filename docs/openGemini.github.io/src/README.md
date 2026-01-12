---
home: true
title: Home
icon: home
heroImage: images/logo.svg
heroText: openGemini Docs
tagline: openGemini document site
actions:
    - text: Quick Start
      link: /guide/quick_start/get_started
      type: primary
    - text: Introduction
      link: /guide/introduction/introduction
      type: secondary
features:
    - title: high performance
      # icon: http://www.opengemini.org/uploads/2022/03/ca7b37532e80da54c9df4812c793b72a.png
      details: Support billion-level timeline and PB-level time series data management, write tens of millions of data per second and query response at millisecond level. Compared with InfluxDB, the performance of simple query is improved by 2-5 times, and the performance of complex query is improved by 60 times
      # link: /zh/guide/quick_start/get_started.md
    - title: distributed
      details: It adopts MPP large-scale parallel processing layered architecture, which consists of three components ts-sql, ts-meta, and ts-store. Each component can be independently expanded, and supports large-scale cluster deployment of 100+ nodes.
    - title: Integration of storage analysis
      details: The built-in AI data analysis platform provides real-time anomaly detection capabilities for time series data, and realizes complete closed-loop management of data from storage to analysis.
    - title: high data compression
      details: Columnar storage method is adopted to provide efficient data compression algorithm. Under the same amount of data, the storage cost is only 1/20 of relational database and 1/10 of NoSQL.

# 采用默认footer /.vuepress/theme.ts
footer: true
---
