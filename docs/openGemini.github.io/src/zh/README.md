---
home: true
title: 首页
icon: home
heroImage: images/logo.svg
heroText: openGemini Docs
tagline: openGemini 统一文档站点
actions:
    - text: 快速上手
      link: /zh/guide/quick_start/get_started
      type: primary
    - text: 项目简介
      link: /zh/guide/introduction/introduction
      type: secondary
features:
    - title: 高性能
      # icon: http://www.opengemini.org/uploads/2022/03/ca7b37532e80da54c9df4812c793b72a.png
      details: 支持亿级时间线和PB级时序数据管理，每秒千万级数据写入和毫秒级查询响应，相比InfluxDB，简单查询性能提升2-5倍，复杂查询性能提升60倍
      # link: /zh/guide/quick_start/get_started.md
    - title: 分布式
      details: 采用MPP大规模并行处理分层架构，由ts-sql、ts-meta、ts-store三个组件组成，各组件可独立扩展，支持100+节点的大规模集群部署
    - title: 存储分析一体化
      details: 内置AI数据分析平台，提供了对时序数据的实时异常检测能力，实现了数据从存储到分析完整的闭环管理。
    - title: 高数据压缩率
      details: 采用列式存储方式，提供高效数据压缩算法，相同数据量下存储成本仅有关系型数据库的1/20，NoSQL的1/10

footer: true
---
