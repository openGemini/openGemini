---
title: 驱动程序
order: 1
---

## 兼容SDK

openGemini兼容InfluxDB，因此可以使用InfluxDB的SDK开发openGemini相关应用，安装和用法Demo可以点击下方连接，在对应github仓库中可以找到。
- [C/C++](https://github.com/openGemini/openGemini/blob/main)
- [Java](https://github.com/influxdata/influxdb-java)
- [Java(支持集群负载均衡)](https://github.com/xiangyu5632/influxdb-java)
- [JavaScript](https://github.com/node-influx/node-influx)
- [Python](https://github.com/influxdata/influxdb-python)
- [GO](https://github.com/influxdata/influxdb1-client)
- [Ruby](https://github.com/influxdata/influxdb-ruby)
- [PHP](https://github.com/influxdata/influxdb-php)

## 自研SDK

openGemini SDK除基本读写功能外，还将支持如下特性：

1. **负载均衡**

   针对openGemini集群存在多个ts-sql的情况，SDK可以同时连接多个ts-sql，根据指定负载均衡算法选择合适的ts-sql处理请求。

2. **健康检测**

   SDK在发送业务请求之前，对当前的网络和ts-sql的服务状态进行检测，提前发现问题。

3. **批处理**

   支持数据批量写入和单点写入自动转为批处理，提升数据写入效率。

4. **鉴权和传输加密**

   支持用户密码鉴权和HTTPS传输加密

5. **数据压缩传输**

   支持数据Gzip压缩后发给数据库服务端，节省网络传输带宽，但一定程度会增加服务侧的开销。

6. **DDL操作**

   支持`create database`, `drop database`, `create retention policy`, `show measurements`等独立接口，便于应用自动化集成。

7. **多种数据格式写入**

   支持InfluxDB Line Protocol和Apache Arrow等多种数据格式写入

若对SDK有新的功能需求，欢迎您在社区进行反馈：

https://github.com/openGemini/openGemini/issues/427



:::tip
以上为规划功能，可能在多个版本中提供
:::
