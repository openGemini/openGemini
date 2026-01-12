---
title: Scale-out
order: 2
---

::: tip
- **ts-sql**为无状态节点，扩容不涉及数据迁移
- **ts-meta**无需扩容
- **ts-store**在本地存储数据，扩容后，新写入的数据会写到新加入的节点上，不涉及数据迁移。<font color=red>需要注意的是，ts-store扩容后，并不意味着立刻就能往新的节点写数据，必须要等到新的shardgroup duration开始后才会写入新数据到新节点，默认情况下，数据按时间线打散，新节点的数据只是一部分时间线数据。</font>
**举个例子：** 假设从2022年8月1日开始计算，shardgroup duration为7天，如果8月3日集群扩容了ts-store节点，那么要等到8月8日，新的shardgroup duration周期开启后才有数据写入新节点。

:::
以扩ts-store组件为例，按部署方式可分为三种情况：

1. **新增组件ts-store部署在已有节点上，该节点已存在ts-store组件**
这种情况下，ts-store的各个端口需要重新分配。
部署方式如图所示：

![6](https://user-images.githubusercontent.com/49023462/200800553-73d0bb25-de2c-4cf2-b401-8d8ddb00ded2.png)

为新增节点单独准备配置文件，具体配置如下：
```
[common]
# 保持不变
meta-join = [meta-join = ["192.168.0.1:8092", "192.168.0.2:8092", "192.168.0.3:8092"]
…
[data]
store-ingest-addr = "192.168.0.3:8402"
store-select-addr = "192.168.0.3:8403"
store-data-dir = "/path/to/openGemini/data/2"
store-wal-dir = "/path/to/openGemini/data/2"
store-meta-dir = "/path/to/openGemini/data/meta/2"
…
[logging]
# 建议修改目录
path = "/path/openGemini/logs"
[gossip]
bind-address = "192.168.0.3"
store-bind-port = 8012
# 保持不变
members = ["192.168.0.1:8010", "192.168.0.2:8010", "192.168.0.3:8010"]
```
2. **新增组件ts-store部署在已有节点上，该节点无ts-store组件**
这种情况下，不需要重新分配端口，除非端口被其他应用程序占用。
部署方式如图所示：

![7](https://user-images.githubusercontent.com/49023462/200800580-2d1b0f70-fb89-42bd-864f-29da12cd3336.png)

可以该节点其他组件共用同一个配置文件，只需修改ts-store对应的配置项即可(IP和目录)
```
[data]
store-ingest-addr = "192.168.0.2:8400"
store-select-addr = "192.168.0.2:8401"
store-data-dir = "/path/to/openGemini/data/1"
store-wal-dir = "/path/to/openGemini/data/1"
store-meta-dir = "/path/to/openGemini/data/meta/1"
…
[logging]
# 建议修改目录
path = "/path/openGemini/logs"
[gossip]
bind-address = "192.168.0.2"
store-bind-port = 8011
# 保持不变
members = ["192.168.0.1:8010", "192.168.0.2:8010", "192.168.0.3:8010"]
```
3. **新增组件ts-store部署在新节点上，该节点无ts-store组件**
这种情况下，不需要重新分配端口，除非端口被其他应用程序占用。
部署方式如图所示：

![8](https://user-images.githubusercontent.com/49023462/200800601-896711db-17ee-45c5-8cee-1b9f4d342e63.png)

配置文件的配置与第二种情况一样
```
[common]
# 保持不变
meta-join = [meta-join = ["192.168.0.1:8092", "192.168.0.2:8092", "192.168.0.3:8092"]
…
[data]
store-ingest-addr = "192.168.0.4:8400"
store-select-addr = "192.168.0.4:8401"
store-data-dir = "/path/to/openGemini/data/1"
store-wal-dir = "/path/to/openGemini/data/1"
store-meta-dir = "/path/to/openGemini/data/meta/1"
…
[logging]
# 建议修改目录
path = "/path/openGemini/logs"
[gossip]
bind-address = "192.168.0.4"
store-bind-port = 8011
# 保持不变
members = ["192.168.0.1:8010", "192.168.0.2:8010", "192.168.0.3:8010"]
```
