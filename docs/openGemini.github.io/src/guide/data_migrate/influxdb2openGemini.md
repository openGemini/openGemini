---
title: Influx2openGemini
order: 1
---

## 仓库地址

[data-migration-tools](https://github.com/openGemini/data-migration-tools)

工具名称是 dataMigrate。 它用于将 InfluxDB 数据迁移到 openGemini。
dataMigrate直接从InfluxDB的TSM文件中读取数据写入openGemini。
:::tip
如果源数据库的表存在上千字段时，该工具需要更大的内容，因为在内部要把所有列读取后再组装成行再写入openGemini，这部分比较耗内存资源
:::

## 快速开始

- **环境信息**
Go version >1.16

Setting Environment Variables
```
> export GOPATH=/path/to/dir
> export GO111MODULE=on
> export GONOSUMDB=*
> export GOSUMDB=off
```

- **编译**
```
> bash build.sh
```

- **数据迁移命令**
```
> dataMigrate --from path/to/tsm-file --to ip:port --database dbname
```

```
Usage: dataMigrate [flags]

-database string
Optional: the database to read
-end string
Optional: the end time to read (RFC3339 format)
-from string
Data storage path (default "/var/lib/Influxdb/data")
-retention string
Optional: the retention policy to read (requires -database)
-start string
Optional: the start time to read (RFC3339 format)
-to string
Destination host to write data to (default "127.0.0.1:8086",which is the openGemini service default address)
```
