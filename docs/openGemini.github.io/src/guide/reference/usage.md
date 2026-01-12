---
title: Telemetry data
order: 2
---


# 遥测数据

开启遥测后，openGimini 会收集使用情况信息，并将这些信息分享给 openGemini 用于改善产品。

::: warning

在**任何情况**下，集群中用户存储的数据都**不会**被收集。

:::

## openGemini

当 openGemini 遥测功能开启时，openGemini 集群将会在meta启动**1小时后**收集部分环境和版本信息，包括（但不限于）：

- 随机生成的集群标识符
- 集群的部署情况，包括组件版本号、操作系统和操作系统架构等

## 开启遥测功能

**修改配置文件中的遥测配置**

```
[common]
  report-enable = true
```

**使用修改后的配置文件，启动openGemini**
