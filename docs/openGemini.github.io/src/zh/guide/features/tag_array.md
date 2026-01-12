---
title: TAG数组
order: 8
---
这一章主要介绍tag数组功能。

## **背景**

在合部场景下，为了节省带宽，上传者会将同一台机器的各个组件指标打包上报，这种情况下会将公共字段部分合并上报。如果数据库支持TAG数组功能，将会极大节省这种情况下的带宽。

:::tip

ts-cli暂不支持tag array语法，可使用Influx客户端进行使用。

:::

## **功能及优点**

openGemini数据库支持数组类型的tag，而且分组、过滤时，数据库自动识别数组tag，并将其拆开，然后按照正常tag处理。
openGemini支持选择tag数组进行查询插入，同时兼容正常查询插入。
支持数组tag会降低存储成本和写入流量。

## **写入流程**

- 开启tag数组功能

```sql
> create database db0 tag attribute array
```

通过创建数据库时指定支持tag array，只能一开始就指定，暂不支持修改已有数据库支持tag array，若用以如下方式创建的数据库默认是不支持tag array 功能

```sql
> create database db0
```

- 实际写入模型
```sql
insert cpu,tk1=value1,tk2=value2,tk3=[value3,value33,value333] value=3 1670491328000000000

insert cpu,tk1=value1,tk2=[value2,value22,value222],tk3=[value3,value33,value333] value=2 1670491329000000000
```
- 逻辑写入模型
```sql
insert cpu,tk1=value1,tk2=value2,tk3=value3 value=3 1670491328000000000
insert cpu,tk1=value1,tk2=value2,tk3=value33 value=3 1670491328000000000
insert cpu,tk1=value1,tk2=value2,tk3=value333 value=3 1670491328000000000
insert cpu,tk1=value1,tk2=value2,tk3=value3 value=2 1670491329000000000
insert cpu,tk1=value1,tk2=value22,tk3=value33 value=2 1670491329000000000
insert cpu,tk1=value1,tk2=value222,tk3=value333 value=2 1670491329000000000
```
可以发现，数组内有几个成员就会被转换为几条语句，不同数组的长度一致，不同数组内相同位置数据构成一个元组进行写入。

## **写入要求**

1. tagvalue可以不是数组
2. 存在所有的tagvalue数组长度要保持一致
3. serieskey按照数组下标排列

## **写入举例**

```sql
insert cpu,tk1=value1,tk2=value2,tk3=[value3,value33,value333] value=3
insert cpu,tk1=value1,tk2=[value2,value22,value222],tk3=[value3,value33,value333] value=2
```
