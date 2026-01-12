---
title: 日志检索
order: 3
---
:::tip
创建表参考[表操作](../schema/measurement.md)
:::
## 精确匹配 (MATCH)
精确匹配是指只有整个单词与关键字相同才匹配。比如在日志检索中，根据特定错误码检索所有相关的日志信息。
```sql
> SELECT server_name，message FROM mst WHERE MATCH(message, "504") AND server_name="server0" AND time > now() - 1d
```
查询过去一天内，server0服务的全部504日志

## 短语匹配 (MATCHPHRASE)
同MATCH查询一样，短语匹配是标准全文检索中的一种最常用查询方式，MATCH 和 MATCHPHRASE都是精确匹配，不同的是，MATCHPHRASE是按短语查询，而MATCH是单个单词。
```sql
> SELECT COUNT(message) FROM mst WHERE MATCHPHRASE(message, 'GET images backnews.gif') AND time > now() - 1h
```
统计过去1小时内包含'GET images backnews.gif'内容的日志总数

## 模糊匹配 (LIKE)
模糊匹配是另一种常见的全文检索查询方式，可以返回带有某个前缀或者后缀的一类字符串，比如在业务日志中，某些错误都有固定的前缀，但是错误码是不一样的，但都表示某一类错误信息，这时你会想到使用模糊匹配
```sql
> SELECT * FROM mst WHERE message LIKE 'NET%'
```
查询包含NET开头关键字的全部日志数据
