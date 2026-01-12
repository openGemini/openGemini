---
title: 如何开发一个系统函数
order: 2
---

## 新增一个聚合算子步骤如下

1. open_src/influx/query/compile.go, 在compileExpr()或者compileFunction()方法中添加新聚合函数名称和编译实现。编译的主要作用是完成函数的参数合法性检查，以及compiledStatement属性填充。

2. 在engine/executor/agg_iterator.gen.go.tmpl中新增Iterator, 并实现对应的reduce()和merge()方法，再通过engine/executor/call_processor.go的NewProcessors()方法完成对迭代器的注册。

上述步骤暂不考虑算子能力下沉。

在此之前，您需要先对整体的计算引擎框架和流程有所了解，参考[openGemini计算引擎框架](../kernel/query_engine.md)
