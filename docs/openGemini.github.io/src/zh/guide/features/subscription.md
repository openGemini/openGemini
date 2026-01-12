---
title: 数据订阅
order: 7
---

这一章主要介绍数据订阅功能。数据订阅意味着向一个openGemini集群发送的写入请求可以被转发到其它订阅结点。接下来的内容将介绍如何使用订阅功能。

## 配置项
配置项包括：
```toml
[subscriber]
  enabled = false
  http-timeout = "30s"
  insecure-skip-verify = false
  https-certificate = ""
  write-buffer-size = 100
  write-concurrency = 15
```
* `enabled`: 只有当 `enabled` 为true时，这个功能才会运行
* `http-timeout`: http客户端转发请求的超时时间
* `insecure-skip-verify`: 是否与订阅结点建立不安全的https连接
* `https-certificat`: CA证书存放路径，如果这一项是空字符串，将会使用系统默认的证书
* `write-buffer-size`: 写入通道中暂存的未来得及转发的写入请求数量
* `write-concurrency`: 消费同一个通道的协程数量。默认值是cpu数*2

## 创建订阅
可以在ts-cli中使用下列命令创建订阅：
```sql
-- Pattern:
CREATE SUBSCRIPTION "<subscription_name>" ON "<db_name>"."<retention_policy>" DESTINATIONS <ALL|ANY> "<subscription_endpoint_host>"
-- 使用默认的retention policy
CREATE SUBSCRIPTION "<subscription_name>" ON "<db_name>" DESTINATIONS <ALL|ANY> "<subscription_endpoint_host>"

-- Examples:
-- Create a SUBSCRIPTION on database 'mydb' and retention policy 'autogen' that sends data to 'example.com:9090' via HTTP.
CREATE SUBSCRIPTION "sub0" ON "mydb"."autogen" DESTINATIONS ALL 'http://example.com:9090'

-- Create a SUBSCRIPTION on database 'mydb' and default retention policy that round-robins the data to 'h1.example.com:9090' and 'h2.example.com:9090' via https.
CREATE SUBSCRIPTION "sub0" ON "mydb" DESTINATIONS ANY 'https://h1.example.com:9090', 'https://h2.example.com:9090'
```
如果订阅节点开启了鉴权，可以通过修改url来设置认证：
```sql
CREATE SUBSCRIPTION "sub0" ON "mydb"."autogen" DESTINATIONS ALL 'http://username:password@example.com:9090'
```

## 显示订阅
显示订阅是非常简单的，只有一种SQL语句写法：
```sql
show subscriptions
```

## 删除订阅
可以在ts-cli中使用如下命令删除订阅：
```sql
--Pattern:
--在指定的db_name和retention_policy上删除订阅
DROP SUBSCRIPTION "<subscription_name>" ON "<db_name>"."<retention_policy>"
--Example:
DROP SUBSCRIPTION "sub0" ON "db0"."autogen"

--Pattern:
--在db_name上找到指定的订阅，并删除
DROP SUBSCRIPTION "<subscription_name>" ON "<db_name>"
--Example:
DROP SUBSCRIPTION "sub0" ON "db0"

--Pattern:
--在指定的db上删除所有订阅
DROP ALL SUBSCRIPTIONS ON "<db_name>"
--Example:
DROP ALL SUBSCRIPTIONS ON "db0"

--Pattern:
--删除所有订阅
DROP ALL SUBSCRIPTIONS
```
