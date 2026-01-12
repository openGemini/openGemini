---
title: Subscription
order: 7
---

This chapter mainly introduces the data subscription feature. Data subscription means that the write request to one openGemini cluster will be forwarded to the subscriber endpoints. The following content will let you know how to use this feature.

## Configuration Items
Configurations items are:
```toml
[subscriber]
  enabled = false
  http-timeout = "30s"
  insecure-skip-verify = false
  https-certificate = ""
  write-buffer-size = 100
  write-concurrency = 15
```
* `enabled`: this feature works only when `enabled` is true
* `http-timeout`: http timeout of http client
* `insecure-skip-verify`: whether to use unsafe https connection with subscribers or not.
* `https-certificat`: location of CA certs file, if this item is empty string, default cert of your system will be used.
* `write-buffer-size`: number of in-flight write requests buffered in the channel.
* `write-concurrency`: number of goroutines to consume a single write channel. Default value is 2 * cpu num.

## Create Subscriptions
You can use the following commands in ts-cli to create subscription:
```sql
-- Pattern:
CREATE SUBSCRIPTION "<subscription_name>" ON "<db_name>"."<retention_policy>" DESTINATIONS <ALL|ANY> "<subscription_endpoint_host>"
-- use default retention policy
CREATE SUBSCRIPTION "<subscription_name>" ON "<db_name>" DESTINATIONS <ALL|ANY> "<subscription_endpoint_host>"

-- Examples:
-- Create a SUBSCRIPTION on database 'mydb' and retention policy 'autogen' that sends data to 'example.com:9090' via HTTP.
CREATE SUBSCRIPTION "sub0" ON "mydb"."autogen" DESTINATIONS ALL 'http://example.com:9090'

-- Create a SUBSCRIPTION on database 'mydb' and default retention policy that round-robins the data to 'h1.example.com:9090' and 'h2.example.com:9090' via https.
CREATE SUBSCRIPTION "sub0" ON "mydb" DESTINATIONS ANY 'https://h1.example.com:9090', 'https://h2.example.com:9090'
```
If the subscriber have opened the authorization, you can change the url to set basic auth:
```sql
CREATE SUBSCRIPTION "sub0" ON "mydb"."autogen" DESTINATIONS ALL 'http://username:password@example.com:9090'
```

## Show Subscription
Show subscription is quite simple, there is only one kind of sql pattern:
```sql
show subscriptions
```

## Drop Subscription
You can use the following commands in ts-cli to drop subscription:
```sql
--Pattern:
--Drop subscription on the specific db & retention policy
DROP SUBSCRIPTION "<subscription_name>" ON "<db_name>"."<retention_policy>"
--Example:
DROP SUBSCRIPTION "sub0" ON "db0"."autogen"

--Pattern:
--Find the specific subscription on the db, and drop it
DROP SUBSCRIPTION "<subscription_name>" ON "<db_name>"
--Example:
DROP SUBSCRIPTION "sub0" ON "db0"

--Pattern:
--Drop all subscriptions on the specific database
DROP ALL SUBSCRIPTIONS ON "<db_name>"
--Example:
DROP ALL SUBSCRIPTIONS ON "db0"

--Pattern:
--Drop all subscriptions
DROP ALL SUBSCRIPTIONS
```
