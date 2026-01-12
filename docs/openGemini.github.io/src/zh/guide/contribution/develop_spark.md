---
title: 如何将openGemini对接到spark
order: 5
---

## 使用arrow flight协议对接spark
openGemini支持使用arrow flight协议查询数据，可以使用arrow flight从Spark读取openGemini中存储的数据。 
Arrow Flight官方暂未提供支持Spark的连接器。可以参考开源的[Spark-Flight-Connector](https://github.com/qwshen/spark-flight-connector)进行适配修改。
1. 下载Spark-Flight-Connector源码
```shell
> git clone https://github.com/qwshen/spark-flight-connector
```
2. 要通过Spark-Flight-Connector连接openGemini，还需要做一些适配

修改com/qwshen/flight/Table.java的forTable方法
```
public static Table forTable(String tableName, String columnQuote) {
        Function<String, Boolean> isQuery = (t) -> t.replaceAll("[\r|\n]", " ").trim().toLowerCase().matches("^select .+ [from]?.+");
        return new Table(isQuery.apply(tableName) ? String.format("(%s)", tableName) : tableName, columnQuote);
    }
```
修改com/qwshen/flight/Client.java的getOrCreate方法，增加一个query_params参数
```
public static synchronized Client getOrCreate(Configuration config) {
    String cs = config.getConnectionString();
    if (!Client._clients.containsKey(cs)) {
        ...
        final HeaderCallOption clientProperties = (!callHeaders.keys().isEmpty()) ? new HeaderCallOption(callHeaders) : null;

        final CallHeaders queryHeaders = new FlightCallHeaders();
        queryHeaders.insert("query_params", "{\"query_type\": \"sql\"}");
        final HeaderCallOption queryOptions = new HeaderCallOption(queryHeaders);
        Client._clients.put(cs, new Client(client, authenticate(client, config.getUser(), config.getPassword(),
            config.getBearerToken(), clientProperties), queryOptions, cs, allocator));
    }
    return Client._clients.get(cs);
}
```

3. 接下来就能进行数据读取

注意：openGemini支持influx风格的语法，仅是针对spark的数据格式要求进行了一些适配，并不是支持标准sql，因此仅支持部分的功能函数。

目前在spark支持下推到openGemini的计算：

聚合：max, min, count, sum

谓词: 不支持IsNotNull, IsNull, EqualNullSafe

```
Dataset<Row> df = spark.read()
            .format("com.qwshen.flight.spark.FlightSource")
            // server arrow flight host 
            .option("host", "127.0.0.1")
            // server arrow flight port 
            .option("port", "8087")
            // username
            .option("user", "xxx")
            // password
            .option("password", "xxx")
            // enclose column names with "
            .option("column.quote", "\"")
            // measurement name to query, can also be sql here
            .option("table", "db0..mst")
            .load();
        df.show();
```
