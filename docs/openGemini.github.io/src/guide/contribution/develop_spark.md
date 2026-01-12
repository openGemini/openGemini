---
title: Connect openGemini to Spark
order: 5
---

## Interconnecting with Spark using the Arrow Flight protocol
openGemini supports querying data using the Arrow Flight protocol, allowing you to read data stored in openGemini from Spark via Arrow Flight.
The official Arrow Flight does not yet provide a connector for Spark. You can refer to the open-source [Spark-Flight-Connector](https://github.com/qwshen/spark-flight-connector) for adaptation and modification.
1. clone Spark-Flight-Connector source code
```shell
> git clone https://github.com/qwshen/spark-flight-connector
```
2. To connect to openGemini via Spark-Flight-Connector, some additional adaptations are required.

Modify the forTable method in com/qwshen/flight/Table.java
```
public static Table forTable(String tableName, String columnQuote) {
        Function<String, Boolean> isQuery = (t) -> t.replaceAll("[\r|\n]", " ").trim().toLowerCase().matches("^select .+ [from]?.+");
        return new Table(isQuery.apply(tableName) ? String.format("(%s)", tableName) : tableName, columnQuote);
    }
```
Modify the `getOrCreate` method in `com/qwshen/flight/Client.java` to add a `query_params` parameter.
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

3. Next, data reading can proceed.

Note: openGemini supports Influx-style syntax and has only made some adaptations to meet Spark's data format requirements. It does not support standard SQL, so it only supports a limited set of functions.

Currently, the calculations pushed from Spark to openGemini include:

Aggregation: max, min, count, sum
   
Predicates: IsNotNull, IsNull, EqualNullSafe are not supported.

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
