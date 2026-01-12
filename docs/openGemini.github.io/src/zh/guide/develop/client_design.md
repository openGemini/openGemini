---
title: Client Design
order: 2
---

# 背景

由于Influxdb 1.X的客户端已经基本处于维护状态，同时openGemini仍在不断发展中，为了能够更好地支持openGemini，如支持对接多个服务端地址、支持对接Apache
Arrow Flight协议等，社区决定开发属于openGemini自己的客户端SDK。

# 客户端SDK规划功能

- 支持对接多个服务端地址
- 支持对接Apache Arrow Flight协议
- 支持Sql查询、结构化查询、写入、批量写入等，详见下文UML图
- 默认超时，连接超时10秒，读写超时30秒

本文的方法假定编程语言不支持重载，如编程语言支持重载，可以对方法名进行一些优化调整。

# 客户端构造参数设计

```mermaid
classDiagram
    class OpenGeminiClient {
        + List~Address~ addresses
        + AuthConfig authConfig // nullable, if null, means no auth
        + BatchConfig batchConfig // nullable, if null, means batch is disabled
        + timeout
        + connectTimeout
        + enum contentType // json, csv, msgpack
        + enum compressMethod // gzip, zstd, br
        + TlsConfig tlsConfig // nullable, language specific
        + void close()
        + GrpcConfig grpcConfig // if null, call WriteByGrpc will nothing to do, otherwise send write request by gRPC
    }

    class Address {
        + String host
        + int Port // in rust, it is u16
    }

    class AuthConfig {
        + AuthType authType // enum Password, Token. The server currently does not support the Token type, and the SDK implementation for Token is incomplete.
        + String username
        + String password
        + String token
    }

    class BatchConfig {
        + Duration batchInterval // must be greater than 0
        + int batchSize // must be greater than 0
    }

    class GrpcConfig {
        + List~Address~ addresses
        + AuthConfig authConfig
        + BatchConfig batchConfig
        + enum compressMethod // gzip, zstd, br
        + TlsConfig tlsConfig
        + timeout
    }

    OpenGeminiClient "1" *-- "many" Address: contains
    OpenGeminiClient *-- AuthConfig: contains
    OpenGeminiClient *-- BatchConfig: contains
```

# Database & RetentionPolicy管理设计

```mermaid
classDiagram
    class OpenGeminiClient {
        + void CreateDatabase(String database)
        + void CreateDatabaseWithRp(String database, rpConfig RpConfig)
        + String[] ShowDatabases()
        + void DropDatabase(String database)
        + void CreateRetentionPolicy(String database, RpConfig rpConfig, bool isDefault)
        + void UpdateRetentionPolicy(String database, RpConfig rpConfig, bool isDefault)
        + RetentionPolicy[] ShowRetentionPolicies(String database)
        + void DropRetentionPolicy(String database, String retentionPolicy)
        + void CreateMeasurement(CreateMeasurementBuilder builder)
        + String[] ShowMeasurements(ShowMeasurementBuilder builder)
        + void DropMeasurement(String database, String retentionPolicy, String measurement)
        + Map[String]String[] ShowTagKeys(ShowTagKeysBuilder builder)
        + String[] ShowTagValues(ShowTagValuesBuilder builder)
        + Map[String]Map[String]String ShowFieldKeys(String database, Option<String> measurement)
        + String[] ShowSeries(ShowSeriesBuilder builder)
    }
    class RpConfig {
        + String Name // non-null
        + String Duration // non-null
        + String ShardGroupDuration // nullable
        + String IndexDuration // nullable
    }
    class CreateMeasurementBuilder {
        + CreateMeasurementBuilder Tags(String[] tags)
        + CreateMeasurementBuilder FieldMap(map[String]FieldType fields)
        + CreateMeasurementBuilder ShardType(ShardType shardType)
        + CreateMeasurementBuilder ShardKeys(String[] shardKeys)
        + CreateMeasurementBuilder FullTextIndex()
        + CreateMeasurementBuilder IndexList(String[] indexes)
        + CreateMeasurementBuilder EngineType(EngineType engineType)
        + CreateMeasurementBuilder PrimaryKey(String[] primaryKeys)
        + CreateMeasurementBuilder SortKeys(String[] sortKeys)
        + String build()
    }
    class ShowMeasurementBuilder {
        + ShowMeasurementBuilder Filter(ComparisonOperator operator, String regex)
        + String build()
    }
    class FieldType {
        <<enum>>
        Bool  // BOOL
        Int64  // INT64
        Float64 // FLOAT64
        String // STRING
    }
    class ShardType {
        <<enum>>
        Hash // HASH
        Range // RANGE
    }
    class EngineType {
        <<enum>>
        ColumnStore // columnstore
    }
    class ShowTagKeysBuilder {
        ShowTagKeysBuilder Database(String database)
        ShowTagKeysBuilder Measurement(String measurement)
        ShowTagKeysBuilder RetentionPolicy(String rp)
        ShowTagKeysBuilder Limit(int limit)
        ShowTagKeysBuilder Offset(int offset)
    }
    class ShowTagValuesBuilder {
        ShowTagValuesBuilder Database(String database)
        ShowTagValuesBuilder Measurement(String measurement)
        ShowTagValuesBuilder RetentionPolicy(String rp)
        ShowTagValuesBuilder Limit(int limit)
        ShowTagValuesBuilder Offset(int offset)
        ShowTagValuesBuilder With(String[] keys)
        ShowTagValuesBuilder Where(String key, ComparisonOperator operator, String value)
    }
    class ShowSeriesBuilder {
        ShowSeriesBuilder Database(String database)
        ShowSeriesBuilder Measurement(String measurement)
        ShowSeriesBuilder RetentionPolicy(String rp)
        ShowSeriesBuilder Limit(int limit)
        ShowSeriesBuilder Offset(int offset)
        ShowTagValuesBuilder Where(String key, ComparisonOperator operator, String value)
    }
```

# 写入点位设计

```mermaid
classDiagram
    class OpenGeminiClient {
        + WritePoint(String database, Point point)
        + WritePointWithRp(String database, String rp, Point point)
        + WriteBatchPoints(String database, BatchPoints batchPoints)
        + WriteBatchPointsWithRp(String database, String rp, BatchPoints batchPoints)
        + WriteByGrpc(req WriteRequest) // WriteRequest build from RecordBuilder
    }
    class BatchPoints {
        + List~Point~ points
        + AddPoint(Point)
    }

    class Point {
        + String measurement
        + Precision precision // enum, second, millisecond, microsecond, nanosecond, default is nanosecond
        + Time time // language specific
        + Map~String, String~ tags
        + Map~String, Object~ fields
        + AddTag(string, string) // init container if null
        + AddField(string, int) // init container if null
        + AddField(string, string) // init container if null
        + AddField(string, float) // init container if null
        + AddField(string, bool) // init container if null
        + SetTime(timestamp)
        + SetPrecision(type)
        + SetMeasurement(name)
    }

    class RecordBuilder {   // new from NewRecordBuilder with database and retention policy params
        + RecordBuilder Authenticate(String username, String password)
        + RecordBuilder AddRecord(RecordLine line) // RecordLine build from RecordLineBuilder
        + WriteRequest Build()
    }

    class RecordLineBuilder {   // new from NewRecordLineBuilder with measurement param
        + RecordLineBuilder AddTag(String key, String value)
        + RecordLineBuilder AddTags(map[String]String)
        + RecordLineBuilder AddField(String key, Any value)
        + RecordLineBuilder AddFields(map[String]Any)
        + RecordLine Build(Time time)
    }

    BatchPoints "1" *-- "many" Point: contains
```

# 查询设计

```mermaid
classDiagram
    class Query {
        + String database
        + String retentionPolicy
        + String command
    }
```

```mermaid
classDiagram
    class QueryResult {
        + List~SeriesResult~ results
        + String error
    }
    class SeriesResult {
        + List~Series~ series // Series is an uncountable noun.
        + String error
    }
    class Series {
        + String name
        + Map~String, String~ tags
        + List~String~ columns
        + List~List~ values
    }
    QueryResult "1" *-- "0..*" SeriesResult: contains
    SeriesResult "1" *-- "0..*" Series: contains
```

# 查询构造器设计

```mermaid
classDiagram
    class QueryBuilder {
        + static Create() QueryBuilder
        + Select(Expression[] selectExprs) QueryBuilder
        + From(String[] from) QueryBuilder
        + Where(Condition where) QueryBuilder
        + GroupBy(Expression[] groupByExpressions) QueryBuilder
        + OrderBy(order: SortOrder) QueryBuilder
        + Limit(limit: int64) QueryBuilder
        + Offset(offset: int64) QueryBuilder
        + Timezone(timezone: *time.Location) QueryBuilder
        + Build() Query
    }

    class Expression {
        <<interface>>
    }

    class ConstantExpression {
        - Object value
    }

    class StarExpression {
    }

    class FunctionExpression {
        - FunctionEnum function
        - Expression[] arguments
    }

    class AsExpression {
        - String alias
        - Expression expression
    }

    class ArithmeticExpression {
        - Expression Left
        - Expression Right
        - Operator   ArithmeticOperator
    }

    class Condition {
        <<interface>>
    }

    class ComparisonCondition {
        - String column
        - ComparisonOperator operator
        - Object value
    }

    class CompositeCondition {
        - LogicalOperator logicalOperator
        - Condition[] conditions
    }

    class SortOrder {
        <<enum>>
        Asc
        Desc
    }

    class ComparisonOperator {
        <<enum>>
        Equals
        NotEquals
        GreaterThan
        LessThan
        GreaterThanOrEquals
        LessThanOrEquals
    }

    class LogicalOperator {
        <<enum>>
        And
        Or
    }

    class FunctionEnum {
        <<enum>>
        Mean
        Count
        Sum
        Min
        Max
        Time
    }

    Expression <|-- FieldExpression
    Expression <|-- StarExpression
    Expression <|-- ConstantExpression
    Expression <|-- FunctionExpression
    Expression <|-- AsExpression
    Expression <|-- ArithmeticExpression
    FunctionExpression --> FunctionEnum
    Condition <|-- ComparisonCondition
    Condition <|-- CompositeCondition
    ComparisonCondition --> ComparisonOperator
    CompositeCondition --> LogicalOperator
    QueryBuilder --> Expression
    QueryBuilder --> Condition
    QueryBuilder --> SortOrder
    QueryBuilder --> Query
```

# Ping 设计

```mermaid
classDiagram
    class OpenGeminiClient {
        + Pong ping(int index) // index selects one from multiple servers
    }
    class Pong {
        + String version
    }
```

# Inner Http client 设计

使用类似InnerHttpClient的设计，将鉴权、负载均衡、重试等逻辑封装在内部，对client提供简单的接口。增强模块化和代码清晰度。

```mermaid
classDiagram
    class InnerHttpClient {
        + void executeHttpGetByIdx(int idx, ...) // specify server index
        + void executeHttpRequestByIdx(int idx, String method, ...) // specify server index
        + void executeHttpGet(String method, ...) // load balance
        + void executeHttpRequest(String method, ...) // load balance
        - void executeHttpRequestInner(String url, String method, ...) // inner method
    }
```

```mermaid
graph TD
    executeHttpGetByIdx --> executeHttpRequestByIdx
    executeHttpRequestByIdx --> executeHttpRequestInner
    executeHttpGet --> executeHttpRequest
    executeHttpRequest --> executeHttpRequestInner
```

# 错误处理

## 错误信息

### 场景1 http请求失败

```
$operation request failed, error: $error_details
```

### 场景2 http响应码不符合预期

```
$operation error resp, code: $code, body: $body
```

### 场景3 其他异常

```
$operation failed, error: $error_details
# example:
writePoint failed, unmarshall response body error: json: cannot unmarshal number ...
```
