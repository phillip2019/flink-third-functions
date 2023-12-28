# flink-third-functions
![GitHub](https://img.shields.io/github/license/wwek/flink-third-functions)


## 简介

flink-third-functions 是一个 Flink 适用的自定义函数(UDX)集成包。
目前已包含了：
ip2region IP地址查询归属地信息 UDF
电话号码查询归属地信息 UDF
UserAgent结构化解析 UDF

flink版本要求：
flink >= 1.14.3

## 特性

- 多种函数可用包含ip查询、电话号码查询、UserAgent解析等
- 函数返回结果支持格式化（format）
- 本地化二进制数据库all in one jar包，无外部网络依赖

## 使用
在zeppelin中使用
```
%flink.conf
flink.udf.jars /你的目录/flink-third-functions-1.0-SNAPSHOT.jar

%flink.ssql(type=update)
show functions;

%flink.ssql(type=update)
select ip2region('114.114.114.114',0),phone2region('18798896741', 1);
```

## 可用UDX函数
### 标量函数（UDF）

### **ip2region(ip,format)**

根据format字符串格式化ip归属地信息查询
默认format '%c %P %C %I'

**使用示例**

| 函数调用                                            | 结果                                                         |
| --------------------------------------------------- | ------------------------------------------------------------ |
| ip2region('101.105.35.57') （format='%c %P %C %I'） | 中国 浙江省 杭州市 阿里云                                    |
| ip2region('101.105.35.57','%c %P')                  | 中国 浙江省                                                  |
| ip2region('101.105.35.57','head%c %P\|%C')          | head中国 浙江省\|杭州市                                      |
| ip2region('101.105.35.57','%C')                     | 杭州市                                                       |
| ip2region('101.105.35.57','%JSON')                  | {"country":"中国","area":"0","province":"浙江省","city":"杭州市","isp":"阿里云"} |

**ip2region format参照表**

| Specifier | FieldName | ValueExample | Description                     |
| --------- | --------- | ------------ | ------------------------------- |
| %c        | country   | 中国         | 国家                            |
| %P        | province  | 浙江省       | 省份                            |
| %C        | city      | 杭州市       | 城市                            |
| %I        | isp       | 阿里云       | 运营商                          |
| %A        | area      | 华东         | 区域                            |
| %JSON     |           |              | json 以json的方式结构化所有字段 |



### **phone2region(phone,format)**

根据format字符串格式化电话归属地信息查询
默认format '%P %C %I'

**使用示例**

| 函数调用                                          | 结果                                                         |
| ------------------------------------------------- | ------------------------------------------------------------ |
| phone2region('13888888888') （format='%P %C %I'） | 云南 昆明 中国移动                                           |
| phone2region('13888888888','head%P %C')           | head云南 昆明                                                |
| phone2region('13888888888','%C')                  | 昆明                                                         |
| phone2region('13888888888','%JSON')               | {"province":"云南","city":"昆明","isp":"中国移动","zipcode":"650000","areacode":"0871"} |

**phone2region format参照表**

| Specifier | FieldName | ValueExample | Description                     |
| --------- | --------- | ------------ | ------------------------------- |
| %P        | province  | 云南         | 省份                            |
| %C        | city      | 昆明         | 城市                            |
| %I        | isp       | 中国移动     | 运营商                          |
| %zc       | zipcode   | 650000       | 邮编                            |
| %ac       | areacode  | 0871         | 区划码                          |
| %JSON     | 所有字段  |              | json 以json的方式结构化所有字段 |



### **parseuseragent(ua,format)**

根据format字符串格式化ua查询（使用yauua数据源）
默认format '%DN %OSNV %LENV %ANV'

**使用示例**

| 函数调用                                                     | 结果                                                         |
| ------------------------------------------------------------ | ------------------------------------------------------------ |
| parseuseragent('Mozilla/5.0 (Linux; Android 7.0; Nexus 6 Build/NBD90Z) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.124 Mobile Safari/537.36') （format='%DN %OSNV %LENV %ANV'） | Apple Macintosh Mac OS X 10.15.7 Blink 85.0 Chrome 85.0.4183.121 |
| parseuseragent('Mozilla/5.0 (Linux; Android 7.0; Nexus 6 Build/NBD90Z) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.124 Mobile Safari/537.36','%DN') | Apple Macintosh                                              |
| parseuseragent('Mozilla/5.0 (Linux; Android 7.0; Nexus 6 Build/NBD90Z) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.124 Mobile Safari/537.36','%OSNV') | Mac OS X 10.15.7                                             |
| parseuseragent('Mozilla/5.0 (Linux; Android 7.0; Nexus 6 Build/NBD90Z) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.124 Mobile Safari/537.36','%LENV') | Blink 85.0                                                   |
| parseuseragent('Mozilla/5.0 (Linux; Android 7.0; Nexus 6 Build/NBD90Z) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.124 Mobile Safari/537.36','%ANV') | Chrome 85.0.4183.121                                         |
| parseuseragent('Mozilla/5.0 (Linux; Android 7.0; Nexus 6 Build/NBD90Z) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.124 Mobile Safari/537.36','%JSON') | {"DeviceClass":"Desktop","DeviceName":"Apple Macintosh","DeviceBrand":"Apple","OperatingSystemClass":"Desktop","OperatingSystemName":"Mac OS X","OperatingSystemVersion":"10.15.7","OperatingSystemNameVersion":"Mac OS X 10.15.7","OperatingSystemVersionBuild":"??","LayoutEngineClass":"Browser","LayoutEngineName":"Blink","LayoutEngineVersion":"85.0","LayoutEngineVersionMajor":"85","LayoutEngineNameVersion":"Blink 85.0","LayoutEngineNameVersionMajor":"Blink 85","AgentClass":"Browser","AgentName":"Chrome","AgentVersion":"85.0.4183.121","AgentVersionMajor":"85","AgentNameVersion":"Chrome 85.0.4183.121","AgentNameVersionMajor":"Chrome 85"} |

**parseuseragent format参照表**

| Specifier | FieldName                    | ValueExample         | Description                |
| --------- | ---------------------------- | -------------------- | -------------------------- |
| %DC       | DeviceClass                  | Phone                |                            |
| %DN       | DeviceName                   | Google Nexus 6       |                            |
| %DB       | DeviceBrand                  | Google               |                            |
| %OSC      | OperatingSystemClass         | Mobile               |                            |
| %OSN      | OperatingSystemName          | Android              |                            |
| %OSV      | OperatingSystemVersion       | 7.0                  |                            |
| %OSNV     | OperatingSystemNameVersion   | Android 7.0          |                            |
| %OSVB     | OperatingSystemVersionBuild  | NBD90Z               |                            |
| %LEC      | LayoutEngineClass            | Browser              |                            |
| %LEN      | LayoutEngineName             | Blink                |                            |
| %LEV      | LayoutEngineVersion          | 53.0                 |                            |
| %LEVM     | LayoutEngineVersionMajor     | 53                   |                            |
| %LENV     | LayoutEngineNameVersion      | Blink 53.0           |                            |
| %LENVM    | LayoutEngineNameVersionMajor | Blink 53             |                            |
| %AC       | AgentClass                   | Browser              |                            |
| %AN       | AgentName                    | Chrome               |                            |
| %AV       | AgentVersion                 | 53.0.2785.124        |                            |
| %AVM      | AgentVersionMajor            | 53                   |                            |
| %ANV      | AgentNameVersion             | Chrome 53.0.2785.124 |                            |
| %ANVM     | AgentNameVersionMajor        | Chrome 53            |                            |
| %JSON     | 所有字段                     |                      | 以json的方式结构化所有字段 |



### **parseuseragentdd(ua,format)**

根据format字符串格式化ua查询（使用mngsk/device-detector数据源）

默认format '%DN %OSNV %LENV %ANV'

**使用示例**

| 函数调用                                                     | 结果                                                         |
| ------------------------------------------------------------ | ------------------------------------------------------------ |
| parseuseragent('Mozilla/5.0 (Linux; Android 7.0; Nexus 6 Build/NBD90Z) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.124 Mobile Safari/537.36') （format='%DN %OSNV %LENV %ANV'） | Apple Macintosh Mac OS X 10.15.7 Blink 85.0 Chrome 85.0.4183.121 |
| parseuseragent('Mozilla/5.0 (Linux; Android 7.0; Nexus 6 Build/NBD90Z) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.124 Mobile Safari/537.36','%DN') | Apple Macintosh                                              |
| parseuseragent('Mozilla/5.0 (Linux; Android 7.0; Nexus 6 Build/NBD90Z) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.124 Mobile Safari/537.36','%OSNV') | Mac OS X 10.15.7                                             |
| parseuseragent('Mozilla/5.0 (Linux; Android 7.0; Nexus 6 Build/NBD90Z) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.124 Mobile Safari/537.36','%LENV') | Blink 85.0                                                   |
| parseuseragent('Mozilla/5.0 (Linux; Android 7.0; Nexus 6 Build/NBD90Z) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.124 Mobile Safari/537.36','%ANV') | Chrome 85.0.4183.121                                         |
| parseuseragent('Mozilla/5.0 (Linux; Android 7.0; Nexus 6 Build/NBD90Z) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.124 Mobile Safari/537.36','%JSON') | {"DeviceClass":"Desktop","DeviceName":"Apple Macintosh","DeviceBrand":"Apple","OperatingSystemClass":"Desktop","OperatingSystemName":"Mac OS X","OperatingSystemVersion":"10.15.7","OperatingSystemNameVersion":"Mac OS X 10.15.7","OperatingSystemVersionBuild":"??","LayoutEngineClass":"Browser","LayoutEngineName":"Blink","LayoutEngineVersion":"85.0","LayoutEngineVersionMajor":"85","LayoutEngineNameVersion":"Blink 85.0","LayoutEngineNameVersionMajor":"Blink 85","AgentClass":"Browser","AgentName":"Chrome","AgentVersion":"85.0.4183.121","AgentVersionMajor":"85","AgentNameVersion":"Chrome 85.0.4183.121","AgentNameVersionMajor":"Chrome 85"} |

**parseuseragent format参照表**

| Specifier | FieldName                    | ValueExample         | Description                |
| --------- | ---------------------------- | -------------------- | -------------------------- |
| %DC       | DeviceClass                  | Phone                |                            |
| %DN       | DeviceName                   | Google Nexus 6       |                            |
| %DB       | DeviceBrand                  | Google               |                            |
| %OSC      | OperatingSystemClass         | Mobile               |                            |
| %OSN      | OperatingSystemName          | Android              |                            |
| %OSV      | OperatingSystemVersion       | 7.0                  |                            |
| %OSNV     | OperatingSystemNameVersion   | Android 7.0          |                            |
| %OSVB     | OperatingSystemVersionBuild  | NBD90Z               |                            |
| %LEC      | LayoutEngineClass            | Browser              |                            |
| %LEN      | LayoutEngineName             | Blink                |                            |
| %LEV      | LayoutEngineVersion          | 53.0                 |                            |
| %LEVM     | LayoutEngineVersionMajor     | 53                   |                            |
| %LENV     | LayoutEngineNameVersion      | Blink 53.0           |                            |
| %LENVM    | LayoutEngineNameVersionMajor | Blink 53             |                            |
| %AC       | AgentClass                   | Browser              |                            |
| %AN       | AgentName                    | Chrome               |                            |
| %AV       | AgentVersion                 | 53.0.2785.124        |                            |
| %AVM      | AgentVersionMajor            | 53                   |                            |
| %ANV      | AgentNameVersion             | Chrome 53.0.2785.124 |                            |
| %ANVM     | AgentNameVersionMajor        | Chrome 53            |                            |
| %JSON     | 所有字段                     |                      | 以json的方式结构化所有字段 |



### HTTP Sink
The following example shows the minimum Table API example to create a [HttpDynamicSink](src/main/java/com/getindata/connectors/http/internal/table/HttpDynamicSink.java) that writes JSON values to an HTTP endpoint using POST method, assuming Flink has JAR of [JSON serializer](https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/connectors/table/formats/json/) installed:

```roomsql
CREATE TABLE http (
  id bigint,
  some_field string
) WITH (
  'connector' = 'http-sink',
  'url' = 'http://example.com/myendpoint',
  'format' = 'json'
)
```

Then use `INSERT` SQL statement to send data to your HTTP endpoint:

```roomsql
INSERT INTO http VALUES (1, 'Ninette'), (2, 'Hedy')
```

Due to the fact that `HttpSink` sends bytes inside HTTP request's body, one can easily swap `'format' = 'json'` for some other [format](https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/connectors/table/formats/overview/).

Other examples of usage of the Table API can be found in [some tests](src/test/java/com/getindata/connectors/http/table/HttpDynamicSinkInsertTest.java).

### Request submission
Starting from version 0.10 HTTP Sink by default submits events in batch. Before version 0.10 the default and only submission type was `single`.
This is a breaking compatibility change.

The submission mode can be changed using `gid.connector.http.sink.writer.request.mode` property using `single` or `batch` as property value.

#### Batch submission mode
In batch mode, a number of events (processed elements) will be batched and submitted in one HTTP request.
In this mode, HTTP PUT/POST request's body contains a Json array, where every element of this array represents
individual event.

An example of Http Sink batch request body containing data for three events:
```json
[
  {
    "id": 1,
    "first_name": "Ninette",
    "last_name": "Clee",
    "gender": "Female",
    "stock": "CDZI",
    "currency": "RUB",
    "tx_date": "2021-08-24 15:22:59"
  },
  {
    "id": 2,
    "first_name": "Rob",
    "last_name": "Zombie",
    "gender": "Male",
    "stock": "DGICA",
    "currency": "GBP",
    "tx_date": "2021-10-25 20:53:54"
  },
  {
    "id": 3,
    "first_name": "Adam",
    "last_name": "Jones",
    "gender": "Male",
    "stock": "DGICA",
    "currency": "PLN",
    "tx_date": "2021-10-26 20:53:54"
  }
]
```

By default, batch size is set to 500 which is the same as Http Sink's `maxBatchSize` property and has value of 500.
The `maxBatchSize' property sets maximal number of events that will by buffered by Flink runtime before passing it to Http Sink for processing.

In order to change submission batch size use `gid.connector.http.sink.request.batch.size` property. For example:

Streaming API:
```java
HttpSink.<String>builder()
      .setEndpointUrl("http://example.com/myendpoint")
      .setElementConverter(
          (s, _context) -> new HttpSinkRequestEntry("POST", s.getBytes(StandardCharsets.UTF_8)))
      .setProperty("gid.connector.http.sink.request.batch.size", "50")
      .build();
```
SQL:
```roomsql
CREATE TABLE http (
  id bigint,
  some_field string
) WITH (
  'connector' = 'http-sink',
  'url' = 'http://example.com/myendpoint',
  'format' = 'json',
  'gid.connector.http.sink.request.batch.size' = '50'
)
```

#### Single submission mode
In this mode every processed event is submitted as individual HTTP POST/PUT request.

Streaming API:
```java
HttpSink.<String>builder()
      .setEndpointUrl("http://example.com/myendpoint")
      .setElementConverter(
          (s, _context) -> new HttpSinkRequestEntry("POST", s.getBytes(StandardCharsets.UTF_8)))
      .setProperty("gid.connector.http.sink.writer.request.mode", "single")
      .build();
```
SQL:
```roomsql
CREATE TABLE http (
  id bigint,
  some_field string
) WITH (
  'connector' = 'http-sink',
  'url' = 'http://example.com/myendpoint',
  'format' = 'json',
  'gid.connector.http.sink.writer.request.mode' = 'single'
)
```

#### Http headers
It is possible to set HTTP headers that will be added to HTTP request send by sink connector.
Headers are defined via property key `gid.connector.http.sink.header.HEADER_NAME = header value` for example:
`gid.connector.http.sink.header.X-Content-Type-Options = nosniff`.
Properties can be set via Sink builder or Property object:
```java
HttpSink.<String>builder()
      .setEndpointUrl("http://example.com/myendpoint")
      .setElementConverter(
          (s, _context) -> new HttpSinkRequestEntry("POST", s.getBytes(StandardCharsets.UTF_8)))
      .setProperty("gid.connector.http.sink.header.X-Content-Type-Options", "nosniff")
      .build();
```
or

```java
Properties properties = Properties();
properties.setProperty("gid.connector.http.sink.header.X-Content-Type-Options", "nosniff");

HttpSink.<String>builder()
      .setEndpointUrl("http://example.com/myendpoint")
      .setElementConverter(
          (s, _context) -> new HttpSinkRequestEntry("POST", s.getBytes(StandardCharsets.UTF_8)))
      .setProperties(properties)
      .build();
```

In Table/SQL API, headers can be set using http sink table DDL. In example below, HTTP request done for `http` table will contain three headers:
- `Origin`
- `X-Content-Type-Options`
- `Content-Type`

```roomsql
CREATE TABLE http (
  id bigint,
  some_field string
) WITH (
  'connector' = 'http-sink',
  'url' = 'http://example.com/myendpoint',
  'format' = 'json',
  'gid.connector.http.sink.header.Origin' = '*',
  'gid.connector.http.sink.header.X-Content-Type-Options' = 'nosniff',
  'gid.connector.http.sink.header.Content-Type' = 'application/json'
)
```

#### Custom request/response callback
Http Sink processes responses that it gets from the HTTP endpoint along their respective requests. One can customize the
behaviour of the additional stage of processing done by Table API Sink by implementing
[HttpPostRequestCallback](src/main/java/com/getindata/connectors/http/HttpPostRequestCallback.java) and
[HttpPostRequestCallbackFactory](src/main/java/com/getindata/connectors/http/HttpPostRequestCallbackFactory.java)
interfaces. Custom implementations of `HttpSinkRequestCallbackFactory` can be registered along other factories in
`resources/META-INF.services/org.apache.flink.table.factories.Factory` file and then referenced by their identifiers in
the HttpSink DDL property field `gid.connector.http.sink.request-callback`.

A default implementation that logs those pairs as *INFO* level logs using Slf4j
([Slf4jHttpPostRequestCallback](src/main/java/com/getindata/connectors/http/internal/table/sink/Slf4jHttpPostRequestCallback.java))
is provided.

## HTTP status code handler
Http Sink and Lookup Source connectors allow defining list of HTTP status codes that should be treated as errors.
By default all 400s and 500s response codes will be interpreted as error code.

This behavior can be changed by using below properties in table definition (DDL) for Sink and Lookup Source or passing it via
`setProperty' method from Sink's builder. The property names are:
- `gid.connector.http.sink.error.code` and `gid.connector.http.source.lookup.error.code` used to defined HTTP status code value that should be treated as error for example 404.
  Many status codes can be defined in one value, where each code should be separated with comma, for example:
  `401, 402, 403`. User can use this property also to define a type code mask. In that case, all codes from given HTTP response type will be treated as errors.
  An example of such a mask would be `3XX, 4XX, 5XX`. In this case, all 300s, 400s and 500s status codes will be treated as errors.
- `gid.connector.http.sink.error.code.exclude` and `gid.connector.http.source.lookup.error.code.exclude` used to exclude a HTTP code from error list.
  Many status codes can be defined in one value, where each code should be separated with comma, for example:
  `401, 402, 403`. In this example, codes 401, 402 and 403 would not be interpreted as error codes.

## TLS and mTLS support
Both Http Sink and Lookup Source connectors supports Https communication using TLS 1.2 and mTLS.
To enable Https communication simply use `https` protocol in endpoint's URL.
If certificate used by HTTP server is self-signed, or it is signed byt not globally recognize CA
you would have to add this certificate to connector's keystore as trusted certificate.
In order to do so, use `gid.connector.http.security.cert.server` connector property,
which value is a path to the certificate. You can also use your organization's CA Root certificate.
You can specify many certificate, separating each path with `,`.

You can also configure connector to use mTLS. For this simply use `gid.connector.http.security.cert.client`
and `gid.connector.http.security.key.client` connector properties to specify path to certificate and
private key that should be used by connector. Key MUST be in `PCKS8` format. Both PEM and DER keys are
allowed.

All properties can be set via Sink's builder `.setProperty(...)` method or through Sink and Source table DDL.

For non production environments it is sometimes necessary to use Https connection and accept all certificates.
In this special case, you can configure connector to trust all certificates without adding them to keystore.
To enable this option use `gid.connector.http.security.cert.server.allowSelfSigned` property setting its value to `true`.

## Basic Authentication
Connector supports Basic Authentication mechanism using HTTP `Authorization` header.
The header value can set via properties same as other headers. Connector will convert passed value to Base64 and use it for request.
If the used value starts from prefix `Basic `, it will be used as header value as is, without any extra modification.

## Table API Connector Options
### HTTP TableLookup Source
| Option                                                     | Required | Description/Value                                                                                                                                                                                                                  |
|------------------------------------------------------------|----------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| connector                                                  | required | The Value should be set to _rest-lookup_                                                                                                                                                                                           |
| format                                                     | required | Flink's format name that should be used to decode REST response, Use `json` for a typical REST endpoint.                                                                                                                           |
| url                                                        | required | The base URL that should be use for GET requests. For example _http://localhost:8080/client_                                                                                                                                       |
| asyncPolling                                               | optional | true/false - determines whether Async Pooling should be used. Mechanism is based on Flink's Async I/O.                                                                                                                             |
| lookup-method                                              | optional | GET/POST/PUT (and any other) - determines what REST method should be used for lookup REST query. If not specified, `GET` method will be used.                                                                                      |
| gid.connector.http.lookup.error.code                       | optional | List of HTTP status codes that should be treated as errors by HTTP Source, separated with comma.                                                                                                                                   |
| gid.connector.http.lookup.error.code.exclude               | optional | List of HTTP status codes that should be excluded from the `gid.connector.http.lookup.error.code` list, separated with comma.                                                                                                      |
| gid.connector.http.security.cert.server                    | optional | Path to trusted HTTP server certificate that should be add to connectors key store. More than one path can be specified using `,` as path delimiter.                                                                               |
| gid.connector.http.security.cert.client                    | optional | Path to trusted certificate that should be used by connector's HTTP client for mTLS communication.                                                                                                                                 |
| gid.connector.http.security.key.client                     | optional | Path to trusted private key that should be used by connector's HTTP client for mTLS communication.                                                                                                                                 |
| gid.connector.http.security.cert.server.allowSelfSigned    | optional | Accept untrusted certificates for TLS communication.                                                                                                                                                                               |
| gid.connector.http.source.lookup.request.timeout           | optional | Sets HTTP request timeout in seconds. If not specified, the default value of 30 seconds will be used.                                                                                                                              |
| gid.connector.http.source.lookup.request.thread-pool.size  | optional | Sets the size of pool thread for HTTP lookup request processing. Increasing this value would mean that more concurrent requests can be processed in the same time. If not specified, the default value of 8 threads will be used.  |
| gid.connector.http.source.lookup.response.thread-pool.size | optional | Sets the size of pool thread for HTTP lookup response processing. Increasing this value would mean that more concurrent requests can be processed in the same time. If not specified, the default value of 4 threads will be used. |

### HTTP Sink
| Option                                                  | Required | Description/Value                                                                                                                                                                                                                                |
|---------------------------------------------------------|----------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| connector                                               | required | Specify what connector to use. For HTTP Sink it should be set to _'http-sink'_.                                                                                                                                                                  |
| url                                                     | required | The base URL that should be use for HTTP requests. For example _http://localhost:8080/client_.                                                                                                                                                   |
| format                                                  | required | Specify what format to use.                                                                                                                                                                                                                      |
| insert-method                                           | optional | Specify which HTTP method to use in the request. The value should be set either to `POST` or `PUT`.                                                                                                                                              |
| sink.batch.max-size                                     | optional | Maximum number of elements that may be passed in a batch to be written downstream.                                                                                                                                                               |
| sink.requests.max-inflight                              | optional | The maximum number of in flight requests that may exist, if any more in flight requests need to be initiated once the maximum has been reached, then it will be blocked until some have completed.                                               |
| sink.requests.max-buffered                              | optional | Maximum number of buffered records before applying backpressure.                                                                                                                                                                                 |
| sink.flush-buffer.size                                  | optional | The maximum size of a batch of entries that may be sent to the HTTP endpoint measured in bytes.                                                                                                                                                  |
| sink.flush-buffer.timeout                               | optional | Threshold time in milliseconds for an element to be in a buffer before being flushed.                                                                                                                                                            |
| gid.connector.http.sink.request-callback                | optional | Specify which `HttpPostRequestCallback` implementation to use. By default, it is set to `slf4j-logger` corresponding to `Slf4jHttpPostRequestCallback`.                                                                                          |
| gid.connector.http.sink.error.code                      | optional | List of HTTP status codes that should be treated as errors by HTTP Sink, separated with comma.                                                                                                                                                   |
| gid.connector.http.sink.error.code.exclude              | optional | List of HTTP status codes that should be excluded from the `gid.connector.http.sink.error.code` list, separated with comma.                                                                                                                      |
| gid.connector.http.security.cert.server                 | optional | Path to trusted HTTP server certificate that should be add to connectors key store. More than one path can be specified using `,` as path delimiter.                                                                                             |
| gid.connector.http.security.cert.client                 | optional | Path to trusted certificate that should be used by connector's HTTP client for mTLS communication.                                                                                                                                               |
| gid.connector.http.security.key.client                  | optional | Path to trusted private key that should be used by connector's HTTP client for mTLS communication.                                                                                                                                               |
| gid.connector.http.security.cert.server.allowSelfSigned | optional | Accept untrusted certificates for TLS communication.                                                                                                                                                                                             |
| gid.connector.http.sink.request.timeout                 | optional | Sets HTTP request timeout in seconds. If not specified, the default value of 30 seconds will be used.                                                                                                                                            |
| gid.connector.http.sink.writer.thread-pool.size         | optional | Sets the size of pool thread for HTTP Sink request processing. Increasing this value would mean that more concurrent requests can be processed in the same time. If not specified, the default value of 1 thread will be used.                   |
| gid.connector.http.sink.writer.request.mode             | optional | Sets Http Sink request submission mode. Two modes are available to select, `single` and `batch` which is the default mode if option is not specified.                                                                                            |
| gid.connector.http.sink.request.batch.size              | optional | Applicable only for `gid.connector.http.sink.writer.request.mode = batch`. Sets number of individual events/requests that will be submitted as one HTTP request by HTTP sink. The default value is 500 which is same as HTTP Sink `maxBatchSize` |
| gid.connector.http.sink.writer.request.encryption.mode  | optional | Sets Http Sink request encryption mode. Two modes are available to select, `plain` and `xsyk` which is the default mode if option is not specified                                                                                               |

## Build and deployment
To build the project locally you need to have `maven 3` and Java 11+. </br>

Project build command: `mvn package`. </br>
Detailed test report can be found under `target/site/jacoco/index.xml`.

## Demo application
**Note**: This demo works only for Flink-1.15x.

You can test this connector using simple mock http server provided with this repository and Flink SQL-client.
The mock server can be started from IDE (currently only this way) by running `HttpStubApp::main` method.
It will start HTTP server listening on `http://localhost:8080/client`

Steps to follow:
- Run Mock HTTP server from `HttpStubApp::main` method.
- Start your Flink cluster, for example as described under https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/try-flink/local_installation/
- Start Flink SQL Client [6] by calling: `./bin/sql-client.sh -j flink-http-connector-1.0-SNAPSHOT.jar`
- Execute SQL statements:
  Create Data Stream source Table:
```roomsql
CREATE TABLE Orders (id STRING, id2 STRING, proc_time AS PROCTIME()
) WITH (
'connector' = 'datagen', 
'rows-per-second' = '1', 
'fields.id.kind' = 'sequence', 
'fields.id.start' = '1', 
'fields.id.end' = '120', 
'fields.id2.kind' = 'sequence', 
'fields.id2.start' = '2', 
'fields.id2.end' = '120'
);
```

Create Http Connector Lookup Table:
```roomsql
CREATE TABLE Customers (
	id STRING,
	id2 STRING,
	msg STRING,
	uuid STRING,
	details ROW<
	  isActive BOOLEAN,
	  nestedDetails ROW<
	    balance STRING
	  >
	>
) WITH (
'connector' = 'rest-lookup',
'format' = 'json',
'url' = 'http://localhost:8080/client', 
'asyncPolling' = 'true'
);
```

Submit SQL Select query to join both tables:
```roomsql
SELECT o.id, o.id2, c.msg, c.uuid, c.isActive, c.balance FROM Orders AS o JOIN Customers FOR SYSTEM_TIME AS OF o.proc_time AS c ON o.id = c.id AND o.id2 = c.id2;
```

As a result, you should see a table with joined records like so:
![join-result](docs/JoinTable.PNG)

The `msg` column shows parameters used with REST call for given JOIN record.

## Implementation
### HTTP Source
Implementation of an HTTP source connector is based on Flink's `TableFunction` and `AsyncTableFunction` classes.  
To be more specific we are using a `LookupTableSource`. Unfortunately Flink's new unified source interface [2] cannot be used for this type of source.
Issue was discussed on Flink's user mailing list - https://lists.apache.org/thread/tx2w1m15zt5qnvt924mmbvr7s8rlyjmw

Implementation of an HTTP Sink is based on Flink's `AsyncSinkBase` introduced in Flink 1.15 [3, 4].

#### Http Response to Table schema mapping
The mapping from Http Json Response to SQL table schema is done via Flink's Json Format [5].

## Breaking changes
- Version 0.10
    - Http Sink submission mode changed from single to batch. From now, body of HTTP POUT/POST request will contain a Json array.
    - Changed API for public HttpSink builder. The `setHttpPostRequestCallback` expects a `PostRequestCallback`
      of generic type [HttpRequest](src/main/java/com/getindata/connectors/http/internal/sink/httpclient/HttpRequest.java)
      instead `HttpSinkRequestEntry`.

### HTTP TableLookup Source
- Implement caches.
- Think about Retry Policy for Http Request
- Check other `//TODO`'s.

### HTTP Sink
- Make `HttpSink` retry the failed requests. Currently, it does not retry those at all, only adds their count to the `numRecordsSendErrors` metric. It should be thoroughly thought over how to do it efficiently and then implemented.

### 
[1] https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/dev/table/sql/queries/joins/#lookup-join
</br>
[2] https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/dev/datastream/sources/
</br>
[3] https://cwiki.apache.org/confluence/display/FLINK/FLIP-171%3A+Async+Sink
</br>
[4] https://nightlies.apache.org/flink/flink-docs-release-1.15/api/java/org/apache/flink/connector/base/sink/AsyncSinkBase.html
</br>
[5] https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/table/formats/json/
</br>
[6] https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/sqlclient/
</br>


### 聚合函数（UDAF）

暂无

### 表值函数（UDTF）
暂无


## 开发flink-third-functions

编译打包
```
mvn clean package -Dmaven.test.skip=true
```

## 问题和建议

发issue || 发pr

## Todo

*[ ] 实现查询缓存提高性能
*[ ] 对象实例化未使用单例
*[ ] Benchmark

