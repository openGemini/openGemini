---
title: Line Protocol Insert
order: 1
---

Line Protocol is a text-based data format proposed by InfluxDB, and openGemini uses the same Line Protocol to write points to openGemini. To learn more about the detailed definition, usage, special characters, etc. of the Line Protocol, you can refer to the [InfluxDB Line Protocol](https://docs.influxdata.com/influxdb/cloud/reference/syntax/line-protocol) or the [openGemini data line protocol](../reference/line_protocol.md).

## Data format of Line Protocol

A single line of text in the Line protocol format represents a point, that is to say, a point is composed of a measurement (table name), a tag set (one or more tag), a field set (one or more field), and a timestamp.

**For Example**

<img src="../../../static/img/guide/write_data/line_protocol_one.png" style="zoom: 30%;" />

It means that this Point belongs to the Weather table, and Location indicates that the temperature sensor is in the us-midwest. At this time (1465839830100400200) the temperature is 82 °F.

**For Another Example**

<img src="../../../static/img/guide/write_data/line_protocol_two.png" style="zoom: 50%;" />

This means that the Point belongs to the "monitor" table, and the "region" and "host" identify the machine with the IP address 10.0.1.11 in the Beijing area. At this time (1465839830100400200), the CPU usage is 43% and memory usage is 1465839KB.

:::tip

Remember the following tips:

- Use a **" , "** to separate `Measurement` and `Tag`
- Use **" , "** to separate` Tags` and `Fields`, and **there should be no spaces after the " , "**
- Use "space"  to separate `Tags` and `Fields`, as well as `Fields` and the `Timestamp`

:::

## Write data

::: tabs

@tab CLI

Use the ts-cli command to write data into the openGemini database, and place the ```Insert``` in front of the line procotol:

```sql
> INSERT h2o_feet,location=coyote_creek water_level=2.927,description="below 3 feet" 1566102600
```

Import Line protocol data from a file using the CLI，suppose the data is saved in the file data.txt in line protocol format
```
# DDL
CREATE DATABASE NOAA_water_database
# DML
# CONTEXT-DATABASE: NOAA_water_database
h2o_feet,location=coyote_creek water_level=2.927,description="below 3 feet" 1566102600
h2o_feet,location=coyote_creek water_level=2.831,description="below 3 feet" 1566102960
h2o_feet,location=coyote_creek water_level=2.743,description="below 3 feet" 1566103320
h2o_feet,location=coyote_creek water_level=2.667,description="below 3 feet" 1566103680
h2o_feet,location=coyote_creek water_level=2.589,description="below 3 feet" 1566104040
h2o_feet,location=coyote_creek water_level=2.523,description="below 3 feet" 1566104400
h2o_feet,location=coyote_creek water_level=2.464,description="below 3 feet" 1566104760
h2o_feet,location=coyote_creek water_level=2.408,description="below 3 feet" 1566105120
h2o_feet,location=coyote_creek water_level=2.379,description="below 3 feet" 1566105480
h2o_feet,location=coyote_creek water_level=2.352,description="below 3 feet" 1566105840
h2o_feet,location=coyote_creek water_level=2.343,description="below 3 feet" 1566106200
h2o_feet,location=coyote_creek water_level=2.346,description="below 3 feet" 1566106560
```
Run the following command
```
> ts-cli -import -path=data.txt -host=127.0.0.1 -port=8086 -precision=s
```

@tab API

- Use the HTTP API to write data to openGemini
use `POST` to make a request to the `/write method, and provide your line protocol in the request body:

```bash
curl -i -XPOST "http://localhost:8086/write?db=science_is_cool" --data-binary 'h2o_feet,location=coyote_creek water_level=2.927,description="below 3 feet" 1566102600'
```

- Write data in batches

```bash
curl -i -XPOST "http://localhost:8086/write?db=db0" --data-binary '
h2o_feet,location=coyote_creek water_level=2.927,description="below 3 feet" 1566102600
h2o_feet,location=coyote_creek water_level=2.831,description="below 3 feet" 1566102960
h2o_feet,location=coyote_creek water_level=2.743,description="below 3 feet" 1566103320
'
```
- Write file data
suppose the data is saved in the file data.txt in line protocol format
```
// data.txt
h2o_feet,location=coyote_creek water_level=2.927,description="below 3 feet" 1566102600
h2o_feet,location=coyote_creek water_level=2.831,description="below 3 feet" 1566102960
h2o_feet,location=coyote_creek water_level=2.743,description="below 3 feet" 1566103320
h2o_feet,location=coyote_creek water_level=2.667,description="below 3 feet" 1566103680
h2o_feet,location=coyote_creek water_level=2.589,description="below 3 feet" 1566104040
h2o_feet,location=coyote_creek water_level=2.523,description="below 3 feet" 1566104400
h2o_feet,location=coyote_creek water_level=2.464,description="below 3 feet" 1566104760
h2o_feet,location=coyote_creek water_level=2.408,description="below 3 feet" 1566105120
h2o_feet,location=coyote_creek water_level=2.379,description="below 3 feet" 1566105480
h2o_feet,location=coyote_creek water_level=2.352,description="below 3 feet" 1566105840
h2o_feet,location=coyote_creek water_level=2.343,description="below 3 feet" 1566106200
h2o_feet,location=coyote_creek water_level=2.346,description="below 3 feet" 1566106560
```
Use the following command to write data into openGemini
```
> curl -i -XPOST 'http://localhost:8086/write?db=db0' --data-binary @data.txt
```

:::
