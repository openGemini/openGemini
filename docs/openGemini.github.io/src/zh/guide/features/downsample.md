---
title: 多级降采样
order: 4
---
**背景介绍**
在DevOps或Iot等场景中，用户对近期数据更敏感，很多时候只需要对近期指定数据进行完整的数据查询，而对较久远的数据仅进行数据趋势查询，即采样数据的查询，因此对久远的低价值数据，我们可以通过降采样的方式进行数据的采样缩放。

**降采样**
降采样即一定时间间隔内的数据点，基于一定规则，聚合为一个或一组值，从而达到降低采样点数，减少整体存储数据量，进而减轻存储和查询计算的压力。

**多级降采样**
多级降采样与普通降采样的区别在于，多级降采样可以对不同时间段的数据进行不同的降采样策略。
实际业务中用户对不同时间段的降采样要求是不一样的，用户可能对近期的数据比较敏感而对长远的数据需求较少，所以需要根据实际业务对数据进行不同的多级降采样策略。采用多级降采样的方式既满足了用户对高价值数据的查询需求，又兼顾了存储效率。

**场景举例**
多重降采样场景举例：7天内原始数据直接入库，7-30天数据，15min粒度降采样后入库，30天-12个月的数据，1h粒度降采样后入库。
如下图所示，假设今天是2022-12-31，蓝色部分是7天内的数据，黄色部分是7-30天数据，橙色部分是30天-12个月的数据。每过一段时间，数据库会把蓝色部分的数据以15min为粒度聚合放到黄色区域中；每过一段时间，数据库会把黄色部分以1h为粒度聚合放到橙色区域中，注意这里1h是15min的倍数，所以可以方便地聚合。
![多级降采样场景图](../../../../static/img/guide/features/downsample_1.jpg)


## 创建降采样
**语法：**

```sql
Create DownSample [on <rp_name>| on <dbname>.<rp_name>|  ]((dataType(aggregators)...)) With Duration <timeDuration> SampleInterval(time Durations) TimeInterval(time Durations) WaterMark(time Durations)
```
**参数说明：**

|  Duration |SampleInterval   |TimeInterval   |WaterMark   |
| ------------ | ------------ | ------------ | ------------ |
| 数据保留时间  |执行下一级降采样时间  |采样Interval   | 采样滞后时间  |

**聚合方法定义格式：**

```sql
dataType(aggfunctions...)
```
**聚合方法举例：**

 ```sql
int(first,sum,count,last,min,max)
 ```
```sql
int(min,max),float(sum)
```
**限制说明：**

- SampleInterval，TimeInterval，WaterMark指定的采样策略数量必须相同，比如其中有一项的数量是3，其他两项的数量也必须是3；
- SampleInterval、 TimeInterval、WaterMark为一一对应关系、每个数组内为倍数关系；

**举例说明：**

```sql
 Create DownSample
 (float(sum,last),integer(max,min))
 With Duration 7d
 sampleinterval(1d,2d)
 timeinterval(1m,3m)
 watermark(1s,10s)
```
Duration是7d,说明这个降采样数据保存最长是7d，这里的Duration需要小于指定的RP的Duration。

1d内的数据采样间隔为1m，1d至2d内的数据采样为3m，采样滞后分别为1s和10s。这里(1d,1m,1s)和(2d,3m,10s)两个元组相对应，同时2d是1d的倍数，3m是1m的倍数，10s是1s的倍数，符合限制条件。

采样聚合数据为sum，last，max，min。float(sum,last)代表sum和last聚合数据是float格式，integer(max,min)代表max和min聚合数据是integer格式。


## 显示降采样
**语法：**
show 默认database 所有 downsample tasks：

```sql
SHOW DOWNSAMPLES
```
show 指定database 所有 downsample tasks:
```sql
SHOW DOWNSAMPLES ON <dtabase name>
```
**举例说明：**

```sql
> show downsamples on db0
rpName  field_operator                   duration  sampleInterval    timeInterval waterMark
------  --------------                   --------  --------------    ------------ ---------
autogen float{sum,last},integer{max,min} 168h0m0s  24h0m0s,48h0m0s   1m0s,3m0s    1s,10s
rp1     float{sum,last},integer{max,min} 720h0m0s  24h0m0s,168h0m0s  1m0s,3m0s    1s,1m0s
rp2     bool{last},int{max,min,first}    2400h0m0s 168h0m0s,360h0m0s 30m0s,1h0m0s 10m0s,10h0m0s
```
## 删除降采样
**语法：**
删除数据库的所有降采样：
```sql
Drop DownSamples
Drop DownSamples on db0
```
删除指定RP的降采样：
```sql
Drop DownSample on rp1
Drop DownSample on db.rp
```
