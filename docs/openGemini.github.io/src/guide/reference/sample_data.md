---
title: Sample data
order: 1
---


# 示例数据

为了进一步学习GeminiQL，本节将提供示例数据供您下载，并教您如何将数据导入数据库。数据探索、Schema探索和GeminiQL函数等章节都会引用到这些示例数据。

```
https://s3.amazonaws.com/noaa.water-database/NOAA_data.txt
```

## 导入数据：
```shell
curl -G  https://s3.amazonaws.com/noaa.water-database/NOAA_data.txt > NOAA_data.txt

ts-cli import --path=NOAA_data.txt --host=127.0.0.1 --port=8086 --precision=s
```

数据自动导入到 database: NOAA_water_database
