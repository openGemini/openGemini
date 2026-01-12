---
title: client-go
order: 4
---

### **项目地址**

https://github.com/openGemini/opengemini-client-go

### **最新版本**

v0.3.0，欢迎使用和反馈

版本链接：https://github.com/openGemini/opengemini-client-go/releases/tag/v0.3.0

### 接口参考文档

https://pkg.go.dev/github.com/openGemini/opengemini-client-go@v0.3.0/opengemini

### 用法

#### 引入客户端库：

<i><font color=gray>示例使用点引用法，用户可结合具体需要选择适合的引用方式。</font></i>

```go
import . "github.com/openGemini/opengemini-client-go/opengemini"
```

#### 创建客户端：

```go
config := &Config{
	Addresses: []*Address{
		{
			Host: "127.0.0.1",
			Port: 8086,
		},
	},
}
client, err := NewClient(config)
if err != nil {
	fmt.Println(err)
}
```

#### 创建数据库：

```go
exampleDatabase := "ExampleDatabase"
err = client.CreateDatabase(exampleDatabase)
if err != nil {
	fmt.Println(err)
	return
}
```

#### 写入单个点：

```go
exampleMeasurement := "ExampleMeasurement"
point := &Point{}
point.SetMeasurement(exampleMeasurement)
point.AddTag("Weather", "foggy")
point.AddField("Humidity", 87)
point.AddField("Temperature", 25)
err = client.WritePoint(context.Background(),exampleDatabase, point, func(err error) {
	if err != nil {
		fmt.Printf("write point failed for %s", err)
	}
})
if err != nil {
	fmt.Println(err)
}
```

#### 批量写入点：

```go
exampleMeasurement := "ExampleMeasurement"
bp := &BatchPoints{}
var tagList []string
tagList = append(tagList, "sunny", "rainy", "windy")
for i := 0; i < 10; i++ {
	p := &Point{}
	p.SetMeasurement(exampleMeasurement)
	p.AddTag("Weather", tagList[rand.Int31n(3)])
	p.AddField("Humidity", rand.Int31n(100))
	p.AddField("Temperature", rand.Int31n(40))
	p.SetTime(time.Now())
	bp.AddPoint(p)
	time.Sleep(time.Nanosecond)
}
err = client.WriteBatchPoints(exampleDatabase, bp)
if err != nil {
	fmt.Println(err)
}
```

#### 执行查询：

```go
q := Query{
	Database: exampleDatabase,
	Command:  "select * from " + exampleMeasurement,
}
res, err := client.Query(q)
if err != nil {
	fmt.Println(err)
}
for _, r := range res.Results {
	for _, s := range r.Series {
		for _, v := range s.Values {
			for _, i := range v {
				fmt.Print(i)
				fmt.Print(" | ")
			}
			fmt.Println()
		}
	}
}
```
