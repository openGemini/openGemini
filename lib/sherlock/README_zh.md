[TOC]



# sherlock

基于规则的自动Golang Profile Dumper.

作为一名"懒惰"的程序员，如何避免在线上Golang系统半夜宕机（一般是OOM导致的）时起床保存现场呢？又或者如何dump压测时性能尖刺时刻的profile文件呢？

sherlock 或许能帮助您解决以上问题。

# 设计


sherlock 每隔一段时间收集一次以下应用指标：

* 协程数，通过`runtime.NumGoroutine`。
* 当前应用所占用的物理内存，通过[gopsutil](https://github.com/shirou/gopsutil)第三方库。
* CPU使用率，比如8C的机器，如果使用了4C，则使用率为50%，通过[gopsutil](https://github.com/shirou/gopsutil)第三方库。

在预热阶段（应用启动后，sherlock会收集十次指标）结束后，sherlock会比较当前指标是否满足用户所设置的阈值/规则，如果满足的话，则dump profile，
以二进制文件的格式保留现场。

# 如何用

```go
func main() {
  s := initSherlock()

  // start the metrics collect and dump loop
  s.Start()
  ......

  // quit the application and stop the dumper
  s.Stop()
}

func initSherlock() *Sherlock {
    s := sherlock.New(
        sherlock.WithMonitorInterval(10 * time.Second),
        sherlock.WithCPUMax(90),
        sherlock.WithSavePath("/tmp"),
        sherlock.WithLogger(logger.NewLogger(errno.ModuleUnknown)),
        sherlock.WithCPURule(20, 25, 80, time.Minute),
        sherlock.WithMemRule(20, 25, 80, time.Minute),
        sherlock.WithGrtRule(1000, 25, 20000, 0, time.Minute),
    )
    s.EnableCPUDump()
    s.EnableMemDump()
    s.EnableGrtDump()
    return s
}
```

## sherlock支持对以下几种应用指标进行监控:

```
* mem: 内存分配
* cpu: cpu使用率
* goroutine: 协程数
```

# Dump Goroutine profile

```go
s := sherlock.New(
    sherlock.WithMonitorInterval(10 * time.Second),
    sherlock.WithCPUMax(90),
    sherlock.WithSavePath("/tmp"),
    sherlock.WithLogger(logger.NewLogger(errno.ModuleUnknown)),
    sherlock.WithGoroutineDump(1000, 20, 10000, 3*10000, time.Minute),
)
s.EnableGrtDump()

// start the metrics collect and dump loop
s.Start()
......

// quit the application and stop the dumper
s.Stop()
```

- WithMonitorInterval(10 * time.Second) 每10s采集一次当前应用的各项指标。
- WithCPUMax(90) 当CPU使用率超过90时，禁止dump profile，防止压倒*骆驼*。
- WithSavePath("/tmp") profile文件保存路径。
- WithLogger() 初始化logger模块。
- WithGoroutineDump(1000, 20, 10000, 3*10000, time.Minute) 当goroutine指标满足以下条件时，将会触发dump操作。
  - `current_goroutine_num >= 1000 && current_goroutine_num <= 10000 && current_goroutine_num >= 125% * previous_mean_goroutine_num`
  - `current_goroutine_num > 10000`
  - `time.Minute` 是两次dump操作之间最小时间间隔，避免频繁profiling对性能产生的影响。

WithGoroutineDump(min int, diff int, abs int, max int, coolDown time.Duration) 当应用所启动的goroutine number大于`Max` 时，会跳过dump操作，因为当goroutine number很大时， dump goroutine profile操作成本很高（STW && dump），有可能拖垮应用。当`Max`=0 时代表没有限制。

# Dump Heap Memory Profile

```go
s := sherlock.New(
    sherlock.WithMonitorInterval(10 * time.Second),
    sherlock.WithCPUMax(90),
    sherlock.WithSavePath("/tmp"),
    sherlock.WithLogger(logger.NewLogger(errno.ModuleUnknown)),
    sherlock.WithMemRule(30, 25, 80, time.Minute),
)
s.EnableMemDump()

// start the metrics collect and dump loop
s.Start()
......

// quit the application and stop the dumper
s.Stop()
```

- WithMemDump(30, 25, 80, time.Minute) 会在满足以下条件时抓取heap profile
  - `memory usage >= 30% && memory usage <= 80% && memory usage >= 125% * previous mean memory usage`
  -  `memory usage > 80%`
  - `time.Minute` 是两次dump操作之间最小时间间隔，避免频繁profiling对性能产生的影响。

# Dump CPU Profile

同 [Dump Heap Memory Profile](#Dump Heap Memory Profile)

# 致谢

sherlock 思路参考 https://xargin.com/autodumper-for-go/。

代码实现参考[holmes](https://github.com/mosn/holmes) 优秀开源项目。
