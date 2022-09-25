# dictpool

[![Test status](https://github.com/savsgio/dictpool/actions/workflows/test.yml/badge.svg?branch=master)](https://github.com/savsgio/dictpool/actions?workflow=test)
[![Go Report Card](https://goreportcard.com/badge/github.com/savsgio/dictpool)](https://goreportcard.com/report/github.com/savsgio/dictpool)
[![GoDev](https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white)](https://pkg.go.dev/github.com/savsgio/dictpool)

Memory store like `map[string]interface{}` with better performance when reuse memory.

**Very useful when reuse memory (_sync.Pool_) to avoid extra allocations and increase the performance**.

## Benchmarks:

```
BenchmarkDict-4         	34935307	        35.2 ns/op	       0 B/op	       0 allocs/op
BenchmarkStdMap-4       	20123041	        59.5 ns/op	       0 B/op	       0 allocs/op
```

_Benchmark with Go 1.15_

## Example:

```go
d := dictpool.AcquireDict()
// d.BinarySearch = true  // Useful on big heaps

key := "foo"

d.Set(key, "Hello DictPool")

if d.Has(key){
    fmt.Println(d.Get(key))  // Output: Hello DictPool
}

d.Del(key)

dictpool.ReleaseDict(d)
```
