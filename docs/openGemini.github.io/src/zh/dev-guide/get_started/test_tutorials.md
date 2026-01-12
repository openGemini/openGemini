---
order: 4
---

# 测试教程

## 如何运行单元测试

### 运行所有测试

您始终可以通过在 Makefile 中执行 `gotest` 目标来运行所有测试：

```bash
make gotest
```

这几乎等同于 `go test ./...` 但它在运行测试之前和之后启用和禁用失败点。

[pingcap/failpoint](https://github.com/pingcap/failpoint) 是 Golang 的 [failpoints](https://www.freebsd.org/cgi/man.cgi?query=fail) 的一个实现。 失败点用于添加可以注入错误的代码点。 失败点是一个代码片段，只有在相应的失败点处于活动状态时才会执行。

### 运行单个测试

要运行单个测试，您可以手动重复 `make gotest` 所做的并缩小一个测试或一个包的范围：

```bash
cd /path/to/dir/of/test
go test -v -run TestSchemaValidator # or with any other test flags
```

如果想将测试编译成调试二进制文件以便在调试器中运行，还可以使用 `go test -gcflags="all=-N -l" -o ./t`，它会删除所有优化并输出 ` t` 准备好使用的二进制文件，如 `dlv exec ./t` 或将其与上述组合以仅调试单个测试 `dlv exec ./t -- -test.run "^TestT$" -check.f TestBinaryOpFunction`。

要显示有关所有测试标志的信息，请输入 `go help testflag`。

如果您使用 **GoLand** 进行开发，您还可以通过手动启用和禁用故障点的 IDE 运行测试。 有关详细信息，请参阅 [文档](https://www.jetbrains.com/help/go/performing-tests.html)。

如果您使用 **VS Code** 进行开发，您还可以通过手动启用和禁用故障点的编辑器运行测试。 有关详细信息，请参阅 [文档](https://code.visualstudio.com/docs/languages/go#_test)。

### 为拉取请求运行测试 (TODO)

::: warning

如果您还没有加入该组织，您应该等待成员用"/ok-to-test"评论您的拉取请求。

:::

在合并拉取请求之前，它必须通过所有测试。

通常，持续集成 (CI) 会为您运行测试； 但是，如果你想运行有条件的测试或在失败时重新运行测试，你应该知道如何去做，当 CI 测试失败时，将发送**重新运行**指令。

#### `/retest`

重新运行所有失败的 CI 测试用例。

#### `/test {{test1}} {{testN}}`

运行给定的 CI 失败测试。

## 如何找到失败的测试

测试失败的常见原因有几种。

### 断言失败

测试失败的最常见原因是断言失败。 它的失败报告如下：

```
=== RUN   TestTopology
    info_test.go:72:
            Error Trace:    info_test.go:72
            Error:          Not equal:
                            expected: 1282967700000
                            actual  : 1628585893
            Test:           TestTopology
--- FAIL: TestTopology (0.76s)
```

要查找此类故障，请输入 `grep -i "FAIL"` 以搜索报告输出。

### 数据竞争

Golang 测试支持通过使用 `-race` 标志运行测试来检测数据竞争。 它的失败报告如下：

```
[2021-06-21T15:36:38.766Z] ==================
[2021-06-21T15:36:38.766Z] WARNING: DATA RACE
[2021-06-21T15:36:38.766Z] Read at 0x00c0055ce380 by goroutine 108:
...
[2021-06-21T15:36:38.766Z] Previous write at 0x00c0055ce380 by goroutine 169:
[2021-06-21T15:36:38.766Z]   [failed to restore the stack]
```

### 协程泄漏

我们使用 goleak 来检测 goroutine 泄漏以进行测试。 它的失败报告如下：

```
goleak: Errors on successful test run: found unexpected goroutines:
[Goroutine 104 in state chan receive, with go.etcd.io/etcd/pkg/logutil.(*MergeLogger).outputLoop on top of the stack:
goroutine 104 [chan receive]:
go.etcd.io/etcd/pkg/logutil.(*MergeLogger).outputLoop(0xc000197398)
    /go/pkg/mod/go.etcd.io/etcd@v0.5.0-alpha.5.0.20200824191128-ae9734ed278b/pkg/logutil/merge_logger.go:173 +0x3ac
created by go.etcd.io/etcd/pkg/logutil.NewMergeLogger
    /go/pkg/mod/go.etcd.io/etcd@v0.5.0-alpha.5.0.20200824191128-ae9734ed278b/pkg/logutil/merge_logger.go:91 +0x85

```

要确定包泄漏的来源，请参阅 [文档](https://github.com/uber-go/goleak/#determine-source-of-package-leaks)

### 超时

每个测试用例最多应运行五秒钟。

如果测试用例花费的时间更长，其失败报告如下所示：

```
[2021-08-09T03:33:57.661Z] The following test cases take too long to finish:
[2021-08-09T03:33:57.661Z] PASS: server_test.go:874: serverTestSerialSuite.TestTLS  7.388s
[2021-08-09T03:33:57.661Z] --- PASS: TestCluster (5.20s)
```

## 如何运行集成测试

::: tip

openGemini的所有集成测试文件放在`tests`文件夹下面。

:::

首先需要本地启动测试环境，推荐使用如下命令，快速启动本地伪集群：

```bash
# 1. build source code
make go-build
# 2. start cluster for test (prefer linux)
bash scripts/install_cluster.sh
```

### 运行所有测试

您始终可以通过在 Makefile 中执行 `integration-test` 目标来运行所有测试：

```bash
make integration-test
```

### 运行单个测试

要运行单个测试，您可以执行如下命令：

```bash
URL=http://127.0.0.1:8086 go test -mod=mod -test.parallel 1 -timeout 10m ./tests -run TestServer_FullJoin  -v GOCACHE=off
```

### 如何编写集成测试

请参考已有的集成测试风格，新增您的测试用例。
