---

order: 4

---

# Test Tutorials

## How to run unit tests

### Running all tests

You can always run all tests by executing the `gotest` target in Makefile:

```bash
make gotest
```

This is almost equivalent to `go test ./...` but it enables and disables fail points before and after running tests.

[pingcap/failpoint](https://github.com/pingcap/failpoint) is an implementation of [failpoints](https://www.freebsd.org/cgi/man.cgi?query=fail) for Golang. A fail point is used to add code points where you can inject errors. Fail point is a code snippet that is only executed when the corresponding fail point is active.

### Running a single test

To run a single test, you can manually repeat what `make gotest` does and narrow the scope in one test or one package:

```bash
cd /path/to/dir/of/test
go test -v -run TestSchemaValidator # or with any other test flags
```

If one want to compile the test into a debug binary for running in a debugger, one can also use `go test -gcflags="all=-N -l" -o ./t`, which removes any optimisations and outputs a `t` binary file ready to be used, like `dlv exec ./t` or combine it with the above to only debug a single test `dlv exec ./t -- -test.run "^TestT$" -check.f TestBinaryOpFunction`.

To display information on all the test flags, enter `go help testflag`.

If you develop with **GoLand**, you can also run a test from the IDE with manually enabled and disabled fail points. See the [documentation](https://www.jetbrains.com/help/go/performing-tests.html) for details.

If you develop with **VS Code**, you can also run a test from the editor with manually enabled and disabled fail points. See the [documentation](https://code.visualstudio.com/docs/languages/go#_test) for details.

### Running tests for a pull request (TODO)

> If you haven't joined the organization, you should wait for a member to comment with `/ok-to-test` to your pull request.

Before you merge a pull request, it must pass all tests.

Generally, continuous integration (CI) runs the tests for you; however, if you want to run tests with conditions or rerun tests on failure, you should know how to do that, the the rerun guide comment will be sent when the CI tests failed.

#### `/retest`

Rerun all failed CI test cases.

#### `/test {{test1}} {{testN}}`

Run given CI failed tests.

## How to find failed tests

There are several common causes of failed tests.

### Assertion failed

The most common cause of failed tests is that assertion failed. Its failure report looks like:

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

To find this type of failure, enter `grep -i "FAIL"` to search the report output.

### Data race

Golang testing supports detecting data race by running tests with the `-race` flag. Its failure report looks like:

```
[2021-06-21T15:36:38.766Z] ==================
[2021-06-21T15:36:38.766Z] WARNING: DATA RACE
[2021-06-21T15:36:38.766Z] Read at 0x00c0055ce380 by goroutine 108:
...
[2021-06-21T15:36:38.766Z] Previous write at 0x00c0055ce380 by goroutine 169:
[2021-06-21T15:36:38.766Z]   [failed to restore the stack]
```

### Goroutine leak

We use goleak to detect goroutine leak for tests. Its failure report looks like:

```
goleak: Errors on successful test run: found unexpected goroutines:
[Goroutine 104 in state chan receive, with go.etcd.io/etcd/pkg/logutil.(*MergeLogger).outputLoop on top of the stack:
goroutine 104 [chan receive]:
go.etcd.io/etcd/pkg/logutil.(*MergeLogger).outputLoop(0xc000197398)
    /go/pkg/mod/go.etcd.io/etcd@v0.5.0-alpha.5.0.20200824191128-ae9734ed278b/pkg/logutil/merge_logger.go:173 +0x3ac
created by go.etcd.io/etcd/pkg/logutil.NewMergeLogger
    /go/pkg/mod/go.etcd.io/etcd@v0.5.0-alpha.5.0.20200824191128-ae9734ed278b/pkg/logutil/merge_logger.go:91 +0x85

```

To  determine the source of package leaks, see the [documentation](https://github.com/uber-go/goleak/#determine-source-of-package-leaks)

### Timeout

Every test case should run in at most five seconds.

If a test case takes longer, its failure report looks like:

```
[2021-08-09T03:33:57.661Z] The following test cases take too long to finish:
[2021-08-09T03:33:57.661Z] PASS: server_test.go:874: serverTestSerialSuite.TestTLS  7.388s
[2021-08-09T03:33:57.661Z] --- PASS: TestCluster (5.20s)
```

## How to run integration tests

::: tip

All integration test files of openGemini are placed under the `tests` folder.

:::

First, you need to start the test environment locally. It is recommended to use the following command to quickly start the local pseudo-cluster:

```bash
# 1. build source code
make go-build
# 2. start cluster for test (prefer linux)
bash scripts/install_cluster.sh
```

### Running all tests

You can always run all tests by executing the `integration-test` target in your Makefile:

```bash
make integration-test
```

### Running a single test

To run a single test, you can execute a command like this:

```bash
URL=http://127.0.0.1:8086 go test -mod=mod -test.parallel 1 -timeout 10m ./tests -run TestServer_FullJoin  -v GOCACHE=off
```

### How to write integration tests

Please refer to the existing integration test style to add your test cases.
