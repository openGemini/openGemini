# AGENTS.md

Guidance for coding agents working in the openGemini repository. Human
contributors should start with [CONTRIBUTION.md](CONTRIBUTION.md); this file
summarizes the same rules plus the practical details needed to build, test and
submit changes.

## Project overview

openGemini is a distributed time series database written in Go. It is
compatible with the InfluxDB v1 line protocol and InfluxQL, and also serves
Prometheus remote read/write, PromQL, OpenTelemetry and Arrow Flight APIs.

The Go module is `github.com/openGemini/openGemini`. Use the Go version from
`go.mod`; `make go-version-check` fails when the local toolchain's major.minor
version differs from it.

## Repository layout

| Path | Contents |
| --- | --- |
| `app/` | Entry points. `ts-sql` (SQL/HTTP layer), `ts-meta` (Raft-based metadata service), `ts-store` (storage node), `ts-server` (standalone: sql + meta + store in one process), `ts-data` (sql + store), `ts-monitor`, `ts-recover`, `ts-task`. |
| `coordinator/` | ts-sql side of the cluster: routes writes to shards (`points_writer.go`, `record_writer.go`), maps shards for queries and runs meta statements. |
| `engine/` | Storage engine: shards and partitions, WAL, memtables (`mutable/`), TSSP and column store files (`immutable/`), indexes (`index/`), query cursors, and the query execution framework (`executor/`, `hybridqp/`, `op/`). |
| `lib/` | Shared libraries: configuration (`config/`), error codes (`errno/`), logging (`logger/`), RPC transport (`spdy/`, `netstorage/`, `msgservice/`), meta client (`metaclient/`), encoding and compression, records, statistics and more. |
| `lib/util/lifted/` | Code derived from other projects (InfluxDB, Prometheus, VictoriaMetrics, HashiCorp, ...). This includes the InfluxQL parser, HTTP handler, query executor and meta data model under `lib/util/lifted/influx/`. |
| `services/` | Background services such as continuous queries, downsampling, retention, stream computing, hierarchical storage, Arrow Flight and the Castor analysis service. |
| `config/` | Sample configuration files: `openGemini.conf` (cluster), `openGemini.singlenode.conf` (standalone), `monitor.conf`. |
| `scripts/` | Install scripts (`install.sh`, `install_cluster.sh`) and the CI check scripts in `scripts/ci/`. |
| `tests/` | Integration tests that run against a live cluster. They are not part of the unit test run. |
| `python/` | Python components (UDF worker, agent, MCP server). |
| `docker/`, `Dockerfile.nightly` | Container images and deployment notes. |

Request flow, in short: HTTP requests reach the handler in
`lib/util/lifted/influx/httpd`. Writes go through `coordinator.PointsWriter` to
ts-store over the `lib/spdy` transport, where `app/ts-store/transport` passes
them to the engine. Queries are parsed by
`lib/util/lifted/influx/influxql`, planned by `lib/util/lifted/influx/query`
and `engine/executor`, and the store-side part of the plan is executed on each
ts-store node.

## Build

Requirements: Go (version from `go.mod`), Python 3 with the `click` package,
and Git.

```shell
python3 build.py --clean   # or: make go-build
```

Binaries are written to `build/`. To build only what you touched, plain
`go build ./app/ts-server` (or `./app/ts-sql`, `./app/ts-store`, ...) is
enough.

Start a standalone server or a local pseudo-cluster:

```shell
./build/ts-server run -config=config/openGemini.singlenode.conf
bash scripts/install_cluster.sh
```

Runtime data and logs go to `/tmp/openGemini`.

## Tests

Unit tests live next to the code as `*_test.go` files and use
`github.com/stretchr/testify` (`assert`/`require`). Many tests patch functions
with `gomonkey`, so they must be built with inlining and optimizations
disabled. Use the Makefile, which sets the right flags and gives every package
a fresh `/tmp/openGemini` directory:

```shell
# One or more packages
make gotest PACKAGES_OPEN_GEMINI_TESTS="./engine/executor ./lib/config"

# Equivalent manual invocation for a quick run
go test -gcflags "all=-N -l" -tags failpoint -short -count 1 -p 1 -run TestName ./lib/config
```

- `make gotest` without arguments runs every package except `tests/` and is
  slow; prefer running only the packages you changed.
- `make gotest` removes and recreates `/tmp/openGemini`, so do not keep data
  you need there. It also writes `coverage_<n>.txt` files to the repository
  root; delete them instead of committing them.
- Fault injection uses the `failpoint` build tag (`lib/failpoint`); tests that
  rely on it only run with `-tags failpoint`.
- Integration tests in `tests/` need a running cluster on
  `127.0.0.1:8086` (`make go-build`, then `bash scripts/install_cluster.sh`)
  and run with `make integration-test`. CI also runs `make start-subscriber`
  first, which edits `config/openGemini.conf`; revert it with
  `make stop-subscriber` and do not commit that change.
- Add or update tests with every behaviour change. Bug fixes should include a
  test that fails without the fix.

## Code generation

Generated files are committed and CI fails if regenerating them produces a
diff (`make go-generate`, see `scripts/ci/go_generate.sh`). Never edit them by
hand:

- `*.gen.go` files come from the `*.tmpl` templates next to them through
  `//go:generate tmpl ...` directives. Change the template or its `tmpldata`
  file and run `go generate` in that package.
- `y.go` parsers are generated from `sql.y` (InfluxQL in
  `lib/util/lifted/influx/influxql`, log queries in `lib/util/lifted/logparser`)
  with `goyacc` plus post-processing in `scripts/ci/go_generate.sh`. The
  grammar must have no shift/reduce or reduce/reduce conflicts.
- `*.pb.go` files are generated with `protoc` 32.0, and msgp files with
  `msgp`. The tool versions are pinned by the `install-*` targets in
  `Makefile.common`.

## Static checks

CI runs these on every pull request that touches code (see
`.github/workflows/ci.yml`):

```shell
make license-check
make static-check     # staticcheck
make style-check      # goimports-reviser and go mod tidy
make go-vet-check
make go-generate
make go-version-check
```

- Every new Go file outside `lib/util/lifted` and `tests` needs the Apache 2.0
  license header used by recent files, starting with
  `// Copyright <year> openGemini Authors.`. Keep the original copyright
  notices on code derived from other projects.
- Imports are formatted by `goimports-reviser`: the standard library first,
  then all other imports in a single group.
- `go vet` and `staticcheck` skip `tests` and `lifted`; other packages must be
  free of warnings.

## Coding conventions

- Follow the style of the surrounding code and keep changes focused; avoid
  unrelated refactors and formatting changes.
- Return errors defined in `lib/errno` (`errno.NewError(errno.SomeCode, ...)`)
  where the surrounding code does so, and log with `lib/logger` (zap fields)
  instead of `fmt` or the standard `log` package.
- Configuration lives in `lib/config`. When adding an option, give it a
  default, validate it where the section does, and document it in the
  matching file under `config/`. Options listed by `SHOW CONFIGS` come from
  the section's `ShowConfigs()` method; if you add one there, update the
  expected output in `tests/server_suite.go`.
- InfluxQL syntax changes go into `sql.y` (with regenerated `y.go`), the AST in
  `lib/util/lifted/influx/influxql/ast.go`, and parser tests.

## Commits and pull requests

- Create a branch from `main`, named after the change type, for example
  `fix/show-shards-on-database` or `docs/agents-md`.
- Commit messages follow [Conventional Commits](https://www.conventionalcommits.org/)
  with the types from `.commitlint.yaml`: `feat`, `fix`, `docs`, `style`,
  `refactor`, `perf`, `test`, `build`, `ci`, `chore`, `revert`. The header must
  be 10 to 100 characters long.
- Sign off every commit for the DCO: `git commit -s`.
- PR titles use the same format, and the description after the type must be
  at most 72 characters long (checked by `pr-title-checker`).
- Fill in `.github/pull_request_template.md`. The description must contain an
  `Issue Number:` line that links the issue with `close`, `fix`, `resolve` or
  `ref`, so open an issue first if none exists.
