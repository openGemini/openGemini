# openGemini Skill Catalog

## Goal

Provide a single index for the current openGemini skill set so future maintenance can see which workflow each skill owns, which keywords should trigger it, and where scope boundaries are intentionally drawn.

## Foundation Skills

| Skill | Primary Trigger | Typical Keywords | Canonical Files |
|---|---|---|---|
| `opengemini-local-dev` | local build and startup | `build.py`, `install.sh`, `install_cluster.sh`, `ts-server`, `ts-meta`, `ts-store`, `ts-sql` | `build.py`, `scripts/install.sh`, `scripts/install_cluster.sh` |
| `opengemini-ci-gates` | pre-PR validation and CI reproduction | `make gotest`, `static-check`, `style-check`, `go-vet`, `integration-test`, `ut_test.sh` | `Makefile`, `Makefile.common`, `.github/workflows/ci.yml`, `scripts/ut_test.sh` |

## Runtime and Testing Skills

| Skill | Primary Trigger | Typical Keywords | Canonical Files |
|---|---|---|---|
| `opengemini-config-editing` | config mutation and placeholder-sensitive changes | `openGemini.conf`, `subscriber`, `shelf-mode`, `time-range-limit`, `ptnum-pernode`, `domain` | `config/openGemini.conf`, `config/openGemini.singlenode.conf`, `docker/README.md`, `scripts/ut_test.sh` |
| `opengemini-query-compat-testing` | protocol-facing regression tests | `InfluxQL`, `PromQL`, `/query`, `/write`, `tests/server_test.go`, `tests/prom_test.go` | `tests/server_test.go`, `tests/prom_test.go`, `tests/server_helpers.go` |
| `opengemini-distributed-debugging` | multi-process local cluster failures | `raft`, `migrate`, `balance`, `subscriber`, `shard routing`, `/tmp/openGemini/logs` | `app/ts-meta/meta/`, `app/ts-store/run/`, `app/ts-sql/sql/`, `coordinator/` |
| `opengemini-go-performance-style` | hot-path Go coding and review | `sync.Pool`, `bufferpool`, `lib/pool`, `resourceallocator`, `GC`, `goroutine fan-out`, `memory reuse` | `lib/pool/union_pool.go`, `lib/bufferpool/pool.go`, `lib/record/record_pool.go`, `services/continuousquery/service.go`, `app/ts-store/storage/storage.go` |

## Ecosystem Skills

| Skill | Primary Trigger | Typical Keywords | Canonical Files |
|---|---|---|---|
| `opengemini-python-udf` | Python UDF and pyworker work | `pyworker`, `python/ts-udf`, `python/agent`, `test_base_udf` | `python/pyworker.sh`, `python/agent/openGemini_udf/agent.py`, `python/ts-udf/server/server.py` |
| `opengemini-mcp-server` | MCP server config and safety rules | `python/mcp`, `OPENGEMINI_HOST`, `SELECT`, `SHOW`, `execute_influxql` | `python/mcp/README.md`, `python/mcp/mcp_openGemini/server.py`, `python/mcp/pyproject.toml` |
| `opengemini-docker-k8s-deploy` | Docker, K8s, and KubeEdge deployment flows | `OPEN_GEMINI_LAUNCH`, `OPEN_GEMINI_DOMAIN`, `OPEN_GEMINI_CONFIG`, `docker run`, `domain =` | `docker/README.md`, `docker/server/README.md`, `docker/server/entrypoint.sh` |

## Boundary Rules

- Use `opengemini-local-dev` to start binaries and find logs; switch to `opengemini-distributed-debugging` once the issue spans roles or internal cluster behavior.
- Use `opengemini-ci-gates` for validation order; switch to `opengemini-query-compat-testing` when the question is where protocol regressions belong.
- Use `opengemini-config-editing` for source config changes; do not overload it with deployment architecture or incident-response guidance.
- Use `opengemini-docker-k8s-deploy` for container env vars and domain substitution; do not use it for generic config edits unrelated to containers.
- Use `opengemini-python-udf` and `opengemini-mcp-server` only for the Python-side subsystems, not for general Go server changes.
- Use `opengemini-go-performance-style` for hot-path allocation, reuse, and bounded-concurrency review; do not use it as a substitute for benchmark or pprof methodology.

## Maintenance Notes

- Keep descriptions narrow; they should describe when to load the skill, not summarize the workflow.
- Prefer adding references over bloating `SKILL.md` when a file map or env-var matrix grows.
- When a new openGemini workflow appears, add it here before deciding whether it deserves a new skill or belongs inside an existing one.
