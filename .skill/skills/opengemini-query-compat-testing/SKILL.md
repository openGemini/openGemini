---
name: opengemini-query-compat-testing
description: Use when changing openGemini query or write-path behavior and needing repository-native compatibility tests for InfluxQL, PromQL, HTTP query endpoints, or write APIs.
---

# openGemini Query Compat Testing

Use the repository's protocol suites before inventing ad hoc query checks.

## When to Use

- You changed InfluxQL parsing, planning, execution, or HTTP `/query` behavior
- You changed PromQL, Prometheus remote read or write validation, or Prometheus-compatible APIs
- You changed write-path behavior that must stay compatible with InfluxDB-style clients
- You need to add or update an end-to-end regression in `./tests`

## Suite Selection

- `tests/server_test.go`: broad InfluxQL, HTTP query, write, admin, and result-shape compatibility
- `tests/prom_test.go`: PromQL and Prometheus-oriented compatibility coverage
- `tests/server_helpers.go`: local and remote test harness helpers

## Iteration Loop

### 1. Put the regression in the right suite

If the user-visible behavior is HTTP- or protocol-facing, prefer `./tests` over a narrow package unit test.

### 2. Iterate with a targeted test command

```bash
go test ./tests -run '<Pattern>' -v
```

Use a narrow regex while developing, then run broader validation before completion.

### 3. Escalate to integration parity when needed

If the change affects cluster startup, config-sensitive behavior, or protocol compatibility across components, finish with:

```bash
make integration-test
```

CI brings the cluster up with `make go-build`, `make start-subscriber`, and `bash scripts/install_cluster.sh` before integration coverage.

## Common Mistakes

- Adding a package-local unit test when the regression is really protocol compatibility
- Editing golden JSON expectations without understanding whether the API contract changed
- Running `go test ./tests` without the required server or cluster context for the chosen test path
- Forgetting that InfluxQL and PromQL coverage live in different suites

## Canonical Files

- `tests/server_test.go`
- `tests/prom_test.go`
- `tests/server_helpers.go`
