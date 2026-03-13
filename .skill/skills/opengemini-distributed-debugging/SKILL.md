---
name: opengemini-distributed-debugging
description: Use when openGemini local clusters fail across ts-meta, ts-store, and ts-sql, or when debugging raft, migration, balance, subscriber, or shard-routing issues across processes.
---

# openGemini Distributed Debugging

Start with process roles, logs, and startup scripts. Do not jump into random packages first.

## When to Use

- `scripts/install_cluster.sh` starts partially or one role dies
- a query or write issue spans `ts-meta`, `ts-store`, and `ts-sql`
- the symptom involves raft, migration, balance, subscriber, or shard routing
- you need to map a local cluster failure to the next code area to inspect

## Role Map

- `ts-meta`: metadata, raft, cluster membership, migration and balance decisions
- `ts-store`: shard storage, ingest, select, and engine behavior
- `ts-sql`: HTTP/SQL entrypoint, routing, and coordinator-facing query or write orchestration

## First-Response Workflow

### 1. Reproduce with the canonical local cluster path

```bash
python3 build.py --clean
bash scripts/install_cluster.sh
```

### 2. Confirm process and file layout

Use the logs and pid paths in [references/log-map.md](references/log-map.md).

### 3. Triage by role before reading code

- `ts-meta` failure: inspect `app/ts-meta/run/` and `app/ts-meta/meta/` for raft, migrate, balance, and cluster-manager paths
- `ts-store` failure: inspect `app/ts-store/run/`, `app/ts-store/transport/`, and `engine/`
- `ts-sql` failure: inspect `app/ts-sql/sql/` and `coordinator/`
- subscriber-sensitive issue: inspect `[subscriber]` in `config/openGemini.conf` and any `make start-subscriber` usage

## Common Mistakes

- Reading deep engine code before checking which process actually failed
- Ignoring `/tmp/openGemini/logs` and trying to infer failure only from test output
- Treating generated `config/openGemini-*.conf` as hand-maintained source files
- Mixing startup problems with protocol-compat problems that belong in `./tests`
