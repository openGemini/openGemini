---
name: opengemini-python-udf
description: Use when working on openGemini Python UDF or pyworker code, starting the checked-in UDF server, or testing Python-side batch-processing behavior under python/agent or python/ts-udf.
---

# openGemini Python UDF

Treat the Python UDF stack as its own runtime surface. Do not mix it into the main Go server workflow.

## When to Use

- You are editing `python/agent` or `python/ts-udf`
- You need to start or stop the checked-in pyworker lifecycle
- You need the UDF server entrypoint, config expectations, or Python test locations
- You are debugging batch handling or anomaly-detection glue code rather than core Go execution

## Layout

- `python/agent`: packaged agent implementation such as `openGemini_udf/agent.py`
- `python/ts-udf/server`: runtime server wrappers and handlers
- `python/ts-udf/tests`: Python-side test suite
- `python/pyworker.sh`: start/stop/restart/status lifecycle wrapper

## Workflow

### 1. Pick the right entrypoint

- server startup logic: `python/ts-udf/server/server.py`
- pyworker wrapper: `python/pyworker.sh`
- agent internals and batching: `python/agent/openGemini_udf/agent.py`

### 2. Use the checked-in lifecycle wrapper

`python/pyworker.sh` supports `start`, `stop`, `restart`, and `status`, and requires config, ip, port, pidfile, and log arguments.

### 3. Run Python tests from the dedicated suite

Start with focused tests under `python/ts-udf/tests`, for example `test_base_udf.py`, before expanding to the full Python-side suite.

## Runtime Map

Read [references/runtime-map.md](references/runtime-map.md) for the entrypoint, lifecycle, and test file map.

## Common Mistakes

- Looking for Python UDF behavior under Go package tests first
- Editing `python/agent` but forgetting the runtime wrapper in `python/ts-udf/server`
- Starting ad hoc Python processes instead of using `python/pyworker.sh`
