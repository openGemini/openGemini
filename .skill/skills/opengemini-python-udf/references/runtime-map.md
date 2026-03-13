# Runtime Map

## Main code locations

- `python/agent/openGemini_udf/agent.py`: batching, IO, worker dispatch, agent internals
- `python/agent/setup.py`: Python package metadata for `openGemini_udf`
- `python/ts-udf/server/server.py`: server runner, config loading, pidfile handling
- `python/ts-udf/server/handler.py`: handler entrypoint used by pyworker

## Lifecycle wrapper

`python/pyworker.sh` commands:
- `start`
- `stop`
- `restart`
- `status`

The wrapper expects:
- `-config`
- `-ip`
- `-port`
- `-pidfile`
- `-log`

## Python test suite

- `python/ts-udf/tests/test_base_udf.py`
- `python/ts-udf/tests/test_batch_handler.py`
- `python/ts-udf/tests/test_detector_del.py`
