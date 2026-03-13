# openGemini Third Batch Skills Design

## Goal

Add the ecosystem-oriented openGemini skills that are useful beyond core Go server development:
- `opengemini-python-udf`
- `opengemini-mcp-server`
- `opengemini-docker-k8s-deploy`

## Scope

### `opengemini-python-udf`

Cover Python UDF and pyworker workflows under `python/agent`, `python/ts-udf`, and `python/pyworker.sh`.

Must include:
- where the UDF server entrypoints and tests live
- the distinction between `python/agent` and `python/ts-udf`
- pyworker lifecycle commands and config-file expectations

Must not include:
- generic Python packaging advice
- deep model algorithm design

### `opengemini-mcp-server`

Cover the MCP server packaged under `python/mcp`.

Must include:
- install path and Python package location
- environment variables and client config keys
- the read-only safety envelope around `SELECT` and `SHOW`

Must not include:
- generic MCP theory
- open-ended database operations beyond the checked-in server behavior

### `opengemini-docker-k8s-deploy`

Cover the repository's Docker and K8s/KubeEdge deployment guidance.

Must include:
- `OPEN_GEMINI_LAUNCH`, `OPEN_GEMINI_DOMAIN`, and `OPEN_GEMINI_CONFIG`
- why Docker needs `domain = "{{domain}}"`
- differences between the simple server image path and the multi-role container deployment path

Must not include:
- a full production hardening guide
- generic Kubernetes tutoring

## Source Material

- `python/agent/openGemini_udf/agent.py`
- `python/agent/setup.py`
- `python/ts-udf/server/server.py`
- `python/ts-udf/tests/`
- `python/pyworker.sh`
- `python/mcp/README.md`
- `python/mcp/mcp_openGemini/server.py`
- `python/mcp/pyproject.toml`
- `docker/README.md`
- `docker/server/README.md`
- `docker/server/entrypoint.sh`
- `docker/Dockerfile`

## Verification Strategy

Static verification only:
- file existence
- frontmatter correctness
- command, env var, and path anchors present
- references point to real files

## Risks

- `python-udf` can sprawl into algorithm docs. Keep it on runtime and testing workflow.
- `mcp-server` can become generic MCP guidance. Keep it tied to the checked-in server capabilities.
- `docker-k8s-deploy` can become broad ops documentation. Keep it specific to this repo's images, env vars, and config substitution model.
