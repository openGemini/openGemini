# openGemini Second Batch Skills Design

## Goal

Add the next three openGemini skills that sit on top of the local-dev and ci-gates foundation:
- `opengemini-config-editing`
- `opengemini-query-compat-testing`
- `opengemini-distributed-debugging`

## Scope

### `opengemini-config-editing`

Cover safe editing of `config/openGemini.conf`, `config/openGemini.singlenode.conf`, and Docker-specific config substitutions.

Must include:
- placeholder awareness for `{{meta_addr_n}}`, `{{addr}}`, `{{id}}`, and Docker `{{domain}}`
- config areas frequently mutated by scripts or tests, especially `subscriber`, `shelf-mode`, `ptnum-pernode`, and `time-range-limit`
- warning that integration scripts modify config files in place and must be restored

Must not include:
- deployment architecture decisions beyond pointing to Docker docs
- full debugging of startup failures

### `opengemini-query-compat-testing`

Cover repository-native testing for InfluxQL, PromQL, and read/write compatibility behavior.

Must include:
- where the large integration suites live
- when to use `tests/server_test.go` versus `tests/prom_test.go`
- how to prefer targeted `go test ./tests -run ...` while iterating and broader integration coverage before completion
- reminder that these tests assume a running cluster or server context depending on harness setup

Must not include:
- generic SQL testing advice
- distributed debugging beyond test setup prerequisites

### `opengemini-distributed-debugging`

Cover first-response debugging for multi-process local clusters and role interactions.

Must include:
- role split between `ts-meta`, `ts-store`, and `ts-sql`
- startup script locations
- canonical log and pid paths under `/tmp/openGemini`
- where to look next in code for raft, migration, balance, subscriber, and shard-routing problems

Must not include:
- a full incident playbook
- production HA automation beyond pointing to helper scripts

## Source Material

- `config/openGemini.conf`
- `config/openGemini.singlenode.conf`
- `docker/README.md`
- `scripts/install.sh`
- `scripts/install_cluster.sh`
- `scripts/ut_test.sh`
- `tests/server_test.go`
- `tests/prom_test.go`
- `tests/server_helpers.go`
- `app/ts-meta/run/server.go`
- `app/ts-store/run/server.go`
- `app/ts-sql/sql/server.go`
- `app/ts-meta/meta/`
- `coordinator/`

## Verification Strategy

Static verification only for this batch:
- file existence
- frontmatter correctness
- command and file anchors exist in the repo
- references resolve to real files

## Risks

- `config-editing` can become a config encyclopedia. Keep only high-risk edit surfaces.
- `query-compat-testing` can become a generic test guide. Keep it tied to openGemini protocol suites.
- `distributed-debugging` can expand into a full operations manual. Keep it focused on local reproduction, log-first triage, and code navigation.
