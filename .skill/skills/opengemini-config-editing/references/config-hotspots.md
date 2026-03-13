# Config Hotspots

## Placeholder-sensitive keys

- `meta-join = ["{{meta_addr_1}}:8092", ...]`
- `bind-address = "{{addr}}:..."`
- `store-ingest-addr = "{{addr}}:8400"`
- `store-select-addr = "{{addr}}:8401"`
- path keys containing `{{id}}`
- Docker-specific `domain = "{{domain}}"`

## Frequently mutated by test or helper scripts

- `# ptnum-pernode = 1`
- `# time-range-limit = ["72h", "24h"]`
- `[subscriber]`
- `# shelf-mode = false`

These are modified directly by `scripts/ut_test.sh`, so review the diff after running that script or related integration workflows.

## File choice guide

- `config/openGemini.singlenode.conf`: standalone local server
- `config/openGemini.conf`: cluster template and CI path
- `docker/README.md`: domain-substitution and container deployment rules
