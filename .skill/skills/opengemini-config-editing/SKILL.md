---
name: opengemini-config-editing
description: Use when editing openGemini config files, placeholder-driven cluster settings, or Docker config substitutions and needing to avoid generated or script-mutated config drift.
---

# openGemini Config Editing

Edit source templates deliberately. Do not treat generated configs or test-mutated configs as canonical.

## When to Use

- You are changing `config/openGemini.conf` or `config/openGemini.singlenode.conf`
- You need to reason about `{{meta_addr_n}}`, `{{addr}}`, `{{id}}`, or Docker `{{domain}}`
- You are enabling or disabling `subscriber`, `shelf-mode`, `ptnum-pernode`, or `time-range-limit`
- A script or integration test changed config in place and you need to restore or audit it

## Workflow

### 1. Start from the correct source file

- Use `config/openGemini.singlenode.conf` for standalone `ts-server`
- Use `config/openGemini.conf` for pseudo-cluster, CI, and Docker-derived config flows
- Treat `config/openGemini-*.conf` as generated output from `scripts/install_cluster.sh`, not source of truth

### 2. Preserve placeholder semantics

- `meta-join` depends on `{{meta_addr_1}}`, `{{meta_addr_2}}`, `{{meta_addr_3}}`
- process-local addresses use `{{addr}}`
- per-node storage and logging paths use `{{id}}`
- Docker and K8s flows may add `domain = "{{domain}}"` to `meta` and `data`

### 3. Assume scripts may mutate config

`scripts/ut_test.sh` edits `config/openGemini.conf` in place for `time-range-limit`, `ptnum-pernode`, `subscriber`, and `shelf-mode` scenarios. Inspect and restore config diffs after running integration helpers.

## High-Risk Hotspots

Read [references/config-hotspots.md](references/config-hotspots.md) before making nontrivial config changes.

## Common Mistakes

- Editing generated `config/openGemini-*.conf` and expecting the change to persist
- Forgetting that `scripts/ut_test.sh` rewrites `subscriber` or `shelf-mode`
- Changing Docker `domain =` behavior in one section but not the matching `meta` or `data` section
- Testing cluster config changes with the standalone file or vice versa
