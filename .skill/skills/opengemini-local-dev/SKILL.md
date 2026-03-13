---
name: opengemini-local-dev
description: Use when building openGemini locally, starting ts-server or the local pseudo-cluster, locating runtime logs or pid files, or handling macOS local cluster setup.
---

# openGemini Local Dev

Use the checked-in build and startup scripts before inventing custom launch commands.

## When to Use

- You need fresh binaries in `build/`
- You need a local `ts-server` for standalone work
- You need a local pseudo-cluster with `ts-meta`, `ts-store`, and `ts-sql`
- You need to find the active config, log, or pid files
- You are on macOS and the cluster scripts need loopback aliases

## Quick Start

### 1. Build the repo

```bash
python3 build.py --clean
```

This produces `build/ts-server`, `build/ts-meta`, `build/ts-store`, `build/ts-sql`, and related binaries.

### 2. Start a standalone node

Preferred manual command:

```bash
./build/ts-server run -config=config/openGemini.singlenode.conf
```

Scripted background startup:

```bash
bash scripts/install.sh
```

### 3. Start a local pseudo-cluster

Build first, then run:

```bash
bash scripts/install_cluster.sh
```

This brings up three local meta nodes, three store nodes, and one sql node using generated `config/openGemini-*.conf` files.

## Workflow Notes

- Prefer `config/openGemini.singlenode.conf` for standalone work.
- Prefer `config/openGemini.conf` plus `scripts/install_cluster.sh` for cluster-like behavior.
- On macOS, `scripts/install_cluster.sh` may require `sudo ifconfig lo0 alias 127.0.0.2 up` and `127.0.0.3 up`.
- If `sed -i` behaves differently on macOS, use GNU `sed` as the script suggests.

## Runtime Layout

Read [references/runtime-layout.md](references/runtime-layout.md) for the main binary, config, log, and pid paths.

## Common Mistakes

- Running cluster scripts before `python3 build.py --clean`
- Editing generated `config/openGemini-*.conf` instead of the source template
- Looking for logs under the repository root instead of `/tmp/openGemini/logs`
- Treating this skill as a debugging guide; for inter-node failures, use the logs first and escalate to a dedicated debugging workflow
