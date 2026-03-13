# openGemini Skill Library

This repository keeps project skills under `.skill/skills/`. Treat this directory as the only editable source.

## Available skills

- `opengemini-local-dev`: local build, standalone startup, pseudo-cluster startup, and runtime file layout
- `opengemini-ci-gates`: local CI order, style checks, failpoint-aware test flow
- `opengemini-config-editing`: safe edits for `config/openGemini.conf`, cluster templates, and Docker substitutions
- `opengemini-query-compat-testing`: InfluxQL, PromQL, and HTTP compatibility test workflow
- `opengemini-distributed-debugging`: `ts-meta`, `ts-store`, and `ts-sql` role-based debugging
- `opengemini-python-udf`: Python UDF runtime, worker lifecycle, and tests
- `opengemini-mcp-server`: MCP server setup and query boundaries
- `opengemini-docker-k8s-deploy`: container and Kubernetes deployment workflow
- `opengemini-go-performance-style`: review rules for memory reuse, pools, and bounded concurrency

## Layout

```text
.skill/
├── README.md
├── platforms.md
└── skills/
   └── <skill-name>/
      ├── SKILL.md
      └── references/
```

## Editing rule

Edit the files under `.skill/skills/` directly. Do not maintain separate copies in `~/.codex/skills`, `~/.claude/skills`, or `.opencode/skills`.

## Setup

Run:

```bash
bash scripts/setup-skills.sh
```

Then read `.skill/platforms.md` for platform-specific behavior.
