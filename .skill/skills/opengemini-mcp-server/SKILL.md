---
name: opengemini-mcp-server
description: Use when configuring, testing, or extending the openGemini MCP server under python/mcp, especially for client env vars, server capabilities, or read-only query safety constraints.
---

# openGemini MCP Server

Use the checked-in server behavior as the contract. Do not assume arbitrary query execution is allowed.

## When to Use

- You are editing `python/mcp`
- You need to install or configure the MCP server for a client
- You need the supported tool or resource surface
- You are debugging why a query is rejected by the server safety checks

## Quick Start

From `python/mcp`:

```bash
pip install .
```

The server module is `mcp_openGemini.server`.

## Required Environment

Client config needs:
- `OPENGEMINI_HOST`
- `OPENGEMINI_PORT`
- `OPENGEMINI_USER`
- `OPENGEMINI_PASSWORD`

## Safety Envelope

The checked-in `execute_influxql` tool only permits single-statement queries that start with `SELECT` or `SHOW`.

Rejected patterns include:
- multiple statements
- `DROP`
- `ALTER`
- `INSERT`
- `CREATE`

## Canonical Files

- `python/mcp/README.md`
- `python/mcp/mcp_openGemini/server.py`
- `python/mcp/pyproject.toml`

## Common Mistakes

- Treating this MCP server as a write-capable admin surface
- Forgetting to set `OPENGEMINI_HOST` and `OPENGEMINI_PORT`
- Editing README examples without matching the server's actual `SELECT` and `SHOW` restrictions
