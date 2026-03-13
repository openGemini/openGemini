# openGemini Third Batch Skills Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the Python UDF, MCP server, and Docker/K8s deployment skills for openGemini.

**Architecture:** Create three focused skills under `~/.codex/skills`, using references only when filesystem maps or env-var matrices would otherwise bloat the main skill. Verify every command, env var, and path against the current repository state.

**Tech Stack:** Markdown skill files, shell verification with `rg`, `test`, and `find`.

---

## Chunk 1: Design and setup

### Task 1: Persist design and create directories

**Files:**
- Create: `docs/superpowers/specs/2026-03-13-opengemini-third-batch-skills-design.md`
- Create: `docs/superpowers/plans/2026-03-13-opengemini-third-batch-skills.md`
- Create: `/Users/ryan/.codex/skills/opengemini-python-udf/`
- Create: `/Users/ryan/.codex/skills/opengemini-mcp-server/`
- Create: `/Users/ryan/.codex/skills/opengemini-docker-k8s-deploy/`

- [ ] **Step 1: Write the design doc and plan**
- [ ] **Step 2: Verify they exist**
Run: `test -f docs/superpowers/specs/2026-03-13-opengemini-third-batch-skills-design.md && test -f docs/superpowers/plans/2026-03-13-opengemini-third-batch-skills.md`
Expected: exit 0
- [ ] **Step 3: Create the skill directories**
Run: `mkdir -p /Users/ryan/.codex/skills/opengemini-python-udf/references /Users/ryan/.codex/skills/opengemini-mcp-server /Users/ryan/.codex/skills/opengemini-docker-k8s-deploy/references`
Expected: directories exist

## Chunk 2: Implement `opengemini-python-udf`

### Task 2: Create the Python UDF skill

**Files:**
- Create: `/Users/ryan/.codex/skills/opengemini-python-udf/SKILL.md`
- Create: `/Users/ryan/.codex/skills/opengemini-python-udf/references/runtime-map.md`

- [ ] **Step 1: Verify `SKILL.md` does not exist yet**
Run: `test -f /Users/ryan/.codex/skills/opengemini-python-udf/SKILL.md`
Expected: non-zero exit status before creation
- [ ] **Step 2: Write the skill and reference**
- [ ] **Step 3: Verify anchors**
Run: `rg -n "pyworker|python/ts-udf|python/agent|test_base_udf|name: opengemini-python-udf" /Users/ryan/.codex/skills/opengemini-python-udf/SKILL.md /Users/ryan/.codex/skills/opengemini-python-udf/references/runtime-map.md`
Expected: matches found

## Chunk 3: Implement `opengemini-mcp-server`

### Task 3: Create the MCP server skill

**Files:**
- Create: `/Users/ryan/.codex/skills/opengemini-mcp-server/SKILL.md`

- [ ] **Step 1: Verify `SKILL.md` does not exist yet**
Run: `test -f /Users/ryan/.codex/skills/opengemini-mcp-server/SKILL.md`
Expected: non-zero exit status before creation
- [ ] **Step 2: Write the skill**
- [ ] **Step 3: Verify anchors**
Run: `rg -n "OPENGEMINI_HOST|OPENGEMINI_PORT|SELECT|SHOW|python/mcp|name: opengemini-mcp-server" /Users/ryan/.codex/skills/opengemini-mcp-server/SKILL.md`
Expected: matches found

## Chunk 4: Implement `opengemini-docker-k8s-deploy`

### Task 4: Create the Docker and K8s deployment skill

**Files:**
- Create: `/Users/ryan/.codex/skills/opengemini-docker-k8s-deploy/SKILL.md`
- Create: `/Users/ryan/.codex/skills/opengemini-docker-k8s-deploy/references/env-matrix.md`

- [ ] **Step 1: Verify `SKILL.md` does not exist yet**
Run: `test -f /Users/ryan/.codex/skills/opengemini-docker-k8s-deploy/SKILL.md`
Expected: non-zero exit status before creation
- [ ] **Step 2: Write the skill and reference**
- [ ] **Step 3: Verify anchors**
Run: `rg -n "OPEN_GEMINI_LAUNCH|OPEN_GEMINI_DOMAIN|OPEN_GEMINI_CONFIG|domain =|docker run|name: opengemini-docker-k8s-deploy" /Users/ryan/.codex/skills/opengemini-docker-k8s-deploy/SKILL.md /Users/ryan/.codex/skills/opengemini-docker-k8s-deploy/references/env-matrix.md`
Expected: matches found

## Chunk 5: Final verification

### Task 5: Verify new skill files and repo anchors

**Files:**
- Modify: `/Users/ryan/.codex/skills/opengemini-python-udf/SKILL.md`
- Modify: `/Users/ryan/.codex/skills/opengemini-mcp-server/SKILL.md`
- Modify: `/Users/ryan/.codex/skills/opengemini-docker-k8s-deploy/SKILL.md`

- [ ] **Step 1: List all new skill files**
Run: `find /Users/ryan/.codex/skills/opengemini-python-udf /Users/ryan/.codex/skills/opengemini-mcp-server /Users/ryan/.codex/skills/opengemini-docker-k8s-deploy -maxdepth 2 -type f | sort`
Expected: five files listed
- [ ] **Step 2: Verify core repo anchors still exist**
Run: `test -f python/pyworker.sh && test -f python/mcp/README.md && test -f python/mcp/mcp_openGemini/server.py && test -f docker/README.md && test -f docker/server/entrypoint.sh`
Expected: exit 0
