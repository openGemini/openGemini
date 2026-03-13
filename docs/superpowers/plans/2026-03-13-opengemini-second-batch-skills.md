# openGemini Second Batch Skills Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the next three openGemini skills for config editing, protocol compatibility testing, and distributed debugging.

**Architecture:** Create three focused skills under `~/.codex/skills`, adding references only where file-path maps or configuration hotspots would bloat the main skill body. Verify all anchors against repository files and keep each skill tightly scoped so triggering remains accurate.

**Tech Stack:** Markdown skill files, shell verification with `rg`, `test`, and `find`.

---

## Chunk 1: Design and setup

### Task 1: Persist second-batch design and create skill directories

**Files:**
- Create: `docs/superpowers/specs/2026-03-13-opengemini-second-batch-skills-design.md`
- Create: `docs/superpowers/plans/2026-03-13-opengemini-second-batch-skills.md`
- Create: `/Users/ryan/.codex/skills/opengemini-config-editing/`
- Create: `/Users/ryan/.codex/skills/opengemini-query-compat-testing/`
- Create: `/Users/ryan/.codex/skills/opengemini-distributed-debugging/`

- [ ] **Step 1: Write the design doc and plan**
- [ ] **Step 2: Verify they exist**
Run: `test -f docs/superpowers/specs/2026-03-13-opengemini-second-batch-skills-design.md && test -f docs/superpowers/plans/2026-03-13-opengemini-second-batch-skills.md`
Expected: exit 0
- [ ] **Step 3: Create the skill directory structure**
Run: `mkdir -p /Users/ryan/.codex/skills/opengemini-config-editing/references /Users/ryan/.codex/skills/opengemini-query-compat-testing /Users/ryan/.codex/skills/opengemini-distributed-debugging/references`
Expected: directories exist

## Chunk 2: Implement `opengemini-config-editing`

### Task 2: Create the config editing skill

**Files:**
- Create: `/Users/ryan/.codex/skills/opengemini-config-editing/SKILL.md`
- Create: `/Users/ryan/.codex/skills/opengemini-config-editing/references/config-hotspots.md`

- [ ] **Step 1: Verify `SKILL.md` does not exist yet**
Run: `test -f /Users/ryan/.codex/skills/opengemini-config-editing/SKILL.md`
Expected: non-zero exit status before creation
- [ ] **Step 2: Write the skill and reference file**
- [ ] **Step 3: Verify config anchors**
Run: `rg -n "meta-join|subscriber|shelf-mode|time-range-limit|ptnum-pernode|domain =" /Users/ryan/.codex/skills/opengemini-config-editing/SKILL.md /Users/ryan/.codex/skills/opengemini-config-editing/references/config-hotspots.md`
Expected: matches found

## Chunk 3: Implement `opengemini-query-compat-testing`

### Task 3: Create the protocol compatibility testing skill

**Files:**
- Create: `/Users/ryan/.codex/skills/opengemini-query-compat-testing/SKILL.md`

- [ ] **Step 1: Verify `SKILL.md` does not exist yet**
Run: `test -f /Users/ryan/.codex/skills/opengemini-query-compat-testing/SKILL.md`
Expected: non-zero exit status before creation
- [ ] **Step 2: Write the skill**
- [ ] **Step 3: Verify query-suite anchors**
Run: `rg -n "tests/server_test.go|tests/prom_test.go|InfluxQL|PromQL|go test ./tests" /Users/ryan/.codex/skills/opengemini-query-compat-testing/SKILL.md`
Expected: matches found

## Chunk 4: Implement `opengemini-distributed-debugging`

### Task 4: Create the distributed debugging skill

**Files:**
- Create: `/Users/ryan/.codex/skills/opengemini-distributed-debugging/SKILL.md`
- Create: `/Users/ryan/.codex/skills/opengemini-distributed-debugging/references/log-map.md`

- [ ] **Step 1: Verify `SKILL.md` does not exist yet**
Run: `test -f /Users/ryan/.codex/skills/opengemini-distributed-debugging/SKILL.md`
Expected: non-zero exit status before creation
- [ ] **Step 2: Write the skill and reference file**
- [ ] **Step 3: Verify debugging anchors**
Run: `rg -n "ts-meta|ts-store|ts-sql|raft|migrate|balance|subscriber|/tmp/openGemini/logs|/tmp/openGemini/pid" /Users/ryan/.codex/skills/opengemini-distributed-debugging/SKILL.md /Users/ryan/.codex/skills/opengemini-distributed-debugging/references/log-map.md`
Expected: matches found

## Chunk 5: Final verification

### Task 5: Verify resulting files and repo anchors

**Files:**
- Modify: `/Users/ryan/.codex/skills/opengemini-config-editing/SKILL.md`
- Modify: `/Users/ryan/.codex/skills/opengemini-query-compat-testing/SKILL.md`
- Modify: `/Users/ryan/.codex/skills/opengemini-distributed-debugging/SKILL.md`

- [ ] **Step 1: List all new skill files**
Run: `find /Users/ryan/.codex/skills/opengemini-config-editing /Users/ryan/.codex/skills/opengemini-query-compat-testing /Users/ryan/.codex/skills/opengemini-distributed-debugging -maxdepth 2 -type f | sort`
Expected: five files listed
- [ ] **Step 2: Verify core repo anchors still exist**
Run: `test -f config/openGemini.conf && test -f docker/README.md && test -f tests/server_test.go && test -f tests/prom_test.go && test -f scripts/install_cluster.sh`
Expected: exit 0
