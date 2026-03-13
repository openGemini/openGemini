# openGemini Go Performance Style Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build an openGemini-specific skill for high-performance Go coding style, focused on memory reuse, GC reduction, and bounded concurrency.

**Architecture:** Implement one skill under `~/.codex/skills` with a short normative `SKILL.md` and one reference file that maps the repository's existing pool, buffer reuse, allocator, and goroutine-limiting patterns to concrete code locations.

**Tech Stack:** Markdown skill files, shell verification with `rg`, `test`, and `find`.

---

## Chunk 1: Design and setup

### Task 1: Persist design and create directories

**Files:**
- Create: `docs/superpowers/specs/2026-03-13-opengemini-go-performance-style-design.md`
- Create: `docs/superpowers/plans/2026-03-13-opengemini-go-performance-style.md`
- Create: `/Users/ryan/.codex/skills/opengemini-go-performance-style/`

- [ ] **Step 1: Write the design doc and plan**
- [ ] **Step 2: Verify they exist**
Run: `test -f docs/superpowers/specs/2026-03-13-opengemini-go-performance-style-design.md && test -f docs/superpowers/plans/2026-03-13-opengemini-go-performance-style.md`
Expected: exit 0
- [ ] **Step 3: Create the skill directory structure**
Run: `mkdir -p /Users/ryan/.codex/skills/opengemini-go-performance-style/references`
Expected: directory exists

## Chunk 2: Implement the skill

### Task 2: Create the performance style skill

**Files:**
- Create: `/Users/ryan/.codex/skills/opengemini-go-performance-style/SKILL.md`
- Create: `/Users/ryan/.codex/skills/opengemini-go-performance-style/references/pattern-map.md`

- [ ] **Step 1: Verify `SKILL.md` does not exist yet**
Run: `test -f /Users/ryan/.codex/skills/opengemini-go-performance-style/SKILL.md`
Expected: non-zero exit status before creation
- [ ] **Step 2: Write the skill and reference file**
- [ ] **Step 3: Verify anchors**
Run: `rg -n "lib/pool|bufferpool|sync.Pool|resourceallocator|goroutine|tokens|name: opengemini-go-performance-style" /Users/ryan/.codex/skills/opengemini-go-performance-style/SKILL.md /Users/ryan/.codex/skills/opengemini-go-performance-style/references/pattern-map.md`
Expected: matches found

## Chunk 3: Final verification

### Task 3: Verify new skill files and repo anchors

**Files:**
- Modify: `/Users/ryan/.codex/skills/opengemini-go-performance-style/SKILL.md`

- [ ] **Step 1: List created files**
Run: `find /Users/ryan/.codex/skills/opengemini-go-performance-style -maxdepth 2 -type f | sort`
Expected: `SKILL.md` and `references/pattern-map.md`
- [ ] **Step 2: Verify core repo anchors still exist**
Run: `test -f lib/pool/union_pool.go && test -f lib/bufferpool/pool.go && test -f lib/record/record_pool.go && test -f services/continuousquery/service.go && test -f app/ts-store/storage/storage.go`
Expected: exit 0
