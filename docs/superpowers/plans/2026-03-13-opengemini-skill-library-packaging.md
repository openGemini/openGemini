# openGemini Skill Library Packaging Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Package the openGemini skill set inside the repository under `.skill/` and document how Claude Code, Codex, and OpenCode use it.

**Architecture:** Treat `.skill/skills/` as the only editable source, expose it to Claude Code and OpenCode through project-local symlinks, and expose it to Codex through a setup script that links the repository skills into `~/.codex/skills`.

**Tech Stack:** Markdown docs, symlinks, shell script setup, and repository file migration.

---

## Chunk 1: Design and setup

### Task 1: Persist design and create directories

**Files:**
- Create: `docs/superpowers/specs/2026-03-13-opengemini-skill-library-packaging-design.md`
- Create: `docs/superpowers/plans/2026-03-13-opengemini-skill-library-packaging.md`
- Create: `.skill/skills/`

- [ ] **Step 1: Write the design doc and plan**
- [ ] **Step 2: Verify they exist**
Run: `test -f docs/superpowers/specs/2026-03-13-opengemini-skill-library-packaging-design.md && test -f docs/superpowers/plans/2026-03-13-opengemini-skill-library-packaging.md`
Expected: exit 0
- [ ] **Step 3: Create the repository skill library directories**
Run: `mkdir -p .skill/skills`
Expected: directory exists

## Chunk 2: Migrate skill files

### Task 2: Copy repository skill source into `.skill/skills`

**Files:**
- Create: `.skill/skills/opengemini-*/SKILL.md`
- Create: `.skill/skills/opengemini-*/references/*.md`

- [ ] **Step 1: Verify target directories do not already contain the migrated skills**
Run: `find .skill/skills -maxdepth 2 -type f -name 'SKILL.md'`
Expected: empty before migration
- [ ] **Step 2: Copy the nine skills into `.skill/skills/`**
- [ ] **Step 3: Verify the migrated files exist**
Run: `find .skill/skills -maxdepth 3 -type f | sort`
Expected: all skill files and references listed

## Chunk 3: Add platform docs and setup script

### Task 3: Document usage and wire platform access

**Files:**
- Create: `.skill/README.md`
- Create: `.skill/platforms.md`
- Create: `scripts/setup-skills.sh`
- Create: `.claude/skills`
- Create: `.opencode/skills`

- [ ] **Step 1: Write `.skill/README.md` and `.skill/platforms.md`**
- [ ] **Step 2: Write `scripts/setup-skills.sh`**
- [ ] **Step 3: Create project-local symlinks for Claude Code and OpenCode**
- [ ] **Step 4: Verify paths and executable bit**
Run: `test -x scripts/setup-skills.sh && test -L .claude/skills && test -L .opencode/skills && readlink .claude/skills && readlink .opencode/skills`
Expected: script executable and both symlinks point to `../.skill/skills`

## Chunk 4: Add entry points in repo docs

### Task 4: Point contributors to the repository skill library

**Files:**
- Modify: `README.md`
- Modify: `CONTRIBUTION.md`

- [ ] **Step 1: Add a short project-skills section to `README.md`**
- [ ] **Step 2: Add contributor guidance to `CONTRIBUTION.md`**
- [ ] **Step 3: Verify anchor text exists**
Run: `rg -n "Project Skills|setup-skills.sh|.skill/skills|Claude Code|Codex|OpenCode" README.md CONTRIBUTION.md .skill/README.md .skill/platforms.md`
Expected: matches found

## Chunk 5: Final verification

### Task 5: Verify library shape and git status

**Files:**
- Modify: all newly created library files as needed

- [ ] **Step 1: Verify nine skills migrated**
Run: `find .skill/skills -maxdepth 2 -type f -name 'SKILL.md' | sort | wc -l`
Expected: `9`
- [ ] **Step 2: Verify platform docs and setup script exist**
Run: `test -f .skill/README.md && test -f .skill/platforms.md && test -x scripts/setup-skills.sh`
Expected: exit 0
- [ ] **Step 3: Inspect resulting status**
Run: `git status --short`
Expected: new `.skill/`, `.claude/`, `.opencode/`, script, and doc changes are visible
