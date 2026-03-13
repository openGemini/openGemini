# openGemini Go Performance Style Design

## Goal

Create a repository-specific coding-style skill that captures how openGemini expects high-performance Go code to be written and reviewed, especially when reducing GC pressure, reusing memory, and controlling concurrency in hot paths.

## Scope

This skill is normative, not tutorial-first.

It covers:
- memory reuse rules for hot paths
- when to prefer existing pools over fresh allocations
- when `sync.Pool`, `lib/pool`, `bufferpool`, or allocator-based reuse are appropriate
- when bounded goroutine concurrency is required
- review checklist items for GC-heavy changes

It does not cover:
- generic Go language teaching
- pprof or benchmark methodology
- subsystem-specific business design
- performance claims without evidence

## Repository Anchors

The skill should ground its rules in current openGemini patterns, including:
- `lib/pool/union_pool.go`
- `lib/bufferpool/pool.go`
- `lib/record/record_pool.go`
- `coordinator/context.go`
- `coordinator/points_writer.go`
- `coordinator/record_writer.go`
- `services/writer/context.go`
- `services/writer/decoder.go`
- `services/continuousquery/service.go`
- `app/ts-store/storage/storage.go`

## Intended Shape

Skill name:
- `opengemini-go-performance-style`

Files:
- `/Users/ryan/.codex/skills/opengemini-go-performance-style/SKILL.md`
- `/Users/ryan/.codex/skills/opengemini-go-performance-style/references/pattern-map.md`

`SKILL.md` should contain:
- triggering conditions
- core rules
- forbidden or suspicious patterns
- review checklist

`references/pattern-map.md` should contain:
- existing openGemini pool and reuse examples
- concurrency-control examples
- mapping from common optimization tasks to canonical files

## Core Rules

- Reuse existing repository pools before introducing new pools.
- Do not add pooling to cold paths or tiny objects without evidence.
- Poolable objects must have a clear reset boundary before reuse.
- Prefer slice and buffer reuse to repeated allocation in hot loops.
- Do not create unbounded goroutines in hot paths; use tokens, pool abstractions, or resource allocators.
- Do not trade correctness or state isolation for lower allocation count.

## Verification Strategy

Static verification only:
- skill file exists
- frontmatter is valid
- repo anchors referenced by the skill exist
- reference file contains the expected pool and concurrency patterns
