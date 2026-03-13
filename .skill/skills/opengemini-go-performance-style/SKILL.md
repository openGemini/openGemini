---
name: opengemini-go-performance-style
description: Use when writing or reviewing hot-path Go code in openGemini, especially for memory reuse, object pooling, bounded concurrency, and changes intended to reduce GC pressure.
---

# openGemini Go Performance Style

This is a coding-style and code-review skill. Use repository patterns first; do not invent new performance abstractions casually.

## When to Use

- You are changing write, query, encode/decode, cache, WAL, transport, or storage hot paths
- The change adds or removes allocations, buffering, pooling, or goroutine fan-out
- You need to decide whether `sync.Pool`, `lib/pool`, `bufferpool`, or an allocator-style pattern is appropriate
- You are reviewing code that claims to reduce GC pressure or improve throughput

## Core Rules

### 1. Prefer existing repository reuse primitives

Check existing abstractions before adding a new pool:
- `lib/pool`
- `lib/bufferpool`
- `lib/record/record_pool.go`
- existing context pools in `coordinator/` and `services/writer/`

A new pool needs a concrete reason that existing primitives cannot cover the lifecycle.

### 2. Reuse slices, buffers, and records in hot loops

If code runs per row, per record, per chunk, or per request batch, repeated allocation is suspicious. Prefer resetting and reusing existing buffers or pooled objects.

### 3. Pool only objects with a clear reset boundary

Reusable objects must be fully reset before being returned. If state cannot be cleaned reliably, do not pool it.

### 4. Do not pool cold-path or tiny objects by default

Pooling adds complexity, lifetime coupling, and stale-state risk. It must be justified by path frequency or object size, not by vague fear of GC.

### 5. Bound concurrency in hot paths

Do not spawn unbounded goroutines on data-dependent input sizes. Use token channels, worker-pool style control, or repository allocators when concurrency must be capped.

### 6. Correctness beats allocation count

Do not reuse memory across requests, shards, or tasks if it risks data corruption, stale metadata, or unsafe aliasing.

## Suspicious Patterns

- `make([]byte, ...)` or `bytes.Buffer{}` inside hot loops when a reusable buffer exists
- introducing a fresh `sync.Pool` while `lib/pool` or `bufferpool` already fits
- returning pooled objects without deep reset or ownership cleanup
- goroutine-per-item fan-out with no upper bound
- claiming “reduce GC” without identifying the hot path or reused object lifecycle

## Review Checklist

- Is this path actually hot or allocation-sensitive?
- Does the repository already have a pool or buffer type for this object?
- Is every pooled object reset before reuse?
- Could this reuse leak state across requests, shards, or tasks?
- Is goroutine creation bounded?
- Is a resource allocator or token limit more appropriate than ad hoc concurrency?
- Does the optimization preserve readability relative to the gain?

## Pattern Map

Read [references/pattern-map.md](references/pattern-map.md) for concrete openGemini examples of pool selection, buffer reuse, record reuse, and bounded concurrency.
