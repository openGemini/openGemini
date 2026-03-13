# Pattern Map

## Existing reuse primitives

- `lib/pool/union_pool.go`: generic pooled-object abstraction with local cache, size guards, and optional stats
- `lib/bufferpool/pool.go`: byte-slice reuse with local cache and pooled fallback
- `lib/record/record_pool.go`: record-specific reuse with reset rules and reuse thresholds

## Canonical repository examples

### Context and object reuse

- `coordinator/context.go`: pooled ingestion context usage
- `coordinator/points_writer.go`: pooled ingestion and write contexts, shard-key reuse
- `services/writer/context.go`: `pool.NewDefaultUnionPool[WriteContext]` for reusable write context objects
- `services/writer/decoder.go`: decoder reuse through `pool.NewDefaultUnionPool`
- `engine/shelf/blob.go`: pooled blob groups

### Buffer reuse

- `coordinator/record_writer.go`: `bufferpool.GetPoints()` / `bufferpool.PutPoints()` around record marshaling
- `lib/fileops/fs_writer.go`: pooled file buffers
- `engine/shelf/wal.go` and `engine/shelf/wal_reader.go`: reused WAL buffers

### Record reuse

- `lib/record/record_pool.go`: record pooling with `ResetDeep()` and size-based abort rules
- `lib/record/record.go`: column value memory reuse comments and supporting behavior

### Bounded concurrency and resource control

- `services/continuousquery/service.go`: token channel bounds concurrent CQ execution
- `app/ts-store/storage/storage.go`: `resourceallocator.InitResAllocator(...)` controls chunk-reader, shard, and series parallelism

## Normative guidance from these examples

- Prefer repository abstractions with reset and size limits over bare `sync.Pool`.
- Use `sync.Pool` directly only when no repository abstraction matches and object ownership is simple.
- Reuse buffers by slicing to zero length before refill when ownership is local and reset is explicit.
- Reject reuse for oversized objects when existing patterns already cap retained memory.
- Bound goroutines with explicit limits when work scales with measurements, shards, records, or queries.
