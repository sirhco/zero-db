# Zero-DB

> **Status: Alpha (0.0.0).** Not production-ready. On-disk format is unstable. No durability guarantees yet (no WAL). API and module surface will change. Do not store data you cannot afford to lose.

Serverless key-value store written in Zig, designed to run on Google Cloud Run with Google Cloud Storage as the persistent layer.

## Architecture

LSM-tree shaped, single-writer / multi-reader:

```
write:  set/delete → MemTable (in-RAM skiplist) → freeze → flush to immutable SSTable
read:   active MemTable → frozen MemTables (newest→oldest) → SSTables (newest→oldest)
        first hit wins; tombstones shadow lower tiers
```

SSTables live as immutable blob objects. Each SSTable layout:

```
[data blocks ...][bloom filter block][index block][footer (64 bytes)]
```

Every data block ends with a 4-byte CRC32C trailer (Castagnoli) — corruption surfaces as a typed error rather than garbage values.

The "secret sauce" is Zig's manual memory model: the read path stays off any GC, hot keys come from packed in-RAM structures with `@bitCast` zero-parse access, per-request `FixedBufferAllocator` arenas keep allocation churn off the steady-state path.

## What's Implemented (as of Phase 10)

- **Engine** — LSM orchestrator with set/get/delete, flush, and full compaction
- **MemTable** — skiplist with put/putTombstone/get/iterator
- **SSTable Index Parser** — `@bitCast`-driven, zero-allocation, alignment-safe
- **SSTable Reader** — footer → bloom → index → data block, lazy + cached
- **SSTable Writer** — block-rolling builder with bloom + index + footer assembly
- **K-Way Compaction** — merges overlapping SSTables, drops dead tombstones on full passes
- **Bloom Filter** — Wyhash-based, sized from expected entry count
- **LRU Cache** — DoublyLinkedList + HashMap (block cache wiring still pending)
- **GCS Storage Adapter** — `Storage` interface + `gcs.Client` (with `FakeServer` for tests; real HTTP transport pending)
- **HTTP Server** — `GET/PUT/DELETE /v1/kv/{key}`, `POST /admin/flush`, `POST /admin/compact`, `GET /admin/stats`, `GET /healthz`
- **Util** — LEB128 varint, CRC32C (Castagnoli), little-endian assertion

## Build / Run

Requires Zig **0.16+**.

```bash
# Build
zig build

# Run all tests (3 artifacts: module, exe, integration)
zig build test --summary all

# Release build
zig build -Doptimize=ReleaseFast

# In-process smoke test (no network)
./zig-out/bin/zero_db --selftest

# Serve HTTP on $PORT (default 8080)
./zig-out/bin/zero_db
```

## Roadmap

Detailed in `docs/superpowers/plans/2026-05-03-zero-db-completion-roadmap.md`. Per-phase plans land under `docs/superpowers/plans/` as each phase begins.

| Phase | Theme | Status |
|-------|-------|--------|
| 1 | Skeleton + SSTable Index Parser | shipped |
| 2 | Footer, Bloom Filter, LRU Cache | shipped |
| 3 | MemTable + GCS Request Plumbing | shipped |
| 4 | SSTable Writer + Data Blocks | shipped |
| 5 | SSTable Reader + Storage Abstraction | shipped |
| 6 | LSM Engine Orchestration | shipped |
| 7 | GCS Storage Adapter + HTTP Seams | shipped |
| 8 | Full Compaction + K-Way Merge | shipped |
| 9 | HTTP Server + V1 API | shipped |
| **10** | **Util Backfill (varint + CRC32C) + Data Block CRC Trailer** | **shipped** |
| 11 | Real HTTP Transport + GCE Auth (token refresh) | next |
| 12 | GCS Upload Path + Manifest (durable SSTable list) | planned |
| 13 | Block Cache Wiring (parsed index/bloom + raw data block LRU) | planned |
| 14 | Memory Ceiling (TrackingAllocator admission control) | planned |
| 15 | Background Compaction Worker | planned |
| 16 | WAL Durability (open question: local disk vs. GCS streaming) | planned |
| 17 | Adaptive Prefetch (co-occurrence-driven Range extension) | planned |
| 18 | Cloud Run Packaging (Dockerfile, deploy script, IAM) | planned |

## Outstanding Stubs

Modules that still return `error.NotImplemented` or have no behavior:

| File | Phase that fills it |
|------|---------------------|
| `src/storage/auth.zig` | 11 |
| `src/cache/block_cache.zig` | 13 |
| `src/alloc/tracking.zig` | 14 |
| `src/alloc/arena_pool.zig` | 14 |
| `src/engine/compaction.zig` (background worker) | 15 |
| `src/prefetch/adaptive.zig` | 17 |

## Out of Scope (Pre-1.0)

- Multi-writer Cloud Run (manifest needs GCS preconditions)
- Multi-tenant key namespacing
- Encryption at rest beyond GCS default
- Replication / cross-region failover
- Snapshot isolation / MVCC reads

## License

Apache 2.0 — see `LICENSE`.
