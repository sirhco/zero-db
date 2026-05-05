# Zero-DB

> **Status: Alpha (0.0.0).** Single-writer only; multi-writer Cloud Run requires manifest preconditions still TBD. On-disk format is unstable across versions. API and module surface will continue to evolve. Cold-restart durability via GCS + local WAL is in place; do not yet store data without an out-of-band backup.

Serverless key-value store written in Zig, designed to run on Google Cloud Run with Google Cloud Storage as the persistent layer.

**New here?** Read [`docs/OVERVIEW.md`](docs/OVERVIEW.md) for the end-user pitch — what Zero-DB is, how the read/write path works end-to-end, when to pick it over Firestore / Memorystore / Cloud SQL, and the cost + latency model.

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

## What's Implemented (as of Phase 18)

- **Engine** — LSM orchestrator with set/get/delete, flush, full compaction, optional GCS-backed cold-restart durability, optional WAL replay, optional background flush + L0 auto-merge
- **MemTable** — skiplist with put/putTombstone/get/iterator
- **SSTable Index Parser** — `@bitCast`-driven, zero-allocation, alignment-safe
- **SSTable Reader** — footer → bloom → index → data block, lazy + cached, optional shared `BlockCache`
- **SSTable Writer** — block-rolling builder with bloom + index + footer assembly + per-block CRC32C trailer
- **Manifest** — JSON-serialized live SSTable list, atomic-per-object rewrite on every flush + compaction
- **WAL** — local file with fsync-per-record, CRC32C-trailed entries, replay-on-open survives in-process crashes
- **K-Way Compaction** — merges overlapping SSTables, drops dead tombstones on full passes
- **Bloom Filter** — Wyhash-based, sized from expected entry count
- **LRU Cache + BlockCache** — bytes-bounded LRU keyed on `(sstable_id, kind, offset)`, shared across every Reader the engine constructs
- **TrackingAllocator** — bytes-bounded child allocator wrapper; engine surfaces `OverMemoryBudget` from `set` / `delete` once the ceiling is crossed
- **Background Compactor** — opt-in `Engine.startCompactor()` spawns a worker thread; flushes drain off a queue, L0 ≥ threshold auto-schedules a merge
- **GCS Storage Adapter** — `Storage` interface + `gcs.Client` with `RealTransport` over `std.http.Client` (production) and `FakeServer` (tests)
- **GCE Auth** — `auth.TokenSource` fetches + caches + refreshes a bearer token from the GCE metadata service; threads into every `gcs.Client` request
- **Adaptive Prefetch** — single-step transition learner; reader-side wiring deferred to v1
- **HTTP Server** — `GET/PUT/DELETE /v1/kv/{key}`, `POST /admin/flush`, `POST /admin/compact`, `GET /admin/stats`, `GET /healthz`
- **Util** — LEB128 varint, CRC32C (Castagnoli), little-endian assertion
- **Deploy** — `deploy/Dockerfile` (multi-stage, distroless runtime) + `deploy/deploy.sh` (Artifact Registry + Cloud Run)

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

### Cloud Run

```bash
GCP_PROJECT=my-project GCP_REGION=us-central1 \
  GCS_BUCKET=my-zero-db AR_REPO=zero-db \
  ./deploy/deploy.sh
```

The runtime service account needs `roles/storage.objectAdmin` on the bucket. Tokens come from the GCE metadata service automatically — no static credentials. See `deploy/deploy.sh` for full env-var requirements.

## Roadmap


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
| 10 | Util Backfill (varint + CRC32C) + Data Block CRC Trailer | shipped |
| 11 | Real HTTP Transport + GCE Auth (token refresh) | shipped |
| 12 | GCS Upload Path + Manifest (durable SSTable list) | shipped |
| 13 | Block Cache Wiring (parsed index/bloom + raw data block LRU) | shipped |
| 14 | Memory Ceiling (TrackingAllocator admission control) | shipped |
| 15 | Background Compaction Worker (+ L0 auto-merge) | shipped |
| 16 | WAL Durability (Option A — local file, fsync per record) | shipped |
| 17 | Adaptive Prefetch (single-step transition learner) | shipped |
| **18** | **Cloud Run Packaging (Dockerfile, deploy.sh, IAM)** | **shipped** |

Test count grew from 145 (post-Phase 10) to **190** across Phases 11–18.

## Outstanding Stubs

Every roadmap stub has been filled. The lone remaining unwired module:

| File | Status |
|------|--------|
| `src/alloc/arena_pool.zig` | type-only; no consumer needs it yet — wire when a workload demands per-request arena reuse |

## Known v0 Limitations

- **Multi-writer Cloud Run** — manifest rewrite assumes a single writer. Multi-instance deployments need GCS `x-goog-if-generation-match` preconditions before the manifest path is safe.
- **WAL truncation** — the local WAL grows unbounded; replay walks the whole file on cold start. A future phase will park a `wal_committed_bytes` checkpoint in the manifest so absorbed prefix can be skipped or trimmed.
- **Adaptive prefetch reader-side wiring** — the tracker exposes hints; the reader does not yet consume them. Wire once production traffic exposes the hot key shapes.
- **Refresh-on-401** — `TokenSource` refreshes by clock only. A mid-request 401 retry is straightforward future work.

## Out of Scope (Pre-1.0)

- Multi-writer Cloud Run (manifest needs GCS preconditions)
- Multi-tenant key namespacing
- Encryption at rest beyond GCS default
- Replication / cross-region failover
- Snapshot isolation / MVCC reads

## License

Apache 2.0 — see `LICENSE`.
