# Zero-DB: Overview

> A serverless key-value store that lives entirely in **one Cloud Run container** and **one Google Cloud Storage bucket**. No VPC, no managed database service, no static credentials. Cold-start in milliseconds, warm reads in tens of microseconds, durability you can verify by `gsutil cat`.

This document is for someone evaluating Zero-DB for a real workload — not for someone hacking on the internals. It covers what the system is, how it works end-to-end, when it is the right tool, and how it compares to the obvious alternatives.

---

## TL;DR

| Question | Answer |
|---|---|
| **What is it?** | A single-binary LSM-tree KV store backed by GCS objects. |
| **Where does data live?** | One GCS bucket. Each SSTable is a blob. Manifest is one JSON object. |
| **How is it deployed?** | One Cloud Run service. No sidecars, no VPC connector, no Memorystore. |
| **What does the API look like?** | `GET / PUT / DELETE /v1/kv/{key}` over plain HTTPS. |
| **What is the latency?** | Sub-ms warm reads (BlockCache hit). Tens of ms on a GCS round-trip. |
| **What is the cost model?** | Cloud Run compute + GCS storage + GCS Class-A operations. No 24/7 instance to pay for when idle. |
| **Who is it not for?** | Multi-region writes, transactions across keys, secondary indexes, high write contention from many concurrent writers. |

---

## The Problem It Solves

Cloud Run is the cheapest "always available, scales to zero" runtime on GCP. But Cloud Run has a structural mismatch with stateful workloads:

- **Firestore** is the obvious GCP-native KV. It's expensive, has per-document write quotas, and locks you into a specific consistency model. Reads + writes both cost money per operation forever.
- **Memorystore** (Redis) requires a VPC connector, costs even when idle, and is gone the moment the instance is destroyed.
- **Cloud SQL** is a real database but pays the always-on tax and adds an SQL surface you may not want.
- **Spanner / AlloyDB / Bigtable** are appropriate for orders of magnitude more scale than a Cloud Run service typically needs.

Zero-DB exists for the workload that is **read-mostly, key-shaped, and small enough that one Cloud Run instance is the right scale**. Examples: feature flags + experiment configs, per-tenant settings, session blobs, read-through caches in front of a slow upstream, edge-caching, indexed metadata for a content store.

The trade is explicit: you give up SQL, transactions across keys, and multi-writer scaling. You get **low cost, fast cold start, and a fully serverless data plane**.

---

## How It Works (End to End)

### Storage shape

Every piece of state lives in **one GCS bucket** as one of three object kinds:

```
my-bucket/
├── manifest.json                  ← live SSTable list + WAL checkpoint
├── sstables/
│   ├── 000001.sst
│   ├── 000002.sst
│   └── 000003.sst
└── (instance-local) /tmp/wal.log  ← write-ahead log on Cloud Run disk
```

- **`sstables/*.sst`**: immutable sorted blocks of `(key → value)` records, with a bloom filter, an index, and a CRC32C trailer per block.
- **`manifest.json`**: `{version, sstables: [...entries...], wal_committed_bytes}`. Atomic per object — readers see either the old or new manifest, never a torn version.
- **WAL** (instance-local file): every `set` / `delete` is appended + `fdatasync`'d before the entry is acknowledged.

Nothing else touches the bucket. There is no metadata server, no leader election, no sidecar.

### The write path

```
client                        Zero-DB                            disk          GCS
  │                              │                                │             │
  │  PUT /v1/kv/foo  bar         │                                │             │
  │ ───────────────────────────▶ │                                │             │
  │                              │  WAL.appendSet(foo, bar)       │             │
  │                              │ ─────────────────────────────▶ │             │
  │                              │                          fsync ▼             │
  │                              │  active.put(foo, bar)                        │
  │                              │  (in-memory skiplist)                        │
  │  204                         │                                              │
  │ ◀─────────────────────────── │                                              │
  │                              │ ┌─ on threshold ─┐                           │
  │                              │ │  freeze active │                           │
  │                              │ │  enqueue flush │                           │
  │                              │ └───┬────────────┘                           │
  │                              │     ▼ (background thread)                    │
  │                              │  build SSTable bytes                         │
  │                              │  putObject sstables/000007.sst              │
  │                              │ ───────────────────────────────────────────▶ │
  │                              │  rewrite manifest.json (atomic)              │
  │                              │ ───────────────────────────────────────────▶ │
  │                              │  truncate WAL                                │
```

Every successful write is durable on the local disk before the response. If the Cloud Run instance dies between the WAL fsync and the SSTable upload, a fresh instance replays the WAL from `manifest.wal_committed_bytes` onward and ends up with the same active MemTable contents.

### The read path

```
get(key) →
  1. active MemTable               (in-RAM skiplist; nanoseconds)
  2. frozen MemTables (newest→oldest)
  3. SSTables (newest→oldest):
       fetch footer (cached on Reader)
       fetch bloom filter (cached in BlockCache)
       check bloom — miss? skip this SSTable
       fetch index block (cached in BlockCache)
       binary search → data block locator
       fetch data block (cached in BlockCache; CRC32C verified)
       linear scan inside block
  4. miss → return null
```

First hit wins. Tombstones (deleted keys) shadow lower tiers and surface as `null` to the caller.

The **BlockCache** is the load-bearing optimization for read latency. It is a bytes-bounded LRU keyed on `(sstable_id, kind, offset)` and **shared across every Reader the engine constructs**. Once a hot key's index, bloom filter, and data block are warm, subsequent reads issue zero GCS round-trips.

### Compaction

Background thread merges level-0 SSTables when their count crosses a threshold (default 4). The merge uses a k-way priority-queue cursor over readers and produces a single new SSTable, dropping dead tombstones along the way. Manifest swaps atomically; old object paths are best-effort `deleteObject`'d.

### Cold restart

```
Engine.init →
  load manifest.json from GCS    ← takes a few ms
  rebuild Reader per SSTable     ← no I/O; lazy footer fetch on first get
  open WAL file (or create)
  replay WAL records past manifest.wal_committed_bytes into active MemTable
```

A fresh container is **answering reads in tens of milliseconds** even with a multi-GB SSTable corpus, because the engine only fetches block-level data lazily and on demand.

---

## When to Use Zero-DB

Use it when **all** of these are true:

- The data shape is `(string key → bytes value)` — you don't need joins, secondary indexes, or aggregations.
- Working set fits in `O(GB)` per region, not `O(TB+)`.
- Reads dominate writes (typical ratio 10:1 or higher).
- You can tolerate **eventual consistency across multiple writer instances**. Zero-DB is single-writer-safe today; multi-writer needs the (planned) `If-Generation-Match` precondition path before it's correct.
- You want the data plane to be **inspectable with `gsutil`** and the operational model to be **"redeploy a container"**.
- You want to scale to zero when idle.

## When NOT to Use Zero-DB

- You need transactions that touch multiple keys atomically.
- You need secondary indexes / range queries on non-primary fields.
- You have many writers all hitting hot keys at high QPS — Zero-DB serializes through one writer instance per region by design.
- You need replication across regions with quorum semantics.
- You need snapshot isolation / MVCC reads.
- Your data is `O(TB)` or larger — Zero-DB scales fine in storage, but compaction over very large corpora is currently single-threaded and synchronous-ish.

---

## How It Compares

| Property | Zero-DB | Firestore | Memorystore (Redis) | Cloud SQL | RocksDB-as-a-service |
|----------|---------|-----------|---------------------|-----------|----------------------|
| Idle cost | $0 (Cloud Run scales to zero) | per-document storage charge | always-on instance | always-on instance | always-on instance |
| Cold start | hundreds of ms | n/a (managed) | n/a | seconds (provisioned) | n/a |
| Latency (warm read) | ~50 µs (BlockCache hit) | tens of ms | sub-ms | tens of ms | sub-ms |
| Latency (cold read) | one GCS round-trip (~30–50 ms) | tens of ms | sub-ms | tens of ms | depends on disk |
| VPC connector required | no | no | yes | yes (recommended) | yes |
| Durability after instance death | yes (GCS + WAL replay) | yes | no (unless persisted to disk + RDB) | yes | depends |
| Multi-writer safe | not yet (single writer v0) | yes | yes | yes | yes |
| Transactions across keys | no | yes (limited) | yes (MULTI/EXEC) | yes | yes |
| Secondary indexes | no | yes | no | yes | no |
| Cost knobs | bucket + Cloud Run | per-op + per-doc | instance size | instance size | instance size |
| Inspectable with gsutil | yes | no | no | no | no |
| Open formats | LSM + JSON manifest | proprietary | RDB / AOF | MySQL/Postgres | RocksDB SST |

**The honest summary**: Zero-DB is cheaper and simpler than every alternative on this list **for the specific workload it targets**. It is more expensive in development time and operational ceremony than Firestore for anything Firestore can do well. Pick accordingly.

---

## Performance + Cost Model

### Latency

All numbers are order-of-magnitude on a Cloud Run service with default sizing. No formal benchmark suite exists yet.

- **Active MemTable hit**: ~1 µs (skiplist lookup).
- **BlockCache hit (warm)**: ~50 µs (no I/O, parse + scan).
- **BlockCache miss (cold)**: ~30–50 ms — one GCS HTTPS round-trip per block fetched. The bloom filter usually limits this to one data-block fetch per get, even across N SSTables.
- **Write**: ~3–5 ms — dominated by the WAL `fdatasync`. The actual SSTable upload is asynchronous on the background compactor thread.
- **Cold start**: hundreds of ms for the container boot + manifest fetch. Reads start being served immediately after; lazy footer/bloom fetches happen on first hit.

### Cost

Three line items:

1. **Cloud Run compute** — pay only when handling requests. Idle = $0.
2. **GCS storage** — flat per-GB, region-dependent. SSTables compact down so dead tombstones don't accrue cost forever.
3. **GCS operations** — Class-A for `putObject` (writes + manifest rewrites), Class-B for `rangeGet`. The BlockCache is the dominant cost-saver here: hot keys cost $0 in Class-B operations after warm-up.

A typical small-tenant footprint: tens of MB stored, sub-million reads/day → cents per month.

---

## Operational Model

### Deployment

```bash
# One-time: create a dedicated runtime service account
gcloud iam service-accounts create zero-db-runtime \
  --display-name="zero-db Cloud Run runtime"

SA=zero-db-runtime@my-project.iam.gserviceaccount.com
gcloud storage buckets add-iam-policy-binding gs://my-zero-db \
  --member="serviceAccount:${SA}" \
  --role="roles/storage.objectAdmin"

# Deploy
GCP_PROJECT=my-project \
GCP_REGION=us-central1 \
GCS_BUCKET=my-zero-db \
AR_REPO=zero-db \
RUNTIME_SA="${SA}" \
./deploy/deploy.sh
```

`deploy/deploy.sh` cross-builds for `linux/amd64` (so it works from Apple Silicon hosts), pushes to Artifact Registry, and deploys with:

- `--service-account=${RUNTIME_SA}` — dedicated identity instead of the default compute SA
- `--no-cpu-throttling` — keeps the background compactor making progress between requests
- `--max-instances=1` — single-writer safety until multi-writer manifest preconditions are exercised in production
- CA bundle baked into the distroless runtime image so Zig's `std.http.Client` can validate `storage.googleapis.com`

Tokens come from the GCE metadata service automatically — no static credentials, no secret manager integration required.

### Verifying a deploy

```bash
URL=$(gcloud run services describe zero-db --region us-central1 --format='value(status.url)')

curl -X PUT -d 'hello' "${URL}/v1/kv/foo"
curl -X POST "${URL}/admin/flush"
curl "${URL}/admin/stats"      # expect: {"sstables":1,"active_entries":0,"compaction_failures":0}

gcloud storage ls gs://my-zero-db/sstables/
gcloud storage cat gs://my-zero-db/manifest.json
```

A `compaction_failures` value greater than zero indicates that a background flush hit an error — check `gcloud run services logs read zero-db --region us-central1` for `compaction failed: <ErrorName>` and the more specific `persistAndOpen: putObject(...) failed: <ErrorName>` to identify the failing step.

### Failure modes the system survives

- **Container crash mid-write**: WAL fsync was durable; replay-on-restart reconstructs the active MemTable.
- **Container crash mid-flush**: WAL is durable; manifest atomicity guarantees the on-disk state is consistent. Worst case: an orphan SSTable object that the next compaction deletes.
- **Container crash mid-compaction**: same as above — atomic manifest rewrite means the engine sees either the pre-merge or post-merge state.
- **Bucket bit-rot**: every block is CRC32C-trailed. Corruption surfaces as a typed `BadDataBlock` error rather than a silent garbage value.

### Failure modes that need operator action

- **Multi-writer conflict**: `If-Generation-Match` is wired through `Client.putObjectIfMatch`; a concurrent writer's update surfaces as `error.ManifestConflict`. The shipped `deploy.sh` still pins `--max-instances=1` until the multi-writer path has run hours of production traffic. Loosen to `--max-instances=N` when you have telemetry + a documented retry-on-conflict client.
- **Bucket permissions revoked mid-flight**: the engine surfaces `AuthFailed` and stops accepting writes; logs include the failed operation. Restoring permissions and redeploying recovers.
- **Bucket deletion**: nothing recoverable. Use object versioning + retention policies on the bucket if you care.

### Observability

The engine exposes counters that monitoring can scrape:

- `compaction_failure_count` — bumped when a background flush fails.
- `tracking_allocator.refused_count` — how many writes were rejected for OverMemoryBudget.
- `block_cache.bytes_used` — current resident metadata + data bytes.
- `fake_server.call_count` (in tests) — tracks GCS round-trips.

A `GET /admin/stats` endpoint surfaces a JSON snapshot. A `GET /healthz` returns 200 once the engine has finished cold-start manifest replay.

---

## Where the Internals Live

If you came here from a "how does this work" angle and want to dig deeper, the code organization mirrors the data flow:

| Concern | Path |
|---|---|
| HTTP entry points | `src/server/http.zig` |
| LSM orchestrator | `src/engine/engine.zig` |
| MemTable (skiplist) | `src/engine/memtable.zig` |
| Background compactor | `src/engine/compaction.zig` |
| WAL | `src/engine/wal.zig` |
| SSTable writer + reader | `src/sstable/writer.zig`, `src/sstable/reader.zig` |
| Manifest | `src/sstable/manifest.zig` |
| Block cache | `src/cache/block_cache.zig` (over `src/cache/lru.zig`) |
| GCS HTTP client | `src/storage/gcs.zig` |
| GCE metadata auth | `src/storage/auth.zig` |
| Adaptive prefetch | `src/prefetch/adaptive.zig` |
