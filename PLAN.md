# Zero-DB — Project Skeleton + SSTable Index Parser

## Context

Zero-DB is a serverless KV store written in Zig, intended to run on Google Cloud Run with Google Cloud Storage as the persistent layer. The architecture is LSM-tree shaped: writes accumulate in an in-memory MemTable, are flushed as immutable SSTables into a GCS bucket, and reads consult an in-memory cache → MemTable → SSTable index → GCS Range request chain. The "secret sauce" is using Zig's manual memory model to keep the read path off any GC and to serve hot keys from packed in-RAM structures with zero parsing overhead.

This plan covers two deliverables:

1. The full project layout for the codebase (every directory and module the system will need), populated with empty stubs and doc-comments describing each module's responsibility — so the architecture is committed and future work has clear seams.
2. A complete, tested implementation of the **SSTable Index Parser**: read a binary index block from a `[]const u8` buffer and binary-search it to find the data-block offset for a given key.

Everything else (GCS client, MemTable, Bloom filter, LRU, prefetcher, server) is stubbed only — to be filled in subsequent passes. Targeting Zig **0.16+** (the existing `build.zig` uses the new `std.Io` API and `std.process.Init`-style entry point, so we stay on that track).

## Project Layout

```
zero-db/
├── build.zig                    # extended: add module imports + per-package tests
├── build.zig.zon                # unchanged for now (no external deps yet)
├── src/
│   ├── main.zig                 # Cloud Run HTTP entry point (stub: prints + exits)
│   ├── root.zig                 # public re-exports: Engine, KeyValue, errors
│   ├── engine/
│   │   ├── engine.zig           # top-level Engine.get/set/delete (stub)
│   │   ├── memtable.zig         # SkipList-backed MemTable (stub)
│   │   └── compaction.zig       # background flush + level merge (stub)
│   ├── sstable/
│   │   ├── format.zig           # on-disk constants, magic, footer layout
│   │   ├── footer.zig           # 64-byte fixed footer (packed struct, stub)
│   │   ├── reader.zig           # SSTable reader: footer → bloom → index → data (stub)
│   │   ├── writer.zig           # SSTable builder from frozen MemTable (stub)
│   │   └── index.zig            # ◆ FULLY IMPLEMENTED: IndexBlock parser + bsearch
│   ├── bloom/
│   │   └── filter.zig           # Wyhash-based Bloom filter (stub: struct + sig only)
│   ├── cache/
│   │   ├── lru.zig              # DoublyLinkedList + HashMap LRU (stub)
│   │   └── block_cache.zig      # caches parsed IndexBlocks + data blocks (stub)
│   ├── storage/
│   │   ├── gcs.zig              # std.http.Client w/ Range headers (stub)
│   │   └── auth.zig             # GCE metadata-service token fetch (stub)
│   ├── prefetch/
│   │   └── adaptive.zig         # access co-occurrence tracker (stub)
│   ├── alloc/
│   │   ├── arena_pool.zig       # per-request FixedBufferAllocator pool (stub)
│   │   └── tracking.zig         # accounting allocator (Cloud Run mem cap) (stub)
│   └── util/
│       ├── varint.zig           # LEB128 helpers (stub: signatures only)
│       ├── endian.zig           # comptime LE assertion + helpers
│       └── crc.zig              # block checksum helpers (stub)
└── tests/
    └── sstable_index_test.zig   # integration-style tests pulled in via build.zig
```

Each stub file contains:
- Module-level `//!` doc comment stating purpose and invariants.
- Public type / function signatures with `unreachable;` or `return error.NotImplemented;` bodies, so the call graph compiles end-to-end.
- One `test "compiles"` block per file so `zig build test` exercises every module.

## SSTable On-Disk Format (Reference Only)

For context — the index parser must match this. Only the index block + footer are touched in this pass.

```
+----------------------+
| Data blocks          |   variable; sorted KV pairs, optional restart points (later)
+----------------------+
| Bloom filter block   |   length determined by footer (later)
+----------------------+
| Index block          |   format below
+----------------------+
| Footer (64 bytes)    |   magic + index_off + index_len + bloom_off + bloom_len + version
+----------------------+
```

## Index Block Format (Implemented)

Layout, all little-endian, contiguous in a single `[]const u8` buffer:

```
offset  size              field                 notes
------  ----------------  --------------------  ------------------------------------
0       16                IndexHeader (packed)  see struct below
16      4 * entry_count   offsets[]: u32        byte offsets into entry heap, sorted
                                                ascending by key (the bsearch axis)
...     heap_size         entry heap            variable-length entries, see below
```

`IndexHeader` (packed, 16 bytes):

```zig
pub const IndexHeader = packed struct {
    magic: u32,            // 'ZIDX' = 0x5A494458
    version: u16,          // = 1
    flags: u16,            // reserved, must be 0 in v1
    entry_count: u32,      // N
    heap_size: u32,        // bytes occupied by entry heap (after the offsets table)
};
```

Each entry in the heap (packed prefix + trailing key bytes):

```zig
pub const IndexEntryHeader = packed struct {
    block_offset: u64,     // byte offset of the target data block in the SSTable
    block_size: u32,       // length of that data block
    key_len: u16,          // number of key bytes that follow this header
};
// followed by exactly `key_len` raw key bytes
```

Constraints (asserted in parser):
- `key_len <= MAX_KEY_LEN` (constant in `format.zig`, default 4096).
- Entries in the heap are referenced by `offsets[i]`; the offsets array is in **key-sorted order**, which is the array bsearch indexes into. The heap itself is not required to be physically sorted — writer is free to lay it out for cache locality.
- `offsets[i] + sizeof(IndexEntryHeader) + key_len <= heap_size`.

## Index Parser API (`src/sstable/index.zig`)

```zig
pub const IndexBlockError = error{
    BufferTooSmall,
    BadMagic,
    UnsupportedVersion,
    BadFlags,
    OffsetOutOfRange,
    KeyOutOfRange,
    KeyTooLong,
};

pub const BlockLocator = struct {
    block_offset: u64,
    block_size: u32,
};

pub const IndexBlock = struct {
    header: IndexHeader,
    offsets: []align(1) const u32,   // raw little-endian u32s; alignment = 1
    heap: []const u8,                // entry heap (after header + offsets table)

    /// Validate buffer and produce a zero-copy view over it. The returned
    /// IndexBlock borrows `buf`; caller must keep `buf` alive.
    pub fn parse(buf: []const u8) IndexBlockError!IndexBlock;

    /// Number of indexed data blocks.
    pub fn count(self: IndexBlock) u32;

    /// Binary-search for `key`. Semantics: returns the entry whose key is the
    /// **smallest key >= key** (the "lower-bound" entry). This matches LSM
    /// semantics — that's the data block that could contain `key`.
    /// Returns null only if `key` is strictly greater than the last key in the
    /// index (i.e., key cannot exist in this SSTable).
    pub fn find(self: IndexBlock, key: []const u8) ?BlockLocator;

    /// Direct accessor used by find() and by tests; returns the entry at
    /// sorted-index `i` (0..count()). Bounds-checked via assert in debug.
    pub fn entryAt(self: IndexBlock, i: u32) IndexEntryView;
};

pub const IndexEntryView = struct {
    key: []const u8,
    block_offset: u64,
    block_size: u32,
};
```

### parse() — validation steps

1. `buf.len >= @sizeOf(IndexHeader)` else `BufferTooSmall`.
2. Read header by `@bitCast` from the first 16 bytes (LE asserted at comptime in `util/endian.zig`).
3. Check `magic == 0x5A494458`, `version == 1`, `flags == 0`.
4. Compute `offsets_bytes = header.entry_count * 4`. Check `16 + offsets_bytes + header.heap_size <= buf.len`.
5. Build the `offsets: []align(1) const u32` view via `std.mem.bytesAsSlice(u32, ...)` over the 4*N region (alignment-1 because GCS-fetched buffers won't be u32-aligned).
6. Build `heap = buf[16 + offsets_bytes ..][0..header.heap_size]`.
7. Return the view. No validation of every entry up front — entry validation happens lazily on `entryAt`/`find` so a malformed entry doesn't penalize cold lookups for valid neighbors.

### entryAt() — per-access checks

- Read `off = offsets[i]` (debug-assert `i < entry_count`).
- `OffsetOutOfRange` if `off + sizeof(IndexEntryHeader) > heap.len`.
- `@bitCast` `IndexEntryHeader` from `heap[off..][0..14]`.
- `KeyTooLong` if `header.key_len > MAX_KEY_LEN`.
- `KeyOutOfRange` if `off + 14 + header.key_len > heap.len`.
- Return view: `.key = heap[off+14..][0..header.key_len]`, plus offsets.

(These error returns are surfaced through `find` as `IndexBlockError` so a corrupt SSTable fails the lookup rather than crashing the process.)

### find() — binary search

Standard lower-bound bsearch over `[0, entry_count)`:

```
lo = 0; hi = entry_count;
while lo < hi:
    mid = lo + (hi - lo) / 2
    cmp = mem.order(u8, entryAt(mid).key, target)
    if cmp == .lt: lo = mid + 1
    else:          hi = mid
if lo == entry_count: return null
return BlockLocator{ .block_offset, .block_size } from entryAt(lo)
```

Use `std.mem.order(u8, a, b)` for lexicographic compare. This is the same primitive LevelDB/RocksDB use and is the right semantic for sorted byte keys.

### Why this design satisfies the brief

- **Packed structs / zero parse overhead** — `IndexHeader` and `IndexEntryHeader` are `packed struct`s `@bitCast` directly from buffer bytes; only variable-length key bytes require slicing. No per-field decode loop.
- **LE asserted** — `util/endian.zig` has `comptime { std.debug.assert(builtin.cpu.arch.endian() == .little); }`. Cloud Run targets x86_64 + arm64, both LE.
- **Alignment-safe** — GCS-fetched buffers have no guarantee of u32 alignment. We use `[]align(1) const u32` for the offsets view; `@bitCast` of packed structs is byte-addressable, so it works at any alignment.
- **No allocations** — `IndexBlock` is a pure view over the caller's buffer. Friendly to per-request `FixedBufferAllocator` lifetime.
- **Fail-soft** — corrupt buffers return `IndexBlockError`, never UB / panic in release.

## Build Wiring (`build.zig`)

Minimal additions to the existing `build.zig`:

- Keep the `zero_db` module rooted at `src/root.zig`.
- `root.zig` adds `pub const sstable_index = @import("sstable/index.zig");` so consumers can reach the parser; later passes re-export `Engine` from `engine/engine.zig`.
- The existing `mod_tests` test step already discovers `test` blocks transitively via `@import`, so adding files under `src/` is enough to get them tested. Add a single new test file `tests/sstable_index_test.zig` and wire it as a second test artifact (so integration tests are isolated from unit tests).

## Testing (`src/sstable/index.zig` + `tests/sstable_index_test.zig`)

Unit tests inline in `index.zig`:

1. **roundtrip-3** — build a buffer with 3 keys (`"alpha"`, `"mango"`, `"zeta"`), fixed dummy offsets; assert `find("alpha")`, `find("mango")`, `find("zeta")` each hit their entry.
2. **lower-bound semantics** — `find("apple")` returns the `"mango"` entry (smallest key ≥ target that's present) — wait, no: smallest key ≥ "apple" is "mango"… correct: assert it returns mango. `find("zzz")` returns null.
3. **empty index** — `entry_count == 0` → `find(anything)` returns null without OOB.
4. **single entry** — degenerate bsearch path.
5. **large index** — generate 10_000 lexicographically-ordered keys (`"key_00000000"`..`"key_00009999"`), assert random lookups land on exact entries; assert `find("key_99999999")` returns null.
6. **corruption** — mutated magic / version / flags / truncated buffer / offset past heap — each should return the matching `IndexBlockError`.

Integration test in `tests/sstable_index_test.zig`:

- A small in-test "writer" helper that takes `[]const struct { key, block_offset, block_size }` and emits a valid index block to a buffer.
- Re-uses the helper to drive every unit case above; round-trips through the same encoder consumers would use, so the format is documented in code, not just in this plan.

A helper `pub fn buildIndexBlockForTesting(...)` lives in `index.zig` behind a `if (builtin.is_test)` gate.

## Critical Files

- `build.zig` — extend module wiring; add second test artifact for `tests/`.
- `src/root.zig` — re-export the index parser; add stub re-exports for the rest.
- `src/sstable/format.zig` — magic constants, `MAX_KEY_LEN`, footer layout (struct only, no impl).
- `src/sstable/index.zig` — **the** implementation (parser, bsearch, encoder helper, unit tests).
- `src/util/endian.zig` — comptime LE assertion + small helpers.
- `tests/sstable_index_test.zig` — integration tests.
- All other `src/**/*.zig` files — created as stubs per the layout above.

## Verification

After implementation:

1. `zig build` — compiles cleanly under Zig 0.16+ (no warnings under `-fno-warnings=false`).
2. `zig build test` — all inline + integration tests pass.
3. `zig build test --summary all` — confirm the integration test artifact actually ran.
4. `zig build -Doptimize=ReleaseFast` — release build succeeds; the parser type sizes report cleanly (`@sizeOf(IndexHeader) == 16`, `@sizeOf(IndexEntryHeader) == 14`) — assert this at comptime in `format.zig`.
5. Manual sanity: a tiny driver in `main.zig` (gated behind a CLI flag, e.g. `--selftest`) builds an index from a hard-coded key list, calls `find`, prints `BlockLocator`, exits. Confirms the parser is reachable from the executable, not just from tests.

## Out of Scope (Tracked for Next Passes)

- GCS HTTP client + Range requests (`storage/gcs.zig`).
- GCE metadata token fetch + auth refresh (`storage/auth.zig`).
- MemTable (skiplist) + flush trigger (`engine/memtable.zig`, `engine/compaction.zig`).
- SSTable footer/reader/writer wiring (`sstable/footer.zig`, `reader.zig`, `writer.zig`) — types defined, bodies stubbed.
- Bloom filter (`bloom/filter.zig`).
- LRU + block cache (`cache/`).
- Adaptive prefetch (`prefetch/adaptive.zig`).
- HTTP server / Cloud Run handler (`main.zig` beyond `--selftest`).
- WAL durability story — open question: is single-replica MemTable acceptable for v0, or do writes need to land in GCS before ack? Decide before implementing `engine/engine.zig`.
