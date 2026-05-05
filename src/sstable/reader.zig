//! SSTable reader.
//!
//! Orchestrates the LSM read path against a `Storage` interface:
//!   1. Tail-Range fetch the 64-byte footer (cached after first call).
//!   2. Range fetch the bloom filter (cached). Skip data fetch on a miss.
//!   3. Range fetch the index block (cached). Bsearch for the data block
//!      that could contain the key.
//!   4. Range fetch the resolved data block. Linear scan for the exact key.
//!
//! Every metadata buffer is loaded lazily and cached on the Reader, so the
//! per-key amortized GCS cost converges to one round-trip (the data block).
//! Block cache and adaptive prefetching are out of scope here — Reader is
//! the leaf the higher-level cache wraps.

const std = @import("std");

const blob = @import("../storage/blob.zig");
const bloom = @import("../bloom/filter.zig");
const crc_mod = @import("../util/crc.zig");
const data_block = @import("data_block.zig");
const fmt = @import("format.zig");
const footer_mod = @import("footer.zig");
const index_mod = @import("index.zig");
const block_cache_mod = @import("../cache/block_cache.zig");

pub const Storage = blob.Storage;

pub const ReaderError = error{
    BadFooter,
    BadBloom,
    BadIndex,
    BadDataBlock,
    ShortRead,
    OutOfMemory,
} ||
    footer_mod.FooterError ||
    bloom.FilterError ||
    index_mod.IndexBlockError;

pub const Lookup = union(enum) {
    /// Value bytes copied into a buffer owned by the `out_gpa` passed to
    /// `get`. Caller frees.
    present: []u8,
    /// Key was explicitly deleted in this SSTable. Engine must NOT fall
    /// through to lower-level SSTables for this key.
    tombstone,
};

/// Strip and verify the 4-byte little-endian CRC32C trailer the writer
/// appends to every data block. Returns the payload slice (`buf` minus
/// trailer). `error.BadDataBlock` if `buf` is shorter than the trailer or
/// the recomputed CRC does not match.
fn verifyDataBlock(buf: []const u8) error{BadDataBlock}![]const u8 {
    if (buf.len < fmt.DATA_BLOCK_CRC_LEN) return error.BadDataBlock;
    const payload = buf[0 .. buf.len - fmt.DATA_BLOCK_CRC_LEN];
    const want = std.mem.readInt(u32, buf[buf.len - fmt.DATA_BLOCK_CRC_LEN ..][0..4], .little);
    if (crc_mod.crc32c(payload) != want) return error.BadDataBlock;
    return payload;
}

pub const Reader = struct {
    gpa: std.mem.Allocator,
    storage: Storage,

    /// Identifies this SSTable for the optional shared `BlockCache`.
    /// Defaulted to 0 for legacy callers that do not wire a cache; in
    /// that mode the field is ignored.
    sstable_id: u64 = 0,

    /// Optional shared cross-Reader cache. When set, every loadIndex /
    /// loadBloom / data-block fetch consults the cache first; on miss
    /// the bytes are populated into the cache for the next call.
    block_cache: ?*block_cache_mod.BlockCache = null,

    /// Cached metadata. Loaded on demand; reused across all `get` calls.
    /// When `block_cache` is set, these stay null — the cache is the
    /// single source of truth.
    footer_cached: ?footer_mod.Footer = null,
    bloom_buf: ?[]u8 = null,
    index_buf: ?[]u8 = null,

    pub fn init(gpa: std.mem.Allocator, storage: Storage) Reader {
        return .{ .gpa = gpa, .storage = storage };
    }

    pub fn initWithCache(
        gpa: std.mem.Allocator,
        storage: Storage,
        sstable_id: u64,
        cache: *block_cache_mod.BlockCache,
    ) Reader {
        return .{ .gpa = gpa, .storage = storage, .sstable_id = sstable_id, .block_cache = cache };
    }

    pub fn deinit(self: *Reader) void {
        if (self.bloom_buf) |b| self.gpa.free(b);
        if (self.index_buf) |b| self.gpa.free(b);
        self.* = undefined;
    }

    /// Look up `key`. Returns:
    ///   - `.present(value)` — value bytes owned by `out_gpa`.
    ///   - `.tombstone`     — explicit delete present in this SSTable.
    ///   - `null`            — key is not in this SSTable (bloom miss, index
    ///                         lower-bound out of range, or data-block scan
    ///                         miss).
    pub fn get(self: *Reader, key: []const u8, out_gpa: std.mem.Allocator) ReaderError!?Lookup {
        const f = try self.loadFooter();

        const bloom_bytes = try self.loadBloom(f);
        const filter = try bloom.Filter.parse(bloom_bytes);
        if (!filter.maybeContains(key)) return null;

        const idx_bytes = try self.loadIndex(f);
        const idx = try index_mod.IndexBlock.parse(idx_bytes);
        const loc = (try idx.find(key)) orelse return null;

        // Resolve the data block bytes, preferring the shared BlockCache
        // when configured. On a cache miss we still own a temporary local
        // buffer for the lifetime of the scan; on a cache hit the slice
        // is borrowed and must NOT be freed.
        if (self.block_cache) |bc| {
            const k: block_cache_mod.Key = .{
                .sstable_id = self.sstable_id,
                .kind = .data,
                .offset = loc.block_offset,
            };
            if (bc.get(k)) |cached| {
                const payload = try verifyDataBlock(cached);
                return scanDataBlock(payload, key, out_gpa);
            }
        }

        const block_buf = try self.gpa.alloc(u8, loc.block_size);
        defer self.gpa.free(block_buf);
        const got = self.storage.rangeGet(block_buf, loc.block_offset, loc.block_size) catch
            return error.BadDataBlock;
        if (got != loc.block_size) return error.ShortRead;

        if (self.block_cache) |bc| {
            const k: block_cache_mod.Key = .{
                .sstable_id = self.sstable_id,
                .kind = .data,
                .offset = loc.block_offset,
            };
            // Best-effort cache fill. OverCapacity (single block exceeds
            // configured ceiling) silently bypasses; OOM propagates.
            bc.put(k, block_buf) catch |err| switch (err) {
                error.OverCapacity => {},
                error.OutOfMemory => return error.OutOfMemory,
            };
        }

        const payload = try verifyDataBlock(block_buf);
        return scanDataBlock(payload, key, out_gpa);
    }

    fn loadFooter(self: *Reader) ReaderError!footer_mod.Footer {
        if (self.footer_cached) |f| return f;
        const total = self.storage.size() catch return error.BadFooter;
        if (total < footer_mod.FOOTER_BYTES) return error.BadFooter;
        var buf: [footer_mod.FOOTER_BYTES]u8 = undefined;
        const got = self.storage.rangeGet(
            &buf,
            total - footer_mod.FOOTER_BYTES,
            footer_mod.FOOTER_BYTES,
        ) catch return error.BadFooter;
        if (got != footer_mod.FOOTER_BYTES) return error.ShortRead;
        const f = try footer_mod.parse(&buf);
        self.footer_cached = f;
        return f;
    }

    fn loadBloom(self: *Reader, f: footer_mod.Footer) ReaderError![]const u8 {
        if (self.block_cache) |bc| {
            const k: block_cache_mod.Key = .{
                .sstable_id = self.sstable_id,
                .kind = .bloom,
                .offset = f.bloom_offset,
            };
            if (bc.get(k)) |cached| return cached;

            const buf = try self.gpa.alloc(u8, f.bloom_size);
            defer self.gpa.free(buf);
            const got = self.storage.rangeGet(buf, f.bloom_offset, f.bloom_size) catch
                return error.BadBloom;
            if (got != f.bloom_size) return error.ShortRead;

            bc.put(k, buf) catch |err| switch (err) {
                error.OverCapacity => return self.fallbackPerReaderBloom(buf),
                error.OutOfMemory => return error.OutOfMemory,
            };
            return bc.get(k) orelse return error.OutOfMemory;
        }
        if (self.bloom_buf) |b| return b;
        const buf = try self.gpa.alloc(u8, f.bloom_size);
        errdefer self.gpa.free(buf);
        const got = self.storage.rangeGet(buf, f.bloom_offset, f.bloom_size) catch
            return error.BadBloom;
        if (got != f.bloom_size) return error.ShortRead;
        self.bloom_buf = buf;
        return buf;
    }

    fn fallbackPerReaderBloom(self: *Reader, src: []const u8) ReaderError![]const u8 {
        // Block too big for the shared cache. Fall back to per-Reader
        // ownership — same path the no-cache mode uses.
        if (self.bloom_buf) |b| return b;
        const owned = try self.gpa.dupe(u8, src);
        self.bloom_buf = owned;
        return owned;
    }

    fn loadIndex(self: *Reader, f: footer_mod.Footer) ReaderError![]const u8 {
        if (self.block_cache) |bc| {
            const k: block_cache_mod.Key = .{
                .sstable_id = self.sstable_id,
                .kind = .index,
                .offset = f.index_offset,
            };
            if (bc.get(k)) |cached| return cached;

            const buf = try self.gpa.alloc(u8, f.index_size);
            defer self.gpa.free(buf);
            const got = self.storage.rangeGet(buf, f.index_offset, f.index_size) catch
                return error.BadIndex;
            if (got != f.index_size) return error.ShortRead;

            bc.put(k, buf) catch |err| switch (err) {
                error.OverCapacity => return self.fallbackPerReaderIndex(buf),
                error.OutOfMemory => return error.OutOfMemory,
            };
            return bc.get(k) orelse return error.OutOfMemory;
        }
        if (self.index_buf) |b| return b;
        const buf = try self.gpa.alloc(u8, f.index_size);
        errdefer self.gpa.free(buf);
        const got = self.storage.rangeGet(buf, f.index_offset, f.index_size) catch
            return error.BadIndex;
        if (got != f.index_size) return error.ShortRead;
        self.index_buf = buf;
        return buf;
    }

    fn fallbackPerReaderIndex(self: *Reader, src: []const u8) ReaderError![]const u8 {
        if (self.index_buf) |b| return b;
        const owned = try self.gpa.dupe(u8, src);
        self.index_buf = owned;
        return owned;
    }
};

/// One entry yielded by `ScanIterator`. `value == null` is a tombstone.
/// `key` and `value` borrow into the iterator's currently-loaded data
/// block; both are invalidated by the next call to `next()` or `deinit()`.
pub const ScanEntry = struct {
    key: []const u8,
    value: ?[]const u8,
};

/// In-order iteration over every entry in an SSTable. Walks the index
/// block then fetches each data block in turn, scanning entries within.
/// Used by compaction to merge sorted streams from multiple SSTables.
///
/// Lifetime: returned `ScanEntry` slices reference the iterator's current
/// data block buffer; consumers must copy out before calling `next()`
/// again. The iterator owns one block buffer at a time, freeing it before
/// loading the next.
pub const ScanIterator = struct {
    reader: *Reader,
    f: footer_mod.Footer,
    idx: index_mod.IndexBlock,
    block_idx: u32 = 0,
    block_buf: ?[]u8 = null,
    block_pos: usize = 0,
    payload_end: usize = 0,

    pub fn deinit(it: *ScanIterator) void {
        if (it.block_buf) |b| {
            it.reader.gpa.free(b);
            it.block_buf = null;
        }
    }

    pub fn next(it: *ScanIterator) ReaderError!?ScanEntry {
        while (true) {
            // Need to load the next data block?
            if (it.block_buf == null or it.block_pos >= it.payload_end) {
                if (it.block_idx >= it.idx.count()) {
                    it.deinit();
                    return null;
                }
                if (it.block_buf) |b| {
                    it.reader.gpa.free(b);
                    it.block_buf = null;
                }
                const e = try it.idx.entryAt(it.block_idx);
                it.block_idx += 1;

                const buf = try it.reader.gpa.alloc(u8, e.block_size);
                errdefer it.reader.gpa.free(buf);
                const got = it.reader.storage.rangeGet(buf, e.block_offset, e.block_size) catch
                    return error.BadDataBlock;
                if (got != e.block_size) return error.ShortRead;

                const payload = try verifyDataBlock(buf);
                if (payload.len < data_block.DATA_BLOCK_HEADER_BYTES) return error.BadDataBlock;
                const hdr_bytes: [data_block.DATA_BLOCK_HEADER_BYTES]u8 =
                    payload[0..data_block.DATA_BLOCK_HEADER_BYTES].*;
                const hdr: data_block.DataBlockHeader = @bitCast(hdr_bytes);
                if (hdr.magic != data_block.DATA_BLOCK_MAGIC) return error.BadDataBlock;
                if (hdr.version != data_block.DATA_BLOCK_VERSION) return error.BadDataBlock;

                const payload_end = data_block.DATA_BLOCK_HEADER_BYTES + @as(usize, hdr.payload_size);
                if (payload_end > payload.len) return error.BadDataBlock;

                it.block_buf = buf;
                it.block_pos = data_block.DATA_BLOCK_HEADER_BYTES;
                it.payload_end = payload_end;
            }

            const buf = it.block_buf.?;
            if (it.block_pos + 8 > it.payload_end) return error.BadDataBlock;
            const klen = std.mem.readInt(u32, buf[it.block_pos..][0..4], .little);
            const vlen = std.mem.readInt(u32, buf[it.block_pos + 4 ..][0..4], .little);
            it.block_pos += 8;
            if (it.block_pos + klen > it.payload_end) return error.BadDataBlock;
            const k = buf[it.block_pos .. it.block_pos + klen];
            it.block_pos += klen;
            const is_tombstone = vlen == data_block.TOMBSTONE_VALUE_LEN;
            const value_len_in_block: usize = if (is_tombstone) 0 else vlen;
            if (it.block_pos + value_len_in_block > it.payload_end) return error.BadDataBlock;
            const v: ?[]const u8 = if (is_tombstone) null else buf[it.block_pos .. it.block_pos + vlen];
            it.block_pos += value_len_in_block;
            return ScanEntry{ .key = k, .value = v };
        }
    }
};

/// Open an in-order scan over the entire SSTable. Lazily fetches data
/// blocks via the underlying `Storage`. Caller must `deinit` the iterator
/// (idempotent — `next` returning null also frees the trailing block).
pub fn openScan(reader: *Reader) ReaderError!ScanIterator {
    const f = try reader.loadFooter();
    const idx_buf = try reader.loadIndex(f);
    const idx = try index_mod.IndexBlock.parse(idx_buf);
    return .{ .reader = reader, .f = f, .idx = idx };
}

/// Parse a data block buffer and linear-scan for `key`. Pure function — no
/// IO, no allocation beyond the returned value's `out_gpa.dupe`. Exposed
/// for testing and for the (future) block cache, which holds parsed
/// blocks and re-runs the scan without re-fetching.
pub fn scanDataBlock(
    block_buf: []const u8,
    key: []const u8,
    out_gpa: std.mem.Allocator,
) ReaderError!?Lookup {
    if (block_buf.len < data_block.DATA_BLOCK_HEADER_BYTES) return error.BadDataBlock;
    const hdr_bytes: [data_block.DATA_BLOCK_HEADER_BYTES]u8 =
        block_buf[0..data_block.DATA_BLOCK_HEADER_BYTES].*;
    const hdr: data_block.DataBlockHeader = @bitCast(hdr_bytes);

    if (hdr.magic != data_block.DATA_BLOCK_MAGIC) return error.BadDataBlock;
    if (hdr.version != data_block.DATA_BLOCK_VERSION) return error.BadDataBlock;
    if (hdr.flags != 0) return error.BadDataBlock;

    const payload_start = data_block.DATA_BLOCK_HEADER_BYTES;
    const payload_end = payload_start + @as(usize, hdr.payload_size);
    if (payload_end > block_buf.len) return error.BadDataBlock;

    var off: usize = payload_start;
    while (off < payload_end) {
        if (off + 8 > payload_end) return error.BadDataBlock;
        const klen = std.mem.readInt(u32, block_buf[off..][0..4], .little);
        const vlen = std.mem.readInt(u32, block_buf[off + 4 ..][0..4], .little);
        off += 8;
        if (off + klen > payload_end) return error.BadDataBlock;
        const k = block_buf[off .. off + klen];
        off += klen;

        const is_tombstone = vlen == data_block.TOMBSTONE_VALUE_LEN;
        const value_bytes_in_block: usize = if (is_tombstone) 0 else vlen;
        if (off + value_bytes_in_block > payload_end) return error.BadDataBlock;

        switch (std.mem.order(u8, k, key)) {
            .eq => {
                if (is_tombstone) return Lookup.tombstone;
                const out = try out_gpa.dupe(u8, block_buf[off .. off + vlen]);
                return Lookup{ .present = out };
            },
            // Entries are sorted ascending; once we pass the target there
            // is no point continuing the scan.
            .gt => return null,
            .lt => {},
        }
        off += value_bytes_in_block;
    }
    return null;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const writer = @import("writer.zig");
const memtable_mod = @import("../engine/memtable.zig");
const testing = std.testing;

fn buildSstFromKVs(gpa: std.mem.Allocator, entries: []const struct { key: []const u8, value: ?[]const u8 }) ![]u8 {
    var b = try writer.Builder.init(gpa, .{ .expected_entries = entries.len });
    defer b.deinit();
    for (entries) |e| {
        if (e.value) |v| try b.add(e.key, v) else try b.addTombstone(e.key);
    }
    return b.finish();
}

test "Reader: simple roundtrip" {
    const gpa = testing.allocator;
    const sst = try buildSstFromKVs(gpa, &.{
        .{ .key = "alpha", .value = "AAA" },
        .{ .key = "bravo", .value = "BB" },
        .{ .key = "charlie", .value = "CCCC" },
    });
    defer gpa.free(sst);

    var ms = blob.MemoryStorage.init(sst);
    var r = Reader.init(gpa, ms.storage());
    defer r.deinit();

    const a = (try r.get("alpha", gpa)).?;
    defer gpa.free(a.present);
    try testing.expectEqualStrings("AAA", a.present);

    const c = (try r.get("charlie", gpa)).?;
    defer gpa.free(c.present);
    try testing.expectEqualStrings("CCCC", c.present);

    try testing.expectEqual(@as(?Lookup, null), try r.get("zzz", gpa));
}

test "Reader: tombstone resolves to .tombstone, not null" {
    const gpa = testing.allocator;
    const sst = try buildSstFromKVs(gpa, &.{
        .{ .key = "alpha", .value = "alive" },
        .{ .key = "bravo", .value = null },
        .{ .key = "charlie", .value = "alive" },
    });
    defer gpa.free(sst);

    var ms = blob.MemoryStorage.init(sst);
    var r = Reader.init(gpa, ms.storage());
    defer r.deinit();

    const got = (try r.get("bravo", gpa)).?;
    try testing.expectEqual(Lookup.tombstone, got);
}

test "Reader: bloom miss short-circuits without fetching index or data" {
    const gpa = testing.allocator;
    const sst = try buildSstFromKVs(gpa, &.{
        .{ .key = "alpha", .value = "1" },
        .{ .key = "bravo", .value = "2" },
        .{ .key = "charlie", .value = "3" },
    });
    defer gpa.free(sst);

    var ms = blob.MemoryStorage.init(sst);
    var counter = blob.CountingStorage.init(ms.storage());
    var r = Reader.init(gpa, counter.storage());
    defer r.deinit();

    // Probe a key clearly absent. With high probability the bloom filter
    // rejects, so only footer + bloom are fetched (size + 2 rangeGet).
    // We can't guarantee bloom rejects every absent key (false-positive
    // semantics), so we probe many and assert that at least one hit the
    // short-circuit path.
    var saw_bloom_short_circuit = false;
    var i: u32 = 0;
    while (i < 16) : (i += 1) {
        var keybuf: [32]u8 = undefined;
        const k = try std.fmt.bufPrint(&keybuf, "absent_{d}", .{i});
        const before = counter.range_get_calls;
        try testing.expectEqual(@as(?Lookup, null), try r.get(k, gpa));
        const after = counter.range_get_calls;

        // First iteration loads footer + bloom (+ possibly index/data on
        // false positive). After the first call, footer + bloom are cached;
        // subsequent calls cost 0 if bloom rejects, or 2 (index + data) on
        // a false positive.
        if (i > 0 and after - before == 0) saw_bloom_short_circuit = true;
    }
    try testing.expect(saw_bloom_short_circuit);
}

test "Reader: footer / bloom / index loaded once and cached" {
    const gpa = testing.allocator;
    const sst = try buildSstFromKVs(gpa, &.{
        .{ .key = "alpha", .value = "1" },
        .{ .key = "bravo", .value = "2" },
        .{ .key = "charlie", .value = "3" },
    });
    defer gpa.free(sst);

    var ms = blob.MemoryStorage.init(sst);
    var counter = blob.CountingStorage.init(ms.storage());
    var r = Reader.init(gpa, counter.storage());
    defer r.deinit();

    // First get: size(1) + footer(1) + bloom(1) + index(1) + data(1) = 4 rangeGets.
    const a = (try r.get("alpha", gpa)).?;
    defer gpa.free(a.present);
    const after_first_range = counter.range_get_calls;
    const after_first_size = counter.size_calls;
    try testing.expectEqual(@as(u32, 4), after_first_range);
    try testing.expectEqual(@as(u32, 1), after_first_size);

    // Second get on a different present key: only the data-block fetch.
    const b = (try r.get("bravo", gpa)).?;
    defer gpa.free(b.present);
    try testing.expectEqual(after_first_range + 1, counter.range_get_calls);
    try testing.expectEqual(after_first_size, counter.size_calls);

    // Third get on the same key as the second: still one new fetch (no
    // block cache at this layer).
    const b2 = (try r.get("bravo", gpa)).?;
    defer gpa.free(b2.present);
    try testing.expectEqual(after_first_range + 2, counter.range_get_calls);
}

test "Reader: multi-block SSTable, every key resolvable" {
    const gpa = testing.allocator;

    var b = try writer.Builder.init(gpa, .{
        .target_block_size = 64,
        .expected_entries = 32,
    });
    defer b.deinit();

    const N: u32 = 32;
    var i: u32 = 0;
    while (i < N) : (i += 1) {
        var kbuf: [16]u8 = undefined;
        const k = try std.fmt.bufPrint(&kbuf, "key_{d:0>5}", .{i});
        var vbuf: [16]u8 = undefined;
        const v = try std.fmt.bufPrint(&vbuf, "v_{d}", .{i});
        try b.add(k, v);
    }
    const sst = try b.finish();
    defer gpa.free(sst);

    // Confirm we actually got multiple blocks.
    const f = try footer_mod.parse(sst);
    const idx = try index_mod.IndexBlock.parse(sst[f.index_offset .. f.index_offset + f.index_size]);
    try testing.expect(idx.count() > 1);

    var ms = blob.MemoryStorage.init(sst);
    var r = Reader.init(gpa, ms.storage());
    defer r.deinit();

    var j: u32 = 0;
    while (j < N) : (j += 1) {
        var kbuf: [16]u8 = undefined;
        const k = try std.fmt.bufPrint(&kbuf, "key_{d:0>5}", .{j});
        const got = (try r.get(k, gpa)).?;
        defer gpa.free(got.present);
        var vbuf: [16]u8 = undefined;
        const want = try std.fmt.bufPrint(&vbuf, "v_{d}", .{j});
        try testing.expectEqualStrings(want, got.present);
    }
}

test "Reader: end-to-end MemTable → Writer → Reader" {
    const gpa = testing.allocator;

    var mt = try memtable_mod.MemTable.init(gpa);
    defer mt.deinit();
    try mt.put("alpha", "a-val");
    try mt.put("delta", "d-val");
    try mt.put("mango", "m-val");
    try mt.putTombstone("bravo");
    try mt.put("zeta", "z-val");
    mt.freeze();

    const sst = try writer.writeFromMemTable(gpa, &mt, .{ .target_block_size = 64 });
    defer gpa.free(sst);

    var ms = blob.MemoryStorage.init(sst);
    var r = Reader.init(gpa, ms.storage());
    defer r.deinit();

    const a = (try r.get("alpha", gpa)).?;
    defer gpa.free(a.present);
    try testing.expectEqualStrings("a-val", a.present);

    const tomb = (try r.get("bravo", gpa)).?;
    try testing.expectEqual(Lookup.tombstone, tomb);

    const z = (try r.get("zeta", gpa)).?;
    defer gpa.free(z.present);
    try testing.expectEqualStrings("z-val", z.present);

    try testing.expectEqual(@as(?Lookup, null), try r.get("never_inserted", gpa));
}

test "Reader: corrupt footer → BadMagic" {
    const gpa = testing.allocator;
    const sst = try buildSstFromKVs(gpa, &.{
        .{ .key = "alpha", .value = "AAA" },
    });
    defer gpa.free(sst);

    // Stomp the footer magic (bytes [len-8, len)).
    std.mem.writeInt(u64, sst[sst.len - 8 ..][0..8], 0xDEAD_BEEF_DEAD_BEEF, .little);

    var ms = blob.MemoryStorage.init(sst);
    var r = Reader.init(gpa, ms.storage());
    defer r.deinit();

    try testing.expectError(error.BadMagic, r.get("alpha", gpa));
}

test "scanDataBlock: rejects buffer shorter than header" {
    const gpa = testing.allocator;
    const tiny = [_]u8{0} ** 4;
    try testing.expectError(error.BadDataBlock, scanDataBlock(&tiny, "x", gpa));
}

test "ScanIterator: walks every entry across multiple data blocks in order" {
    const gpa = testing.allocator;

    // Force multiple data blocks via small target_block_size.
    var b = try writer.Builder.init(gpa, .{ .target_block_size = 32, .expected_entries = 16 });
    defer b.deinit();
    const inputs = [_]struct { k: []const u8, v: ?[]const u8 }{
        .{ .k = "alpha", .v = "1" },
        .{ .k = "bravo", .v = null }, // tombstone
        .{ .k = "charlie", .v = "3" },
        .{ .k = "delta", .v = "4" },
        .{ .k = "echo", .v = "5" },
        .{ .k = "foxtrot", .v = null },
    };
    for (inputs) |i| {
        if (i.v) |v| try b.add(i.k, v) else try b.addTombstone(i.k);
    }
    const sst = try b.finish();
    defer gpa.free(sst);

    var ms = blob.MemoryStorage.init(sst);
    var r = Reader.init(gpa, ms.storage());
    defer r.deinit();

    var it = try openScan(&r);
    defer it.deinit();

    // Confirm we actually rolled multiple blocks.
    const f = try footer_mod.parse(sst);
    const idx = try index_mod.IndexBlock.parse(sst[f.index_offset .. f.index_offset + f.index_size]);
    try testing.expect(idx.count() > 1);

    var i: usize = 0;
    while (try it.next()) |entry| : (i += 1) {
        try testing.expectEqualStrings(inputs[i].k, entry.key);
        if (inputs[i].v) |v| {
            try testing.expectEqualStrings(v, entry.value.?);
        } else {
            try testing.expect(entry.value == null);
        }
    }
    try testing.expectEqual(inputs.len, i);
}

test "reader returns BadDataBlock when CRC trailer mismatches payload" {
    const gpa = testing.allocator;

    var b = try writer.Builder.init(gpa, .{ .expected_entries = 4 });
    defer b.deinit();
    try b.add("alpha", "AAA");
    try b.add("bravo", "BB");
    var sst = try b.finish();
    defer gpa.free(sst);

    const f = try footer_mod.parse(sst);
    const idx = try index_mod.IndexBlock.parse(sst[f.index_offset .. f.index_offset + f.index_size]);
    const first = try idx.entryAt(0);
    // Flip the first byte of the payload (definitely covered by the CRC).
    sst[first.block_offset] ^= 0xFF;

    var ms = blob.MemoryStorage.init(sst);
    var r = Reader.init(gpa, ms.storage());
    defer r.deinit();

    try testing.expectError(error.BadDataBlock, r.get("alpha", gpa));
}

test "verifyDataBlock rejects buffers shorter than the trailer" {
    var tiny: [3]u8 = .{ 0, 0, 0 };
    try testing.expectError(error.BadDataBlock, verifyDataBlock(&tiny));
    var empty: [0]u8 = .{};
    try testing.expectError(error.BadDataBlock, verifyDataBlock(&empty));
}

test "Reader: BlockCache cuts cross-Reader rangeGets to footer-only" {
    const gpa = testing.allocator;
    const gcs = @import("../storage/gcs.zig");
    const gcs_storage_mod = @import("../storage/gcs_storage.zig");

    var b = try writer.Builder.init(gpa, .{ .expected_entries = 4 });
    defer b.deinit();
    try b.add("alpha", "AAA");
    try b.add("bravo", "BBBB");
    try b.add("charlie", "CCCCC");
    const sst = try b.finish();
    defer gpa.free(sst);

    var fs = gcs.FakeServer.init(gpa, gcs.DEFAULT_BASE_URL, "bk", "obj.sst", sst);
    defer fs.deinit();
    var client = gcs.Client.init(gpa, fs.transport(), .{});
    defer client.deinit();

    var bc = block_cache_mod.BlockCache.init(gpa, 64 * 1024);
    defer bc.deinit();

    var storage1 = gcs_storage_mod.GcsStorage.init(&client, "bk", "obj.sst", sst.len);
    var r1 = Reader.initWithCache(gpa, storage1.storage(), 42, &bc);
    defer r1.deinit();

    const a1 = (try r1.get("alpha", gpa)).?;
    defer gpa.free(a1.present);
    try testing.expectEqualStrings("AAA", a1.present);

    const baseline_after_first_get = fs.call_count;

    // Second Reader against the same SSTable. footer is per-Reader so it
    // is fetched again; index/bloom/data come from the shared BlockCache
    // → exactly 1 additional rangeGet (the footer tail-read).
    var storage2 = gcs_storage_mod.GcsStorage.init(&client, "bk", "obj.sst", sst.len);
    var r2 = Reader.initWithCache(gpa, storage2.storage(), 42, &bc);
    defer r2.deinit();

    const a2 = (try r2.get("alpha", gpa)).?;
    defer gpa.free(a2.present);
    try testing.expectEqualStrings("AAA", a2.present);

    try testing.expectEqual(baseline_after_first_get + 1, fs.call_count);
}

test "Reader: BlockCache hit on second get within same Reader skips data fetch" {
    const gpa = testing.allocator;
    const gcs = @import("../storage/gcs.zig");
    const gcs_storage_mod = @import("../storage/gcs_storage.zig");

    var b = try writer.Builder.init(gpa, .{ .expected_entries = 4 });
    defer b.deinit();
    try b.add("alpha", "AAA");
    try b.add("bravo", "BBBB");
    const sst = try b.finish();
    defer gpa.free(sst);

    var fs = gcs.FakeServer.init(gpa, gcs.DEFAULT_BASE_URL, "bk", "o", sst);
    defer fs.deinit();
    var client = gcs.Client.init(gpa, fs.transport(), .{});
    defer client.deinit();

    var bc = block_cache_mod.BlockCache.init(gpa, 64 * 1024);
    defer bc.deinit();

    var storage = gcs_storage_mod.GcsStorage.init(&client, "bk", "o", sst.len);
    var r = Reader.initWithCache(gpa, storage.storage(), 7, &bc);
    defer r.deinit();

    const a = (try r.get("alpha", gpa)).?;
    defer gpa.free(a.present);

    const after_first = fs.call_count;
    // Second get of the same key: footer is on-struct, bloom + index +
    // data all hit the shared cache → zero additional rangeGets.
    const a2 = (try r.get("alpha", gpa)).?;
    defer gpa.free(a2.present);
    try testing.expectEqual(after_first, fs.call_count);
}
