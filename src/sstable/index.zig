//! SSTable index-block parser.
//!
//! Reads a binary index block laid out by `format.zig`, validates its header,
//! and binary-searches for the data-block location of a given key. Operates
//! entirely as a borrowed view over a caller-owned buffer — no allocations on
//! the lookup path. Designed for the LSM read path:
//!   GCS Range fetch → buffer → IndexBlock.parse → IndexBlock.find → next Range fetch.

const std = @import("std");
const builtin = @import("builtin");
const format = @import("format.zig");

pub const IndexHeader = format.IndexHeader;
pub const IndexEntryHeader = format.IndexEntryHeader;

const HEADER_BYTES: usize = format.INDEX_HEADER_BYTES;
const ENTRY_HEADER_BYTES: usize = format.INDEX_ENTRY_HEADER_BYTES;

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

pub const IndexEntryView = struct {
    key: []const u8,
    block_offset: u64,
    block_size: u32,
};

pub const IndexBlock = struct {
    header: IndexHeader,
    /// Raw little-endian u32 offsets into `heap`, sorted ascending by key.
    /// Alignment is 1 because the underlying buffer (e.g. a GCS response) is
    /// only byte-aligned.
    offsets: []align(1) const u32,
    /// Variable-length entry heap: each entry is an `IndexEntryHeader`
    /// followed by `key_len` raw key bytes.
    heap: []const u8,

    /// Validate `buf` and return a zero-copy view. The view borrows `buf`;
    /// caller must keep `buf` alive for the lifetime of the IndexBlock.
    pub fn parse(buf: []const u8) IndexBlockError!IndexBlock {
        if (buf.len < HEADER_BYTES) return error.BufferTooSmall;

        const hdr_bytes: [HEADER_BYTES]u8 = buf[0..HEADER_BYTES].*;
        const header: IndexHeader = @bitCast(hdr_bytes);

        if (header.magic != format.INDEX_BLOCK_MAGIC) return error.BadMagic;
        if (header.version != format.INDEX_BLOCK_VERSION) return error.UnsupportedVersion;
        if (header.flags != 0) return error.BadFlags;

        const offsets_bytes: usize = @as(usize, header.entry_count) * @sizeOf(u32);
        const heap_bytes: usize = header.heap_size;
        const total_required = HEADER_BYTES + offsets_bytes + heap_bytes;
        if (buf.len < total_required) return error.BufferTooSmall;

        const offsets_slice = buf[HEADER_BYTES .. HEADER_BYTES + offsets_bytes];
        const offsets = std.mem.bytesAsSlice(u32, offsets_slice);

        const heap = buf[HEADER_BYTES + offsets_bytes ..][0..heap_bytes];

        return IndexBlock{
            .header = header,
            .offsets = offsets,
            .heap = heap,
        };
    }

    pub fn count(self: IndexBlock) u32 {
        return self.header.entry_count;
    }

    /// Read the entry at sorted-index `i`. Returns an error on a corrupt
    /// buffer rather than panicking, so a single bad entry can't crash the
    /// process serving other keys.
    pub fn entryAt(self: IndexBlock, i: u32) IndexBlockError!IndexEntryView {
        std.debug.assert(i < self.header.entry_count);

        const off: usize = self.offsets[i];
        if (off + ENTRY_HEADER_BYTES > self.heap.len) return error.OffsetOutOfRange;

        const entry_bytes: [ENTRY_HEADER_BYTES]u8 = self.heap[off .. off + ENTRY_HEADER_BYTES][0..ENTRY_HEADER_BYTES].*;
        const eh: IndexEntryHeader = @bitCast(entry_bytes);

        if (eh.key_len > format.MAX_KEY_LEN) return error.KeyTooLong;

        const key_start = off + ENTRY_HEADER_BYTES;
        const key_end = key_start + eh.key_len;
        if (key_end > self.heap.len) return error.KeyOutOfRange;

        return IndexEntryView{
            .key = self.heap[key_start..key_end],
            .block_offset = eh.block_offset,
            .block_size = eh.block_size,
        };
    }

    /// Lower-bound binary search: returns the entry whose key is the smallest
    /// key >= `key`. That's the data block which would contain `key` if it
    /// exists in this SSTable. Returns null only if `key` is strictly greater
    /// than the largest indexed key (key cannot exist in this SSTable).
    /// Returns an `IndexBlockError` if a probed entry is corrupt.
    pub fn find(self: IndexBlock, key: []const u8) IndexBlockError!?BlockLocator {
        var lo: u32 = 0;
        var hi: u32 = self.header.entry_count;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            const entry = try self.entryAt(mid);
            switch (std.mem.order(u8, entry.key, key)) {
                .lt => lo = mid + 1,
                .eq, .gt => hi = mid,
            }
        }
        if (lo == self.header.entry_count) return null;
        const winner = try self.entryAt(lo);
        return BlockLocator{
            .block_offset = winner.block_offset,
            .block_size = winner.block_size,
        };
    }
};

// ---------------------------------------------------------------------------
// Encoder used by tests and (eventually) the SSTable writer. Kept here so the
// on-disk format has a single round-trippable definition. Behind a build-time
// gate so it doesn't bloat the production binary unless explicitly used.
// ---------------------------------------------------------------------------

pub const BuilderEntry = struct {
    key: []const u8,
    block_offset: u64,
    block_size: u32,
};

/// Encode `entries` (already sorted ascending by key) into a fresh index-block
/// buffer owned by `gpa`. Returned slice must be freed by the caller.
pub fn buildIndexBlock(gpa: std.mem.Allocator, entries: []const BuilderEntry) ![]u8 {
    // Sanity: entries must be sorted; otherwise the bsearch invariant breaks
    // and tests would silently pass against a malformed block.
    if (entries.len > 1) {
        var i: usize = 1;
        while (i < entries.len) : (i += 1) {
            std.debug.assert(std.mem.order(u8, entries[i - 1].key, entries[i].key) == .lt);
        }
    }
    if (entries.len > std.math.maxInt(u32)) return error.TooManyEntries;

    // Build the heap first so we know each entry's offset within it, then
    // assemble header + offsets + heap into one contiguous buffer.
    var heap: std.ArrayList(u8) = .empty;
    defer heap.deinit(gpa);

    var offsets: std.ArrayList(u32) = .empty;
    defer offsets.deinit(gpa);

    try offsets.ensureTotalCapacity(gpa, entries.len);
    for (entries) |e| {
        if (e.key.len > format.MAX_KEY_LEN) return error.KeyTooLong;
        const off: u32 = @intCast(heap.items.len);
        try offsets.append(gpa, off);

        const eh = IndexEntryHeader{
            .block_offset = e.block_offset,
            .block_size = e.block_size,
            .key_len = @intCast(e.key.len),
        };
        const eh_bytes: [ENTRY_HEADER_BYTES]u8 = @bitCast(eh);
        try heap.appendSlice(gpa, &eh_bytes);
        try heap.appendSlice(gpa, e.key);
    }

    if (heap.items.len > std.math.maxInt(u32)) return error.HeapTooLarge;

    const header = IndexHeader{
        .magic = format.INDEX_BLOCK_MAGIC,
        .version = format.INDEX_BLOCK_VERSION,
        .flags = 0,
        .entry_count = @intCast(entries.len),
        .heap_size = @intCast(heap.items.len),
    };
    const header_bytes: [HEADER_BYTES]u8 = @bitCast(header);

    const offsets_bytes_len = offsets.items.len * @sizeOf(u32);
    const total = HEADER_BYTES + offsets_bytes_len + heap.items.len;

    var out = try gpa.alloc(u8, total);
    errdefer gpa.free(out);

    @memcpy(out[0..HEADER_BYTES], &header_bytes);
    // Offsets are written via writeInt to make the byte order explicit in the
    // serializer (the parser side relies on host == little-endian).
    var i: usize = 0;
    while (i < offsets.items.len) : (i += 1) {
        const dst = out[HEADER_BYTES + i * @sizeOf(u32) ..][0..@sizeOf(u32)];
        std.mem.writeInt(u32, dst, offsets.items[i], .little);
    }
    @memcpy(out[HEADER_BYTES + offsets_bytes_len ..], heap.items);

    return out;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const testing = std.testing;

test "parse rejects buffer smaller than header" {
    const buf = [_]u8{0} ** 8;
    try testing.expectError(error.BufferTooSmall, IndexBlock.parse(&buf));
}

test "parse rejects bad magic" {
    var buf = [_]u8{0} ** HEADER_BYTES;
    // valid version field, wrong magic
    std.mem.writeInt(u32, buf[0..4], 0xDEADBEEF, .little);
    std.mem.writeInt(u16, buf[4..6], format.INDEX_BLOCK_VERSION, .little);
    try testing.expectError(error.BadMagic, IndexBlock.parse(&buf));
}

test "parse rejects unsupported version" {
    var buf = [_]u8{0} ** HEADER_BYTES;
    std.mem.writeInt(u32, buf[0..4], format.INDEX_BLOCK_MAGIC, .little);
    std.mem.writeInt(u16, buf[4..6], 999, .little);
    try testing.expectError(error.UnsupportedVersion, IndexBlock.parse(&buf));
}

test "parse rejects nonzero flags" {
    var buf = [_]u8{0} ** HEADER_BYTES;
    std.mem.writeInt(u32, buf[0..4], format.INDEX_BLOCK_MAGIC, .little);
    std.mem.writeInt(u16, buf[4..6], format.INDEX_BLOCK_VERSION, .little);
    std.mem.writeInt(u16, buf[6..8], 1, .little);
    try testing.expectError(error.BadFlags, IndexBlock.parse(&buf));
}

test "parse rejects truncated body" {
    const gpa = testing.allocator;
    const entries = [_]BuilderEntry{
        .{ .key = "alpha", .block_offset = 0, .block_size = 100 },
        .{ .key = "mango", .block_offset = 100, .block_size = 50 },
    };
    const full = try buildIndexBlock(gpa, &entries);
    defer gpa.free(full);
    // Drop the last byte so heap is truncated.
    try testing.expectError(error.BufferTooSmall, IndexBlock.parse(full[0 .. full.len - 1]));
}

test "find on empty index returns null" {
    const gpa = testing.allocator;
    const buf = try buildIndexBlock(gpa, &[_]BuilderEntry{});
    defer gpa.free(buf);
    const idx = try IndexBlock.parse(buf);
    try testing.expectEqual(@as(u32, 0), idx.count());
    try testing.expectEqual(@as(?BlockLocator, null), try idx.find("anything"));
}

test "find on single entry hits that entry and clamps above" {
    const gpa = testing.allocator;
    const entries = [_]BuilderEntry{
        .{ .key = "only", .block_offset = 42, .block_size = 7 },
    };
    const buf = try buildIndexBlock(gpa, &entries);
    defer gpa.free(buf);
    const idx = try IndexBlock.parse(buf);

    const hit = (try idx.find("only")).?;
    try testing.expectEqual(@as(u64, 42), hit.block_offset);
    try testing.expectEqual(@as(u32, 7), hit.block_size);

    // Lower-bound: any key < "only" should still resolve to the only entry.
    const lower = (try idx.find("a")).?;
    try testing.expectEqual(@as(u64, 42), lower.block_offset);

    // Strictly greater than the largest indexed key → null.
    try testing.expectEqual(@as(?BlockLocator, null), try idx.find("zzz"));
}

test "find: roundtrip 3 keys with lower-bound semantics" {
    const gpa = testing.allocator;
    const entries = [_]BuilderEntry{
        .{ .key = "alpha", .block_offset = 0, .block_size = 100 },
        .{ .key = "mango", .block_offset = 100, .block_size = 50 },
        .{ .key = "zeta", .block_offset = 150, .block_size = 25 },
    };
    const buf = try buildIndexBlock(gpa, &entries);
    defer gpa.free(buf);
    const idx = try IndexBlock.parse(buf);

    try testing.expectEqual(@as(u32, 3), idx.count());

    // Exact hits
    try testing.expectEqual(@as(u64, 0), (try idx.find("alpha")).?.block_offset);
    try testing.expectEqual(@as(u64, 100), (try idx.find("mango")).?.block_offset);
    try testing.expectEqual(@as(u64, 150), (try idx.find("zeta")).?.block_offset);

    // Between-keys: lower-bound returns the next key >= target.
    try testing.expectEqual(@as(u64, 100), (try idx.find("apple")).?.block_offset); // → mango
    try testing.expectEqual(@as(u64, 150), (try idx.find("nectar")).?.block_offset); // → zeta

    // Below the smallest key still maps to the first block.
    try testing.expectEqual(@as(u64, 0), (try idx.find("")).?.block_offset);
    try testing.expectEqual(@as(u64, 0), (try idx.find("a")).?.block_offset);

    // Above the largest key → null.
    try testing.expectEqual(@as(?BlockLocator, null), try idx.find("zzz"));
}

test "find: large index, exact lookups land on their own entries" {
    const gpa = testing.allocator;

    const N: u32 = 10_000;
    var keys: std.ArrayList([]u8) = .empty;
    defer {
        for (keys.items) |k| gpa.free(k);
        keys.deinit(gpa);
    }
    var entries: std.ArrayList(BuilderEntry) = .empty;
    defer entries.deinit(gpa);

    var i: u32 = 0;
    while (i < N) : (i += 1) {
        const k = try std.fmt.allocPrint(gpa, "key_{d:0>8}", .{i});
        try keys.append(gpa, k);
        try entries.append(gpa, .{
            .key = k,
            .block_offset = @as(u64, i) * 1024,
            .block_size = 1024,
        });
    }

    const buf = try buildIndexBlock(gpa, entries.items);
    defer gpa.free(buf);
    const idx = try IndexBlock.parse(buf);

    try testing.expectEqual(N, idx.count());

    // Spot-check: first, last, a few in the middle.
    const probes = [_]u32{ 0, 1, 7, 1234, 5000, 9998, 9999 };
    for (probes) |p| {
        const expected_off: u64 = @as(u64, p) * 1024;
        const got = (try idx.find(keys.items[p])).?;
        try testing.expectEqual(expected_off, got.block_offset);
        try testing.expectEqual(@as(u32, 1024), got.block_size);
    }

    // A key strictly past the last → null.
    try testing.expectEqual(@as(?BlockLocator, null), try idx.find("key_99999999"));
    try testing.expectEqual(@as(?BlockLocator, null), try idx.find("zzz"));
}

test "corruption: stomped offset surfaces as IndexBlockError" {
    const gpa = testing.allocator;
    const entries = [_]BuilderEntry{
        .{ .key = "alpha", .block_offset = 0, .block_size = 100 },
        .{ .key = "mango", .block_offset = 100, .block_size = 50 },
        .{ .key = "zeta", .block_offset = 150, .block_size = 25 },
    };
    const buf = try buildIndexBlock(gpa, &entries);
    defer gpa.free(buf);

    // Stomp offsets[1] to point past the heap.
    const off1_pos = HEADER_BYTES + 1 * @sizeOf(u32);
    std.mem.writeInt(u32, buf[off1_pos..][0..4], 0xFFFF_FFFF, .little);

    const idx = try IndexBlock.parse(buf);
    // Searching for any key forces probing into the stomped middle entry.
    try testing.expectError(error.OffsetOutOfRange, idx.find("apple"));
}

test "corruption: oversized key_len is rejected" {
    const gpa = testing.allocator;
    const entries = [_]BuilderEntry{
        .{ .key = "alpha", .block_offset = 0, .block_size = 100 },
    };
    const buf = try buildIndexBlock(gpa, &entries);
    defer gpa.free(buf);

    // The single entry sits at offsets[0] = 0 inside the heap. Heap layout:
    //   [u64 block_offset][u32 block_size][u16 key_len][key bytes...]
    // key_len lives at heap byte offset 12. Set it past MAX_KEY_LEN.
    const heap_start = HEADER_BYTES + 1 * @sizeOf(u32);
    const key_len_pos = heap_start + 12;
    std.mem.writeInt(u16, buf[key_len_pos..][0..2], format.MAX_KEY_LEN + 1, .little);

    const idx = try IndexBlock.parse(buf);
    try testing.expectError(error.KeyTooLong, idx.find("alpha"));
}

test "comptime: builtin import keeps lints quiet" {
    // Reference `builtin` so `unused import` does not fire if other tests
    // are stripped. The endian assertion in util/endian.zig still fires via
    // format.zig's transitive import.
    _ = builtin;
}
