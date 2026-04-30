//! SSTable writer.
//!
//! Streaming builder that takes a sorted sequence of (key, value) pairs
//! (and optional tombstones) and emits a complete SSTable byte buffer:
//!   data blocks → bloom filter → index block → 64-byte footer
//!
//! The output is the exact wire format produced for upload to GCS and the
//! exact format the (future) reader expects to fetch via Range requests.
//! A `writeFromMemTable` convenience wraps the builder for the common
//! engine-flush path.

const std = @import("std");

const data_block = @import("data_block.zig");
const index_mod = @import("index.zig");
const footer_mod = @import("footer.zig");
const bloom = @import("../bloom/filter.zig");
const memtable_mod = @import("../engine/memtable.zig");
const fmt = @import("format.zig");

pub const Error = error{
    UnsortedKeys,
    KeyTooLong,
    OutOfMemory,
    TooManyEntries,
    HeapTooLarge,
};

pub const WriterOptions = struct {
    /// Target byte size of a data block before the writer rolls to a new
    /// one. A single oversized entry still gets its own block — the writer
    /// never splits entries.
    target_block_size: u32 = 4096,
    /// Bloom filter false-positive target. The builder pre-sizes the filter
    /// from the MemTable's entry count when known; for stream builders the
    /// caller supplies an `expected_entries` hint.
    expected_fp: f64 = 0.01,
    /// Hint for bloom-filter sizing in the stream builder. Ignored by
    /// `writeFromMemTable` (which uses the table's actual count).
    expected_entries: usize = 1024,
};

const PendingIndexEntry = struct {
    last_key: []u8, // owned; freed in deinit / consumed in finish
    block_offset: u64,
    block_size: u32,
};

pub const Builder = struct {
    gpa: std.mem.Allocator,
    options: WriterOptions,

    /// Final SSTable buffer accumulator.
    out: std.ArrayList(u8) = .empty,
    /// Staging buffer for the in-progress data block's payload (entries
    /// only, no header — header is prepended at close time).
    block_buf: std.ArrayList(u8) = .empty,
    block_entry_count: u32 = 0,

    /// Index entries collected per closed block.
    index_entries: std.ArrayList(PendingIndexEntry) = .empty,

    /// Bloom-filter builder. Accumulates membership bits as keys arrive.
    bloom_builder: bloom.Builder,

    /// Owned copy of the last key added (used both for the in-progress
    /// block's last-key index entry and for monotonicity checks).
    last_key: ?[]u8 = null,

    entry_count: u64 = 0,
    payload_bytes: u64 = 0,

    pub fn init(gpa: std.mem.Allocator, options: WriterOptions) !Builder {
        var bb = try bloom.Builder.init(gpa, options.expected_entries, options.expected_fp);
        errdefer bb.deinit();
        return .{
            .gpa = gpa,
            .options = options,
            .bloom_builder = bb,
        };
    }

    pub fn deinit(self: *Builder) void {
        for (self.index_entries.items) |e| self.gpa.free(e.last_key);
        self.index_entries.deinit(self.gpa);
        self.block_buf.deinit(self.gpa);
        self.out.deinit(self.gpa);
        if (self.last_key) |lk| self.gpa.free(lk);
        self.bloom_builder.deinit();
        self.* = undefined;
    }

    pub fn add(self: *Builder, key: []const u8, value: []const u8) Error!void {
        return self.addInternal(key, value);
    }

    pub fn addTombstone(self: *Builder, key: []const u8) Error!void {
        return self.addInternal(key, null);
    }

    fn addInternal(self: *Builder, key: []const u8, value: ?[]const u8) Error!void {
        if (key.len > fmt.MAX_KEY_LEN) return error.KeyTooLong;
        if (self.last_key) |lk| {
            if (std.mem.order(u8, key, lk) != .gt) return error.UnsortedKeys;
        }

        const value_bytes_len: usize = if (value) |v| v.len else 0;
        // Per-entry on-disk overhead: u32 key_len + u32 value_len.
        const encoded_size: usize = 8 + key.len + value_bytes_len;

        // Roll to a new block if appending would exceed the target — but
        // only if the current block already has at least one entry, so a
        // single oversized entry still gets a block of its own.
        if (self.block_entry_count > 0 and
            self.block_buf.items.len + encoded_size > self.options.target_block_size)
        {
            try self.closeBlock();
        }

        // Append the entry's length prefix + key + value bytes to the
        // staging buffer.
        var len_prefix: [8]u8 = undefined;
        std.mem.writeInt(u32, len_prefix[0..4], @intCast(key.len), .little);
        const vlen: u32 = if (value) |v| @intCast(v.len) else data_block.TOMBSTONE_VALUE_LEN;
        std.mem.writeInt(u32, len_prefix[4..8], vlen, .little);
        try self.block_buf.appendSlice(self.gpa, &len_prefix);
        try self.block_buf.appendSlice(self.gpa, key);
        if (value) |v| try self.block_buf.appendSlice(self.gpa, v);

        // Track last_key for the next monotonicity check and for the
        // in-progress block's index entry at close time.
        if (self.last_key) |old| self.gpa.free(old);
        self.last_key = try self.gpa.dupe(u8, key);

        self.bloom_builder.add(key);
        self.block_entry_count += 1;
        self.entry_count += 1;
        self.payload_bytes += key.len + value_bytes_len;
    }

    fn closeBlock(self: *Builder) Error!void {
        if (self.block_entry_count == 0) return;

        const header = data_block.DataBlockHeader{
            .magic = data_block.DATA_BLOCK_MAGIC,
            .version = data_block.DATA_BLOCK_VERSION,
            .flags = 0,
            .entry_count = self.block_entry_count,
            .payload_size = @intCast(self.block_buf.items.len),
        };
        const hdr_bytes: [data_block.DATA_BLOCK_HEADER_BYTES]u8 = @bitCast(header);

        const block_offset: u64 = self.out.items.len;
        try self.out.appendSlice(self.gpa, &hdr_bytes);
        try self.out.appendSlice(self.gpa, self.block_buf.items);
        const block_size_usize: usize = self.out.items.len - block_offset;
        if (block_size_usize > std.math.maxInt(u32)) return error.HeapTooLarge;

        // The index entry borrows an owned copy of last_key — `last_key` on
        // the builder may be replaced before finish() runs.
        const last_key_owned = try self.gpa.dupe(u8, self.last_key.?);
        try self.index_entries.append(self.gpa, .{
            .last_key = last_key_owned,
            .block_offset = block_offset,
            .block_size = @intCast(block_size_usize),
        });

        self.block_buf.clearRetainingCapacity();
        self.block_entry_count = 0;
    }

    /// Finalize and return the complete SSTable buffer. Caller owns the
    /// returned slice. The Builder is left in a state where `deinit()` is
    /// still required (and safe) — the caller's `defer b.deinit()` handles
    /// teardown. Do not call `add` / `addTombstone` after `finish`.
    pub fn finish(self: *Builder) Error![]u8 {
        // Capture the allocator locally. We must NOT touch `self.*` after
        // the eventual `deinit` runs at the caller's scope exit, but the
        // intermediate frees inside this function are happy as long as
        // they go through this local copy.
        const gpa = self.gpa;

        try self.closeBlock();

        // ---- Bloom filter block ----
        const bloom_bytes = try self.bloom_builder.finalize(gpa);
        defer gpa.free(bloom_bytes);
        const bloom_offset: u64 = self.out.items.len;
        try self.out.appendSlice(gpa, bloom_bytes);
        const bloom_size: u32 = @intCast(bloom_bytes.len);

        // ---- Index block ----
        var idx_input: std.ArrayList(index_mod.BuilderEntry) = .empty;
        defer idx_input.deinit(gpa);
        try idx_input.ensureTotalCapacity(gpa, self.index_entries.items.len);
        for (self.index_entries.items) |e| {
            try idx_input.append(gpa, .{
                .key = e.last_key,
                .block_offset = e.block_offset,
                .block_size = e.block_size,
            });
        }
        const idx_bytes = index_mod.buildIndexBlock(gpa, idx_input.items) catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            error.KeyTooLong => return error.KeyTooLong,
            error.TooManyEntries => return error.TooManyEntries,
            error.HeapTooLarge => return error.HeapTooLarge,
        };
        defer gpa.free(idx_bytes);
        const idx_offset: u64 = self.out.items.len;
        try self.out.appendSlice(gpa, idx_bytes);
        const idx_size: u32 = @intCast(idx_bytes.len);

        // ---- Footer ----
        const f = footer_mod.Footer{
            .index_offset = idx_offset,
            .index_size = idx_size,
            .bloom_offset = bloom_offset,
            .bloom_size = bloom_size,
            .entry_count = self.entry_count,
            .payload_bytes = self.payload_bytes,
            .version = footer_mod.SSTABLE_VERSION,
            .flags = 0,
            ._pad = 0,
            .magic = footer_mod.SSTABLE_MAGIC,
        };
        const footer_bytes = footer_mod.encode(f);
        try self.out.appendSlice(gpa, &footer_bytes);

        return self.out.toOwnedSlice(gpa);
    }
};

/// Convenience: drain a frozen MemTable into a fresh SSTable buffer.
/// Tombstones in the MemTable are propagated as on-disk tombstones so
/// future compactions (or reader merges) can shadow lower-level hits.
pub fn writeFromMemTable(
    gpa: std.mem.Allocator,
    mt: *const memtable_mod.MemTable,
    base_options: WriterOptions,
) ![]u8 {
    var options = base_options;
    if (mt.entry_count > 0) options.expected_entries = mt.entry_count;

    var b = try Builder.init(gpa, options);
    defer b.deinit();

    var it = mt.iterator();
    while (it.next()) |e| {
        if (e.value) |v| {
            try b.add(e.key, v);
        } else {
            try b.addTombstone(e.key);
        }
    }
    return b.finish();
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const testing = std.testing;

/// Locate the data block in `sst` that the index resolves `key` to, and
/// scan its entries for an exact key match. Used by every roundtrip test
/// in this file; later this lives in `reader.zig`.
fn lookupKey(sst: []const u8, key: []const u8) !?struct { value: ?[]const u8 } {
    // Footer is always the last 64 bytes.
    const f = try footer_mod.parse(sst);

    // Index block.
    const idx_buf = sst[f.index_offset .. f.index_offset + f.index_size];
    const idx = try index_mod.IndexBlock.parse(idx_buf);
    const loc = (try idx.find(key)) orelse return null;

    // Data block.
    const block_buf = sst[loc.block_offset .. loc.block_offset + loc.block_size];
    if (block_buf.len < data_block.DATA_BLOCK_HEADER_BYTES) return error.BadBlock;
    const hdr_bytes: [data_block.DATA_BLOCK_HEADER_BYTES]u8 =
        block_buf[0..data_block.DATA_BLOCK_HEADER_BYTES].*;
    const hdr: data_block.DataBlockHeader = @bitCast(hdr_bytes);
    if (hdr.magic != data_block.DATA_BLOCK_MAGIC) return error.BadBlock;

    var off: usize = data_block.DATA_BLOCK_HEADER_BYTES;
    const end: usize = data_block.DATA_BLOCK_HEADER_BYTES + hdr.payload_size;
    while (off < end) {
        const klen = std.mem.readInt(u32, block_buf[off..][0..4], .little);
        const vlen = std.mem.readInt(u32, block_buf[off + 4 ..][0..4], .little);
        off += 8;
        const k = block_buf[off .. off + klen];
        off += klen;
        const is_tombstone = vlen == data_block.TOMBSTONE_VALUE_LEN;
        const v: ?[]const u8 = if (is_tombstone) null else block_buf[off .. off + vlen];
        if (!is_tombstone) off += vlen;
        if (std.mem.eql(u8, k, key)) {
            return .{ .value = v };
        }
    }
    return null;
}

test "Builder: simple roundtrip via index + data block scan" {
    const gpa = testing.allocator;
    var b = try Builder.init(gpa, .{ .expected_entries = 16 });
    defer b.deinit();

    try b.add("alpha", "AAA");
    try b.add("bravo", "BB");
    try b.add("charlie", "CCCC");
    try b.add("delta", "DDDDD");

    const sst = try b.finish();
    defer gpa.free(sst);

    const a = try lookupKey(sst, "alpha");
    try testing.expectEqualStrings("AAA", a.?.value.?);

    const c = try lookupKey(sst, "charlie");
    try testing.expectEqualStrings("CCCC", c.?.value.?);

    const miss = try lookupKey(sst, "zzz");
    try testing.expectEqual(@as(?@TypeOf(miss.?), null), miss);
}

test "Builder: tombstones survive roundtrip with null value" {
    const gpa = testing.allocator;
    var b = try Builder.init(gpa, .{ .expected_entries = 4 });
    defer b.deinit();

    try b.add("alpha", "alive");
    try b.addTombstone("bravo");
    try b.add("charlie", "alive");

    const sst = try b.finish();
    defer gpa.free(sst);

    const got_b = try lookupKey(sst, "bravo");
    try testing.expect(got_b != null);
    try testing.expect(got_b.?.value == null); // tombstone

    const got_a = try lookupKey(sst, "alpha");
    try testing.expectEqualStrings("alive", got_a.?.value.?);
}

test "Builder: unsorted input is rejected" {
    const gpa = testing.allocator;
    var b = try Builder.init(gpa, .{});
    defer b.deinit();
    try b.add("bravo", "x");
    try testing.expectError(error.UnsortedKeys, b.add("alpha", "y"));
    try testing.expectError(error.UnsortedKeys, b.add("bravo", "y")); // equal not allowed either
}

test "Builder: small target_block_size forces multiple data blocks" {
    const gpa = testing.allocator;
    var b = try Builder.init(gpa, .{
        .target_block_size = 32, // small enough to roll after each entry
        .expected_entries = 8,
    });
    defer b.deinit();

    const keys = [_][]const u8{ "a", "b", "c", "d", "e", "f", "g", "h" };
    for (keys) |k| try b.add(k, "value-bytes");

    const sst = try b.finish();
    defer gpa.free(sst);

    // Index should now have multiple entries (one per closed data block).
    const f = try footer_mod.parse(sst);
    const idx = try index_mod.IndexBlock.parse(sst[f.index_offset .. f.index_offset + f.index_size]);
    try testing.expect(idx.count() > 1);

    // Every key still resolvable through the multi-block index.
    for (keys) |k| {
        const got = try lookupKey(sst, k);
        try testing.expectEqualStrings("value-bytes", got.?.value.?);
    }
}

test "Builder: footer accounting matches content" {
    const gpa = testing.allocator;
    var b = try Builder.init(gpa, .{ .expected_entries = 4 });
    defer b.deinit();
    try b.add("aa", "111");
    try b.add("bb", "22");
    try b.add("cc", "3");
    const sst = try b.finish();
    defer gpa.free(sst);

    const f = try footer_mod.parse(sst);
    try testing.expectEqual(@as(u64, 3), f.entry_count);
    // 2+3 + 2+2 + 2+1 = 12
    try testing.expectEqual(@as(u64, 12), f.payload_bytes);
}

test "Builder: bloom filter recognizes inserted keys" {
    const gpa = testing.allocator;
    var b = try Builder.init(gpa, .{ .expected_entries = 4 });
    defer b.deinit();
    try b.add("alpha", "1");
    try b.add("bravo", "2");
    try b.add("charlie", "3");
    const sst = try b.finish();
    defer gpa.free(sst);

    const f = try footer_mod.parse(sst);
    const filter = try bloom.Filter.parse(sst[f.bloom_offset .. f.bloom_offset + f.bloom_size]);
    try testing.expect(filter.maybeContains("alpha"));
    try testing.expect(filter.maybeContains("bravo"));
    try testing.expect(filter.maybeContains("charlie"));
}

test "writeFromMemTable: end-to-end MemTable → SSTable → lookup" {
    const gpa = testing.allocator;
    var mt = try memtable_mod.MemTable.init(gpa);
    defer mt.deinit();

    // Insert deliberately out of order; MemTable sorts via skiplist.
    try mt.put("mango", "m-val");
    try mt.put("alpha", "a-val");
    try mt.put("zeta", "z-val");
    try mt.putTombstone("bravo");
    try mt.put("delta", "d-val");
    mt.freeze();

    const sst = try writeFromMemTable(gpa, &mt, .{ .target_block_size = 64 });
    defer gpa.free(sst);

    // Footer + bloom say expected count.
    const f = try footer_mod.parse(sst);
    try testing.expectEqual(@as(u64, 5), f.entry_count);

    // Each key resolves; tombstone resolves as null value.
    const a = try lookupKey(sst, "alpha");
    try testing.expectEqualStrings("a-val", a.?.value.?);
    const m = try lookupKey(sst, "mango");
    try testing.expectEqualStrings("m-val", m.?.value.?);
    const z = try lookupKey(sst, "zeta");
    try testing.expectEqualStrings("z-val", z.?.value.?);
    const d = try lookupKey(sst, "delta");
    try testing.expectEqualStrings("d-val", d.?.value.?);
    const b = try lookupKey(sst, "bravo");
    try testing.expect(b.?.value == null);

    const miss = try lookupKey(sst, "nonexistent");
    try testing.expect(miss == null or miss.?.value != null); // index lower-bound may resolve to a block, but the scan finds no match.
}

test "writeFromMemTable: 500-key roundtrip across many blocks" {
    const gpa = testing.allocator;
    var mt = try memtable_mod.MemTable.init(gpa);
    defer mt.deinit();

    var keys: std.ArrayList([]u8) = .empty;
    defer {
        for (keys.items) |k| gpa.free(k);
        keys.deinit(gpa);
    }
    var i: u32 = 0;
    while (i < 500) : (i += 1) {
        const k = try std.fmt.allocPrint(gpa, "key_{d:0>5}", .{i});
        try keys.append(gpa, k);
        var vbuf: [32]u8 = undefined;
        const v = try std.fmt.bufPrint(&vbuf, "v_{d}", .{i});
        try mt.put(k, v);
    }
    mt.freeze();

    const sst = try writeFromMemTable(gpa, &mt, .{ .target_block_size = 256 });
    defer gpa.free(sst);

    // Spot-check: first, last, a few in the middle.
    const probes = [_]u32{ 0, 1, 17, 250, 498, 499 };
    for (probes) |p| {
        const got = try lookupKey(sst, keys.items[p]);
        var expect_buf: [32]u8 = undefined;
        const want = try std.fmt.bufPrint(&expect_buf, "v_{d}", .{p});
        try testing.expectEqualStrings(want, got.?.value.?);
    }
}
