//! Bytes-bounded block cache for SSTable index/bloom/data blocks.
//!
//! Sits in front of the SSTable Reader so repeated lookups within a
//! popular SSTable — and across multiple Readers in the same Engine —
//! avoid both the GCS round trip and the parse step.
//!
//! Keyed by `(sstable_id, kind, offset)` so the same offset within
//! different block types (an index block and a data block can share an
//! offset across SSTables, or even within one SSTable across kinds) does
//! not collide.
//!
//! Eviction policy: LRU on byte cost. Each `put` evicts the least
//! recently used entries until the new value fits, or rejects with
//! `error.OverCapacity` when a single block exceeds the configured
//! ceiling.

const std = @import("std");
const lru_mod = @import("lru.zig");

pub const Error = error{
    OutOfMemory,
    OverCapacity,
};

pub const Kind = enum(u8) { index, bloom, data };

pub const Key = struct {
    sstable_id: u64,
    kind: Kind,
    offset: u64,
};

pub const BlockCache = struct {
    gpa: std.mem.Allocator,
    inner: lru_mod.LRU(Key, []u8),
    byte_capacity: usize,
    byte_used: usize = 0,

    pub fn init(gpa: std.mem.Allocator, byte_capacity: usize) BlockCache {
        std.debug.assert(byte_capacity > 0);
        return .{
            .gpa = gpa,
            .inner = lru_mod.LRU(Key, []u8).init(gpa, std.math.maxInt(usize)),
            .byte_capacity = byte_capacity,
        };
    }

    pub fn deinit(self: *BlockCache) void {
        // Free every cached value's heap bytes before tearing down the
        // LRU's nodes.
        var it = self.inner.map.valueIterator();
        while (it.next()) |node_ptr| {
            self.gpa.free(node_ptr.*.value);
        }
        self.inner.deinit();
        self.* = undefined;
    }

    /// Returns a borrowed slice of the cached bytes on hit, `null` on
    /// miss. The slice lifetime is the next `put` (which may evict) or
    /// `deinit`. Caller must NOT free.
    pub fn get(self: *BlockCache, key: Key) ?[]const u8 {
        return self.inner.get(key);
    }

    /// Insert (or overwrite) `key`'s value. The cache copies `bytes` into
    /// its own allocation. Evicts LRU entries until the new value fits;
    /// rejects with `OverCapacity` when a single block exceeds the
    /// configured ceiling.
    pub fn put(self: *BlockCache, key: Key, bytes: []const u8) Error!void {
        if (bytes.len > self.byte_capacity) return error.OverCapacity;

        // Overwrite path: free the old value first so the byte counter is
        // accurate before the eviction loop runs.
        if (self.inner.get(key)) |old| {
            self.byte_used -= old.len;
            self.gpa.free(old);
            _ = self.inner.remove(key);
        }

        // Evict until the new entry fits under the ceiling.
        while (self.byte_used + bytes.len > self.byte_capacity) {
            const ev = self.inner.popLru() orelse break;
            self.byte_used -= ev.value.len;
            self.gpa.free(ev.value);
        }

        const owned = self.gpa.dupe(u8, bytes) catch return error.OutOfMemory;
        errdefer self.gpa.free(owned);
        _ = self.inner.put(key, owned) catch return error.OutOfMemory;
        self.byte_used += owned.len;
    }

    pub fn count(self: *const BlockCache) usize {
        return self.inner.count();
    }

    pub fn bytesUsed(self: *const BlockCache) usize {
        return self.byte_used;
    }
};

const testing = std.testing;

test "BlockCache: hit and miss accounting" {
    const gpa = testing.allocator;
    var bc = BlockCache.init(gpa, 1024);
    defer bc.deinit();

    const k = Key{ .sstable_id = 1, .kind = .data, .offset = 0 };
    try testing.expectEqual(@as(?[]const u8, null), bc.get(k));

    try bc.put(k, "hello");
    const got = bc.get(k).?;
    try testing.expectEqualStrings("hello", got);
    try testing.expectEqual(@as(usize, 5), bc.bytesUsed());
    try testing.expectEqual(@as(usize, 1), bc.count());
}

test "BlockCache: kind disambiguates same offset" {
    const gpa = testing.allocator;
    var bc = BlockCache.init(gpa, 1024);
    defer bc.deinit();

    const k_index = Key{ .sstable_id = 1, .kind = .index, .offset = 100 };
    const k_bloom = Key{ .sstable_id = 1, .kind = .bloom, .offset = 100 };
    try bc.put(k_index, "idx");
    try bc.put(k_bloom, "blm");

    try testing.expectEqualStrings("idx", bc.get(k_index).?);
    try testing.expectEqualStrings("blm", bc.get(k_bloom).?);
    try testing.expectEqual(@as(usize, 6), bc.bytesUsed());
}

test "BlockCache: evicts LRU when byte ceiling exceeded" {
    const gpa = testing.allocator;
    var bc = BlockCache.init(gpa, 1500);
    defer bc.deinit();

    var big: [600]u8 = undefined;
    @memset(&big, 'a');

    try bc.put(.{ .sstable_id = 1, .kind = .data, .offset = 0 }, &big);
    try bc.put(.{ .sstable_id = 1, .kind = .data, .offset = 1 }, &big);
    // Two 600-byte entries fit under the 1500 ceiling. A third forces
    // eviction of the LRU one (offset=0).
    try testing.expectEqual(@as(usize, 1200), bc.bytesUsed());

    try bc.put(.{ .sstable_id = 1, .kind = .data, .offset = 2 }, &big);
    try testing.expectEqual(@as(usize, 1200), bc.bytesUsed());

    try testing.expectEqual(@as(?[]const u8, null), bc.get(.{ .sstable_id = 1, .kind = .data, .offset = 0 }));
    try testing.expect(bc.get(.{ .sstable_id = 1, .kind = .data, .offset = 1 }) != null);
    try testing.expect(bc.get(.{ .sstable_id = 1, .kind = .data, .offset = 2 }) != null);
}

test "BlockCache: rejects a single block larger than the ceiling" {
    const gpa = testing.allocator;
    var bc = BlockCache.init(gpa, 64);
    defer bc.deinit();

    var big: [128]u8 = undefined;
    @memset(&big, 'x');

    try testing.expectError(
        error.OverCapacity,
        bc.put(.{ .sstable_id = 1, .kind = .data, .offset = 0 }, &big),
    );
    try testing.expectEqual(@as(usize, 0), bc.bytesUsed());
}

test "BlockCache: overwrite frees the old value (no leak)" {
    const gpa = testing.allocator;
    var bc = BlockCache.init(gpa, 1024);
    defer bc.deinit();

    const k = Key{ .sstable_id = 7, .kind = .index, .offset = 9 };
    try bc.put(k, "first-value");
    try bc.put(k, "second-value-longer");
    try testing.expectEqualStrings("second-value-longer", bc.get(k).?);
    try testing.expectEqual(@as(usize, 1), bc.count());
    try testing.expectEqual(@as(usize, "second-value-longer".len), bc.bytesUsed());
}

test "BlockCache: get promotes hit to MRU so it survives next eviction" {
    const gpa = testing.allocator;
    var bc = BlockCache.init(gpa, 600);
    defer bc.deinit();

    var v: [200]u8 = undefined;
    @memset(&v, 'q');

    const k1 = Key{ .sstable_id = 1, .kind = .data, .offset = 0 };
    const k2 = Key{ .sstable_id = 1, .kind = .data, .offset = 1 };
    const k3 = Key{ .sstable_id = 1, .kind = .data, .offset = 2 };
    try bc.put(k1, &v);
    try bc.put(k2, &v);
    try bc.put(k3, &v);

    // Touch k1 — it becomes MRU. The next insert evicts the next-LRU (k2).
    _ = bc.get(k1).?;

    const k4 = Key{ .sstable_id = 1, .kind = .data, .offset = 3 };
    try bc.put(k4, &v);

    try testing.expect(bc.get(k1) != null);
    try testing.expectEqual(@as(?[]const u8, null), bc.get(k2));
    try testing.expect(bc.get(k3) != null);
    try testing.expect(bc.get(k4) != null);
}
