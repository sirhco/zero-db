//! In-memory write buffer for the LSM write path.
//!
//! Backed by a probabilistic skip list keyed by raw bytes. Values can be
//! data bytes or a tombstone (null), recording deletes that must shadow any
//! lower-level SSTable hit until compaction merges them away.
//!
//! Lifetime: every node, key, and value is allocated from a single arena
//! owned by the MemTable. `deinit` drops the whole arena in one shot —
//! ideal for the LSM model where a frozen MemTable is flushed and discarded
//! atomically.
//!
//! Concurrency: not thread-safe. The engine's write path is expected to be
//! single-writer; reads may happen concurrently with writes only against an
//! externally-frozen MemTable (achieved by swapping the active table before
//! handing it to the compaction worker).

const std = @import("std");

/// Maximum tower height. log2(N) gives the practical search cost; 12 levels
/// keeps lookups O(log N) up to ~4096 entries with the standard 0.5 promote
/// probability, and well past that with no correctness loss (just degraded
/// asymptotics until the table flushes).
pub const MAX_LEVEL: u8 = 12;

/// Marker returned by `get` for keys that have a tombstone in this table.
/// Distinct from "key not present"; the engine must NOT fall through to
/// lower SSTable levels on a tombstone hit.
pub const Lookup = union(enum) {
    present: []const u8,
    tombstone,
};

pub const Entry = struct {
    key: []const u8,
    /// null = tombstone.
    value: ?[]const u8,
};

const Node = struct {
    key: []const u8,
    value: ?[]const u8,
    /// Forward pointers per level; only `level` of these are meaningful, but
    /// we always allocate the full array to keep node layout uniform and
    /// cache-friendly. 12 pointers = 96 bytes — small relative to typical
    /// k/v payload sizes.
    next: [MAX_LEVEL]?*Node = .{null} ** MAX_LEVEL,
    level: u8,
};

pub const MemTable = struct {
    arena: std.heap.ArenaAllocator,
    head: *Node,
    height: u8 = 1,
    rng: std.Random.DefaultPrng,
    /// Sum of key + value bytes across all live entries. Drives the flush
    /// threshold check in the engine. Tombstones contribute key bytes only.
    bytes_used: usize = 0,
    entry_count: u32 = 0,
    frozen: bool = false,

    pub fn init(gpa: std.mem.Allocator) !MemTable {
        var arena = std.heap.ArenaAllocator.init(gpa);
        errdefer arena.deinit();
        const a = arena.allocator();

        const head = try a.create(Node);
        head.* = .{ .key = "", .value = null, .level = MAX_LEVEL };

        // Fixed seed: skiplist correctness does not depend on the level
        // distribution being secret or wall-clock-driven, only that promote
        // probabilities approximate 0.5. A deterministic seed also makes
        // skiplist-shape bugs reproducible across test runs.
        return .{
            .arena = arena,
            .head = head,
            .rng = std.Random.DefaultPrng.init(0x9E37_79B9_7F4A_7C15),
        };
    }

    pub fn deinit(self: *MemTable) void {
        self.arena.deinit();
        self.* = undefined;
    }

    /// Mark the table immutable. Subsequent put/putTombstone calls return
    /// error.Frozen. Reads continue to work; the engine hands a frozen
    /// table to the compaction worker for flushing.
    pub fn freeze(self: *MemTable) void {
        self.frozen = true;
    }

    pub fn isFrozen(self: *const MemTable) bool {
        return self.frozen;
    }

    pub const PutError = error{
        Frozen,
        OutOfMemory,
    };

    pub fn put(self: *MemTable, key: []const u8, value: []const u8) PutError!void {
        return self.upsert(key, value);
    }

    pub fn putTombstone(self: *MemTable, key: []const u8) PutError!void {
        return self.upsert(key, null);
    }

    fn upsert(self: *MemTable, key: []const u8, value: ?[]const u8) PutError!void {
        if (self.frozen) return error.Frozen;

        var update: [MAX_LEVEL]*Node = undefined;
        const existing = self.findPredecessors(key, &update);

        if (existing) |node| {
            // Key match — overwrite. Update bytes_used by the delta of the
            // value (key bytes already counted on first insert).
            const old_value_len: usize = if (node.value) |v| v.len else 0;
            const new_value_len: usize = if (value) |v| v.len else 0;
            self.bytes_used = self.bytes_used + new_value_len - old_value_len;

            const a = self.arena.allocator();
            node.value = if (value) |v| try a.dupe(u8, v) else null;
            return;
        }

        const a = self.arena.allocator();
        const lvl = self.randomLevel();

        const node = try a.create(Node);
        node.* = .{
            .key = try a.dupe(u8, key),
            .value = if (value) |v| try a.dupe(u8, v) else null,
            .level = lvl,
        };

        // Splice into the chain at every level up to the new node's height.
        var i: u8 = 0;
        while (i < lvl) : (i += 1) {
            // Levels above the table's current height all have head as
            // their predecessor; widen the table's effective height.
            const pred = if (i < self.height) update[i] else self.head;
            node.next[i] = pred.next[i];
            pred.next[i] = node;
        }
        if (lvl > self.height) self.height = lvl;

        self.bytes_used += key.len + (if (value) |v| v.len else 0);
        self.entry_count += 1;
    }

    pub fn get(self: *const MemTable, key: []const u8) ?Lookup {
        // Top-down search: at each level, advance while next.key < target.
        var cur: *const Node = self.head;
        var lvl: i32 = @as(i32, self.height) - 1;
        while (lvl >= 0) : (lvl -= 1) {
            const li: usize = @intCast(lvl);
            while (cur.next[li]) |nxt| {
                switch (std.mem.order(u8, nxt.key, key)) {
                    .lt => cur = nxt,
                    .eq => {
                        return if (nxt.value) |v| .{ .present = v } else .tombstone;
                    },
                    .gt => break,
                }
            }
        }
        return null;
    }

    fn findPredecessors(self: *MemTable, key: []const u8, update: *[MAX_LEVEL]*Node) ?*Node {
        var cur: *Node = self.head;
        // Initialize all update slots to head so a level above current height
        // still has a defined predecessor when a new tower extends past it.
        for (update) |*slot| slot.* = self.head;

        var found: ?*Node = null;
        var lvl: i32 = @as(i32, self.height) - 1;
        while (lvl >= 0) : (lvl -= 1) {
            const li: usize = @intCast(lvl);
            while (cur.next[li]) |nxt| {
                switch (std.mem.order(u8, nxt.key, key)) {
                    .lt => cur = nxt,
                    .eq => {
                        update[li] = cur;
                        found = nxt;
                        break;
                    },
                    .gt => break,
                }
            }
            update[li] = cur;
        }
        return found;
    }

    fn randomLevel(self: *MemTable) u8 {
        // Standard 0.5 promote probability. Each successful coin flip lifts
        // the new node one level higher. Capped at MAX_LEVEL.
        var lvl: u8 = 1;
        while (lvl < MAX_LEVEL and self.rng.random().boolean()) lvl += 1;
        return lvl;
    }

    pub const Iterator = struct {
        cur: ?*const Node,

        pub fn next(it: *Iterator) ?Entry {
            const node = it.cur orelse return null;
            it.cur = node.next[0];
            return .{ .key = node.key, .value = node.value };
        }
    };

    /// Iterate level-0 in ascending key order. Skips the head sentinel.
    /// Tombstones are surfaced (value = null); the consumer (SSTable writer
    /// or merge cursor) decides whether to keep them.
    pub fn iterator(self: *const MemTable) Iterator {
        return .{ .cur = self.head.next[0] };
    }
};

const testing = std.testing;

test "empty memtable returns null on get" {
    var mt = try MemTable.init(testing.allocator);
    defer mt.deinit();
    try testing.expectEqual(@as(?Lookup, null), mt.get("anything"));
    try testing.expectEqual(@as(u32, 0), mt.entry_count);
    try testing.expectEqual(@as(usize, 0), mt.bytes_used);
}

test "put + get roundtrip" {
    var mt = try MemTable.init(testing.allocator);
    defer mt.deinit();

    try mt.put("alpha", "AAA");
    try mt.put("bravo", "BB");
    try mt.put("charlie", "CCCC");

    try testing.expectEqualStrings("AAA", mt.get("alpha").?.present);
    try testing.expectEqualStrings("BB", mt.get("bravo").?.present);
    try testing.expectEqualStrings("CCCC", mt.get("charlie").?.present);
    try testing.expectEqual(@as(?Lookup, null), mt.get("delta"));

    try testing.expectEqual(@as(u32, 3), mt.entry_count);
    try testing.expectEqual(@as(usize, "alpha".len + "AAA".len + "bravo".len + "BB".len + "charlie".len + "CCCC".len), mt.bytes_used);
}

test "overwrite existing key updates value and bytes_used" {
    var mt = try MemTable.init(testing.allocator);
    defer mt.deinit();

    try mt.put("k", "short");
    const baseline = mt.bytes_used;

    try mt.put("k", "much-longer-value");
    try testing.expectEqualStrings("much-longer-value", mt.get("k").?.present);
    try testing.expectEqual(baseline + ("much-longer-value".len - "short".len), mt.bytes_used);
    try testing.expectEqual(@as(u32, 1), mt.entry_count);
}

test "tombstone shadows previous value" {
    var mt = try MemTable.init(testing.allocator);
    defer mt.deinit();

    try mt.put("k", "v");
    try mt.putTombstone("k");

    const got = mt.get("k").?;
    try testing.expectEqual(Lookup.tombstone, got);
}

test "tombstone on absent key is recorded" {
    var mt = try MemTable.init(testing.allocator);
    defer mt.deinit();

    try mt.putTombstone("ghost");
    try testing.expectEqual(Lookup.tombstone, mt.get("ghost").?);
    try testing.expectEqual(@as(u32, 1), mt.entry_count);
    try testing.expectEqual(@as(usize, "ghost".len), mt.bytes_used);
}

test "iterator yields keys in ascending order" {
    var mt = try MemTable.init(testing.allocator);
    defer mt.deinit();

    // Insert deliberately out of order.
    const inputs = [_][]const u8{ "mango", "alpha", "zeta", "bravo", "delta" };
    for (inputs) |k| try mt.put(k, k);

    var it = mt.iterator();
    var seen: std.ArrayList([]const u8) = .empty;
    defer seen.deinit(testing.allocator);
    while (it.next()) |e| try seen.append(testing.allocator, e.key);

    const expected = [_][]const u8{ "alpha", "bravo", "delta", "mango", "zeta" };
    try testing.expectEqual(@as(usize, expected.len), seen.items.len);
    for (expected, seen.items) |want, got| {
        try testing.expectEqualStrings(want, got);
    }
}

test "freeze rejects further writes" {
    var mt = try MemTable.init(testing.allocator);
    defer mt.deinit();

    try mt.put("a", "1");
    mt.freeze();
    try testing.expect(mt.isFrozen());

    try testing.expectError(error.Frozen, mt.put("b", "2"));
    try testing.expectError(error.Frozen, mt.putTombstone("a"));

    // Reads still work on a frozen table.
    try testing.expectEqualStrings("1", mt.get("a").?.present);
}

test "stress: 1000 random keys round-trip and iterate sorted" {
    const gpa = testing.allocator;
    var mt = try MemTable.init(gpa);
    defer mt.deinit();

    const N: u32 = 1000;
    var prng = std.Random.DefaultPrng.init(0x1234_5678_DEAD_BEEF);

    var keys: std.ArrayList([]u8) = .empty;
    defer {
        for (keys.items) |k| gpa.free(k);
        keys.deinit(gpa);
    }

    var i: u32 = 0;
    while (i < N) : (i += 1) {
        // Random 8-char hex key — uniform over the keyspace.
        var keybuf: [16]u8 = undefined;
        const r = prng.random().int(u64);
        const k = try std.fmt.bufPrint(&keybuf, "{x:0>16}", .{r});
        const owned = try gpa.dupe(u8, k);
        try keys.append(gpa, owned);
        try mt.put(owned, owned);
    }

    // Every inserted key must be retrievable.
    for (keys.items) |k| {
        const got = mt.get(k).?;
        try testing.expectEqualStrings(k, got.present);
    }

    // Iterator must produce sorted output. Note: keys may have duplicates if
    // the PRNG collided; iteration count == unique count.
    var it = mt.iterator();
    var prev: ?[]const u8 = null;
    var seen_count: u32 = 0;
    while (it.next()) |e| : (seen_count += 1) {
        if (prev) |p| {
            try testing.expect(std.mem.order(u8, p, e.key) == .lt);
        }
        prev = e.key;
    }
    try testing.expectEqual(seen_count, mt.entry_count);
}
