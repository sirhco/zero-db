//! Generic LRU cache: doubly-linked list + AutoHashMap.
//!
//! Head = most-recently-used; tail = least-recently-used. `get` promotes
//! the hit to head; `put` inserts at head and evicts from tail when over
//! capacity. All operations are O(1).
//!
//! K must be a type compatible with `std.AutoHashMap` (integers, enums,
//! fixed-size structs, pointers — not raw byte slices). Callers with
//! byte-string keys should wrap them in a stable handle (e.g. a u64 key id)
//! before inserting.

const std = @import("std");

pub fn LRU(comptime K: type, comptime V: type) type {
    return struct {
        const Self = @This();

        const Node = struct {
            list_node: std.DoublyLinkedList.Node = .{},
            key: K,
            value: V,
        };

        const Map = std.AutoHashMap(K, *Node);

        gpa: std.mem.Allocator,
        capacity: usize,
        list: std.DoublyLinkedList = .{},
        map: Map,

        pub fn init(gpa: std.mem.Allocator, capacity: usize) Self {
            std.debug.assert(capacity > 0);
            return .{
                .gpa = gpa,
                .capacity = capacity,
                .map = Map.init(gpa),
            };
        }

        pub fn deinit(self: *Self) void {
            // Free every node we allocated. The list itself only owns the
            // intrusive pointers — node storage is on the heap.
            var it = self.map.valueIterator();
            while (it.next()) |node_ptr| {
                self.gpa.destroy(node_ptr.*);
            }
            self.map.deinit();
            self.* = undefined;
        }

        pub fn count(self: Self) usize {
            return self.map.count();
        }

        /// Returns the cached value if present, promoting it to MRU on hit.
        pub fn get(self: *Self, key: K) ?V {
            const node = self.map.get(key) orelse return null;
            self.list.remove(&node.list_node);
            self.list.prepend(&node.list_node);
            return node.value;
        }

        /// Insert or overwrite `key`. If insertion exceeds capacity, the
        /// least-recently-used entry is evicted. Returns the evicted entry
        /// (if any) so the caller can free V's contents on its own schedule
        /// — V itself is otherwise stored by value, with no destructor hook.
        pub fn put(self: *Self, key: K, value: V) !?Evicted {
            if (self.map.get(key)) |existing| {
                existing.value = value;
                self.list.remove(&existing.list_node);
                self.list.prepend(&existing.list_node);
                return null;
            }

            const node = try self.gpa.create(Node);
            node.* = .{ .key = key, .value = value };
            try self.map.put(key, node);
            self.list.prepend(&node.list_node);

            if (self.map.count() > self.capacity) {
                return self.evictTail();
            }
            return null;
        }

        /// Explicitly remove `key`. Returns true if it was present.
        pub fn remove(self: *Self, key: K) bool {
            const node = self.map.get(key) orelse return false;
            self.list.remove(&node.list_node);
            _ = self.map.remove(key);
            self.gpa.destroy(node);
            return true;
        }

        pub const Evicted = struct {
            key: K,
            value: V,
        };

        /// Evict the least-recently-used entry and return it. Used by
        /// byte-bounded wrappers (e.g. BlockCache) that need to drop
        /// entries one at a time until under their own ceiling.
        pub fn popLru(self: *Self) ?Evicted {
            return self.evictTail();
        }

        fn evictTail(self: *Self) ?Evicted {
            const tail_node = self.list.pop() orelse return null;
            const node: *Node = @fieldParentPtr("list_node", tail_node);
            const out = Evicted{ .key = node.key, .value = node.value };
            _ = self.map.remove(node.key);
            self.gpa.destroy(node);
            return out;
        }

        // ---- test-only helpers ------------------------------------------------

        /// Walk the list from MRU to LRU, returning the keys in order. For
        /// tests; allocates with the cache's allocator.
        pub fn snapshotKeys(self: *const Self, gpa: std.mem.Allocator) ![]K {
            var out: std.ArrayList(K) = .empty;
            errdefer out.deinit(gpa);
            var cur = self.list.first;
            while (cur) |n| {
                const node: *Node = @fieldParentPtr("list_node", n);
                try out.append(gpa, node.key);
                cur = n.next;
            }
            return out.toOwnedSlice(gpa);
        }
    };
}

const testing = std.testing;

test "LRU(u64, u64): hit, miss, update" {
    var cache = LRU(u64, u64).init(testing.allocator, 4);
    defer cache.deinit();

    try testing.expectEqual(@as(?u64, null), cache.get(1));

    _ = try cache.put(1, 100);
    _ = try cache.put(2, 200);
    try testing.expectEqual(@as(?u64, 100), cache.get(1));
    try testing.expectEqual(@as(?u64, 200), cache.get(2));

    // Overwrite existing key.
    _ = try cache.put(1, 111);
    try testing.expectEqual(@as(?u64, 111), cache.get(1));
    try testing.expectEqual(@as(usize, 2), cache.count());
}

test "LRU eviction order is least-recently-used" {
    const gpa = testing.allocator;
    var cache = LRU(u64, u64).init(gpa, 3);
    defer cache.deinit();

    _ = try cache.put(1, 100);
    _ = try cache.put(2, 200);
    _ = try cache.put(3, 300);

    // Touch 1 → it becomes MRU; expected eviction order is now 2, then 3.
    _ = cache.get(1);

    const evicted_first = try cache.put(4, 400);
    try testing.expect(evicted_first != null);
    try testing.expectEqual(@as(u64, 2), evicted_first.?.key);
    try testing.expectEqual(@as(u64, 200), evicted_first.?.value);

    const evicted_second = try cache.put(5, 500);
    try testing.expectEqual(@as(u64, 3), evicted_second.?.key);

    // Surviving keys: 1, 4, 5 (in MRU order: 5, 4, 1).
    const order = try cache.snapshotKeys(gpa);
    defer gpa.free(order);
    try testing.expectEqualSlices(u64, &[_]u64{ 5, 4, 1 }, order);
}

test "LRU explicit remove" {
    var cache = LRU(u64, u64).init(testing.allocator, 4);
    defer cache.deinit();

    _ = try cache.put(1, 1);
    _ = try cache.put(2, 2);
    try testing.expect(cache.remove(1));
    try testing.expect(!cache.remove(1));
    try testing.expectEqual(@as(?u64, null), cache.get(1));
    try testing.expectEqual(@as(?u64, 2), cache.get(2));
}

test "LRU under capacity does not evict" {
    var cache = LRU(u64, u64).init(testing.allocator, 5);
    defer cache.deinit();

    var i: u64 = 0;
    while (i < 5) : (i += 1) {
        const e = try cache.put(i, i * 10);
        try testing.expect(e == null);
    }
    try testing.expectEqual(@as(usize, 5), cache.count());
}

test "LRU at capacity evicts the oldest each insert" {
    var cache = LRU(u64, u64).init(testing.allocator, 2);
    defer cache.deinit();

    _ = try cache.put(1, 1);
    _ = try cache.put(2, 2);
    const e1 = try cache.put(3, 3);
    try testing.expectEqual(@as(u64, 1), e1.?.key);
    const e2 = try cache.put(4, 4);
    try testing.expectEqual(@as(u64, 2), e2.?.key);
    try testing.expectEqual(@as(?u64, 3), cache.get(3));
    try testing.expectEqual(@as(?u64, 4), cache.get(4));
}
