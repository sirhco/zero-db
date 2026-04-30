//! Generic LRU cache: doubly-linked list + hash map.
//!
//! Hot items at the head, cold items at the tail. Eviction pops the tail
//! when capacity is reached. Used by `block_cache.zig` for parsed
//! IndexBlocks and decompressed data blocks.
//!
//! Stub: signatures only.

const std = @import("std");

pub fn LRU(comptime K: type, comptime V: type) type {
    return struct {
        const Self = @This();

        gpa: std.mem.Allocator,
        capacity: usize,

        pub fn init(gpa: std.mem.Allocator, capacity: usize) Self {
            return .{ .gpa = gpa, .capacity = capacity };
        }

        pub fn deinit(self: *Self) void {
            _ = self;
        }

        pub fn get(self: *Self, key: K) ?V {
            _ = self;
            _ = key;
            return null;
        }

        pub fn put(self: *Self, key: K, value: V) !void {
            _ = self;
            _ = key;
            _ = value;
        }
    };
}

test "lru instantiates" {
    _ = LRU(u64, []const u8);
}
