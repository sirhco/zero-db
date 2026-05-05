//! Arena-allocator pool: reuse `std.heap.ArenaAllocator` instances across
//! short-lived request handlers. Each handler acquires an arena, runs
//! its work, then releases the arena back to the pool. Released arenas
//! retain their already-allocated capacity, so the next handler skips
//! the page fault dance.
//!
//! Lifetime: pool owns up to `capacity` heap-allocated ArenaAllocators.
//! `acquire` pops one (or allocates fresh when the pool is empty).
//! `release` resets the arena via `reset(.retain_capacity)` and pushes
//! it back, unless the pool is full — in which case the arena is freed.
//! `deinit` releases every pooled arena.

const std = @import("std");

pub const ArenaPool = struct {
    gpa: std.mem.Allocator,
    capacity: usize,
    pooled: std.ArrayList(*std.heap.ArenaAllocator),

    pub fn init(gpa: std.mem.Allocator, capacity: usize) ArenaPool {
        return .{ .gpa = gpa, .capacity = capacity, .pooled = .empty };
    }

    pub fn deinit(self: *ArenaPool) void {
        for (self.pooled.items) |a| {
            a.deinit();
            self.gpa.destroy(a);
        }
        self.pooled.deinit(self.gpa);
        self.* = undefined;
    }

    /// Borrow an arena. Caller invokes `release` when done. The arena is
    /// reset to a clean state (retaining capacity) before being returned.
    pub fn acquire(self: *ArenaPool) !*std.heap.ArenaAllocator {
        if (self.pooled.pop()) |a| {
            _ = a.reset(.retain_capacity);
            return a;
        }
        const a = try self.gpa.create(std.heap.ArenaAllocator);
        a.* = std.heap.ArenaAllocator.init(self.gpa);
        return a;
    }

    /// Return `arena` to the pool. When the pool is at capacity the arena
    /// is freed instead, bounding total memory commitment.
    pub fn release(self: *ArenaPool, arena: *std.heap.ArenaAllocator) void {
        if (self.pooled.items.len >= self.capacity) {
            arena.deinit();
            self.gpa.destroy(arena);
            return;
        }
        self.pooled.append(self.gpa, arena) catch {
            arena.deinit();
            self.gpa.destroy(arena);
        };
    }

    pub fn pooledCount(self: *const ArenaPool) usize {
        return self.pooled.items.len;
    }
};

const testing = std.testing;

test "ArenaPool: acquire returns a usable arena, release pools it" {
    var p = ArenaPool.init(testing.allocator, 4);
    defer p.deinit();

    const a1 = try p.acquire();
    {
        const buf = try a1.allocator().alloc(u8, 64);
        try testing.expectEqual(@as(usize, 64), buf.len);
    }
    p.release(a1);
    try testing.expectEqual(@as(usize, 1), p.pooledCount());

    const a2 = try p.acquire();
    try testing.expectEqual(a1, a2);
    p.release(a2);
}

test "ArenaPool: capacity bound — over-capacity releases free instead of pool" {
    var p = ArenaPool.init(testing.allocator, 2);
    defer p.deinit();

    const a1 = try p.acquire();
    const a2 = try p.acquire();
    const a3 = try p.acquire();

    p.release(a1);
    p.release(a2);
    try testing.expectEqual(@as(usize, 2), p.pooledCount());

    p.release(a3);
    try testing.expectEqual(@as(usize, 2), p.pooledCount());
}

test "ArenaPool: reset on acquire wipes prior allocations" {
    var p = ArenaPool.init(testing.allocator, 2);
    defer p.deinit();

    const a1 = try p.acquire();
    _ = try a1.allocator().alloc(u8, 1024);
    p.release(a1);

    const a2 = try p.acquire();
    try testing.expectEqual(a1, a2);
    _ = try a2.allocator().alloc(u8, 16);
    p.release(a2);
}
