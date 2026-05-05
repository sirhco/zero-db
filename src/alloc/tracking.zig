//! Allocator wrapper that accounts for resident bytes.
//!
//! Wraps a child allocator and tracks the live allocation total against a
//! configured ceiling. Lets the engine refuse to admit new cache entries
//! or MemTable inserts when an additional allocation would exceed the
//! Cloud Run memory limit (default 512 MiB).
//!
//! Tracking is byte-exact for `alloc`, `free`, and successful `resize` /
//! `remap` calls. Allocation requests that would drive `live > ceiling`
//! are refused at the wrapper layer (returns null) — the child allocator
//! is never asked. That preserves `live` invariants even when the child
//! is a thin malloc-style allocator that does not track itself.

const std = @import("std");

pub const TrackingAllocator = struct {
    child: std.mem.Allocator,
    ceiling: usize,
    live: usize = 0,
    /// Counter of allocations refused because they would exceed `ceiling`.
    /// Useful for telemetry / per-tenant logging.
    refused_count: u64 = 0,

    pub fn init(child: std.mem.Allocator, ceiling: usize) TrackingAllocator {
        return .{ .child = child, .ceiling = ceiling };
    }

    /// Returns the std.mem.Allocator wrapper for use anywhere a normal
    /// allocator is accepted. Lifetime: borrowed; caller keeps the
    /// `TrackingAllocator` alive.
    pub fn allocator(self: *TrackingAllocator) std.mem.Allocator {
        return .{
            .ptr = @ptrCast(self),
            .vtable = &vtable,
        };
    }

    pub fn liveBytes(self: *const TrackingAllocator) usize {
        return self.live;
    }

    pub fn refusedCount(self: *const TrackingAllocator) u64 {
        return self.refused_count;
    }

    pub fn isOverBudget(self: *const TrackingAllocator) bool {
        return self.live >= self.ceiling;
    }

    const vtable: std.mem.Allocator.VTable = .{
        .alloc = allocImpl,
        .resize = resizeImpl,
        .remap = remapImpl,
        .free = freeImpl,
    };

    fn allocImpl(ptr: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *TrackingAllocator = @ptrCast(@alignCast(ptr));
        if (self.live + len > self.ceiling) {
            self.refused_count += 1;
            return null;
        }
        const out = self.child.vtable.alloc(self.child.ptr, len, alignment, ret_addr) orelse {
            return null;
        };
        self.live += len;
        return out;
    }

    fn resizeImpl(ptr: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
        const self: *TrackingAllocator = @ptrCast(@alignCast(ptr));
        if (new_len > memory.len) {
            const delta = new_len - memory.len;
            if (self.live + delta > self.ceiling) {
                self.refused_count += 1;
                return false;
            }
        }
        const ok = self.child.vtable.resize(self.child.ptr, memory, alignment, new_len, ret_addr);
        if (!ok) return false;
        if (new_len >= memory.len) {
            self.live += new_len - memory.len;
        } else {
            self.live -= memory.len - new_len;
        }
        return true;
    }

    fn remapImpl(ptr: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
        const self: *TrackingAllocator = @ptrCast(@alignCast(ptr));
        if (new_len > memory.len) {
            const delta = new_len - memory.len;
            if (self.live + delta > self.ceiling) {
                self.refused_count += 1;
                return null;
            }
        }
        const out = self.child.vtable.remap(self.child.ptr, memory, alignment, new_len, ret_addr) orelse return null;
        if (new_len >= memory.len) {
            self.live += new_len - memory.len;
        } else {
            self.live -= memory.len - new_len;
        }
        return out;
    }

    fn freeImpl(ptr: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *TrackingAllocator = @ptrCast(@alignCast(ptr));
        self.child.vtable.free(self.child.ptr, memory, alignment, ret_addr);
        // Defensive guard: if freed bytes exceed `live`, clamp to zero
        // rather than underflow. Should never happen if accounting is
        // consistent, but a stray double-free should not corrupt counters.
        if (memory.len > self.live) {
            self.live = 0;
        } else {
            self.live -= memory.len;
        }
    }
};

const testing = std.testing;

test "TrackingAllocator: alloc + free updates live byte count" {
    var ta = TrackingAllocator.init(testing.allocator, 1024);
    const a = ta.allocator();

    try testing.expectEqual(@as(usize, 0), ta.liveBytes());

    const buf = try a.alloc(u8, 100);
    try testing.expectEqual(@as(usize, 100), ta.liveBytes());

    const buf2 = try a.alloc(u8, 200);
    try testing.expectEqual(@as(usize, 300), ta.liveBytes());

    a.free(buf);
    try testing.expectEqual(@as(usize, 200), ta.liveBytes());

    a.free(buf2);
    try testing.expectEqual(@as(usize, 0), ta.liveBytes());
}

test "TrackingAllocator: refuses allocation that would exceed ceiling" {
    var ta = TrackingAllocator.init(testing.allocator, 256);
    const a = ta.allocator();

    const ok = try a.alloc(u8, 200);
    defer a.free(ok);
    try testing.expectEqual(@as(usize, 200), ta.liveBytes());

    try testing.expectError(error.OutOfMemory, a.alloc(u8, 100));
    try testing.expectEqual(@as(u64, 1), ta.refusedCount());
    try testing.expectEqual(@as(usize, 200), ta.liveBytes());
}

test "TrackingAllocator: isOverBudget tracks ceiling crossings" {
    var ta = TrackingAllocator.init(testing.allocator, 100);
    const a = ta.allocator();

    try testing.expect(!ta.isOverBudget());
    const buf = try a.alloc(u8, 100);
    defer a.free(buf);
    try testing.expect(ta.isOverBudget());
}

test "TrackingAllocator: resize down reclaims bytes" {
    var ta = TrackingAllocator.init(testing.allocator, 1024);
    const a = ta.allocator();

    var list: std.ArrayList(u8) = .empty;
    defer list.deinit(a);
    try list.appendSlice(a, "abcdefghij" ** 10);
    try testing.expectEqual(@as(usize, 100), list.items.len);
    const before = ta.liveBytes();

    list.shrinkAndFree(a, 10);
    try testing.expect(ta.liveBytes() <= before);
}

test "TrackingAllocator: ceiling counts every allocation, refused or not" {
    var ta = TrackingAllocator.init(testing.allocator, 64);
    const a = ta.allocator();

    const small = try a.alloc(u8, 32);
    defer a.free(small);

    try testing.expectError(error.OutOfMemory, a.alloc(u8, 64));
    try testing.expectError(error.OutOfMemory, a.alloc(u8, 64));
    try testing.expectEqual(@as(u64, 2), ta.refusedCount());

    const fits = try a.alloc(u8, 32);
    defer a.free(fits);
    try testing.expectEqual(@as(usize, 64), ta.liveBytes());
}
