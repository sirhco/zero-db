//! Per-request FixedBufferAllocator pool.
//!
//! Cloud Run requests are short-lived and bounded; each request leases one
//! pre-allocated arena, allocates freely from it, and resets the arena at
//! request end. This sidesteps general-purpose-allocator overhead on the hot
//! path and matches the "GC-free latency" property the project is built on.
//!
//! Stub: signatures only.

const std = @import("std");

pub const ArenaPool = struct {
    gpa: std.mem.Allocator,
    per_request_bytes: usize,

    pub fn init(gpa: std.mem.Allocator, per_request_bytes: usize) ArenaPool {
        return .{ .gpa = gpa, .per_request_bytes = per_request_bytes };
    }

    pub fn deinit(self: *ArenaPool) void {
        _ = self;
    }
};

test "arena pool type compiles" {
    _ = ArenaPool;
}
