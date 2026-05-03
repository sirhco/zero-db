//! Allocator wrapper that accounts for resident bytes.
//!
//! Wraps a child allocator and tracks live allocation total against a
//! configured ceiling. Exists so the engine can refuse to admit new
//! cache entries / merge tasks when it would exceed the Cloud Run memory
//! limit (default 512 MB).
//!
//! Stub: signatures only.

const std = @import("std");

pub const TrackingAllocator = struct {
    child: std.mem.Allocator,
    live: usize = 0,
    ceiling: usize,

    pub fn init(child: std.mem.Allocator, ceiling: usize) TrackingAllocator {
        return .{ .child = child, .ceiling = ceiling };
    }
};

test "tracking allocator type compiles" {
    _ = TrackingAllocator;
}
