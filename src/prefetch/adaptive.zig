//! Adaptive prefetcher.
//!
//! Tracks per-request key access sequences and learns co-occurrence: if key
//! A is consistently followed by key B within the same request window, the
//! reader extends the next Range fetch to also cover the byte span around B.
//! Reduces GCS round-trips for sequential / clustered access patterns.
//!
//! Stub: signatures only.

const std = @import("std");

pub const Tracker = struct {
    gpa: std.mem.Allocator,

    pub fn init(gpa: std.mem.Allocator) Tracker {
        return .{ .gpa = gpa };
    }

    pub fn deinit(self: *Tracker) void {
        _ = self;
    }

    /// Record that `key` was accessed in the current request window. Returns
    /// the suggested co-fetch span (or null if none).
    pub fn observe(self: *Tracker, key: []const u8) ?struct { offset: u64, len: u32 } {
        _ = self;
        _ = key;
        return null;
    }
};

test "tracker type compiles" {
    _ = Tracker;
}
