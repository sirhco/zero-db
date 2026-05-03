//! Background flush + level merge worker.
//!
//! Receives frozen MemTables, writes them out as level-0 SSTables, then
//! periodically merges overlapping SSTables across levels to keep read
//! amplification bounded. Coordinates with the SSTable writer and the GCS
//! storage client.
//!
//! Stub: signatures only.

const std = @import("std");

pub const Error = error{
    NotImplemented,
};

pub const Compactor = struct {
    gpa: std.mem.Allocator,

    pub fn init(gpa: std.mem.Allocator) Compactor {
        return .{ .gpa = gpa };
    }

    pub fn deinit(self: *Compactor) void {
        _ = self;
    }

    pub fn flushFrozen(self: *Compactor) Error!void {
        _ = self;
        return Error.NotImplemented;
    }
};

test "compactor type compiles" {
    _ = Compactor;
}
