//! In-memory write buffer for the LSM write path.
//!
//! Backed by a SkipList keyed by raw key bytes; values are stored inline. A
//! MemTable is mutable until it reaches `flush_bytes`, at which point it is
//! frozen (immutable view) and handed to the compaction worker, which writes
//! it out as an SSTable to GCS.
//!
//! Stub: signatures only.

const std = @import("std");

pub const Error = error{
    NotImplemented,
    OutOfMemory,
};

pub const MemTable = struct {
    gpa: std.mem.Allocator,
    bytes_used: usize = 0,

    pub fn init(gpa: std.mem.Allocator) MemTable {
        return .{ .gpa = gpa };
    }

    pub fn deinit(self: *MemTable) void {
        _ = self;
    }

    pub fn put(self: *MemTable, key: []const u8, value: []const u8) Error!void {
        _ = self;
        _ = key;
        _ = value;
        return Error.NotImplemented;
    }

    pub fn get(self: *const MemTable, key: []const u8) ?[]const u8 {
        _ = self;
        _ = key;
        return null;
    }
};

test "memtable type compiles" {
    _ = MemTable;
}
