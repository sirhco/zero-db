//! SSTable writer: builds an immutable SSTable from a frozen MemTable and
//! uploads it to GCS via resumable upload.
//!
//! Layout produced (matches reader expectations):
//!   data blocks → bloom filter → index block → 64-byte footer
//!
//! Stub: signatures only.

const std = @import("std");

pub const Error = error{
    NotImplemented,
};

pub const Writer = struct {
    gpa: std.mem.Allocator,

    pub fn init(gpa: std.mem.Allocator) Writer {
        return .{ .gpa = gpa };
    }

    pub fn deinit(self: *Writer) void {
        _ = self;
    }
};

test "writer type compiles" {
    _ = Writer;
}
