//! SSTable reader: footer → bloom filter → index → data block.
//!
//! Uses the GCS client to fetch only the bytes needed for a lookup:
//!   1. Tail-Range fetch for the 64-byte footer.
//!   2. Range fetch for the bloom filter; consult before any data fetch.
//!   3. Range fetch for the index block; bsearch via `index.zig`.
//!   4. Range fetch for the resolved data block; scan for the key.
//!
//! Stub: signatures only.

const std = @import("std");
const index = @import("index.zig");
const footer = @import("footer.zig");

pub const Error = error{
    NotImplemented,
};

pub const Reader = struct {
    gpa: std.mem.Allocator,
    bucket: []const u8,
    object: []const u8,

    pub fn init(gpa: std.mem.Allocator, bucket: []const u8, object: []const u8) Reader {
        return .{ .gpa = gpa, .bucket = bucket, .object = object };
    }

    pub fn deinit(self: *Reader) void {
        _ = self;
    }

    pub fn get(self: *Reader, key: []const u8, out_buf: []u8) Error!?[]const u8 {
        _ = self;
        _ = key;
        _ = out_buf;
        return Error.NotImplemented;
    }
};

test "reader type compiles" {
    _ = Reader;
    _ = index;
    _ = footer;
}
