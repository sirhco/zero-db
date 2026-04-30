//! Top-level Zero-DB Engine.
//!
//! Orchestrates the LSM read/write path: writes go to the in-memory MemTable,
//! reads consult cache → MemTable → SSTable index → GCS data block. The
//! engine owns the long-lived allocator and wires together: MemTable,
//! compaction worker, SSTable reader, block cache, GCS storage client.
//!
//! Stub: types and signatures only. Bodies land in a follow-up pass once
//! every collaborator (MemTable, GCS client, block cache) is implemented.

const std = @import("std");

pub const Error = error{
    NotImplemented,
};

pub const Options = struct {
    /// GCS bucket holding the database's SSTables.
    bucket: []const u8,
    /// Object-name prefix inside the bucket (e.g. "tenants/acme/").
    prefix: []const u8 = "",
    /// MemTable size threshold before flushing to a new SSTable, in bytes.
    memtable_flush_bytes: usize = 16 * 1024 * 1024,
};

pub const Engine = struct {
    gpa: std.mem.Allocator,
    options: Options,

    pub fn init(gpa: std.mem.Allocator, options: Options) !Engine {
        return Engine{ .gpa = gpa, .options = options };
    }

    pub fn deinit(self: *Engine) void {
        _ = self;
    }

    /// Look up `key`. Returned slice is owned by `out_buf` if provided, or
    /// by the engine's per-request arena otherwise.
    pub fn get(self: *Engine, key: []const u8, out_buf: ?[]u8) Error!?[]const u8 {
        _ = self;
        _ = key;
        _ = out_buf;
        return Error.NotImplemented;
    }

    pub fn set(self: *Engine, key: []const u8, value: []const u8) Error!void {
        _ = self;
        _ = key;
        _ = value;
        return Error.NotImplemented;
    }

    pub fn delete(self: *Engine, key: []const u8) Error!void {
        _ = self;
        _ = key;
        return Error.NotImplemented;
    }
};

test "engine type compiles" {
    _ = Engine;
    _ = Options;
}
