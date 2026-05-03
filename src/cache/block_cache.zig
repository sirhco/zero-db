//! Block cache: parsed IndexBlocks + raw/decompressed data blocks.
//!
//! Keyed by (sstable_id, block_offset). Sits in front of the SSTable reader
//! so repeated lookups within a popular SSTable avoid both the GCS round
//! trip and the parse step. Bounded by total resident bytes (Cloud Run mem
//! ceiling).
//!
//! Stub: signatures only.

const std = @import("std");
const lru = @import("lru.zig");

pub const Key = struct {
    sstable_id: u64,
    block_offset: u64,
};

pub const BlockCache = struct {
    gpa: std.mem.Allocator,
    bytes_capacity: usize,

    pub fn init(gpa: std.mem.Allocator, bytes_capacity: usize) BlockCache {
        return .{ .gpa = gpa, .bytes_capacity = bytes_capacity };
    }

    pub fn deinit(self: *BlockCache) void {
        _ = self;
    }
};

test "block cache type compiles" {
    _ = BlockCache;
    _ = lru;
}
