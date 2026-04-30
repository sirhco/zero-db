//! Public surface of the `zero_db` module.
//!
//! Re-exports the subsystems consumers need. Each `pub const` here also
//! anchors the file in the test root so `zig build test` discovers tests
//! across every module via transitive `@import`.

const std = @import("std");
const Io = std.Io;

// Public surface.
pub const sstable_index = @import("sstable/index.zig");
pub const sstable_format = @import("sstable/format.zig");
pub const sstable_footer = @import("sstable/footer.zig");
pub const sstable_reader = @import("sstable/reader.zig");
pub const sstable_writer = @import("sstable/writer.zig");

pub const engine = @import("engine/engine.zig");
pub const memtable = @import("engine/memtable.zig");
pub const compaction = @import("engine/compaction.zig");

pub const bloom = @import("bloom/filter.zig");

pub const lru = @import("cache/lru.zig");
pub const block_cache = @import("cache/block_cache.zig");

pub const gcs = @import("storage/gcs.zig");
pub const auth = @import("storage/auth.zig");

pub const prefetch = @import("prefetch/adaptive.zig");

pub const arena_pool = @import("alloc/arena_pool.zig");
pub const tracking = @import("alloc/tracking.zig");

pub const varint = @import("util/varint.zig");
pub const crc = @import("util/crc.zig");
pub const endian = @import("util/endian.zig");

pub const Engine = engine.Engine;
pub const IndexBlock = sstable_index.IndexBlock;
pub const BlockLocator = sstable_index.BlockLocator;

/// Print a one-line banner to `writer`. Kept for the existing `main.zig`
/// smoke path.
pub fn printAnotherMessage(writer: *Io.Writer) Io.Writer.Error!void {
    try writer.print("Run `zig build test` to run the tests.\n", .{});
}

test "module surface compiles" {
    _ = Engine;
    _ = IndexBlock;
    _ = BlockLocator;
}
