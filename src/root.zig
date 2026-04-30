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

// Force `test` blocks in every submodule to be discovered by `zig build
// test`. A `pub const ns = @import("...")` re-export does not by itself
// pull in the file's tests — only members accessed via `ns.Foo` get
// analyzed. The unnamed test block below is the canonical Zig idiom for
// "include every file's tests, recursively".
test {
    _ = @import("sstable/index.zig");
    _ = @import("sstable/format.zig");
    _ = @import("sstable/footer.zig");
    _ = @import("sstable/reader.zig");
    _ = @import("sstable/writer.zig");
    _ = @import("engine/engine.zig");
    _ = @import("engine/memtable.zig");
    _ = @import("engine/compaction.zig");
    _ = @import("bloom/filter.zig");
    _ = @import("cache/lru.zig");
    _ = @import("cache/block_cache.zig");
    _ = @import("storage/gcs.zig");
    _ = @import("storage/auth.zig");
    _ = @import("prefetch/adaptive.zig");
    _ = @import("alloc/arena_pool.zig");
    _ = @import("alloc/tracking.zig");
    _ = @import("util/varint.zig");
    _ = @import("util/crc.zig");
    _ = @import("util/endian.zig");
}
