//! SSTable on-disk format constants and shared structs.
//!
//! Stable wire format owned here so writers, readers, and parsers share a
//! single source of truth. Anything serialized to GCS must reference a
//! constant or struct from this file.

const std = @import("std");
const endian = @import("../util/endian.zig"); // pulls in comptime LE assertion
comptime {
    _ = endian;
}

/// Maximum byte length of a single key. Keeps individual index entries
/// bounded so a corrupt `key_len` field can't make us walk off a buffer.
pub const MAX_KEY_LEN: u16 = 4096;

/// Magic number at the head of an index block. ASCII 'Z','I','D','X' read
/// little-endian: 0x58_44_49_5A. Distinct constant per block type so a
/// truncated/wrong-block read fails fast.
pub const INDEX_BLOCK_MAGIC: u32 = 0x5844495A;

/// Index block format version. Bump on any incompatible layout change.
pub const INDEX_BLOCK_VERSION: u16 = 1;

/// Fixed 16-byte header at offset 0 of every index block.
///
/// `packed` so a 16-byte slice can be `@bitCast` to a value with no per-field
/// decode. Field order matches little-endian wire bytes 0..16.
pub const IndexHeader = packed struct {
    magic: u32,
    version: u16,
    flags: u16,
    entry_count: u32,
    heap_size: u32,
};

/// Per-entry header inside the heap region of an index block.
///
/// Followed in the buffer by exactly `key_len` raw key bytes. 14 bytes total.
pub const IndexEntryHeader = packed struct {
    block_offset: u64,
    block_size: u32,
    key_len: u16,
};

/// Byte width of `IndexHeader` on disk. Packed structs round up to their
/// underlying integer's alignment for `@sizeOf`, so the wire size has to
/// come from `@bitSizeOf` / 8 — that's the exact packed bit width.
pub const INDEX_HEADER_BYTES: usize = @bitSizeOf(IndexHeader) / 8;

/// Byte width of `IndexEntryHeader` on disk (same caveat as above).
pub const INDEX_ENTRY_HEADER_BYTES: usize = @bitSizeOf(IndexEntryHeader) / 8;

comptime {
    std.debug.assert(INDEX_HEADER_BYTES == 16);
    std.debug.assert(INDEX_ENTRY_HEADER_BYTES == 14);
}

test "format constants stable" {
    try std.testing.expectEqual(@as(u32, 0x5844495A), INDEX_BLOCK_MAGIC);
    try std.testing.expectEqual(@as(u16, 1), INDEX_BLOCK_VERSION);
    try std.testing.expectEqual(@as(usize, 16), INDEX_HEADER_BYTES);
    try std.testing.expectEqual(@as(usize, 14), INDEX_ENTRY_HEADER_BYTES);
}
