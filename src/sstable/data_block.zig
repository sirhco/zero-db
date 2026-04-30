//! On-disk format for SSTable data blocks.
//!
//! A data block holds a contiguous run of sorted KV entries from one
//! "logical" slice of the keyspace. The index block points one entry per
//! data block, addressed by the last key in the block. Reader fetches the
//! block, scans linearly for the target key.
//!
//! Layout, all little-endian:
//!   DataBlockHeader (16 bytes packed)
//!   payload (payload_size bytes), each entry:
//!     u32 key_len
//!     u32 value_len      (== TOMBSTONE_VALUE_LEN ⇒ tombstone, no value bytes)
//!     u8[key_len]  key
//!     u8[value_len] value (omitted on tombstone)

const std = @import("std");
const endian = @import("../util/endian.zig");
comptime {
    _ = endian;
}

/// 'ZDAT' read little-endian.
pub const DATA_BLOCK_MAGIC: u32 = 0x5444415A;
pub const DATA_BLOCK_VERSION: u16 = 1;

/// Sentinel for `value_len`. Distinct from a legitimate empty value
/// (`value_len == 0`) so empty values and tombstones can coexist.
pub const TOMBSTONE_VALUE_LEN: u32 = std.math.maxInt(u32);

pub const DataBlockHeader = packed struct {
    magic: u32,
    version: u16,
    flags: u16,
    entry_count: u32,
    /// Bytes of entry payload that follow this header. Caller-side bounds
    /// check: `header_bytes + payload_size == total block bytes`.
    payload_size: u32,
};

pub const DATA_BLOCK_HEADER_BYTES: usize = @bitSizeOf(DataBlockHeader) / 8;

comptime {
    std.debug.assert(DATA_BLOCK_HEADER_BYTES == 16);
}

test "data block constants stable" {
    try std.testing.expectEqual(@as(u32, 0x5444415A), DATA_BLOCK_MAGIC);
    try std.testing.expectEqual(@as(usize, 16), DATA_BLOCK_HEADER_BYTES);
    try std.testing.expectEqual(std.math.maxInt(u32), TOMBSTONE_VALUE_LEN);
}
