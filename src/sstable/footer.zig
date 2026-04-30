//! Fixed 64-byte SSTable footer.
//!
//! Always located in the last 64 bytes of an SSTable object so a single
//! `Range: bytes=-64` request locates the bloom filter and index without
//! prior knowledge of the file layout. The first GCS round-trip on any cold
//! lookup fetches this footer.
//!
//! Stub: struct + constants. Reader/writer wiring lands when those are
//! implemented.

const std = @import("std");

pub const FOOTER_BYTES: usize = 64;
pub const SSTABLE_MAGIC: u64 = 0x5A45_524F_4442_3031; // "ZERODB01"
pub const SSTABLE_VERSION: u16 = 1;

pub const Footer = packed struct {
    index_offset: u64,
    index_size: u32,
    bloom_offset: u64,
    bloom_size: u32,
    entry_count: u64,
    /// Sum of key + value bytes across all entries; used for capacity planning.
    payload_bytes: u64,
    version: u16,
    flags: u16,
    /// Pad up to 64 bytes; magic anchored at the very end so a tail-fetched
    /// 64-byte buffer can be magic-checked without further address math.
    _pad: u96,
    magic: u64,
};

comptime {
    std.debug.assert(@bitSizeOf(Footer) / 8 == FOOTER_BYTES);
}

test "footer type compiles" {
    _ = Footer;
}
