//! Fixed 64-byte SSTable footer.
//!
//! Always the last 64 bytes of an SSTable object so a single
//! `Range: bytes=-64` request locates the bloom filter and index without
//! prior knowledge of the file layout. The first GCS round-trip of any cold
//! lookup fetches this footer; everything else is byte-range derived from it.

const std = @import("std");
const endian = @import("../util/endian.zig");
comptime {
    _ = endian;
}

pub const FOOTER_BYTES: usize = 64;

/// "ZERODB01" read little-endian. Anchored at the very end of the footer so
/// a tail-fetched 64-byte buffer can be magic-checked at offset 56 without
/// any further parsing.
pub const SSTABLE_MAGIC: u64 = 0x3130_4244_4F52_455A;
pub const SSTABLE_VERSION: u16 = 1;

pub const Footer = packed struct {
    index_offset: u64,
    index_size: u32,
    bloom_offset: u64,
    bloom_size: u32,
    entry_count: u64,
    /// Sum of key + value bytes across all entries. Capacity-planning hint
    /// for the engine; not load-bearing for correctness.
    payload_bytes: u64,
    version: u16,
    flags: u16,
    _pad: u96,
    magic: u64,
};

comptime {
    std.debug.assert(@bitSizeOf(Footer) / 8 == FOOTER_BYTES);
}

pub const FooterError = error{
    BufferTooSmall,
    BadMagic,
    UnsupportedVersion,
    BadFlags,
};

/// Parse the trailing 64 bytes of an SSTable. Caller is responsible for
/// passing exactly the last 64 bytes (e.g. via a `Range: bytes=-64` GET).
pub fn parse(buf: []const u8) FooterError!Footer {
    if (buf.len < FOOTER_BYTES) return error.BufferTooSmall;
    const tail = buf[buf.len - FOOTER_BYTES ..];
    const bytes: [FOOTER_BYTES]u8 = tail[0..FOOTER_BYTES].*;
    const f: Footer = @bitCast(bytes);

    if (f.magic != SSTABLE_MAGIC) return error.BadMagic;
    if (f.version != SSTABLE_VERSION) return error.UnsupportedVersion;
    if (f.flags != 0) return error.BadFlags;
    return f;
}

/// Encode a Footer to its 64-byte wire form. Used by the writer (later) and
/// by tests today.
pub fn encode(f: Footer) [FOOTER_BYTES]u8 {
    var v = f;
    v.magic = SSTABLE_MAGIC;
    v.version = SSTABLE_VERSION;
    v.flags = 0;
    v._pad = 0;
    return @bitCast(v);
}

const testing = std.testing;

test "encode/parse roundtrip" {
    const f = Footer{
        .index_offset = 1024,
        .index_size = 256,
        .bloom_offset = 768,
        .bloom_size = 64,
        .entry_count = 17,
        .payload_bytes = 4096,
        .version = SSTABLE_VERSION,
        .flags = 0,
        ._pad = 0,
        .magic = SSTABLE_MAGIC,
    };
    const wire = encode(f);
    const parsed = try parse(&wire);
    try testing.expectEqual(@as(u64, 1024), parsed.index_offset);
    try testing.expectEqual(@as(u32, 256), parsed.index_size);
    try testing.expectEqual(@as(u64, 768), parsed.bloom_offset);
    try testing.expectEqual(@as(u32, 64), parsed.bloom_size);
    try testing.expectEqual(@as(u64, 17), parsed.entry_count);
    try testing.expectEqual(@as(u64, 4096), parsed.payload_bytes);
}

test "magic anchored at byte offset 56" {
    const f = Footer{
        .index_offset = 0,
        .index_size = 0,
        .bloom_offset = 0,
        .bloom_size = 0,
        .entry_count = 0,
        .payload_bytes = 0,
        .version = SSTABLE_VERSION,
        .flags = 0,
        ._pad = 0,
        .magic = SSTABLE_MAGIC,
    };
    const wire = encode(f);
    var got_magic: u64 = 0;
    got_magic = std.mem.readInt(u64, wire[56..64], .little);
    try testing.expectEqual(SSTABLE_MAGIC, got_magic);
}

test "parse accepts longer buffer (tail-only)" {
    const f = Footer{
        .index_offset = 9,
        .index_size = 9,
        .bloom_offset = 9,
        .bloom_size = 9,
        .entry_count = 9,
        .payload_bytes = 9,
        .version = SSTABLE_VERSION,
        .flags = 0,
        ._pad = 0,
        .magic = SSTABLE_MAGIC,
    };
    const wire = encode(f);
    var prefixed: [128]u8 = undefined;
    @memset(prefixed[0..64], 0xAA); // garbage prefix
    @memcpy(prefixed[64..128], &wire);
    const parsed = try parse(&prefixed);
    try testing.expectEqual(@as(u64, 9), parsed.index_offset);
}

test "parse rejects too-small buffer" {
    const buf = [_]u8{0} ** 32;
    try testing.expectError(error.BufferTooSmall, parse(&buf));
}

test "parse rejects bad magic" {
    var wire = encode(Footer{
        .index_offset = 0,
        .index_size = 0,
        .bloom_offset = 0,
        .bloom_size = 0,
        .entry_count = 0,
        .payload_bytes = 0,
        .version = SSTABLE_VERSION,
        .flags = 0,
        ._pad = 0,
        .magic = SSTABLE_MAGIC,
    });
    std.mem.writeInt(u64, wire[56..64], 0xDEAD_BEEF_DEAD_BEEF, .little);
    try testing.expectError(error.BadMagic, parse(&wire));
}

test "parse rejects unsupported version" {
    var wire = encode(Footer{
        .index_offset = 0,
        .index_size = 0,
        .bloom_offset = 0,
        .bloom_size = 0,
        .entry_count = 0,
        .payload_bytes = 0,
        .version = SSTABLE_VERSION,
        .flags = 0,
        ._pad = 0,
        .magic = SSTABLE_MAGIC,
    });
    // version field lives at bytes 40..42
    std.mem.writeInt(u16, wire[40..42], 999, .little);
    try testing.expectError(error.UnsupportedVersion, parse(&wire));
}

test "parse rejects nonzero flags" {
    var wire = encode(Footer{
        .index_offset = 0,
        .index_size = 0,
        .bloom_offset = 0,
        .bloom_size = 0,
        .entry_count = 0,
        .payload_bytes = 0,
        .version = SSTABLE_VERSION,
        .flags = 0,
        ._pad = 0,
        .magic = SSTABLE_MAGIC,
    });
    std.mem.writeInt(u16, wire[42..44], 1, .little);
    try testing.expectError(error.BadFlags, parse(&wire));
}
