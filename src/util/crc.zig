//! Block checksum helpers — CRC32C (Castagnoli polynomial 0x1EDC6F41).
//!
//! Used as a 4-byte little-endian trailer on each data/index/bloom block.
//! Catches in-flight corruption that GCS's TLS+md5 doesn't (e.g. bit flips
//! between fetch and use inside our own buffers).
//!
//! Implementation is pure-Zig table-driven (256-entry u32 table built at
//! comptime). Cloud Run targets x86_64 and arm64; intrinsic acceleration
//! is a future optimization.

const std = @import("std");

const REFLECTED_POLY: u32 = 0x82F63B78;

const TABLE: [256]u32 = blk: {
    // 256 entries × 8 bit-steps = 2048 branches; 20_000 gives ~10× headroom
    // over the actual comptime work and well over the 1_000 default quota.
    @setEvalBranchQuota(20_000);
    var t: [256]u32 = undefined;
    var i: u32 = 0;
    while (i < 256) : (i += 1) {
        var c: u32 = i;
        var j: u32 = 0;
        while (j < 8) : (j += 1) {
            c = if ((c & 1) != 0) (c >> 1) ^ REFLECTED_POLY else (c >> 1);
        }
        t[i] = c;
    }
    break :blk t;
};

/// Compute CRC32C (Castagnoli) over `data`. Pure-Zig, table-driven.
pub fn crc32c(data: []const u8) u32 {
    var crc: u32 = 0xFFFFFFFF;
    for (data) |b| {
        crc = (crc >> 8) ^ TABLE[@as(u8, @truncate(crc ^ b))];
    }
    return crc ^ 0xFFFFFFFF;
}

const testing = std.testing;

test "crc32c of '123456789' matches Castagnoli reference 0xE3069283" {
    try testing.expectEqual(@as(u32, 0xE3069283), crc32c("123456789"));
}

test "crc32c of empty slice is 0" {
    try testing.expectEqual(@as(u32, 0), crc32c(""));
}

test "crc32c is deterministic (same input -> same output)" {
    const a = crc32c("zero-db");
    const b = crc32c("zero-db");
    try testing.expectEqual(a, b);
}

test "crc32c of single-byte inputs differs across distinct bytes" {
    try testing.expect(crc32c(&.{0x00}) != crc32c(&.{0x01}));
}

test "crc32c handles a 4 KiB block (4 KiB pattern, frozen value)" {
    var buf: [4096]u8 = undefined;
    var i: usize = 0;
    while (i < buf.len) : (i += 1) buf[i] = @intCast(i & 0xFF);
    const got = crc32c(&buf);
    try testing.expectEqual(@as(u32, 0x9c71fe32), got);
}
