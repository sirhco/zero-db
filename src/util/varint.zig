//! LEB128 / prefix-varint helpers.
//!
//! Used by the data block format (key/value length prefixes). Not on the
//! index-block hot path — index entries use fixed-width fields for zero-copy
//! `@bitCast` access.

const std = @import("std");

pub const Error = error{
    BufferTooSmall,
    Truncated,
    Overflow,
};

/// Maximum number of bytes any u64 can occupy in LEB128 (ceil(64/7)).
pub const MAX_U64_BYTES: usize = 10;

/// Number of bytes a u64 will occupy in LEB128 form. Useful for sizing
/// buffers before calling `encodeU64`.
pub fn encodedSizeU64(value: u64) usize {
    var v = value;
    var n: usize = 1;
    while (v >= 0x80) : (n += 1) v >>= 7;
    return n;
}

/// Encodes `value` into `dst` using LEB128. Returns the number of bytes
/// written. `error.BufferTooSmall` if `dst` is shorter than
/// `encodedSizeU64(value)`.
pub fn encodeU64(value: u64, dst: []u8) Error!usize {
    var v = value;
    var i: usize = 0;
    while (true) {
        if (i >= dst.len) return Error.BufferTooSmall;
        const byte: u8 = @intCast(v & 0x7F);
        v >>= 7;
        if (v == 0) {
            dst[i] = byte;
            return i + 1;
        }
        dst[i] = byte | 0x80;
        i += 1;
    }
}

/// Result of `decodeU64`: the parsed value and the byte length consumed.
pub const Decoded = struct { value: u64, bytes_read: usize };

/// Decodes a single LEB128 unsigned varint from the head of `src`.
/// Returns the value plus how many bytes it occupied.
/// `error.Truncated` if `src` ends mid-varint, `error.Overflow` if the
/// encoded value cannot fit in u64.
pub fn decodeU64(src: []const u8) Error!Decoded {
    var value: u64 = 0;
    var shift: usize = 0;
    for (src, 0..) |b, i| {
        if (i >= MAX_U64_BYTES) return Error.Overflow;
        const lo: u64 = @as(u64, b & 0x7F);
        // The 10th byte lands at bit 63. Any payload bit above position 0
        // (i.e., lo > 1) would set bit 64 or higher — that is overflow.
        if (shift == 63 and lo > 1) return Error.Overflow;
        value |= lo << @intCast(shift);
        if ((b & 0x80) == 0) {
            return .{ .value = value, .bytes_read = i + 1 };
        }
        shift += 7;
    }
    return Error.Truncated;
}

const testing = std.testing;

test "encodedSizeU64 boundaries" {
    try testing.expectEqual(@as(usize, 1), encodedSizeU64(0));
    try testing.expectEqual(@as(usize, 1), encodedSizeU64(127));
    try testing.expectEqual(@as(usize, 2), encodedSizeU64(128));
    try testing.expectEqual(@as(usize, 2), encodedSizeU64(16_383));
    try testing.expectEqual(@as(usize, 3), encodedSizeU64(16_384));
    try testing.expectEqual(@as(usize, 10), encodedSizeU64(std.math.maxInt(u64)));
}

test "encode then decode roundtrips across boundary values" {
    const cases = [_]u64{
        0, 1, 63, 127, 128, 129, 255, 256,
        16_383, 16_384, 1 << 32, std.math.maxInt(u64) - 1, std.math.maxInt(u64),
    };
    var buf: [MAX_U64_BYTES]u8 = undefined;
    for (cases) |v| {
        const n = try encodeU64(v, &buf);
        try testing.expectEqual(encodedSizeU64(v), n);
        const got = try decodeU64(buf[0..n]);
        try testing.expectEqual(v, got.value);
        try testing.expectEqual(n, got.bytes_read);
    }
}

test "encode known fixture 624485 -> 0xE5 0x8E 0x26" {
    var buf: [MAX_U64_BYTES]u8 = undefined;
    const n = try encodeU64(624_485, &buf);
    try testing.expectEqual(@as(usize, 3), n);
    try testing.expectEqualSlices(u8, &.{ 0xE5, 0x8E, 0x26 }, buf[0..n]);
}

test "encode returns BufferTooSmall when dst is undersized" {
    var buf: [1]u8 = undefined;
    try testing.expectError(Error.BufferTooSmall, encodeU64(128, &buf));
}

test "decode returns Truncated on a buffer that ends mid-varint" {
    const bad = [_]u8{0x80}; // continuation bit set, no follow-up byte
    try testing.expectError(Error.Truncated, decodeU64(&bad));
}

test "decode returns Overflow when more than 10 continuation bytes" {
    const bad = [_]u8{ 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01 };
    try testing.expectError(Error.Overflow, decodeU64(&bad));
}

test "decode returns Overflow on 10-byte sequence with illegal final byte" {
    // 9 continuation bytes (0x80) + final byte 0x02 → bit 64 would be set
    const bad = [_]u8{ 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x02 };
    try testing.expectError(Error.Overflow, decodeU64(&bad));
}
