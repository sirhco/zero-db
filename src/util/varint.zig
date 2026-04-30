//! LEB128 / prefix-varint helpers.
//!
//! Used by the data block format (key/value length prefixes). Not on the
//! index-block hot path — index entries use fixed-width fields for zero-copy
//! `@bitCast` access.
//!
//! Stub: signatures only.

const std = @import("std");

pub const Error = error{
    NotImplemented,
    Truncated,
};

pub fn encodeU64(value: u64, dst: []u8) Error!usize {
    _ = value;
    _ = dst;
    return Error.NotImplemented;
}

pub fn decodeU64(src: []const u8) Error!struct { value: u64, bytes_read: usize } {
    _ = src;
    return Error.NotImplemented;
}

test "varint signatures compile" {
    _ = encodeU64;
    _ = decodeU64;
}
