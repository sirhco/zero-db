//! Block checksum helpers.
//!
//! CRC32C over each data/index/bloom block, written alongside the block.
//! Catches in-flight corruption that GCS's TLS+md5 doesn't (e.g. bit flips
//! between fetch and use inside our own buffers).
//!
//! Stub: signature only.

const std = @import("std");

pub fn crc32c(data: []const u8) u32 {
    _ = data;
    return 0;
}

test "crc32c signature compiles" {
    _ = crc32c;
}
