//! Bloom filter for SSTable membership checks.
//!
//! Hashed with `std.hash.Wyhash`. One filter per SSTable; consulted before
//! any index/data Range fetch so absent keys cost zero data round-trips.
//! Filter parameters (m bits, k hash funcs) chosen to target ~1% false
//! positives at the SSTable's expected entry count.
//!
//! Stub: signatures only.

const std = @import("std");

pub const Error = error{
    NotImplemented,
};

pub const Filter = struct {
    bits: []const u8,
    k: u8,

    pub fn maybeContains(self: Filter, key: []const u8) bool {
        _ = self;
        _ = key;
        // Conservative stub: always say "maybe" so integration code is forced
        // to fall through to the index until the real filter ships.
        return true;
    }
};

pub fn hash(seed: u64, key: []const u8) u64 {
    return std.hash.Wyhash.hash(seed, key);
}

test "wyhash hash is callable" {
    const a = hash(0, "key");
    const b = hash(0, "key");
    try std.testing.expectEqual(a, b);
}
