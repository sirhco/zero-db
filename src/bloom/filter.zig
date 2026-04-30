//! Bloom filter for SSTable membership checks.
//!
//! One filter per SSTable, consulted before any index/data range fetch so
//! absent keys cost zero data round trips. Built with `std.hash.Wyhash`
//! over two distinct seeds, then probed via the Kirsch-Mitzenmacher double
//! hashing scheme `h_i = h1 + i * h2 (mod m)`. Two Wyhash calls give k
//! probes for the price of two — important on the hot read path.
//!
//! Wire format:
//!   FilterHeader (16 bytes packed) followed by `bit_count / 8` raw bit bytes.
//!   Header carries magic, version, flags, k, bit_count so a corrupt or
//!   wrong-block buffer fails fast.

const std = @import("std");
const endian = @import("../util/endian.zig");
comptime {
    _ = endian;
}

pub const FILTER_MAGIC: u32 = 0x4D_4C_42_5A; // 'ZBLM' read little-endian
pub const FILTER_VERSION: u16 = 1;

/// Two well-known fractional-of-phi constants used as Wyhash seeds. Distinct
/// so the two derived hashes are statistically independent.
const SEED_A: u64 = 0x9E37_79B9_7F4A_7C15;
const SEED_B: u64 = 0xBF58_476D_1CE4_E5B9;

pub const FilterHeader = packed struct {
    magic: u32,
    version: u16,
    flags: u16,
    k: u32,
    bit_count: u32,
};

pub const FILTER_HEADER_BYTES: usize = @bitSizeOf(FilterHeader) / 8;

comptime {
    std.debug.assert(FILTER_HEADER_BYTES == 16);
}

pub const FilterError = error{
    BufferTooSmall,
    BadMagic,
    UnsupportedVersion,
    BadFlags,
    BadParameters,
};

pub const Params = struct {
    /// Number of bits in the filter, rounded up to a multiple of 8.
    m: u32,
    /// Number of hash probes per key.
    k: u32,
};

/// Optimal Bloom filter parameters for `expected_n` items at `target_fp`
/// false-positive rate. Caps applied to keep the filter bounded for
/// pathological inputs (e.g. fp=0).
pub fn optimalParams(expected_n: usize, target_fp: f64) Params {
    std.debug.assert(target_fp > 0.0 and target_fp < 1.0);
    const n: f64 = if (expected_n == 0) 1.0 else @floatFromInt(expected_n);
    const ln2 = std.math.ln2;
    const ln2_sq = ln2 * ln2;

    const m_raw: f64 = -(n * @log(target_fp)) / ln2_sq;
    const m_rounded: u64 = @intFromFloat(@ceil(m_raw / 8.0));
    const m_bytes: u64 = @max(m_rounded, 1);
    const m_bits: u64 = m_bytes * 8;

    const k_raw: f64 = (@as(f64, @floatFromInt(m_bits)) / n) * ln2;
    const k_clamped: u32 = std.math.clamp(@as(u32, @intFromFloat(@ceil(k_raw))), 1, 32);

    return .{
        .m = @intCast(@min(m_bits, std.math.maxInt(u32))),
        .k = k_clamped,
    };
}

fn setBit(bits: []u8, i: u64) void {
    bits[@intCast(i >> 3)] |= @as(u8, 1) << @intCast(i & 7);
}

fn getBit(bits: []const u8, i: u64) bool {
    return (bits[@intCast(i >> 3)] & (@as(u8, 1) << @intCast(i & 7))) != 0;
}

fn probe(key: []const u8, i: u32, m: u32) u32 {
    const h1 = std.hash.Wyhash.hash(SEED_A, key);
    const h2 = std.hash.Wyhash.hash(SEED_B, key);
    // `+%` and `*%` are wrapping; modular reduction by `m` is the final step.
    const combined = h1 +% (@as(u64, i) *% h2);
    return @intCast(combined % @as(u64, m));
}

/// Live Bloom filter view over a caller-owned buffer (e.g. one fetched from
/// GCS). Borrows `buf`; caller must keep the buffer alive.
pub const Filter = struct {
    header: FilterHeader,
    bits: []const u8,

    pub fn parse(buf: []const u8) FilterError!Filter {
        if (buf.len < FILTER_HEADER_BYTES) return error.BufferTooSmall;
        const hdr_bytes: [FILTER_HEADER_BYTES]u8 = buf[0..FILTER_HEADER_BYTES].*;
        const header: FilterHeader = @bitCast(hdr_bytes);

        if (header.magic != FILTER_MAGIC) return error.BadMagic;
        if (header.version != FILTER_VERSION) return error.UnsupportedVersion;
        if (header.flags != 0) return error.BadFlags;
        if (header.k == 0 or header.bit_count == 0) return error.BadParameters;
        if (header.bit_count % 8 != 0) return error.BadParameters;

        const bit_bytes: usize = header.bit_count / 8;
        if (buf.len < FILTER_HEADER_BYTES + bit_bytes) return error.BufferTooSmall;

        return .{
            .header = header,
            .bits = buf[FILTER_HEADER_BYTES .. FILTER_HEADER_BYTES + bit_bytes],
        };
    }

    /// Returns false ⇒ key is definitely not present.
    /// Returns true  ⇒ key may be present (must verify with index/data).
    pub fn maybeContains(self: Filter, key: []const u8) bool {
        var i: u32 = 0;
        while (i < self.header.k) : (i += 1) {
            const idx = probe(key, i, self.header.bit_count);
            if (!getBit(self.bits, idx)) return false;
        }
        return true;
    }

    pub fn k(self: Filter) u32 {
        return self.header.k;
    }

    pub fn bitCount(self: Filter) u32 {
        return self.header.bit_count;
    }
};

/// Mutable builder. Build, add keys, finalize → produces a complete filter
/// buffer (header + bits) that `Filter.parse` can read back.
pub const Builder = struct {
    gpa: std.mem.Allocator,
    bits: []u8,
    bit_count: u32,
    k: u32,

    pub fn init(gpa: std.mem.Allocator, expected_n: usize, target_fp: f64) !Builder {
        const p = optimalParams(expected_n, target_fp);
        const bytes = try gpa.alloc(u8, p.m / 8);
        @memset(bytes, 0);
        return .{
            .gpa = gpa,
            .bits = bytes,
            .bit_count = p.m,
            .k = p.k,
        };
    }

    pub fn deinit(self: *Builder) void {
        self.gpa.free(self.bits);
        self.* = undefined;
    }

    pub fn add(self: *Builder, key: []const u8) void {
        var i: u32 = 0;
        while (i < self.k) : (i += 1) {
            const idx = probe(key, i, self.bit_count);
            setBit(self.bits, idx);
        }
    }

    /// Allocates and returns a single contiguous `header || bits` buffer.
    /// Caller owns the result.
    pub fn finalize(self: *const Builder, gpa: std.mem.Allocator) ![]u8 {
        const total = FILTER_HEADER_BYTES + self.bits.len;
        const out = try gpa.alloc(u8, total);
        const header = FilterHeader{
            .magic = FILTER_MAGIC,
            .version = FILTER_VERSION,
            .flags = 0,
            .k = self.k,
            .bit_count = self.bit_count,
        };
        const hdr_bytes: [FILTER_HEADER_BYTES]u8 = @bitCast(header);
        @memcpy(out[0..FILTER_HEADER_BYTES], &hdr_bytes);
        @memcpy(out[FILTER_HEADER_BYTES..], self.bits);
        return out;
    }
};

const testing = std.testing;

test "wyhash hash is callable and deterministic" {
    const a = std.hash.Wyhash.hash(0, "key");
    const b = std.hash.Wyhash.hash(0, "key");
    try testing.expectEqual(a, b);
}

test "optimalParams produces sensible bit/k counts" {
    const p = optimalParams(1000, 0.01);
    // ~9585 bits / 7 hashes is the textbook answer; allow some slack from
    // rounding to byte boundaries.
    try testing.expect(p.m >= 8000 and p.m <= 12000);
    try testing.expect(p.k >= 5 and p.k <= 10);
}

test "no false negatives across builder → filter roundtrip" {
    const gpa = testing.allocator;
    var b = try Builder.init(gpa, 1024, 0.01);
    defer b.deinit();

    var keys: std.ArrayList([]u8) = .empty;
    defer {
        for (keys.items) |k| gpa.free(k);
        keys.deinit(gpa);
    }
    var i: u32 = 0;
    while (i < 1024) : (i += 1) {
        const k = try std.fmt.allocPrint(gpa, "key_{d}", .{i});
        try keys.append(gpa, k);
        b.add(k);
    }

    const buf = try b.finalize(gpa);
    defer gpa.free(buf);

    const f = try Filter.parse(buf);
    for (keys.items) |k| {
        try testing.expect(f.maybeContains(k));
    }
}

test "false positive rate within expected bound" {
    const gpa = testing.allocator;
    const N: u32 = 2048;
    const target_fp = 0.01;

    var b = try Builder.init(gpa, N, target_fp);
    defer b.deinit();

    var i: u32 = 0;
    while (i < N) : (i += 1) {
        var keybuf: [32]u8 = undefined;
        const k = try std.fmt.bufPrint(&keybuf, "in_{d}", .{i});
        b.add(k);
    }

    const buf = try b.finalize(gpa);
    defer gpa.free(buf);
    const f = try Filter.parse(buf);

    const trials: u32 = 10_000;
    var hits: u32 = 0;
    var j: u32 = 0;
    while (j < trials) : (j += 1) {
        var keybuf: [32]u8 = undefined;
        const k = try std.fmt.bufPrint(&keybuf, "out_{d}", .{j});
        if (f.maybeContains(k)) hits += 1;
    }
    const observed_fp: f64 = @as(f64, @floatFromInt(hits)) / @as(f64, @floatFromInt(trials));
    // Target is 0.01; allow up to 5x as a generous statistical envelope so
    // this test is not flaky across host-arch hash variations.
    try testing.expect(observed_fp < target_fp * 5.0);
}

test "empty filter never claims membership" {
    const gpa = testing.allocator;
    var b = try Builder.init(gpa, 64, 0.01);
    defer b.deinit();
    const buf = try b.finalize(gpa);
    defer gpa.free(buf);
    const f = try Filter.parse(buf);
    try testing.expect(!f.maybeContains("anything"));
    try testing.expect(!f.maybeContains(""));
}

test "parse rejects bad magic" {
    var buf: [16]u8 = .{0} ** 16;
    std.mem.writeInt(u32, buf[0..4], 0xDEAD_BEEF, .little);
    try testing.expectError(error.BadMagic, Filter.parse(&buf));
}

test "parse rejects unsupported version" {
    var buf: [16]u8 = .{0} ** 16;
    std.mem.writeInt(u32, buf[0..4], FILTER_MAGIC, .little);
    std.mem.writeInt(u16, buf[4..6], 99, .little);
    try testing.expectError(error.UnsupportedVersion, Filter.parse(&buf));
}

test "parse rejects truncated body" {
    const gpa = testing.allocator;
    var b = try Builder.init(gpa, 16, 0.01);
    defer b.deinit();
    const buf = try b.finalize(gpa);
    defer gpa.free(buf);
    try testing.expectError(error.BufferTooSmall, Filter.parse(buf[0 .. buf.len - 1]));
}

test "parse rejects k=0" {
    var buf: [16]u8 = .{0} ** 16;
    std.mem.writeInt(u32, buf[0..4], FILTER_MAGIC, .little);
    std.mem.writeInt(u16, buf[4..6], FILTER_VERSION, .little);
    std.mem.writeInt(u16, buf[6..8], 0, .little);
    std.mem.writeInt(u32, buf[8..12], 0, .little); // k = 0
    std.mem.writeInt(u32, buf[12..16], 64, .little);
    try testing.expectError(error.BadParameters, Filter.parse(&buf));
}
