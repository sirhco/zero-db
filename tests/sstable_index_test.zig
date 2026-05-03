//! Integration tests for the SSTable index parser.
//!
//! Lives outside `src/` so it exercises only the public surface in
//! `zero_db` (re-exported from `src/root.zig`). Build wiring registers
//! this file as a second test artifact so failures here can't be hidden
//! by the inline unit tests inside `src/sstable/index.zig`.

const std = @import("std");
const zero_db = @import("zero_db");

const idx = zero_db.sstable_index;

fn alloc() std.mem.Allocator {
    return std.testing.allocator;
}

test "public API: build → parse → find roundtrip" {
    const gpa = alloc();
    const entries = [_]idx.BuilderEntry{
        .{ .key = "alpha", .block_offset = 0, .block_size = 64 },
        .{ .key = "bravo", .block_offset = 64, .block_size = 64 },
        .{ .key = "charlie", .block_offset = 128, .block_size = 64 },
    };
    const buf = try idx.buildIndexBlock(gpa, &entries);
    defer gpa.free(buf);

    const block = try idx.IndexBlock.parse(buf);
    try std.testing.expectEqual(@as(u32, 3), block.count());

    const got = (try block.find("bravo")).?;
    try std.testing.expectEqual(@as(u64, 64), got.block_offset);
    try std.testing.expectEqual(@as(u32, 64), got.block_size);

    try std.testing.expectEqual(@as(?zero_db.BlockLocator, null), try block.find("zulu"));
}

test "public API: lower-bound returns nearest existing entry" {
    const gpa = alloc();
    const entries = [_]idx.BuilderEntry{
        .{ .key = "ddd", .block_offset = 0, .block_size = 1 },
        .{ .key = "fff", .block_offset = 1, .block_size = 2 },
        .{ .key = "hhh", .block_offset = 3, .block_size = 4 },
    };
    const buf = try idx.buildIndexBlock(gpa, &entries);
    defer gpa.free(buf);
    const block = try idx.IndexBlock.parse(buf);

    // "eee" falls between "ddd" and "fff" → returns "fff"'s entry.
    try std.testing.expectEqual(@as(u64, 1), (try block.find("eee")).?.block_offset);

    // "aaa" precedes everything → returns first entry.
    try std.testing.expectEqual(@as(u64, 0), (try block.find("aaa")).?.block_offset);

    // "zzz" follows everything → null.
    try std.testing.expectEqual(@as(?zero_db.BlockLocator, null), try block.find("zzz"));
}

test "public API: parse fails on bad magic" {
    var buf: [16]u8 = .{0} ** 16;
    std.mem.writeInt(u32, buf[0..4], 0xCAFE_BABE, .little);
    std.mem.writeInt(u16, buf[4..6], 1, .little);
    try std.testing.expectError(error.BadMagic, idx.IndexBlock.parse(&buf));
}
