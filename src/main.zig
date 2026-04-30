const std = @import("std");
const Io = std.Io;

const zero_db = @import("zero_db");

pub fn main(init: std.process.Init) !void {
    const arena: std.mem.Allocator = init.arena.allocator();
    const args = try init.minimal.args.toSlice(arena);

    const io = init.io;
    var stdout_buffer: [1024]u8 = undefined;
    var stdout_file_writer: Io.File.Writer = .init(.stdout(), io, &stdout_buffer);
    const stdout_writer = &stdout_file_writer.interface;

    var run_selftest = false;
    for (args[1..]) |arg| {
        if (std.mem.eql(u8, arg, "--selftest")) run_selftest = true;
    }

    if (run_selftest) {
        try selftest(arena, stdout_writer);
    } else {
        try zero_db.printAnotherMessage(stdout_writer);
    }

    try stdout_writer.flush();
}

/// Build a small index block in memory, parse it back, and look a key up.
/// Confirms the index parser is reachable from the binary, not just from
/// the test runner.
fn selftest(gpa: std.mem.Allocator, w: *Io.Writer) !void {
    const idx_mod = zero_db.sstable_index;

    const entries = [_]idx_mod.TestEntry{
        .{ .key = "alpha", .block_offset = 0, .block_size = 100 },
        .{ .key = "mango", .block_offset = 100, .block_size = 50 },
        .{ .key = "zeta", .block_offset = 150, .block_size = 25 },
    };
    const buf = try idx_mod.buildIndexBlock(gpa, &entries);
    defer gpa.free(buf);

    const idx = try idx_mod.IndexBlock.parse(buf);
    try w.print("selftest: index entries = {d}\n", .{idx.count()});

    const probe = "mango";
    if (try idx.find(probe)) |loc| {
        try w.print("selftest: find(\"{s}\") -> offset={d}, size={d}\n", .{
            probe, loc.block_offset, loc.block_size,
        });
    } else {
        try w.print("selftest: find(\"{s}\") -> null\n", .{probe});
    }
}

test "simple test" {
    const gpa = std.testing.allocator;
    var list: std.ArrayList(i32) = .empty;
    defer list.deinit(gpa);
    try list.append(gpa, 42);
    try std.testing.expectEqual(@as(i32, 42), list.pop());
}
