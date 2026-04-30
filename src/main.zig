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

/// Drive the full Engine end-to-end from the binary: set, flush, delete,
/// re-set, get across MemTable + SSTable tiers. Confirms the LSM read/write
/// path is reachable from the executable, not just from the test runner.
fn selftest(gpa: std.mem.Allocator, w: *Io.Writer) !void {
    var e = try zero_db.engine.Engine.init(gpa, .{ .memtable_flush_bytes = 4096 });
    defer e.deinit();

    try e.set("alpha", "AAA");
    try e.set("mango", "first-mango");
    try e.set("zeta", "Z-VALUE");
    try e.flush();

    // Tombstone + override after the first SSTable to exercise tier
    // shadowing on the read path.
    try e.delete("alpha");
    try e.set("mango", "fresher-mango");
    try e.set("delta", "DDD");

    try w.print("selftest: sstables={d}, active_entries={d}\n", .{
        e.sstableCount(), e.entryCountActive(),
    });

    const probes = [_][]const u8{ "alpha", "mango", "zeta", "delta", "ghost" };
    for (probes) |k| {
        if (try e.get(k, gpa)) |v| {
            defer gpa.free(v);
            try w.print("selftest: get(\"{s}\") -> \"{s}\"\n", .{ k, v });
        } else {
            try w.print("selftest: get(\"{s}\") -> null\n", .{k});
        }
    }
}

test "simple test" {
    const gpa = std.testing.allocator;
    var list: std.ArrayList(i32) = .empty;
    defer list.deinit(gpa);
    try list.append(gpa, 42);
    try std.testing.expectEqual(@as(i32, 42), list.pop());
}
