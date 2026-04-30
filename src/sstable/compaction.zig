//! SSTable compaction.
//!
//! Merges a list of source SSTable readers into one output SSTable buffer.
//! Sources are passed newest-first (`sources[0]` shadows `sources[1]`,
//! etc.). The merge is a k-way iterator over each source's `ScanIterator`;
//! ties on key go to the lowest source index (newest-wins). Tombstones are
//! preserved by default; `drop_tombstones` collapses them away — only safe
//! when the merge inputs cover every SSTable below the merge frontier
//! (e.g. full compaction of the entire engine).
//!
//! Pure local logic: takes `*Reader`s, returns a buffer. Engine-level
//! orchestration (atomic SSTable swap, manifest update) lives in
//! `engine/engine.zig`.

const std = @import("std");

const reader_mod = @import("reader.zig");
const writer_mod = @import("writer.zig");

pub const Error = error{
    OutOfMemory,
} || reader_mod.ReaderError || writer_mod.Error;

pub const Options = struct {
    target_block_size: u32 = 4096,
    expected_fp: f64 = 0.01,
    /// Drop tombstones during the merge. Only safe for FULL compaction —
    /// otherwise a dead tombstone could resurrect an older value sitting in
    /// an SSTable that was not part of this merge.
    drop_tombstones: bool = false,
};

pub const MergeIterator = struct {
    gpa: std.mem.Allocator,
    sources: []reader_mod.ScanIterator,
    /// Pre-fetched next entry from each source, or `null` after EOF.
    fronts: []?reader_mod.ScanEntry,

    /// Scratch buffers reused on every `next()` call to hold the winning
    /// entry's bytes for the duration of the caller's read window. Grow
    /// monotonically; freed in `deinit`.
    scratch_key: std.ArrayList(u8) = .empty,
    scratch_value: std.ArrayList(u8) = .empty,
    has_value: bool = false,

    pub fn init(gpa: std.mem.Allocator, sources: []reader_mod.ScanIterator) Error!MergeIterator {
        var fronts = try gpa.alloc(?reader_mod.ScanEntry, sources.len);
        errdefer gpa.free(fronts);
        for (sources, 0..) |*s, i| {
            fronts[i] = try s.next();
        }
        return .{
            .gpa = gpa,
            .sources = sources,
            .fronts = fronts,
        };
    }

    pub fn deinit(self: *MergeIterator) void {
        self.gpa.free(self.fronts);
        self.scratch_key.deinit(self.gpa);
        self.scratch_value.deinit(self.gpa);
        self.* = undefined;
    }

    /// Yields the next merged entry. Returned slices reference the
    /// iterator's internal scratch — valid until the next call to `next()`
    /// or `deinit()`.
    pub fn next(self: *MergeIterator) Error!?reader_mod.ScanEntry {
        // Find smallest-key non-null front. Ties broken by lowest index
        // (sources are newest-first → newer wins).
        var winner: ?usize = null;
        for (self.fronts, 0..) |f, i| {
            const fe = f orelse continue;
            if (winner == null) {
                winner = i;
                continue;
            }
            const cur = self.fronts[winner.?].?;
            switch (std.mem.order(u8, fe.key, cur.key)) {
                .lt => winner = i,
                // .eq: keep current (lower index = newer); .gt: keep current.
                .eq, .gt => {},
            }
        }
        const wi = winner orelse return null;
        const w = self.fronts[wi].?;

        // Snapshot winner into scratch BEFORE advancing any source — the
        // winner's slices live in source[wi]'s data block, which advance()
        // may free.
        self.scratch_key.clearRetainingCapacity();
        try self.scratch_key.appendSlice(self.gpa, w.key);

        if (w.value) |v| {
            self.scratch_value.clearRetainingCapacity();
            try self.scratch_value.appendSlice(self.gpa, v);
            self.has_value = true;
        } else {
            self.has_value = false;
        }

        // Advance every source whose front matches the winning key.
        for (self.fronts, 0..) |f, i| {
            const fe = f orelse continue;
            if (std.mem.eql(u8, fe.key, self.scratch_key.items)) {
                self.fronts[i] = try self.sources[i].next();
            }
        }

        return reader_mod.ScanEntry{
            .key = self.scratch_key.items,
            .value = if (self.has_value) self.scratch_value.items else null,
        };
    }
};

/// Compact `readers` (newest-first) into a single fresh SSTable buffer.
/// Caller owns the returned slice.
pub fn compact(
    gpa: std.mem.Allocator,
    readers: []const *reader_mod.Reader,
    options: Options,
) Error![]u8 {
    var scans = try gpa.alloc(reader_mod.ScanIterator, readers.len);
    defer gpa.free(scans);

    // Open every scan up-front so the merge can pre-fetch fronts.
    var opened: usize = 0;
    errdefer {
        var i: usize = 0;
        while (i < opened) : (i += 1) scans[i].deinit();
    }
    while (opened < readers.len) : (opened += 1) {
        scans[opened] = try reader_mod.openScan(readers[opened]);
    }

    var merge = try MergeIterator.init(gpa, scans);
    defer merge.deinit();

    var b = try writer_mod.Builder.init(gpa, .{
        .target_block_size = options.target_block_size,
        .expected_fp = options.expected_fp,
    });
    defer b.deinit();

    while (try merge.next()) |entry| {
        if (entry.value) |v| {
            try b.add(entry.key, v);
        } else {
            if (options.drop_tombstones) continue;
            try b.addTombstone(entry.key);
        }
    }

    // All scans must be deinit'd; merge.deinit() already freed fronts.
    var i: usize = 0;
    while (i < opened) : (i += 1) scans[i].deinit();
    opened = 0; // tell errdefer not to double-deinit on a successful path

    return b.finish();
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const testing = std.testing;
const blob = @import("../storage/blob.zig");

const TestSST = struct {
    bytes: []u8,
    storage: blob.MemoryStorage,
    reader: reader_mod.Reader,
};

const BuildEntry = struct {
    k: []const u8,
    v: ?[]const u8,
};

fn buildSST(gpa: std.mem.Allocator, entries: []const BuildEntry) !TestSST {
    var b = try writer_mod.Builder.init(gpa, .{ .expected_entries = entries.len + 1 });
    defer b.deinit();
    for (entries) |e| {
        if (e.v) |v| try b.add(e.k, v) else try b.addTombstone(e.k);
    }
    const bytes = try b.finish();
    var ms = blob.MemoryStorage.init(bytes);
    return .{ .bytes = bytes, .storage = ms, .reader = reader_mod.Reader.init(gpa, ms.storage()) };
}

fn closeSST(gpa: std.mem.Allocator, t: *TestSST) void {
    t.reader.deinit();
    gpa.free(t.bytes);
}

fn lookup(gpa: std.mem.Allocator, sst: []const u8, key: []const u8) !?reader_mod.Lookup {
    var ms = blob.MemoryStorage.init(sst);
    var r = reader_mod.Reader.init(gpa, ms.storage());
    defer r.deinit();
    return r.get(key, gpa);
}

test "compact: disjoint keys all survive" {
    const gpa = testing.allocator;
    var older = try buildSST(gpa, &.{ .{ .k = "alpha", .v = "1" }, .{ .k = "charlie", .v = "3" } });
    defer closeSST(gpa, &older);
    // re-bind storage.ptr after struct copy — MemoryStorage closes over
    // a self-reference, which the reader holds via a vtable thunk.
    older.reader.storage = older.storage.storage();

    var newer = try buildSST(gpa, &.{ .{ .k = "bravo", .v = "2" }, .{ .k = "delta", .v = "4" } });
    defer closeSST(gpa, &newer);
    newer.reader.storage = newer.storage.storage();

    const merged = try compact(gpa, &.{ &newer.reader, &older.reader }, .{});
    defer gpa.free(merged);

    const a = (try lookup(gpa, merged, "alpha")).?;
    defer gpa.free(a.present);
    try testing.expectEqualStrings("1", a.present);
    const b = (try lookup(gpa, merged, "bravo")).?;
    defer gpa.free(b.present);
    try testing.expectEqualStrings("2", b.present);
    const c = (try lookup(gpa, merged, "charlie")).?;
    defer gpa.free(c.present);
    try testing.expectEqualStrings("3", c.present);
    const d = (try lookup(gpa, merged, "delta")).?;
    defer gpa.free(d.present);
    try testing.expectEqualStrings("4", d.present);
}

test "compact: newer source wins on key collision" {
    const gpa = testing.allocator;
    var older = try buildSST(gpa, &.{ .{ .k = "k", .v = "OLD" } });
    defer closeSST(gpa, &older);
    older.reader.storage = older.storage.storage();

    var newer = try buildSST(gpa, &.{ .{ .k = "k", .v = "NEW" } });
    defer closeSST(gpa, &newer);
    newer.reader.storage = newer.storage.storage();

    const merged = try compact(gpa, &.{ &newer.reader, &older.reader }, .{});
    defer gpa.free(merged);

    const got = (try lookup(gpa, merged, "k")).?;
    defer gpa.free(got.present);
    try testing.expectEqualStrings("NEW", got.present);
}

test "compact: tombstone shadows older value (kept by default)" {
    const gpa = testing.allocator;
    var older = try buildSST(gpa, &.{ .{ .k = "k", .v = "alive" }, .{ .k = "z", .v = "Z" } });
    defer closeSST(gpa, &older);
    older.reader.storage = older.storage.storage();

    var newer = try buildSST(gpa, &.{ .{ .k = "k", .v = null } });
    defer closeSST(gpa, &newer);
    newer.reader.storage = newer.storage.storage();

    const merged = try compact(gpa, &.{ &newer.reader, &older.reader }, .{});
    defer gpa.free(merged);

    // Tombstone preserved.
    const tomb = (try lookup(gpa, merged, "k")).?;
    try testing.expectEqual(reader_mod.Lookup.tombstone, tomb);

    const z = (try lookup(gpa, merged, "z")).?;
    defer gpa.free(z.present);
    try testing.expectEqualStrings("Z", z.present);
}

test "compact: drop_tombstones removes the entry entirely (full compaction)" {
    const gpa = testing.allocator;
    var older = try buildSST(gpa, &.{ .{ .k = "k", .v = "alive" }, .{ .k = "z", .v = "Z" } });
    defer closeSST(gpa, &older);
    older.reader.storage = older.storage.storage();

    var newer = try buildSST(gpa, &.{ .{ .k = "k", .v = null } });
    defer closeSST(gpa, &newer);
    newer.reader.storage = newer.storage.storage();

    const merged = try compact(gpa, &.{ &newer.reader, &older.reader }, .{ .drop_tombstones = true });
    defer gpa.free(merged);

    // "k" should now resolve to null at the engine level (bloom + index
    // may or may not even reference it).
    const got = try lookup(gpa, merged, "k");
    if (got) |g| {
        // If the index lower-bound resolved to a neighboring block, the
        // scan must have found no match.
        try testing.expect(g != .tombstone);
        try testing.expect(g != .present);
    }

    const z = (try lookup(gpa, merged, "z")).?;
    defer gpa.free(z.present);
    try testing.expectEqualStrings("Z", z.present);
}

test "compact: 5 sources merge into one with all keys preserved" {
    const gpa = testing.allocator;

    // 5 SSTables, each with 4 keys; some keys overlap across tables.
    var ssts: [5]TestSST = undefined;
    const layouts = [_][]const BuildEntry{
        // Newest
        &.{ .{ .k = "key_0010", .v = "v0010-newest" }, .{ .k = "key_0050", .v = "v0050-newest" } },
        &.{ .{ .k = "key_0010", .v = "v0010-mid1" }, .{ .k = "key_0020", .v = "v0020-mid1" } },
        &.{ .{ .k = "key_0030", .v = "v0030-mid2" }, .{ .k = "key_0040", .v = "v0040-mid2" } },
        &.{ .{ .k = "key_0010", .v = "v0010-mid3" }, .{ .k = "key_0060", .v = "v0060-mid3" } },
        // Oldest
        &.{ .{ .k = "key_0010", .v = "v0010-oldest" }, .{ .k = "key_0070", .v = "v0070-oldest" } },
    };
    for (layouts, 0..) |layout, i| {
        ssts[i] = try buildSST(gpa, layout);
        ssts[i].reader.storage = ssts[i].storage.storage();
    }
    defer for (&ssts) |*t| closeSST(gpa, t);

    var readers: [5]*reader_mod.Reader = undefined;
    for (&ssts, 0..) |*t, i| readers[i] = &t.reader;

    const merged = try compact(gpa, &readers, .{ .target_block_size = 64, .drop_tombstones = true });
    defer gpa.free(merged);

    // Newest writer of key_0010 must win.
    const k10 = (try lookup(gpa, merged, "key_0010")).?;
    defer gpa.free(k10.present);
    try testing.expectEqualStrings("v0010-newest", k10.present);

    // Other keys present at their authoritative source.
    const cases = [_]struct { k: []const u8, want: []const u8 }{
        .{ .k = "key_0020", .want = "v0020-mid1" },
        .{ .k = "key_0030", .want = "v0030-mid2" },
        .{ .k = "key_0040", .want = "v0040-mid2" },
        .{ .k = "key_0050", .want = "v0050-newest" },
        .{ .k = "key_0060", .want = "v0060-mid3" },
        .{ .k = "key_0070", .want = "v0070-oldest" },
    };
    for (cases) |c| {
        const got = (try lookup(gpa, merged, c.k)).?;
        defer gpa.free(got.present);
        try testing.expectEqualStrings(c.want, got.present);
    }
}
