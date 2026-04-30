//! Top-level Zero-DB Engine.
//!
//! Wires the LSM tiers together:
//!   write path: set/delete → active MemTable → freeze when full → flush
//!               to immutable SSTable buffer (via writer.writeFromMemTable)
//!   read path:  active MemTable → frozen MemTables (newest→oldest)
//!               → SSTables (newest→oldest); first hit wins, tombstones
//!               shadow lower tiers and surface as `null`.
//!
//! Storage is currently in-memory: each flushed SSTable lives as a byte
//! buffer plus a `MemoryStorage` + `Reader` pair owned by the engine. The
//! `Storage` interface is the seam at which a `GcsStorage` impl drops in
//! later — the engine itself doesn't change.
//!
//! Concurrency: single-writer, multi-reader is the eventual target; this
//! version assumes the caller serializes all entry points (Cloud Run
//! request handler is single-request-per-handler, so this is fine for v0).

const std = @import("std");

const memtable_mod = @import("memtable.zig");
const writer = @import("../sstable/writer.zig");
const reader_mod = @import("../sstable/reader.zig");
const blob = @import("../storage/blob.zig");

pub const MemTable = memtable_mod.MemTable;

pub const Error = error{
    OutOfMemory,
    Frozen,
} || writer.Error || reader_mod.ReaderError;

pub const Options = struct {
    /// Active MemTable size (key+value bytes) at which the engine freezes
    /// it and flushes to a new SSTable. Default is intentionally small for
    /// dev — production would tune via per-tenant config.
    memtable_flush_bytes: usize = 4 * 1024 * 1024,
    /// Target data-block size inside flushed SSTables.
    target_block_size: u32 = 4096,
    /// Bloom filter target false-positive rate.
    expected_fp: f64 = 0.01,
};

pub const Engine = struct {
    gpa: std.mem.Allocator,
    options: Options,

    /// Mutable MemTable accepting writes.
    active: MemTable,

    /// Frozen MemTables awaiting flush. Currently drained synchronously on
    /// the same call that freezes the active table; held as a list so a
    /// future async flush worker can pull from here.
    frozen_tables: std.ArrayList(*MemTable) = .empty,

    /// SSTables in newest-first order — index 0 = newest, last = oldest.
    /// Get walks from index 0 toward the tail.
    sstables: std.ArrayList(SSTableHandle) = .empty,

    next_sstable_id: u64 = 0,

    pub fn init(gpa: std.mem.Allocator, options: Options) !Engine {
        return .{
            .gpa = gpa,
            .options = options,
            .active = try MemTable.init(gpa),
        };
    }

    pub fn deinit(self: *Engine) void {
        for (self.sstables.items) |*h| self.closeSSTable(h);
        self.sstables.deinit(self.gpa);

        for (self.frozen_tables.items) |ft| {
            ft.deinit();
            self.gpa.destroy(ft);
        }
        self.frozen_tables.deinit(self.gpa);

        self.active.deinit();
        self.* = undefined;
    }

    // ---- write path -------------------------------------------------------

    pub fn set(self: *Engine, key: []const u8, value: []const u8) Error!void {
        try self.active.put(key, value);
        try self.maybeFlush();
    }

    pub fn delete(self: *Engine, key: []const u8) Error!void {
        try self.active.putTombstone(key);
        try self.maybeFlush();
    }

    /// Force a flush of the active MemTable regardless of size. Useful for
    /// tests and for graceful shutdown.
    pub fn flush(self: *Engine) Error!void {
        if (self.active.entry_count == 0 and self.frozen_tables.items.len == 0) return;
        try self.flushActive();
    }

    fn maybeFlush(self: *Engine) Error!void {
        if (self.active.bytes_used >= self.options.memtable_flush_bytes) {
            try self.flushActive();
        }
    }

    fn flushActive(self: *Engine) Error!void {
        self.active.freeze();

        // Move the active table into the frozen list. We heap-allocate a
        // new MemTable slot so the frozen list stores stable pointers — a
        // future async worker can hold one of these without disturbing the
        // engine's other state.
        const moved = try self.gpa.create(MemTable);
        moved.* = self.active;
        errdefer self.gpa.destroy(moved);

        try self.frozen_tables.append(self.gpa, moved);

        self.active = try MemTable.init(self.gpa);

        try self.drainFrozenTables();
    }

    fn drainFrozenTables(self: *Engine) Error!void {
        // Flush in-order. For a synchronous engine this preserves the
        // freeze ordering; the resulting SSTables are then prepended to
        // `sstables` so newest sits at index 0.
        for (self.frozen_tables.items) |ft| {
            const sst_bytes = writer.writeFromMemTable(self.gpa, ft, .{
                .target_block_size = self.options.target_block_size,
                .expected_fp = self.options.expected_fp,
            }) catch |err| switch (err) {
                error.OutOfMemory => return error.OutOfMemory,
                error.UnsortedKeys => unreachable, // MemTable iterator is sorted
                error.KeyTooLong => return error.KeyTooLong,
                error.TooManyEntries => return error.TooManyEntries,
                error.HeapTooLarge => return error.HeapTooLarge,
            };
            errdefer self.gpa.free(sst_bytes);

            const handle = try self.openSSTable(sst_bytes);
            // Insert at index 0 so newest is first.
            try self.sstables.insert(self.gpa, 0, handle);

            ft.deinit();
            self.gpa.destroy(ft);
        }
        self.frozen_tables.clearRetainingCapacity();
    }

    // ---- read path --------------------------------------------------------

    /// Look up `key`. Returns:
    ///   - the value bytes copied into a buffer owned by `out_gpa`, OR
    ///   - `null` (key never inserted, or was explicitly deleted by a
    ///     tombstone in some tier above the most recent insert).
    pub fn get(self: *Engine, key: []const u8, out_gpa: std.mem.Allocator) Error!?[]u8 {
        // 1. Active MemTable.
        if (self.active.get(key)) |hit| {
            return try resolveMemTableHit(hit, out_gpa);
        }

        // 2. Frozen MemTables, newest first.
        var i = self.frozen_tables.items.len;
        while (i > 0) {
            i -= 1;
            if (self.frozen_tables.items[i].get(key)) |hit| {
                return try resolveMemTableHit(hit, out_gpa);
            }
        }

        // 3. SSTables in newest-first order.
        for (self.sstables.items) |*h| {
            if (try h.reader.get(key, out_gpa)) |hit| {
                return resolveSSTableHit(hit);
            }
        }

        return null;
    }

    pub fn entryCountActive(self: *const Engine) u32 {
        return self.active.entry_count;
    }

    pub fn sstableCount(self: *const Engine) usize {
        return self.sstables.items.len;
    }

    // ---- internals --------------------------------------------------------

    const SSTableHandle = struct {
        id: u64,
        bytes: []u8,
        storage: *blob.MemoryStorage,
        reader: *reader_mod.Reader,
    };

    fn openSSTable(self: *Engine, sst_bytes: []u8) Error!SSTableHandle {
        const ms = try self.gpa.create(blob.MemoryStorage);
        errdefer self.gpa.destroy(ms);
        ms.* = blob.MemoryStorage.init(sst_bytes);

        const r = try self.gpa.create(reader_mod.Reader);
        errdefer self.gpa.destroy(r);
        r.* = reader_mod.Reader.init(self.gpa, ms.storage());

        const id = self.next_sstable_id;
        self.next_sstable_id += 1;
        return .{ .id = id, .bytes = sst_bytes, .storage = ms, .reader = r };
    }

    fn closeSSTable(self: *Engine, h: *SSTableHandle) void {
        h.reader.deinit();
        self.gpa.destroy(h.reader);
        self.gpa.destroy(h.storage);
        self.gpa.free(h.bytes);
    }
};

fn resolveMemTableHit(hit: memtable_mod.Lookup, out_gpa: std.mem.Allocator) !?[]u8 {
    return switch (hit) {
        .present => |v| try out_gpa.dupe(u8, v),
        .tombstone => null,
    };
}

fn resolveSSTableHit(hit: reader_mod.Lookup) ?[]u8 {
    return switch (hit) {
        // Reader.get already allocated the value bytes from out_gpa.
        .present => |v| v,
        .tombstone => null,
    };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const testing = std.testing;

test "Engine: set/get against active MemTable (no flush)" {
    const gpa = testing.allocator;
    var e = try Engine.init(gpa, .{});
    defer e.deinit();

    try e.set("alpha", "AAA");
    try e.set("bravo", "BB");

    const a = (try e.get("alpha", gpa)).?;
    defer gpa.free(a);
    try testing.expectEqualStrings("AAA", a);

    const b = (try e.get("bravo", gpa)).?;
    defer gpa.free(b);
    try testing.expectEqualStrings("BB", b);

    try testing.expectEqual(@as(?[]u8, null), try e.get("ghost", gpa));
}

test "Engine: explicit flush moves data to SSTable, get still works" {
    const gpa = testing.allocator;
    var e = try Engine.init(gpa, .{});
    defer e.deinit();

    try e.set("alpha", "AAA");
    try e.set("bravo", "BB");
    try e.set("charlie", "CCC");
    try e.flush();

    try testing.expectEqual(@as(usize, 1), e.sstableCount());
    try testing.expectEqual(@as(u32, 0), e.entryCountActive());

    const a = (try e.get("alpha", gpa)).?;
    defer gpa.free(a);
    try testing.expectEqualStrings("AAA", a);

    const c = (try e.get("charlie", gpa)).?;
    defer gpa.free(c);
    try testing.expectEqualStrings("CCC", c);
}

test "Engine: delete tombstone shadows previous SSTable value" {
    const gpa = testing.allocator;
    var e = try Engine.init(gpa, .{});
    defer e.deinit();

    try e.set("k", "v1");
    try e.flush();

    // Tombstone in active MemTable now masks the SSTable value.
    try e.delete("k");
    try testing.expectEqual(@as(?[]u8, null), try e.get("k", gpa));

    // After flushing the tombstone too: still null (tombstone now lives in
    // a newer SSTable, walked first).
    try e.flush();
    try testing.expectEqual(@as(?[]u8, null), try e.get("k", gpa));
    try testing.expectEqual(@as(usize, 2), e.sstableCount());
}

test "Engine: newer SSTable value wins over older value" {
    const gpa = testing.allocator;
    var e = try Engine.init(gpa, .{});
    defer e.deinit();

    try e.set("k", "old");
    try e.flush();

    try e.set("k", "new");
    try e.flush();

    const got = (try e.get("k", gpa)).?;
    defer gpa.free(got);
    try testing.expectEqualStrings("new", got);
    try testing.expectEqual(@as(usize, 2), e.sstableCount());
}

test "Engine: read path crosses active + multiple SSTables" {
    const gpa = testing.allocator;
    var e = try Engine.init(gpa, .{});
    defer e.deinit();

    // Tier 0 (oldest SSTable): a, b, c
    try e.set("a", "1");
    try e.set("b", "2");
    try e.set("c", "3");
    try e.flush();

    // Tier 1 (newer SSTable): d, e, override b
    try e.set("d", "4");
    try e.set("e", "5");
    try e.set("b", "2-NEW");
    try e.flush();

    // Tier 2 (active MemTable): f, override c
    try e.set("f", "6");
    try e.set("c", "3-NEW");

    const cases = [_]struct { k: []const u8, want: []const u8 }{
        .{ .k = "a", .want = "1" }, // oldest tier
        .{ .k = "b", .want = "2-NEW" }, // updated in middle tier
        .{ .k = "c", .want = "3-NEW" }, // updated in active
        .{ .k = "d", .want = "4" },
        .{ .k = "e", .want = "5" },
        .{ .k = "f", .want = "6" }, // only in active
    };
    for (cases) |c| {
        const got = (try e.get(c.k, gpa)).?;
        defer gpa.free(got);
        try testing.expectEqualStrings(c.want, got);
    }

    try testing.expectEqual(@as(?[]u8, null), try e.get("nonexistent", gpa));
}

test "Engine: auto-flush triggers when memtable_flush_bytes is exceeded" {
    const gpa = testing.allocator;
    // Pick a tiny threshold so a few writes trip the flush.
    var e = try Engine.init(gpa, .{ .memtable_flush_bytes = 64 });
    defer e.deinit();

    var i: u32 = 0;
    while (i < 10) : (i += 1) {
        var kbuf: [16]u8 = undefined;
        const k = try std.fmt.bufPrint(&kbuf, "k_{d:0>4}", .{i});
        try e.set(k, "value-bytes");
    }

    try testing.expect(e.sstableCount() > 0);

    // Every key still resolvable.
    var j: u32 = 0;
    while (j < 10) : (j += 1) {
        var kbuf: [16]u8 = undefined;
        const k = try std.fmt.bufPrint(&kbuf, "k_{d:0>4}", .{j});
        const got = (try e.get(k, gpa)).?;
        defer gpa.free(got);
        try testing.expectEqualStrings("value-bytes", got);
    }
}

test "Engine: tombstone in older SSTable is overridden by later set" {
    const gpa = testing.allocator;
    var e = try Engine.init(gpa, .{});
    defer e.deinit();

    try e.set("k", "first");
    try e.flush();

    try e.delete("k");
    try e.flush();

    // Tombstone is in the newer SSTable; key is "deleted".
    try testing.expectEqual(@as(?[]u8, null), try e.get("k", gpa));

    // Now resurrect it with a new put + flush.
    try e.set("k", "resurrected");
    try e.flush();

    const got = (try e.get("k", gpa)).?;
    defer gpa.free(got);
    try testing.expectEqualStrings("resurrected", got);
    try testing.expectEqual(@as(usize, 3), e.sstableCount());
}

test "Engine: empty engine returns null for any get" {
    const gpa = testing.allocator;
    var e = try Engine.init(gpa, .{});
    defer e.deinit();
    try testing.expectEqual(@as(?[]u8, null), try e.get("anything", gpa));
}

test "Engine: flush is a no-op when nothing has been written" {
    const gpa = testing.allocator;
    var e = try Engine.init(gpa, .{});
    defer e.deinit();
    try e.flush();
    try testing.expectEqual(@as(usize, 0), e.sstableCount());
}
