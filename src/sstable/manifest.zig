//! Live-SSTable manifest: a single GCS object listing every flushed
//! SSTable so a fresh process can reconstruct its read set on cold start.
//!
//! On disk the manifest is a JSON object `{ "version": 1, "sstables": [...] }`.
//! JSON because the manifest is not on the hot path (one read on init, one
//! rewrite per flush) and human-readable diagnostics on a stuck instance
//! are worth the cost.
//!
//! Atomic semantics: the manifest is rewritten by `putObject` on each
//! flush. GCS object writes are atomic per object, so a reader either
//! sees the old or the new manifest, never a torn version. This assumes a
//! single writer; multi-writer Cloud Run instances must add
//! `x-goog-if-generation-match` preconditions — out of scope for v0.

const std = @import("std");

pub const Error = error{
    OutOfMemory,
    UnsupportedManifestVersion,
    BadJson,
};

pub const CURRENT_VERSION: u32 = 1;

/// One row in the manifest. All slices are owned by the parent `Manifest`.
pub const Entry = struct {
    id: u64,
    object_path: []const u8,
    size_bytes: u64,
    key_min: []const u8,
    key_max: []const u8,
    created_at_ms: i64,
};

pub const Manifest = struct {
    gpa: std.mem.Allocator,
    version: u32 = CURRENT_VERSION,
    entries: std.ArrayList(Entry),

    pub fn init(gpa: std.mem.Allocator) Manifest {
        return .{ .gpa = gpa, .entries = .empty };
    }

    pub fn deinit(self: *Manifest) void {
        for (self.entries.items) |e| {
            self.gpa.free(e.object_path);
            self.gpa.free(e.key_min);
            self.gpa.free(e.key_max);
        }
        self.entries.deinit(self.gpa);
        self.* = undefined;
    }

    /// Append a copy of `entry`. Strings are duped into `self.gpa`.
    pub fn append(self: *Manifest, entry: Entry) error{OutOfMemory}!void {
        const op_copy = try self.gpa.dupe(u8, entry.object_path);
        errdefer self.gpa.free(op_copy);
        const kmin_copy = try self.gpa.dupe(u8, entry.key_min);
        errdefer self.gpa.free(kmin_copy);
        const kmax_copy = try self.gpa.dupe(u8, entry.key_max);
        errdefer self.gpa.free(kmax_copy);

        try self.entries.append(self.gpa, .{
            .id = entry.id,
            .object_path = op_copy,
            .size_bytes = entry.size_bytes,
            .key_min = kmin_copy,
            .key_max = kmax_copy,
            .created_at_ms = entry.created_at_ms,
        });
    }

    /// Remove the entry with `id`. Returns whether one was removed.
    pub fn removeId(self: *Manifest, id: u64) bool {
        for (self.entries.items, 0..) |e, i| {
            if (e.id == id) {
                self.gpa.free(e.object_path);
                self.gpa.free(e.key_min);
                self.gpa.free(e.key_max);
                _ = self.entries.orderedRemove(i);
                return true;
            }
        }
        return false;
    }

    /// Serialize the manifest to JSON bytes. Caller frees the returned slice.
    pub fn serialize(self: *const Manifest) Error![]u8 {
        const Wire = struct {
            version: u32,
            sstables: []const Entry,
        };
        const wire = Wire{ .version = self.version, .sstables = self.entries.items };
        return std.json.Stringify.valueAlloc(self.gpa, wire, .{}) catch error.OutOfMemory;
    }

    /// Parse manifest bytes. Returned manifest owns its strings and must
    /// be `deinit`ed.
    pub fn parse(gpa: std.mem.Allocator, bytes: []const u8) Error!Manifest {
        const Wire = struct {
            version: u32 = CURRENT_VERSION,
            sstables: []const Entry = &.{},
        };
        var parsed = std.json.parseFromSlice(
            Wire,
            gpa,
            bytes,
            .{ .ignore_unknown_fields = true },
        ) catch return error.BadJson;
        defer parsed.deinit();

        if (parsed.value.version != CURRENT_VERSION) return error.UnsupportedManifestVersion;

        var m = Manifest.init(gpa);
        errdefer m.deinit();
        for (parsed.value.sstables) |e| {
            try m.append(e);
        }
        return m;
    }
};

const testing = std.testing;

test "Manifest: empty roundtrip" {
    const gpa = testing.allocator;
    var m = Manifest.init(gpa);
    defer m.deinit();
    const bytes = try m.serialize();
    defer gpa.free(bytes);

    var parsed = try Manifest.parse(gpa, bytes);
    defer parsed.deinit();
    try testing.expectEqual(@as(u32, 1), parsed.version);
    try testing.expectEqual(@as(usize, 0), parsed.entries.items.len);
}

test "Manifest: roundtrips a 3-entry payload" {
    const gpa = testing.allocator;
    var m = Manifest.init(gpa);
    defer m.deinit();
    try m.append(.{
        .id = 1,
        .object_path = "sstables/000001.sst",
        .size_bytes = 4096,
        .key_min = "alpha",
        .key_max = "yankee",
        .created_at_ms = 1714752000000,
    });
    try m.append(.{
        .id = 2,
        .object_path = "sstables/000002.sst",
        .size_bytes = 8192,
        .key_min = "alpha",
        .key_max = "zulu",
        .created_at_ms = 1714752100000,
    });
    try m.append(.{
        .id = 3,
        .object_path = "sstables/000003.sst",
        .size_bytes = 16384,
        .key_min = "bravo",
        .key_max = "x-ray",
        .created_at_ms = 1714752200000,
    });

    const bytes = try m.serialize();
    defer gpa.free(bytes);

    var parsed = try Manifest.parse(gpa, bytes);
    defer parsed.deinit();
    try testing.expectEqual(@as(usize, 3), parsed.entries.items.len);
    try testing.expectEqual(@as(u64, 1), parsed.entries.items[0].id);
    try testing.expectEqualStrings("sstables/000002.sst", parsed.entries.items[1].object_path);
    try testing.expectEqualStrings("bravo", parsed.entries.items[2].key_min);
    try testing.expectEqual(@as(i64, 1714752200000), parsed.entries.items[2].created_at_ms);
}

test "Manifest: parses a hand-written fixture (catches format drift)" {
    const gpa = testing.allocator;
    const fixture =
        \\{"version":1,"sstables":[{"id":42,"object_path":"sstables/42.sst","size_bytes":1024,"key_min":"a","key_max":"z","created_at_ms":1700000000000}]}
    ;
    var parsed = try Manifest.parse(gpa, fixture);
    defer parsed.deinit();
    try testing.expectEqual(@as(usize, 1), parsed.entries.items.len);
    try testing.expectEqual(@as(u64, 42), parsed.entries.items[0].id);
    try testing.expectEqualStrings("sstables/42.sst", parsed.entries.items[0].object_path);
    try testing.expectEqual(@as(u64, 1024), parsed.entries.items[0].size_bytes);
}

test "Manifest: rejects version != 1" {
    const gpa = testing.allocator;
    const bad =
        \\{"version":2,"sstables":[]}
    ;
    try testing.expectError(error.UnsupportedManifestVersion, Manifest.parse(gpa, bad));
}

test "Manifest: rejects malformed JSON" {
    const gpa = testing.allocator;
    try testing.expectError(error.BadJson, Manifest.parse(gpa, "{not json"));
}

test "Manifest: removeId drops the matching row" {
    const gpa = testing.allocator;
    var m = Manifest.init(gpa);
    defer m.deinit();
    try m.append(.{ .id = 1, .object_path = "a", .size_bytes = 1, .key_min = "a", .key_max = "b", .created_at_ms = 0 });
    try m.append(.{ .id = 2, .object_path = "b", .size_bytes = 1, .key_min = "a", .key_max = "b", .created_at_ms = 0 });
    try testing.expect(m.removeId(1));
    try testing.expectEqual(@as(usize, 1), m.entries.items.len);
    try testing.expectEqual(@as(u64, 2), m.entries.items[0].id);
    try testing.expect(!m.removeId(99));
}
