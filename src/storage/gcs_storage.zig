//! `blob.Storage` adapter that backs SSTable reads with a GCS object.
//!
//! Wraps a `gcs.Client` + (bucket, object, size) tuple. Each `rangeGet`
//! call on the `Storage` interface translates to one `Client.rangeGet`
//! against GCS. `size` is supplied at construction time because GCS
//! objects in Zero-DB are immutable: the engine learns each SSTable's
//! length when the writer hands the buffer to upload, and stamps it on
//! the manifest. Avoids a HEAD round-trip on the read path.
//!
//! Plug into `Reader` via `gcs_storage.GcsStorage.init(...).storage()`.

const std = @import("std");
const blob = @import("blob.zig");
const gcs = @import("gcs.zig");

pub const GcsStorage = struct {
    client: *gcs.Client,
    bucket: []const u8,
    object: []const u8,
    object_size: u64,

    pub fn init(client: *gcs.Client, bucket: []const u8, object: []const u8, object_size: u64) GcsStorage {
        return .{
            .client = client,
            .bucket = bucket,
            .object = object,
            .object_size = object_size,
        };
    }

    pub fn storage(self: *GcsStorage) blob.Storage {
        return .{
            .ptr = @ptrCast(self),
            .vtable = &vtable,
        };
    }

    const vtable: blob.Storage.VTable = .{
        .rangeGet = rangeGetImpl,
        .size = sizeImpl,
    };

    fn rangeGetImpl(ptr: *anyopaque, dst: []u8, start: u64, len: u32) anyerror!usize {
        const self: *GcsStorage = @ptrCast(@alignCast(ptr));
        return self.client.rangeGet(self.bucket, self.object, start, len, dst);
    }

    fn sizeImpl(ptr: *anyopaque) anyerror!u64 {
        const self: *GcsStorage = @ptrCast(@alignCast(ptr));
        return self.object_size;
    }
};

// ---------------------------------------------------------------------------
// Tests — drive the full Reader → GcsStorage → Client → FakeServer chain.
// ---------------------------------------------------------------------------

const testing = std.testing;
const writer_mod = @import("../sstable/writer.zig");
const reader_mod = @import("../sstable/reader.zig");

test "GcsStorage: Reader resolves keys via GCS round-trip (FakeServer)" {
    const gpa = testing.allocator;

    // Build an SSTable buffer in memory.
    var b = try writer_mod.Builder.init(gpa, .{ .expected_entries = 8 });
    defer b.deinit();
    try b.add("alpha", "AAA");
    try b.add("bravo", "BB");
    try b.add("charlie", "CCCC");
    try b.add("delta", "DDDDD");
    const sst = try b.finish();
    defer gpa.free(sst);

    // Stand the buffer up as a fake GCS object.
    var fs = gcs.FakeServer.init(gpa, gcs.DEFAULT_BASE_URL, "test-bucket", "sstables/0001.sst", sst);
    defer fs.deinit();
    var client = gcs.Client.init(gpa, fs.transport(), .{});
    defer client.deinit();

    var storage = GcsStorage.init(&client, "test-bucket", "sstables/0001.sst", sst.len);
    var r = reader_mod.Reader.init(gpa, storage.storage());
    defer r.deinit();

    const a = (try r.get("alpha", gpa)).?;
    defer gpa.free(a.present);
    try testing.expectEqualStrings("AAA", a.present);

    const c = (try r.get("charlie", gpa)).?;
    defer gpa.free(c.present);
    try testing.expectEqualStrings("CCCC", c.present);

    try testing.expectEqual(@as(?reader_mod.Lookup, null), try r.get("zzz", gpa));

    // Each lookup should have exercised the URL builder + Range header at
    // least once.
    try testing.expect(fs.call_count >= 4); // footer + bloom + index + at least one data block
    try testing.expect(fs.last_range_header_seen);
    try testing.expectEqualStrings(
        "https://storage.googleapis.com/test-bucket/sstables/0001.sst",
        fs.last_url.?,
    );
}

test "GcsStorage: tombstone resolves correctly through fake GCS" {
    const gpa = testing.allocator;

    var b = try writer_mod.Builder.init(gpa, .{ .expected_entries = 4 });
    defer b.deinit();
    try b.add("alpha", "alive");
    try b.addTombstone("bravo");
    try b.add("charlie", "alive");
    const sst = try b.finish();
    defer gpa.free(sst);

    var fs = gcs.FakeServer.init(gpa, gcs.DEFAULT_BASE_URL, "b", "o", sst);
    defer fs.deinit();
    var client = gcs.Client.init(gpa, fs.transport(), .{});
    defer client.deinit();

    var storage = GcsStorage.init(&client, "b", "o", sst.len);
    var r = reader_mod.Reader.init(gpa, storage.storage());
    defer r.deinit();

    const tomb = (try r.get("bravo", gpa)).?;
    try testing.expectEqual(reader_mod.Lookup.tombstone, tomb);
}

test "GcsStorage: bearer token plumbs through Client to FakeServer" {
    const gpa = testing.allocator;

    var b = try writer_mod.Builder.init(gpa, .{ .expected_entries = 2 });
    defer b.deinit();
    try b.add("alpha", "AAA");
    const sst = try b.finish();
    defer gpa.free(sst);

    var fs = gcs.FakeServer.init(gpa, gcs.DEFAULT_BASE_URL, "b", "o", sst);
    defer fs.deinit();
    fs.expected_token = "real-token-xyz";
    var client = gcs.Client.init(gpa, fs.transport(), .{ .bearer_token = "real-token-xyz" });
    defer client.deinit();

    var storage = GcsStorage.init(&client, "b", "o", sst.len);
    var r = reader_mod.Reader.init(gpa, storage.storage());
    defer r.deinit();

    const a = (try r.get("alpha", gpa)).?;
    defer gpa.free(a.present);
    try testing.expectEqualStrings("AAA", a.present);

    try testing.expect(fs.last_auth_seen != null);
    try testing.expectEqualStrings("Bearer real-token-xyz", fs.last_auth_seen.?);
}

test "GcsStorage: missing token surfaces as ReaderError on first metadata fetch" {
    const gpa = testing.allocator;

    var b = try writer_mod.Builder.init(gpa, .{ .expected_entries = 2 });
    defer b.deinit();
    try b.add("k", "v");
    const sst = try b.finish();
    defer gpa.free(sst);

    var fs = gcs.FakeServer.init(gpa, gcs.DEFAULT_BASE_URL, "b", "o", sst);
    defer fs.deinit();
    fs.expected_token = "secret";
    var client = gcs.Client.init(gpa, fs.transport(), .{}); // no token configured
    defer client.deinit();

    var storage = GcsStorage.init(&client, "b", "o", sst.len);
    var r = reader_mod.Reader.init(gpa, storage.storage());
    defer r.deinit();

    // The footer fetch is the first network call; with no token, FakeServer
    // returns 401 → Client returns AuthFailed → Reader's loadFooter wraps
    // it as BadFooter (we narrow `anyerror` from the storage seam).
    try testing.expectError(error.BadFooter, r.get("k", gpa));
}
