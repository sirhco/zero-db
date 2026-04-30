//! Google Cloud Storage client.
//!
//! Reads use HTTP GET against the GCS XML API
//! (`https://storage.googleapis.com/<bucket>/<object>`) with a
//! `Range: bytes=A-B` header so we fetch only the bytes the LSM read path
//! actually needs (footer → bloom → index → data block). Authentication via
//! a bearer token from `auth.zig`.
//!
//! This file currently lands the request-formation helpers — URL building
//! and Range header formatting — as pure functions so they can be unit
//! tested without spinning up a real HTTP server. The actual `rangeGet` HTTP
//! call is a thin wrapper around `std.http.Client` that uses these helpers;
//! end-to-end coverage requires either a fake-server harness or live GCS
//! credentials, both of which are out of scope for this phase.

const std = @import("std");

pub const Error = error{
    NotImplemented,
    EmptyRange,
    InvalidObject,
    HttpError,
    AuthFailed,
    OutOfMemory,
};

pub const DEFAULT_BASE_URL: []const u8 = "https://storage.googleapis.com";

pub const Options = struct {
    /// Override for tests / staging. Must NOT have a trailing slash.
    base_url: []const u8 = DEFAULT_BASE_URL,
};

pub const Client = struct {
    gpa: std.mem.Allocator,
    options: Options,

    pub fn init(gpa: std.mem.Allocator, options: Options) Client {
        return .{ .gpa = gpa, .options = options };
    }

    pub fn deinit(self: *Client) void {
        _ = self;
    }

    /// Issue `GET <base>/<bucket>/<encoded-object>` with a Range header for
    /// `[start, start+len)`, copying response bytes into `dst`. Returns the
    /// number of bytes actually written.
    ///
    /// Live HTTP is intentionally not yet wired here — gated until a fake
    /// server / live-creds test path lands. The current body returns
    /// `error.NotImplemented` so callers can compile against the final
    /// signature without us silently shipping a broken read path.
    pub fn rangeGet(
        self: *Client,
        bucket: []const u8,
        object: []const u8,
        start: u64,
        len: u32,
        dst: []u8,
    ) Error!usize {
        _ = self;
        _ = bucket;
        _ = object;
        _ = start;
        _ = len;
        _ = dst;
        return error.NotImplemented;
    }
};

// ---------------------------------------------------------------------------
// Pure helpers — testable without HTTP.
// ---------------------------------------------------------------------------

/// Append a percent-encoded form of `s` to `out`. Encodes per RFC 3986 with
/// the GCS-specific concession that `/` is preserved (object names use it
/// as a virtual path separator and GCS does not require it encoded).
pub fn appendUrlEncoded(out: *std.ArrayList(u8), gpa: std.mem.Allocator, s: []const u8) !void {
    for (s) |c| {
        if (isUnreserved(c) or c == '/') {
            try out.append(gpa, c);
        } else {
            try out.print(gpa, "%{X:0>2}", .{c});
        }
    }
}

fn isUnreserved(c: u8) bool {
    return (c >= 'A' and c <= 'Z') or
        (c >= 'a' and c <= 'z') or
        (c >= '0' and c <= '9') or
        c == '-' or c == '_' or c == '.' or c == '~';
}

/// Build the full GCS object URL: `<base>/<bucket>/<encoded(object)>`.
/// Returns an allocator-owned string the caller must free.
pub fn buildObjectUrl(
    gpa: std.mem.Allocator,
    base_url: []const u8,
    bucket: []const u8,
    object: []const u8,
) Error![]u8 {
    if (object.len == 0) return error.InvalidObject;
    if (object[0] == '/') return error.InvalidObject;

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(gpa);

    try out.appendSlice(gpa, base_url);
    try out.append(gpa, '/');
    try appendUrlEncoded(&out, gpa, bucket);
    try out.append(gpa, '/');
    try appendUrlEncoded(&out, gpa, object);
    return out.toOwnedSlice(gpa);
}

/// Format a `bytes=A-B` range value into `out`, where B is inclusive
/// (`start + len - 1`). Returns the slice of `out` actually written.
pub fn formatRangeHeader(out: []u8, start: u64, len: u32) Error![]const u8 {
    if (len == 0) return error.EmptyRange;
    const end_inclusive: u64 = start + len - 1;
    return std.fmt.bufPrint(out, "bytes={d}-{d}", .{ start, end_inclusive }) catch
        return error.OutOfMemory;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const testing = std.testing;

test "buildObjectUrl: simple case" {
    const gpa = testing.allocator;
    const url = try buildObjectUrl(gpa, DEFAULT_BASE_URL, "my-bucket", "path/to/object.sst");
    defer gpa.free(url);
    try testing.expectEqualStrings(
        "https://storage.googleapis.com/my-bucket/path/to/object.sst",
        url,
    );
}

test "buildObjectUrl: encodes special chars but preserves slashes" {
    const gpa = testing.allocator;
    const url = try buildObjectUrl(gpa, DEFAULT_BASE_URL, "bk", "tenants/acme corp/file?x.sst");
    defer gpa.free(url);
    try testing.expectEqualStrings(
        "https://storage.googleapis.com/bk/tenants/acme%20corp/file%3Fx.sst",
        url,
    );
}

test "buildObjectUrl: percent-encodes UTF-8 byte by byte" {
    const gpa = testing.allocator;
    // "café" in UTF-8 = 63 61 66 C3 A9
    const url = try buildObjectUrl(gpa, DEFAULT_BASE_URL, "b", "café.sst");
    defer gpa.free(url);
    try testing.expectEqualStrings(
        "https://storage.googleapis.com/b/caf%C3%A9.sst",
        url,
    );
}

test "buildObjectUrl: rejects empty / leading-slash object" {
    const gpa = testing.allocator;
    try testing.expectError(error.InvalidObject, buildObjectUrl(gpa, DEFAULT_BASE_URL, "b", ""));
    try testing.expectError(error.InvalidObject, buildObjectUrl(gpa, DEFAULT_BASE_URL, "b", "/oops"));
}

test "buildObjectUrl: respects custom base URL (test harness use)" {
    const gpa = testing.allocator;
    const url = try buildObjectUrl(gpa, "http://127.0.0.1:8080", "b", "x.sst");
    defer gpa.free(url);
    try testing.expectEqualStrings("http://127.0.0.1:8080/b/x.sst", url);
}

test "formatRangeHeader: standard inclusive range" {
    var buf: [64]u8 = undefined;
    const h = try formatRangeHeader(&buf, 0, 1024);
    try testing.expectEqualStrings("bytes=0-1023", h);
}

test "formatRangeHeader: 1-byte range" {
    var buf: [64]u8 = undefined;
    const h = try formatRangeHeader(&buf, 100, 1);
    try testing.expectEqualStrings("bytes=100-100", h);
}

test "formatRangeHeader: large offsets" {
    var buf: [64]u8 = undefined;
    const h = try formatRangeHeader(&buf, 1_000_000_000, 4096);
    try testing.expectEqualStrings("bytes=1000000000-1000004095", h);
}

test "formatRangeHeader: zero-length is rejected" {
    var buf: [64]u8 = undefined;
    try testing.expectError(error.EmptyRange, formatRangeHeader(&buf, 0, 0));
}

test "rangeGet: stub returns NotImplemented (placeholder until HTTP wired)" {
    var c = Client.init(testing.allocator, .{});
    defer c.deinit();
    var dst: [16]u8 = undefined;
    try testing.expectError(error.NotImplemented, c.rangeGet("b", "o", 0, 16, &dst));
}
