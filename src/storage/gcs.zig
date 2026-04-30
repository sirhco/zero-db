//! Google Cloud Storage client.
//!
//! Backed by `std.http.Client`. Issues GET requests with `Range: bytes=A-B`
//! headers against the GCS XML API endpoint
//! (`https://storage.googleapis.com/<bucket>/<object>`). Authentication via
//! a bearer token from `auth.zig`.
//!
//! Stub: signatures only.

const std = @import("std");

pub const Error = error{
    NotImplemented,
    HttpError,
    AuthFailed,
};

pub const Client = struct {
    gpa: std.mem.Allocator,

    pub fn init(gpa: std.mem.Allocator) Client {
        return .{ .gpa = gpa };
    }

    pub fn deinit(self: *Client) void {
        _ = self;
    }

    /// Fetch `[start, start+len)` of the object into `dst`.
    /// Returns the number of bytes actually written (may be < len at EOF).
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
        return Error.NotImplemented;
    }
};

test "gcs client type compiles" {
    _ = Client;
}
