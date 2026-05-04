//! Cloud Run / GCE bearer-token fetch from the metadata service.
//!
//! `http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token`
//! returns `{"access_token":"...","expires_in":3599,"token_type":"Bearer"}`.
//! TokenSource caches the token across calls and refreshes when within
//! REFRESH_LEAD_SECONDS of expiry, so a hot-path request never blocks on
//! a metadata-service round trip.

const std = @import("std");
const gcs = @import("gcs.zig");

pub const Error = error{
    OutOfMemory,
    HttpError,
    BadStatus,
    BadJson,
    MissingAccessToken,
    MissingExpiresIn,
};

pub const DEFAULT_METADATA_URL: []const u8 =
    "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token";
pub const METADATA_FLAVOR_HEADER: gcs.Header = .{ .name = "Metadata-Flavor", .value = "Google" };

/// Refresh the token this many seconds before its actual expiry. Keeps a
/// hot-path request from racing the metadata service when the token is
/// nearly stale.
pub const REFRESH_LEAD_SECONDS: i64 = 60;

pub const Clock = struct {
    ptr: *anyopaque,
    nowFn: *const fn (ptr: *anyopaque) i64,

    pub fn now(self: Clock) i64 {
        return self.nowFn(self.ptr);
    }
};

/// Default clock backed by `posix.clock_gettime(REALTIME)` (whole seconds
/// since the unix epoch). Bypasses `std.Io` so it can run in any context.
pub const SystemClock = struct {
    pub fn clock() Clock {
        const Wrap = struct {
            fn nowImpl(_: *anyopaque) i64 {
                var ts: std.posix.timespec = undefined;
                _ = std.posix.system.clock_gettime(.REALTIME, &ts);
                return @intCast(ts.sec);
            }
        };
        return .{ .ptr = undefined, .nowFn = Wrap.nowImpl };
    }
};

pub const TokenSource = struct {
    gpa: std.mem.Allocator,
    transport: gcs.HttpTransport,
    metadata_url: []const u8,
    clock: Clock,

    cached_token: ?[]u8 = null,
    expires_at_unix_seconds: i64 = 0,

    pub fn init(
        gpa: std.mem.Allocator,
        transport: gcs.HttpTransport,
        metadata_url: []const u8,
        clock: Clock,
    ) TokenSource {
        return .{
            .gpa = gpa,
            .transport = transport,
            .metadata_url = metadata_url,
            .clock = clock,
        };
    }

    pub fn deinit(self: *TokenSource) void {
        if (self.cached_token) |t| self.gpa.free(t);
        self.* = undefined;
    }

    /// Returns the current access token, fetching from the metadata service
    /// if no token is cached or the cached one is within
    /// REFRESH_LEAD_SECONDS of expiry. Caller does NOT free; lifetime is
    /// the next `token` call or `deinit`.
    pub fn token(self: *TokenSource) Error![]const u8 {
        const now = self.clock.now();
        if (self.cached_token) |t| {
            if (now + REFRESH_LEAD_SECONDS < self.expires_at_unix_seconds) {
                return t;
            }
        }
        return self.refresh(now);
    }

    fn refresh(self: *TokenSource, now: i64) Error![]const u8 {
        var body_buf: [4096]u8 = undefined;
        const headers = [_]gcs.Header{METADATA_FLAVOR_HEADER};
        const r = self.transport.get(self.metadata_url, &headers, &body_buf) catch
            return error.HttpError;
        if (r.status != 200) return error.BadStatus;

        const TokenJson = struct {
            access_token: []const u8 = "",
            expires_in: i64 = 0,
        };
        var parsed = std.json.parseFromSlice(
            TokenJson,
            self.gpa,
            body_buf[0..r.body_len],
            .{ .ignore_unknown_fields = true },
        ) catch return error.BadJson;
        defer parsed.deinit();

        if (parsed.value.access_token.len == 0) return error.MissingAccessToken;
        if (parsed.value.expires_in <= 0) return error.MissingExpiresIn;

        const new_token = try self.gpa.dupe(u8, parsed.value.access_token);
        if (self.cached_token) |old| self.gpa.free(old);
        self.cached_token = new_token;
        self.expires_at_unix_seconds = now + parsed.value.expires_in;
        return new_token;
    }
};

const testing = std.testing;

const TestClock = struct {
    fixed_now: i64 = 1_700_000_000,

    pub fn clock(self: *TestClock) Clock {
        const Wrap = struct {
            fn nowImpl(p: *anyopaque) i64 {
                const tc: *TestClock = @ptrCast(@alignCast(p));
                return tc.fixed_now;
            }
        };
        return .{ .ptr = @ptrCast(self), .nowFn = Wrap.nowImpl };
    }
};

/// Test-side `HttpTransport` that returns a canned JSON body. Verifies the
/// `Metadata-Flavor: Google` header is present on every request — missing it
/// is the most common GCE-metadata mistake worth catching at the seam.
const TokenServer = struct {
    gpa: std.mem.Allocator,
    body: []const u8,
    status: u16 = 200,
    call_count: u32 = 0,

    pub fn transport(self: *TokenServer) gcs.HttpTransport {
        return .{ .ptr = @ptrCast(self), .vtable = &vtable };
    }

    const vtable: gcs.HttpTransport.VTable = .{ .get = getImpl };

    fn getImpl(
        ptr: *anyopaque,
        url: []const u8,
        headers: []const gcs.Header,
        body_dst: []u8,
    ) anyerror!gcs.Response {
        const self: *TokenServer = @ptrCast(@alignCast(ptr));
        self.call_count += 1;
        _ = url;
        var saw_flavor = false;
        for (headers) |h| {
            if (std.ascii.eqlIgnoreCase(h.name, "Metadata-Flavor") and
                std.mem.eql(u8, h.value, "Google")) saw_flavor = true;
        }
        if (!saw_flavor) return .{ .status = 400, .body_len = 0 };
        const n = @min(body_dst.len, self.body.len);
        @memcpy(body_dst[0..n], self.body[0..n]);
        return .{ .status = self.status, .body_len = n };
    }
};

test "TokenSource: first call fetches from metadata, caches, returns access_token" {
    const gpa = testing.allocator;
    var clock = TestClock{};
    var server = TokenServer{
        .gpa = gpa,
        .body = "{\"access_token\":\"ya29.test-token-aaa\",\"expires_in\":3600,\"token_type\":\"Bearer\"}",
    };
    var ts = TokenSource.init(gpa, server.transport(), DEFAULT_METADATA_URL, clock.clock());
    defer ts.deinit();

    const t1 = try ts.token();
    try testing.expectEqualStrings("ya29.test-token-aaa", t1);
    try testing.expectEqual(@as(u32, 1), server.call_count);

    // Second call within validity window must NOT hit the server.
    const t2 = try ts.token();
    try testing.expectEqualStrings("ya29.test-token-aaa", t2);
    try testing.expectEqual(@as(u32, 1), server.call_count);
}

test "TokenSource: refreshes when within REFRESH_LEAD_SECONDS of expiry" {
    const gpa = testing.allocator;
    var clock = TestClock{};
    var server = TokenServer{
        .gpa = gpa,
        .body = "{\"access_token\":\"ya29.first\",\"expires_in\":120}",
    };
    var ts = TokenSource.init(gpa, server.transport(), DEFAULT_METADATA_URL, clock.clock());
    defer ts.deinit();

    const t1 = try ts.token();
    try testing.expectEqualStrings("ya29.first", t1);
    try testing.expectEqual(@as(u32, 1), server.call_count);

    // Original now=1_700_000_000, expires_in=120 -> expires at 1_700_000_120.
    // REFRESH_LEAD_SECONDS=60 -> refresh fires when now+60 >= 1_700_000_120,
    // i.e. now >= 1_700_000_060.
    clock.fixed_now = 1_700_000_061;
    server.body = "{\"access_token\":\"ya29.second\",\"expires_in\":3600}";

    const t2 = try ts.token();
    try testing.expectEqualStrings("ya29.second", t2);
    try testing.expectEqual(@as(u32, 2), server.call_count);
}

test "TokenSource: surfaces BadStatus on non-200 metadata response" {
    const gpa = testing.allocator;
    var clock = TestClock{};
    var server = TokenServer{
        .gpa = gpa,
        .body = "denied\n",
        .status = 403,
    };
    var ts = TokenSource.init(gpa, server.transport(), DEFAULT_METADATA_URL, clock.clock());
    defer ts.deinit();

    try testing.expectError(error.BadStatus, ts.token());
}

test "TokenSource: surfaces MissingAccessToken when JSON omits it" {
    const gpa = testing.allocator;
    var clock = TestClock{};
    var server = TokenServer{
        .gpa = gpa,
        .body = "{\"expires_in\":3600}",
    };
    var ts = TokenSource.init(gpa, server.transport(), DEFAULT_METADATA_URL, clock.clock());
    defer ts.deinit();

    try testing.expectError(error.MissingAccessToken, ts.token());
}

test "TokenSource: surfaces BadStatus when Metadata-Flavor header missing" {
    const gpa = testing.allocator;
    var clock = TestClock{};
    var server = TokenServer{
        .gpa = gpa,
        .body = "{}",
    };
    const StripWrap = struct {
        inner: *TokenServer,
        pub fn transport(self: *@This()) gcs.HttpTransport {
            return .{ .ptr = @ptrCast(self), .vtable = &vt };
        }
        const vt: gcs.HttpTransport.VTable = .{ .get = stripGet };
        fn stripGet(ptr: *anyopaque, url: []const u8, _: []const gcs.Header, body_dst: []u8) anyerror!gcs.Response {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return TokenServer.getImpl(@ptrCast(self.inner), url, &.{}, body_dst);
        }
    };
    var stripper = StripWrap{ .inner = &server };
    var ts = TokenSource.init(gpa, stripper.transport(), DEFAULT_METADATA_URL, clock.clock());
    defer ts.deinit();

    try testing.expectError(error.BadStatus, ts.token());
}
