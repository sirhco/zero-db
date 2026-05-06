//! Google Cloud Storage client.
//!
//! Issues `GET` against `https://storage.googleapis.com/<bucket>/<object>`
//! with a `Range: bytes=A-B` header. The actual HTTP transport is hidden
//! behind the `HttpTransport` vtable so:
//!   - production wires `std.http.Client` (a future `RealTransport`)
//!   - tests use `FakeTransport` / `FakeServer` to verify URL + headers +
//!     response handling end-to-end without network or credentials
//!
//! Auth (bearer-token from `auth.zig`) is not yet wired here — `Client`
//! optionally accepts a token and emits an `Authorization: Bearer <token>`
//! header when one is set; production token refresh lives in `auth.zig`.

const std = @import("std");
const auth = @import("auth.zig");

pub const Error = error{
    EmptyRange,
    InvalidObject,
    HttpError,
    BadStatus,
    AuthFailed,
    BodyTooLarge,
    OutOfMemory,
    UrlBufferTooSmall,
    NotImplemented,
    PutFailed,
    PutUnsupported,
    DeleteFailed,
    DeleteUnsupported,
    PreconditionFailed,
};

pub const DEFAULT_BASE_URL: []const u8 = "https://storage.googleapis.com";

pub const Header = struct {
    name: []const u8,
    value: []const u8,
};

pub const Response = struct {
    status: u16,
    /// Bytes the transport wrote into the caller-provided `body_dst`.
    body_len: usize,
    /// GCS object generation, when the transport can read it from the
    /// response (e.g. `x-goog-generation` header). FakeServer always
    /// populates this; RealTransport returns null today — generation-
    /// aware production paths require the lower-level Request API.
    generation: ?i64 = null,
};

/// Pluggable HTTP transport. Implementations answer `GET` (range reads)
/// and `PUT` (whole-object uploads). Other verbs are out of scope; add a
/// vtable slot if a future phase needs DELETE or HEAD.
pub const HttpTransport = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        /// Synchronous GET. Implementations write up to `body_dst.len` body
        /// bytes into `body_dst` and return the actual count via
        /// `Response.body_len`. Status is the HTTP status code.
        get: *const fn (ptr: *anyopaque, url: []const u8, headers: []const Header, body_dst: []u8) anyerror!Response,
        /// Synchronous PUT. Implementations must accept the entire `body`
        /// and return `Response.status`. `Response.body_len` is unused by
        /// callers but should be set to 0.
        put: *const fn (ptr: *anyopaque, url: []const u8, headers: []const Header, body: []const u8) anyerror!Response,
        /// Synchronous DELETE. Returns the HTTP status. `Response.body_len`
        /// unused.
        delete: *const fn (ptr: *anyopaque, url: []const u8, headers: []const Header) anyerror!Response,
    };

    pub fn get(self: HttpTransport, url: []const u8, headers: []const Header, body_dst: []u8) anyerror!Response {
        return self.vtable.get(self.ptr, url, headers, body_dst);
    }

    pub fn put(self: HttpTransport, url: []const u8, headers: []const Header, body: []const u8) anyerror!Response {
        return self.vtable.put(self.ptr, url, headers, body);
    }

    pub fn delete(self: HttpTransport, url: []const u8, headers: []const Header) anyerror!Response {
        return self.vtable.delete(self.ptr, url, headers);
    }

    /// Default `put` impl for transports that intentionally do not support
    /// uploads (e.g. token-fetch transports). Returns `error.PutUnsupported`.
    pub fn unsupportedPut(_: *anyopaque, _: []const u8, _: []const Header, _: []const u8) anyerror!Response {
        return error.PutUnsupported;
    }

    pub fn unsupportedDelete(_: *anyopaque, _: []const u8, _: []const Header) anyerror!Response {
        return error.DeleteUnsupported;
    }
};

pub const Options = struct {
    /// Override for tests / staging. Must NOT have a trailing slash.
    base_url: []const u8 = DEFAULT_BASE_URL,
    /// Static bearer token. Used only when `token_source` is null. Carried
    /// verbatim as `Authorization: Bearer <token>`. Lifetime: borrowed.
    bearer_token: ?[]const u8 = null,
    /// Token source consulted before every request; supersedes
    /// `bearer_token` when both are set. Lifetime: borrowed.
    token_source: ?*auth.TokenSource = null,
};

pub const Client = struct {
    gpa: std.mem.Allocator,
    transport: HttpTransport,
    options: Options,

    pub fn init(gpa: std.mem.Allocator, transport: HttpTransport, options: Options) Client {
        return .{ .gpa = gpa, .transport = transport, .options = options };
    }

    pub fn deinit(self: *Client) void {
        _ = self;
    }

    /// `GET <base>/<bucket>/<object>` with `Range: bytes=start-(start+len-1)`.
    /// Writes up to `len` bytes into `dst`; returns actual count from the
    /// server response. `len` must be `<= dst.len`.
    pub fn rangeGet(
        self: *Client,
        bucket: []const u8,
        object: []const u8,
        start: u64,
        len: u32,
        dst: []u8,
    ) Error!usize {
        const r = try self.rangeGetWithMeta(bucket, object, start, len, dst);
        return r.body_len;
    }

    pub const RangeGetResult = struct {
        body_len: usize,
        generation: ?i64,
    };

    /// Same as `rangeGet` but returns the object's GCS generation when
    /// the transport surfaces it. Used by `Engine.loadFromManifest` to
    /// capture the manifest's generation so subsequent writes carry an
    /// `If-Generation-Match` precondition.
    pub fn rangeGetWithMeta(
        self: *Client,
        bucket: []const u8,
        object: []const u8,
        start: u64,
        len: u32,
        dst: []u8,
    ) Error!RangeGetResult {
        if (len == 0) return error.EmptyRange;
        std.debug.assert(len <= dst.len);

        var url_arena = std.heap.ArenaAllocator.init(self.gpa);
        defer url_arena.deinit();
        const url = buildObjectUrl(url_arena.allocator(), self.options.base_url, bucket, object) catch
            return error.InvalidObject;

        var range_buf: [64]u8 = undefined;
        const range_value = formatRangeHeader(&range_buf, start, len) catch return error.EmptyRange;

        var attempt: u8 = 0;
        while (true) : (attempt += 1) {
            // Real GCS OAuth2 tokens commonly exceed 1 KiB. Sized to 4 KiB
            // to leave headroom for any signed JWT additions.
            var auth_buf: [4096]u8 = undefined;
            const auth_value = try self.resolveAuthHeader(&auth_buf);

            var headers: [2]Header = undefined;
            var n: usize = 1;
            headers[0] = .{ .name = "Range", .value = range_value };
            if (auth_value) |av| {
                headers[1] = .{ .name = "Authorization", .value = av };
                n = 2;
            }

            const resp = self.transport.get(url, headers[0..n], dst[0..len]) catch
                return error.HttpError;

            if (resp.status == 200 or resp.status == 206) {
                return .{ .body_len = resp.body_len, .generation = resp.generation };
            }
            if (resp.status == 401 and attempt == 0 and self.options.token_source != null) {
                std.debug.print("rangeGet: 401 on attempt {d}, invalidating token + retrying\n", .{attempt});
                self.options.token_source.?.invalidate();
                continue;
            }
            std.debug.print("rangeGet: failing with status {d} url={s}\n", .{ resp.status, url });
            return switch (resp.status) {
                401, 403 => error.AuthFailed,
                else => error.BadStatus,
            };
        }
    }

    fn resolveAuthHeader(self: *Client, auth_buf: []u8) Error!?[]const u8 {
        if (self.options.token_source) |ts| {
            const tok = ts.token() catch |err| {
                std.debug.print("resolveAuthHeader: TokenSource.token failed: {s}\n", .{@errorName(err)});
                return error.AuthFailed;
            };
            return std.fmt.bufPrint(auth_buf, "Bearer {s}", .{tok}) catch error.AuthFailed;
        }
        if (self.options.bearer_token) |t| {
            return std.fmt.bufPrint(auth_buf, "Bearer {s}", .{t}) catch error.AuthFailed;
        }
        return null;
    }

    /// `PUT <base>/<bucket>/<object>` with `body` as the object payload.
    /// Uses the GCS XML API simple upload — same URL shape as `rangeGet`.
    /// Returns on success (status 200/201). Maps non-2xx to `error.PutFailed`,
    /// 401/403 to `error.AuthFailed`.
    pub fn putObject(
        self: *Client,
        bucket: []const u8,
        object: []const u8,
        body: []const u8,
    ) Error!void {
        _ = try self.putObjectIfMatch(bucket, object, body, null);
    }

    /// Same as `putObject` but with optional `If-Generation-Match`
    /// precondition. `expected_generation`:
    ///   - `null`     — no precondition (same as `putObject`)
    ///   - `0`        — object must not exist (create-only)
    ///   - `> 0`      — current GCS generation must equal this value
    /// Returns the new generation when the transport surfaces it
    /// (FakeServer always does; RealTransport returns `null` until
    /// response-header readback ships). Returns `error.PreconditionFailed`
    /// on a 412 — caller is expected to reload state and retry.
    pub fn putObjectIfMatch(
        self: *Client,
        bucket: []const u8,
        object: []const u8,
        body: []const u8,
        expected_generation: ?i64,
    ) Error!?i64 {
        var url_arena = std.heap.ArenaAllocator.init(self.gpa);
        defer url_arena.deinit();
        const url = buildObjectUrl(url_arena.allocator(), self.options.base_url, bucket, object) catch
            return error.InvalidObject;

        var len_buf: [32]u8 = undefined;
        const len_value = std.fmt.bufPrint(&len_buf, "{d}", .{body.len}) catch
            return error.OutOfMemory;

        var precond_buf: [32]u8 = undefined;
        const precond_value: ?[]const u8 = if (expected_generation) |g|
            std.fmt.bufPrint(&precond_buf, "{d}", .{g}) catch return error.OutOfMemory
        else
            null;

        var attempt: u8 = 0;
        while (true) : (attempt += 1) {
            // Real GCS OAuth2 tokens commonly exceed 1 KiB. Sized to 4 KiB
            // to leave headroom for any signed JWT additions.
            var auth_buf: [4096]u8 = undefined;
            const auth_value = try self.resolveAuthHeader(&auth_buf);

            var headers: [4]Header = undefined;
            var n: usize = 2;
            headers[0] = .{ .name = "Content-Type", .value = "application/octet-stream" };
            headers[1] = .{ .name = "Content-Length", .value = len_value };
            if (auth_value) |av| {
                headers[n] = .{ .name = "Authorization", .value = av };
                n += 1;
            }
            if (precond_value) |p| {
                headers[n] = .{ .name = "x-goog-if-generation-match", .value = p };
                n += 1;
            }

            const resp = self.transport.put(url, headers[0..n], body) catch
                return error.HttpError;

            if (resp.status == 200 or resp.status == 201) return resp.generation;
            if (resp.status == 412) return error.PreconditionFailed;
            if (resp.status == 401 and attempt == 0 and self.options.token_source != null) {
                std.debug.print("putObjectIfMatch: 401 on attempt {d}, invalidating token + retrying\n", .{attempt});
                self.options.token_source.?.invalidate();
                continue;
            }
            std.debug.print("putObjectIfMatch: failing with status {d} url={s}\n", .{ resp.status, url });
            return switch (resp.status) {
                401, 403 => error.AuthFailed,
                else => error.PutFailed,
            };
        }
    }

    /// `DELETE <base>/<bucket>/<object>`. Returns success on 200 / 204.
    /// 404 (object did not exist) is folded into success — callers using
    /// this to GC orphan objects do not care whether something else
    /// already reaped the target.
    pub fn deleteObject(
        self: *Client,
        bucket: []const u8,
        object: []const u8,
    ) Error!void {
        var url_arena = std.heap.ArenaAllocator.init(self.gpa);
        defer url_arena.deinit();
        const url = buildObjectUrl(url_arena.allocator(), self.options.base_url, bucket, object) catch
            return error.InvalidObject;

        var attempt: u8 = 0;
        while (true) : (attempt += 1) {
            // Real GCS OAuth2 tokens commonly exceed 1 KiB. Sized to 4 KiB
            // to leave headroom for any signed JWT additions.
            var auth_buf: [4096]u8 = undefined;
            const auth_value = try self.resolveAuthHeader(&auth_buf);

            var headers: [1]Header = undefined;
            var n: usize = 0;
            if (auth_value) |av| {
                headers[0] = .{ .name = "Authorization", .value = av };
                n = 1;
            }

            const resp = self.transport.delete(url, headers[0..n]) catch
                return error.HttpError;

            if (resp.status == 200 or resp.status == 204 or resp.status == 404) return;
            if (resp.status == 401 and attempt == 0 and self.options.token_source != null) {
                self.options.token_source.?.invalidate();
                continue;
            }
            return switch (resp.status) {
                401, 403 => error.AuthFailed,
                else => error.DeleteFailed,
            };
        }
    }
};

// ---------------------------------------------------------------------------
// URL + Range header helpers (pure; tested directly).
// ---------------------------------------------------------------------------

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

    out.appendSlice(gpa, base_url) catch return error.OutOfMemory;
    out.append(gpa, '/') catch return error.OutOfMemory;
    appendUrlEncoded(&out, gpa, bucket) catch return error.OutOfMemory;
    out.append(gpa, '/') catch return error.OutOfMemory;
    appendUrlEncoded(&out, gpa, object) catch return error.OutOfMemory;
    return out.toOwnedSlice(gpa) catch return error.OutOfMemory;
}

pub fn formatRangeHeader(out: []u8, start: u64, len: u32) Error![]const u8 {
    if (len == 0) return error.EmptyRange;
    const end_inclusive: u64 = start + len - 1;
    return std.fmt.bufPrint(out, "bytes={d}-{d}", .{ start, end_inclusive }) catch
        return error.OutOfMemory;
}

/// Parse a `Range: bytes=A-B` header value. Returns (start, inclusive_end).
/// Used by `FakeServer` and by future cache-aware transports.
pub fn parseRangeValue(value: []const u8) !struct { start: u64, end: u64 } {
    const prefix = "bytes=";
    if (!std.mem.startsWith(u8, value, prefix)) return error.BadRange;
    const rest = value[prefix.len..];
    const dash = std.mem.indexOfScalar(u8, rest, '-') orelse return error.BadRange;
    const start = try std.fmt.parseInt(u64, rest[0..dash], 10);
    const end = try std.fmt.parseInt(u64, rest[dash + 1 ..], 10);
    if (end < start) return error.BadRange;
    return .{ .start = start, .end = end };
}

fn findHeader(headers: []const Header, name: []const u8) ?[]const u8 {
    for (headers) |h| {
        if (std.ascii.eqlIgnoreCase(h.name, name)) return h.value;
    }
    return null;
}

// ---------------------------------------------------------------------------
// FakeServer — in-memory GCS impostor for testing.
// ---------------------------------------------------------------------------

/// Minimal in-memory GCS impostor for unit tests. Serves byte ranges of
/// one configured object; records the most recent request so tests can
/// assert URL + headers were built correctly. NOT a general-purpose mock —
/// just enough surface area to drive the `Client.rangeGet` happy path,
/// auth path, and a few error paths.
///
/// FakeServer owns gpa-allocated copies of the URL and Authorization
/// strings it observes so tests can assert against them after the call
/// returns (the caller's URL buffer may be a stack-local arena that goes
/// out of scope before the test inspects state).
pub const FakeServer = struct {
    pub const StoredObject = struct {
        body: []u8,
        generation: i64,
    };

    gpa: std.mem.Allocator,
    base_url: []const u8,
    bucket: []const u8,
    object: []const u8,
    object_bytes: []const u8,

    /// When set, requests must carry `Authorization: Bearer <expected>`;
    /// missing / mismatched tokens return 401.
    expected_token: ?[]const u8 = null,

    /// Force a specific status on the next call (cleared afterwards).
    /// Used to exercise non-2xx error paths without contorting the
    /// happy-path setup.
    forced_status: ?u16 = null,

    /// When set, the next PUT returns this status without storing the body.
    /// Cleared after one use. Lets tests exercise the "manifest PUT failed
    /// after SSTable PUT succeeded" scenario.
    forced_put_status: ?u16 = null,

    /// Same one-shot escape hatch for DELETE.
    forced_delete_status: ?u16 = null,

    /// PUT bodies received during the test, keyed by URL. Survives subsequent
    /// GETs of the same URL — that round-trip is the headline test of
    /// Phase 12. Each entry also carries a monotonically-increasing
    /// generation, mirroring GCS's per-object generation semantics; this
    /// is what powers the multi-writer precondition tests.
    received_puts: std.StringHashMap(StoredObject),

    /// Monotonic generation counter applied to every PUT (across all
    /// objects). Mirrors GCS's behavior closely enough for tests.
    next_generation: i64 = 1,

    // Recorded request data — gpa-owned dupes; freed in deinit.
    last_url: ?[]u8 = null,
    last_auth_seen: ?[]u8 = null,
    last_range_start: u64 = 0,
    last_range_end: u64 = 0,
    last_range_header_seen: bool = false,
    call_count: u32 = 0,
    put_count: u32 = 0,
    delete_count: u32 = 0,

    pub fn init(
        gpa: std.mem.Allocator,
        base_url: []const u8,
        bucket: []const u8,
        object: []const u8,
        object_bytes: []const u8,
    ) FakeServer {
        return .{
            .gpa = gpa,
            .base_url = base_url,
            .bucket = bucket,
            .object = object,
            .object_bytes = object_bytes,
            .received_puts = std.StringHashMap(StoredObject).init(gpa),
        };
    }

    pub fn deinit(self: *FakeServer) void {
        if (self.last_url) |u| self.gpa.free(u);
        if (self.last_auth_seen) |a| self.gpa.free(a);
        var it = self.received_puts.iterator();
        while (it.next()) |entry| {
            self.gpa.free(entry.key_ptr.*);
            self.gpa.free(entry.value_ptr.*.body);
        }
        self.received_puts.deinit();
        self.* = undefined;
    }

    pub fn transport(self: *FakeServer) HttpTransport {
        return .{ .ptr = @ptrCast(self), .vtable = &vtable };
    }

    const vtable: HttpTransport.VTable = .{ .get = getImpl, .put = putImpl, .delete = deleteImpl };

    fn recordUrl(self: *FakeServer, url: []const u8) !void {
        if (self.last_url) |u| self.gpa.free(u);
        self.last_url = try self.gpa.dupe(u8, url);
    }

    fn recordAuth(self: *FakeServer, auth_value: ?[]const u8) !void {
        if (self.last_auth_seen) |a| self.gpa.free(a);
        self.last_auth_seen = if (auth_value) |x| try self.gpa.dupe(u8, x) else null;
    }

    fn getImpl(
        ptr: *anyopaque,
        url: []const u8,
        headers: []const Header,
        body_dst: []u8,
    ) anyerror!Response {
        const self: *FakeServer = @ptrCast(@alignCast(ptr));
        self.call_count += 1;
        try self.recordUrl(url);
        try self.recordAuth(findHeader(headers, "Authorization"));
        self.last_range_header_seen = false;

        // Auth check, if configured.
        if (self.expected_token) |want| {
            const got = self.last_auth_seen orelse return Response{ .status = 401, .body_len = 0 };
            const prefix = "Bearer ";
            if (!std.mem.startsWith(u8, got, prefix) or
                !std.mem.eql(u8, got[prefix.len..], want))
            {
                return Response{ .status = 401, .body_len = 0 };
            }
        }

        // Forced-error escape hatch.
        if (self.forced_status) |s| {
            self.forced_status = null;
            return Response{ .status = s, .body_len = 0 };
        }

        // Range header is required (this fake only supports range reads).
        const range_value = findHeader(headers, "Range") orelse
            return Response{ .status = 400, .body_len = 0 };
        const r = try parseRangeValue(range_value);
        self.last_range_header_seen = true;
        self.last_range_start = r.start;
        self.last_range_end = r.end;

        // Resolve the source bytes: PUT-stored objects shadow the legacy
        // single-object path, allowing put -> get round trips.
        const source_bytes = self.resolveBytes(url) orelse
            return Response{ .status = 404, .body_len = 0 };

        if (r.start >= source_bytes.len) return Response{ .status = 416, .body_len = 0 };
        const end_clamped: usize = @intCast(@min(r.end, @as(u64, source_bytes.len) - 1));
        const want_len: usize = end_clamped - @as(usize, @intCast(r.start)) + 1;
        const n: usize = @min(want_len, body_dst.len);
        const s: usize = @intCast(r.start);
        @memcpy(body_dst[0..n], source_bytes[s .. s + n]);
        return Response{ .status = 206, .body_len = n, .generation = self.resolveGeneration(url) };
    }

    fn resolveBytes(self: *FakeServer, url: []const u8) ?[]const u8 {
        var expected_buf: [4096]u8 = undefined;
        var fba = std.heap.FixedBufferAllocator.init(&expected_buf);
        const expected = buildObjectUrl(fba.allocator(), self.base_url, self.bucket, self.object) catch
            return null;
        if (std.mem.eql(u8, url, expected)) return self.object_bytes;
        if (self.received_puts.get(url)) |obj| return obj.body;
        return null;
    }

    fn resolveGeneration(self: *FakeServer, url: []const u8) ?i64 {
        if (self.received_puts.get(url)) |obj| return obj.generation;
        return null;
    }

    fn putImpl(
        ptr: *anyopaque,
        url: []const u8,
        headers: []const Header,
        body: []const u8,
    ) anyerror!Response {
        const self: *FakeServer = @ptrCast(@alignCast(ptr));
        self.put_count += 1;
        try self.recordUrl(url);
        try self.recordAuth(findHeader(headers, "Authorization"));

        if (self.expected_token) |want| {
            const got = self.last_auth_seen orelse return Response{ .status = 401, .body_len = 0 };
            const prefix = "Bearer ";
            if (!std.mem.startsWith(u8, got, prefix) or
                !std.mem.eql(u8, got[prefix.len..], want))
            {
                return Response{ .status = 401, .body_len = 0 };
            }
        }

        if (self.forced_put_status) |s| {
            self.forced_put_status = null;
            return Response{ .status = s, .body_len = 0 };
        }

        // GCS If-Generation-Match precondition. Value is decimal i64;
        // 0 means "object must not exist". Mismatch returns 412.
        if (findHeader(headers, "x-goog-if-generation-match")) |want| {
            const expected = std.fmt.parseInt(i64, want, 10) catch -1;
            const current: i64 = if (self.received_puts.get(url)) |obj| obj.generation else 0;
            if (expected != current) {
                return Response{ .status = 412, .body_len = 0, .generation = if (current == 0) null else current };
            }
        }

        const new_gen = self.next_generation;
        self.next_generation += 1;

        const url_copy = try self.gpa.dupe(u8, url);
        errdefer self.gpa.free(url_copy);
        const body_copy = try self.gpa.dupe(u8, body);
        errdefer self.gpa.free(body_copy);

        if (self.received_puts.fetchRemove(url)) |old| {
            self.gpa.free(old.key);
            self.gpa.free(old.value.body);
        }
        try self.received_puts.put(url_copy, .{ .body = body_copy, .generation = new_gen });
        return Response{ .status = 200, .body_len = 0, .generation = new_gen };
    }

    fn deleteImpl(
        ptr: *anyopaque,
        url: []const u8,
        headers: []const Header,
    ) anyerror!Response {
        const self: *FakeServer = @ptrCast(@alignCast(ptr));
        self.delete_count += 1;
        try self.recordUrl(url);
        try self.recordAuth(findHeader(headers, "Authorization"));

        if (self.expected_token) |want| {
            const got = self.last_auth_seen orelse return Response{ .status = 401, .body_len = 0 };
            const prefix = "Bearer ";
            if (!std.mem.startsWith(u8, got, prefix) or
                !std.mem.eql(u8, got[prefix.len..], want))
            {
                return Response{ .status = 401, .body_len = 0 };
            }
        }

        if (self.forced_delete_status) |s| {
            self.forced_delete_status = null;
            return Response{ .status = s, .body_len = 0 };
        }

        if (self.received_puts.fetchRemove(url)) |old| {
            self.gpa.free(old.key);
            self.gpa.free(old.value.body);
            return Response{ .status = 204, .body_len = 0 };
        }
        // GCS returns 404 when the object does not exist.
        return Response{ .status = 404, .body_len = 0 };
    }
};

// ---------------------------------------------------------------------------
// RealTransport — production HttpTransport over std.http.Client.
// ---------------------------------------------------------------------------

/// Production `HttpTransport` backed by `std.http.Client`. Owns the
/// underlying client across requests so connections can be reused. Caller
/// supplies an `Io` instance (typically from `std.Io.Threaded.init`) at
/// construction so transport selection composes with the rest of the
/// program's I/O strategy.
///
/// Body bytes stream into a temporary `std.Io.Writer.Allocating` and are
/// then memcpy'd into the caller's `body_dst`, capped at `body_dst.len`.
/// `Response.body_len` reports the count actually written.
///
/// Lifetime: caller constructs once at startup and shares across
/// `gcs.Client`s.
pub const RealTransport = struct {
    gpa: std.mem.Allocator,
    inner: std.http.Client,

    pub fn init(gpa: std.mem.Allocator, io: std.Io) RealTransport {
        return .{
            .gpa = gpa,
            .inner = .{ .allocator = gpa, .io = io },
        };
    }

    pub fn deinit(self: *RealTransport) void {
        self.inner.deinit();
        self.* = undefined;
    }

    pub fn transport(self: *RealTransport) HttpTransport {
        return .{ .ptr = @ptrCast(self), .vtable = &vtable };
    }

    const vtable: HttpTransport.VTable = .{ .get = getImpl, .put = putImpl, .delete = deleteImpl };

    fn translateHeaders(headers: []const Header, dst: *[16]std.http.Header) ![]const std.http.Header {
        if (headers.len > dst.len) return error.BodyTooLarge;
        for (headers, 0..) |h, i| {
            dst[i] = .{ .name = h.name, .value = h.value };
        }
        return dst[0..headers.len];
    }

    /// Issue a one-shot request via the lower-level `client.request` API
    /// (vs `fetch`) so we can read response headers — specifically
    /// `x-goog-generation` for multi-writer manifest preconditions.
    /// `body` is null for GET/DELETE, set for PUT.
    /// `body_dst` is filled with up to its length of response bytes; for
    /// PUT/DELETE we still drain the body (small XML/JSON), but the
    /// returned `body_len` may be 0 if `body_dst.len == 0`.
    fn doRequest(
        self: *RealTransport,
        method: std.http.Method,
        url: []const u8,
        headers: []const Header,
        body: ?[]const u8,
        body_dst: []u8,
    ) anyerror!Response {
        var stack_headers: [16]std.http.Header = undefined;
        const extra = try translateHeaders(headers, &stack_headers);

        const uri = try std.Uri.parse(url);
        var req = try self.inner.request(method, uri, .{
            .extra_headers = extra,
            .keep_alive = true,
        });
        defer req.deinit();

        if (body) |b| {
            // sendBodyComplete needs []u8; the GCS client passes the
            // body to us as []const u8. Const-cast is safe: the buffer
            // is read-only-consumed by sendBodyComplete.
            try req.sendBodyComplete(@constCast(b));
        } else {
            try req.sendBodiless();
        }

        var redirect_buf: [8192]u8 = undefined;
        var resp = try req.receiveHead(&redirect_buf);

        // Parse generation header before reading the body — head bytes
        // are invalidated once the body stream is initialized.
        var generation: ?i64 = null;
        var hit = resp.head.iterateHeaders();
        while (hit.next()) |h| {
            if (std.ascii.eqlIgnoreCase(h.name, "x-goog-generation")) {
                generation = std.fmt.parseInt(i64, h.value, 10) catch null;
                break;
            }
        }

        // Drain the response body via a temporary Allocating writer
        // (matches what fetch did internally). Then copy a clamped
        // prefix into the caller's body_dst.
        var transfer_buf: [4096]u8 = undefined;
        const reader = resp.reader(&transfer_buf);
        var aw: std.Io.Writer.Allocating = .init(self.gpa);
        defer aw.deinit();
        _ = reader.streamRemaining(&aw.writer) catch {};
        const resp_body = aw.written();
        const n = @min(body_dst.len, resp_body.len);
        @memcpy(body_dst[0..n], resp_body[0..n]);

        return .{
            .status = @intFromEnum(resp.head.status),
            .body_len = n,
            .generation = generation,
        };
    }

    fn getImpl(
        ptr: *anyopaque,
        url: []const u8,
        headers: []const Header,
        body_dst: []u8,
    ) anyerror!Response {
        const self: *RealTransport = @ptrCast(@alignCast(ptr));
        return self.doRequest(.GET, url, headers, null, body_dst);
    }

    fn putImpl(
        ptr: *anyopaque,
        url: []const u8,
        headers: []const Header,
        body: []const u8,
    ) anyerror!Response {
        const self: *RealTransport = @ptrCast(@alignCast(ptr));
        var no_body: [0]u8 = .{};
        return self.doRequest(.PUT, url, headers, body, &no_body);
    }

    fn deleteImpl(
        ptr: *anyopaque,
        url: []const u8,
        headers: []const Header,
    ) anyerror!Response {
        const self: *RealTransport = @ptrCast(@alignCast(ptr));
        var no_body: [0]u8 = .{};
        return self.doRequest(.DELETE, url, headers, null, &no_body);
    }
};

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

test "formatRangeHeader: zero-length is rejected" {
    var buf: [64]u8 = undefined;
    try testing.expectError(error.EmptyRange, formatRangeHeader(&buf, 0, 0));
}

test "parseRangeValue: roundtrip" {
    const r = try parseRangeValue("bytes=100-199");
    try testing.expectEqual(@as(u64, 100), r.start);
    try testing.expectEqual(@as(u64, 199), r.end);
}

test "parseRangeValue: rejects non-bytes prefix" {
    try testing.expectError(error.BadRange, parseRangeValue("items=0-9"));
}

test "FakeServer: serves a partial range with status 206" {
    const gpa = testing.allocator;
    var fs = FakeServer.init(gpa, "http://test.local", "b", "o", "0123456789abcdef");
    defer fs.deinit();
    const t = fs.transport();

    var dst: [4]u8 = undefined;
    const headers = [_]Header{.{ .name = "Range", .value = "bytes=4-7" }};
    const r = try t.get("http://test.local/b/o", &headers, &dst);
    try testing.expectEqual(@as(u16, 206), r.status);
    try testing.expectEqual(@as(usize, 4), r.body_len);
    try testing.expectEqualSlices(u8, "4567", &dst);
    try testing.expectEqual(@as(u32, 1), fs.call_count);
}

test "FakeServer: 401 when token missing" {
    const gpa = testing.allocator;
    var fs = FakeServer.init(gpa, "http://t", "b", "o", "abc");
    defer fs.deinit();
    fs.expected_token = "secret";
    const t = fs.transport();
    var dst: [4]u8 = undefined;
    const headers = [_]Header{.{ .name = "Range", .value = "bytes=0-2" }};
    const r = try t.get("http://t/b/o", &headers, &dst);
    try testing.expectEqual(@as(u16, 401), r.status);
}

test "FakeServer: 200/206 when token correct" {
    const gpa = testing.allocator;
    var fs = FakeServer.init(gpa, "http://t", "b", "o", "abc");
    defer fs.deinit();
    fs.expected_token = "secret";
    const t = fs.transport();
    var dst: [4]u8 = undefined;
    const headers = [_]Header{
        .{ .name = "Range", .value = "bytes=0-2" },
        .{ .name = "Authorization", .value = "Bearer secret" },
    };
    const r = try t.get("http://t/b/o", &headers, &dst);
    try testing.expectEqual(@as(u16, 206), r.status);
    try testing.expectEqualSlices(u8, "abc", dst[0..3]);
}

test "FakeServer: 404 on URL mismatch" {
    const gpa = testing.allocator;
    var fs = FakeServer.init(gpa, "http://t", "b", "o", "x");
    defer fs.deinit();
    const t = fs.transport();
    var dst: [1]u8 = undefined;
    const headers = [_]Header{.{ .name = "Range", .value = "bytes=0-0" }};
    const r = try t.get("http://t/b/wrong", &headers, &dst);
    try testing.expectEqual(@as(u16, 404), r.status);
}

test "Client.rangeGet: builds URL + Range, writes response into dst" {
    const gpa = testing.allocator;
    var fs = FakeServer.init(gpa, DEFAULT_BASE_URL, "bk", "obj.sst", "abcdefghijklmnop");
    defer fs.deinit();
    var c = Client.init(gpa, fs.transport(), .{});
    defer c.deinit();

    var dst: [8]u8 = undefined;
    const n = try c.rangeGet("bk", "obj.sst", 4, 8, &dst);
    try testing.expectEqual(@as(usize, 8), n);
    try testing.expectEqualSlices(u8, "efghijkl", &dst);
    try testing.expect(fs.last_range_header_seen);
    try testing.expectEqual(@as(u64, 4), fs.last_range_start);
    try testing.expectEqual(@as(u64, 11), fs.last_range_end);
    try testing.expectEqualStrings("https://storage.googleapis.com/bk/obj.sst", fs.last_url.?);
    try testing.expect(fs.last_auth_seen == null);
}

test "Client.rangeGet: emits Authorization when bearer_token set" {
    const gpa = testing.allocator;
    var fs = FakeServer.init(gpa, DEFAULT_BASE_URL, "bk", "o", "abc");
    defer fs.deinit();
    fs.expected_token = "tok-12345";
    var c = Client.init(gpa, fs.transport(), .{ .bearer_token = "tok-12345" });
    defer c.deinit();

    var dst: [3]u8 = undefined;
    _ = try c.rangeGet("bk", "o", 0, 3, &dst);
    try testing.expect(fs.last_auth_seen != null);
    try testing.expectEqualStrings("Bearer tok-12345", fs.last_auth_seen.?);
}

test "Client.rangeGet: maps 401 → AuthFailed, 500 → BadStatus" {
    const gpa = testing.allocator;
    var fs = FakeServer.init(gpa, DEFAULT_BASE_URL, "bk", "o", "abc");
    defer fs.deinit();
    fs.expected_token = "secret";

    var c = Client.init(gpa, fs.transport(), .{}); // no token
    defer c.deinit();

    var dst: [3]u8 = undefined;
    try testing.expectError(error.AuthFailed, c.rangeGet("bk", "o", 0, 3, &dst));

    fs.expected_token = null;
    fs.forced_status = 500;
    var c2 = Client.init(gpa, fs.transport(), .{});
    defer c2.deinit();
    try testing.expectError(error.BadStatus, c2.rangeGet("bk", "o", 0, 3, &dst));
}

test "Client.rangeGet: rejects zero-length" {
    const gpa = testing.allocator;
    var fs = FakeServer.init(gpa, DEFAULT_BASE_URL, "b", "o", "x");
    defer fs.deinit();
    var c = Client.init(gpa, fs.transport(), .{});
    defer c.deinit();
    var dst: [4]u8 = undefined;
    try testing.expectError(error.EmptyRange, c.rangeGet("b", "o", 0, 0, &dst));
}

// ---------------------------------------------------------------------------
// RealTransport tests — drive a localhost std.Io.net + std.http.Server.
// ---------------------------------------------------------------------------

const LoopbackArgs = struct {
    io: std.Io,
    server: *std.Io.net.Server,
    response_status: std.http.Status,
    response_body: []const u8,
    seen_url_path: [256]u8 = undefined,
    seen_url_path_len: usize = 0,
    seen_range_value: [128]u8 = undefined,
    seen_range_len: usize = 0,
    seen_authorization: [256]u8 = undefined,
    seen_authorization_len: usize = 0,
    err: ?anyerror = null,
};

fn loopbackServeOnce(args: *LoopbackArgs) void {
    loopbackServeOnceInner(args) catch |e| {
        args.err = e;
    };
}

fn loopbackServeOnceInner(args: *LoopbackArgs) !void {
    var stream = try args.server.accept(args.io);
    defer stream.close(args.io);

    var read_buf: [4096]u8 = undefined;
    var write_buf: [4096]u8 = undefined;
    var sr = stream.reader(args.io, &read_buf);
    var sw = stream.writer(args.io, &write_buf);
    var http_server = std.http.Server.init(&sr.interface, &sw.interface);

    var req = try http_server.receiveHead();

    // Record observed URL path.
    const path = req.head.target;
    args.seen_url_path_len = @min(path.len, args.seen_url_path.len);
    @memcpy(args.seen_url_path[0..args.seen_url_path_len], path[0..args.seen_url_path_len]);

    // Record selected request headers.
    var it = req.iterateHeaders();
    while (it.next()) |h| {
        if (std.ascii.eqlIgnoreCase(h.name, "Range")) {
            args.seen_range_len = @min(h.value.len, args.seen_range_value.len);
            @memcpy(args.seen_range_value[0..args.seen_range_len], h.value[0..args.seen_range_len]);
        } else if (std.ascii.eqlIgnoreCase(h.name, "Authorization")) {
            args.seen_authorization_len = @min(h.value.len, args.seen_authorization.len);
            @memcpy(args.seen_authorization[0..args.seen_authorization_len], h.value[0..args.seen_authorization_len]);
        }
    }

    try req.respond(args.response_body, .{ .status = args.response_status });
}

test "RealTransport: forwards URL + Range header to a localhost std.http.Server" {
    const gpa = testing.allocator;

    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const addr = try std.Io.net.IpAddress.parseIp4("127.0.0.1", 0);
    var server = try addr.listen(io, .{ .reuse_address = true });
    defer server.deinit(io);
    const bound_port = server.socket.address.getPort();

    var args: LoopbackArgs = .{
        .io = io,
        .server = &server,
        .response_status = .partial_content,
        .response_body = "partial-bytes",
    };
    const server_thread = try std.Thread.spawn(.{}, loopbackServeOnce, .{&args});

    var rt = RealTransport.init(gpa, io);
    defer rt.deinit();
    var url_buf: [256]u8 = undefined;
    const url = try std.fmt.bufPrint(&url_buf, "http://127.0.0.1:{d}/bk/obj.sst", .{bound_port});

    var dst: [32]u8 = undefined;
    const headers = [_]Header{.{ .name = "Range", .value = "bytes=4-15" }};
    const r = try rt.transport().get(url, &headers, &dst);
    server_thread.join();

    if (args.err) |e| return e;
    try testing.expectEqual(@as(u16, 206), r.status);
    try testing.expectEqualStrings("/bk/obj.sst", args.seen_url_path[0..args.seen_url_path_len]);
    try testing.expectEqualStrings("bytes=4-15", args.seen_range_value[0..args.seen_range_len]);
    try testing.expectEqualStrings("partial-bytes", dst[0..r.body_len]);
}

test "RealTransport: maps a 401 response to status=401" {
    const gpa = testing.allocator;

    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const addr = try std.Io.net.IpAddress.parseIp4("127.0.0.1", 0);
    var server = try addr.listen(io, .{ .reuse_address = true });
    defer server.deinit(io);
    const bound_port = server.socket.address.getPort();

    var args: LoopbackArgs = .{
        .io = io,
        .server = &server,
        .response_status = .unauthorized,
        .response_body = "unauthorized\n",
    };
    const server_thread = try std.Thread.spawn(.{}, loopbackServeOnce, .{&args});

    var rt = RealTransport.init(gpa, io);
    defer rt.deinit();
    var url_buf: [128]u8 = undefined;
    const url = try std.fmt.bufPrint(&url_buf, "http://127.0.0.1:{d}/x", .{bound_port});

    var dst: [32]u8 = undefined;
    const headers = [_]Header{};
    const r = try rt.transport().get(url, &headers, &dst);
    server_thread.join();

    if (args.err) |e| return e;
    try testing.expectEqual(@as(u16, 401), r.status);
}

// ---------------------------------------------------------------------------
// TokenSource wiring tests — Client.rangeGet must consult token_source first.
// ---------------------------------------------------------------------------

test "Client.rangeGet: pulls bearer from TokenSource and rotates across calls" {
    const gpa = testing.allocator;

    var fs = FakeServer.init(gpa, DEFAULT_BASE_URL, "bk", "obj.sst", "abcdefgh");
    defer fs.deinit();

    const RotatingMeta = struct {
        n: u32 = 0,

        pub fn transport(self: *@This()) HttpTransport {
            return .{ .ptr = @ptrCast(self), .vtable = &vt };
        }
        const vt: HttpTransport.VTable = .{ .get = getImpl, .put = HttpTransport.unsupportedPut, .delete = HttpTransport.unsupportedDelete };
        fn getImpl(p: *anyopaque, _: []const u8, _: []const Header, body_dst: []u8) anyerror!Response {
            const self: *@This() = @ptrCast(@alignCast(p));
            self.n += 1;
            // First call: short expiry forces refresh on the second rangeGet.
            // Second call: long expiry; would not refresh again on a third.
            const json = if (self.n == 1)
                "{\"access_token\":\"tok-A\",\"expires_in\":1}"
            else
                "{\"access_token\":\"tok-B\",\"expires_in\":3600}";
            const n = @min(body_dst.len, json.len);
            @memcpy(body_dst[0..n], json[0..n]);
            return .{ .status = 200, .body_len = n };
        }
    };
    var meta = RotatingMeta{};

    var ts = auth.TokenSource.init(
        gpa,
        meta.transport(),
        auth.DEFAULT_METADATA_URL,
        auth.SystemClock.clock(),
    );
    defer ts.deinit();

    var c = Client.init(gpa, fs.transport(), .{ .token_source = &ts });
    defer c.deinit();

    var dst: [3]u8 = undefined;

    fs.expected_token = "tok-A";
    _ = try c.rangeGet("bk", "obj.sst", 0, 3, &dst);
    try testing.expectEqualStrings("Bearer tok-A", fs.last_auth_seen.?);

    fs.expected_token = "tok-B";
    _ = try c.rangeGet("bk", "obj.sst", 0, 3, &dst);
    try testing.expectEqualStrings("Bearer tok-B", fs.last_auth_seen.?);

    try testing.expectEqual(@as(u32, 2), meta.n);
}

// ---------------------------------------------------------------------------
// putObject tests — exercise PUT path + put -> get round trip via FakeServer.
// ---------------------------------------------------------------------------

test "Client.putObject: round-trips body through FakeServer" {
    const gpa = testing.allocator;
    var fs = FakeServer.init(gpa, DEFAULT_BASE_URL, "bk", "legacy.sst", "");
    defer fs.deinit();
    var c = Client.init(gpa, fs.transport(), .{});
    defer c.deinit();

    const payload = "hello-from-phase-12-payload-bytes";
    try c.putObject("bk", "sstables/000001.sst", payload);
    try testing.expectEqual(@as(u32, 1), fs.put_count);

    var dst: [64]u8 = undefined;
    const n = try c.rangeGet("bk", "sstables/000001.sst", 0, payload.len, &dst);
    try testing.expectEqual(payload.len, n);
    try testing.expectEqualSlices(u8, payload, dst[0..n]);
}

test "Client.putObject: forwards Authorization header" {
    const gpa = testing.allocator;
    var fs = FakeServer.init(gpa, DEFAULT_BASE_URL, "bk", "x", "");
    defer fs.deinit();
    fs.expected_token = "tok-put";
    var c = Client.init(gpa, fs.transport(), .{ .bearer_token = "tok-put" });
    defer c.deinit();

    try c.putObject("bk", "sstables/000001.sst", "payload");
    try testing.expectEqualStrings("Bearer tok-put", fs.last_auth_seen.?);
}

test "Client.putObject: non-2xx maps to PutFailed" {
    const gpa = testing.allocator;
    var fs = FakeServer.init(gpa, DEFAULT_BASE_URL, "bk", "x", "");
    defer fs.deinit();
    fs.forced_put_status = 500;
    var c = Client.init(gpa, fs.transport(), .{});
    defer c.deinit();

    try testing.expectError(error.PutFailed, c.putObject("bk", "obj", "data"));
}

test "Client.putObject: 401 maps to AuthFailed" {
    const gpa = testing.allocator;
    var fs = FakeServer.init(gpa, DEFAULT_BASE_URL, "bk", "x", "");
    defer fs.deinit();
    fs.expected_token = "secret";
    var c = Client.init(gpa, fs.transport(), .{}); // no token configured
    defer c.deinit();

    try testing.expectError(error.AuthFailed, c.putObject("bk", "obj", "data"));
}

// ---------------------------------------------------------------------------
// deleteObject tests
// ---------------------------------------------------------------------------

test "Client.deleteObject: removes a previously put object" {
    const gpa = testing.allocator;
    var fs = FakeServer.init(gpa, DEFAULT_BASE_URL, "bk", "legacy", "");
    defer fs.deinit();
    var c = Client.init(gpa, fs.transport(), .{});
    defer c.deinit();

    try c.putObject("bk", "sstables/orphan.sst", "junk");
    try c.deleteObject("bk", "sstables/orphan.sst");
    try testing.expectEqual(@as(u32, 1), fs.delete_count);

    // Subsequent rangeGet of the removed object 404s -> BadStatus.
    var dst: [4]u8 = undefined;
    try testing.expectError(error.BadStatus, c.rangeGet("bk", "sstables/orphan.sst", 0, 4, &dst));
}

test "Client.deleteObject: 404 on missing object is folded into success" {
    const gpa = testing.allocator;
    var fs = FakeServer.init(gpa, DEFAULT_BASE_URL, "bk", "x", "");
    defer fs.deinit();
    var c = Client.init(gpa, fs.transport(), .{});
    defer c.deinit();

    try c.deleteObject("bk", "never/existed.sst");
    try testing.expectEqual(@as(u32, 1), fs.delete_count);
}

test "Client.rangeGet: 401 + TokenSource triggers a single refresh-and-retry" {
    const gpa = testing.allocator;

    var fs = FakeServer.init(gpa, DEFAULT_BASE_URL, "bk", "obj.sst", "abcdefgh");
    defer fs.deinit();
    fs.expected_token = "tok-good";

    // Metadata server: first call returns "tok-bad", second "tok-good".
    const RotatingMeta = struct {
        n: u32 = 0,
        pub fn transport(self: *@This()) HttpTransport {
            return .{ .ptr = @ptrCast(self), .vtable = &vt };
        }
        const vt: HttpTransport.VTable = .{
            .get = getImpl,
            .put = HttpTransport.unsupportedPut,
            .delete = HttpTransport.unsupportedDelete,
        };
        fn getImpl(p: *anyopaque, _: []const u8, _: []const Header, body_dst: []u8) anyerror!Response {
            const self: *@This() = @ptrCast(@alignCast(p));
            self.n += 1;
            const json = if (self.n == 1)
                "{\"access_token\":\"tok-bad\",\"expires_in\":3600}"
            else
                "{\"access_token\":\"tok-good\",\"expires_in\":3600}";
            const k = @min(body_dst.len, json.len);
            @memcpy(body_dst[0..k], json[0..k]);
            return .{ .status = 200, .body_len = k };
        }
    };
    var meta = RotatingMeta{};

    var ts = auth.TokenSource.init(
        gpa,
        meta.transport(),
        auth.DEFAULT_METADATA_URL,
        auth.SystemClock.clock(),
    );
    defer ts.deinit();

    var c = Client.init(gpa, fs.transport(), .{ .token_source = &ts });
    defer c.deinit();

    var dst: [3]u8 = undefined;
    const n = try c.rangeGet("bk", "obj.sst", 0, 3, &dst);
    try testing.expectEqual(@as(usize, 3), n);
    // Two metadata calls total: the initial token + the post-401 refresh.
    try testing.expectEqual(@as(u32, 2), meta.n);
}

test "Client.deleteObject: forced 500 maps to DeleteFailed" {
    const gpa = testing.allocator;
    var fs = FakeServer.init(gpa, DEFAULT_BASE_URL, "bk", "x", "");
    defer fs.deinit();
    fs.forced_delete_status = 500;
    var c = Client.init(gpa, fs.transport(), .{});
    defer c.deinit();

    try testing.expectError(error.DeleteFailed, c.deleteObject("bk", "obj"));
}
