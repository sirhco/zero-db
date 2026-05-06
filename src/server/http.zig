//! HTTP request handler — pure routing, socket-free.
//!
//! Exposes `handle()` which takes a parsed `Request` and produces a
//! `Response`. The std.http.Server adapter lives in `main.zig` and is the
//! only place that touches the wire; everything in here is unit-testable
//! against an in-memory `Engine` with no TCP listener required.
//!
//! Routes:
//!   GET    /healthz          → 200 "ok\n"
//!   GET    /v1/kv/{key}      → 200 raw value, or 404
//!   PUT    /v1/kv/{key}      → 204 (body is the value)
//!   DELETE /v1/kv/{key}      → 204
//!   POST   /admin/flush      → 204
//!   POST   /admin/compact    → 204
//!   GET    /admin/stats      → 200 application/json
//!
//! Concurrency: caller must serialize `handle` calls against a single
//! `Engine`. The v0 adapter in main.zig is a single-threaded accept loop,
//! which gives that for free. A future thread-pool would wrap `handle` in
//! an `Io.Mutex` (which requires an `Io` instance to lock/unlock).

const std = @import("std");

const engine_mod = @import("../engine/engine.zig");
const sstable_format = @import("../sstable/format.zig");

const Engine = engine_mod.Engine;

pub const Method = enum { GET, PUT, DELETE, POST, OTHER };

pub const Request = struct {
    method: Method,
    /// URL path with the query string already stripped by the adapter.
    target: []const u8,
    /// Request body. Empty for methods that don't carry one.
    body: []const u8,
    /// Set by the adapter when the wire body exceeded the configured cap so
    /// we can respond 413 without allocating the rejected payload.
    body_too_large: bool = false,
};

pub const Response = struct {
    status: u16,
    content_type: []const u8 = "text/plain; charset=utf-8",
    /// Bytes to send on the wire. Lifetime: arena-owned (the arena passed
    /// to `handle`) for allocated bodies, or static for fixed responses.
    /// Caller must keep the arena alive until the response is written.
    body: []const u8,
};

pub const HandleError = error{OutOfMemory};

/// Route a single request and return the response. Body materialization
/// (JSON formatting, decoded keys, value copies) uses `arena`.
pub fn handle(
    arena: std.mem.Allocator,
    engine: *Engine,
    req: Request,
) HandleError!Response {
    const target = req.target;

    if (std.mem.eql(u8, target, "/healthz")) {
        return methodGuard(req.method, .GET) orelse plain(200, "ok\n");
    }

    if (std.mem.eql(u8, target, "/admin/flush")) {
        return methodGuard(req.method, .POST) orelse adminFlush(engine);
    }
    if (std.mem.eql(u8, target, "/admin/compact")) {
        return methodGuard(req.method, .POST) orelse adminCompact(engine);
    }
    if (std.mem.eql(u8, target, "/admin/stats")) {
        return methodGuard(req.method, .GET) orelse adminStats(arena, engine);
    }

    const kv_prefix = "/v1/kv/";
    if (std.mem.startsWith(u8, target, kv_prefix)) {
        const raw_key = target[kv_prefix.len..];
        return kvRoute(arena, engine, req, raw_key);
    }

    return plain(404, "not found\n");
}

// ---- routes ---------------------------------------------------------------

fn kvRoute(
    arena: std.mem.Allocator,
    engine: *Engine,
    req: Request,
    raw_key: []const u8,
) HandleError!Response {
    if (raw_key.len == 0) return plain(400, "bad request: empty key\n");

    const key = percentDecode(arena, raw_key) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        error.BadEncoding => return plain(400, "bad request: bad percent-encoding\n"),
    };

    if (key.len == 0) return plain(400, "bad request: empty key\n");
    if (key.len > sstable_format.MAX_KEY_LEN) return plain(400, "bad request: key too long\n");

    return switch (req.method) {
        .GET => kvGet(arena, engine, key),
        .PUT => kvPut(engine, req, key),
        .DELETE => kvDelete(engine, key),
        else => plain(405, "method not allowed\n"),
    };
}

fn kvGet(
    arena: std.mem.Allocator,
    engine: *Engine,
    key: []const u8,
) HandleError!Response {
    const got = engine.get(key, arena) catch |err| return engineError(err);
    if (got) |bytes| {
        return .{
            .status = 200,
            .content_type = "application/octet-stream",
            .body = bytes,
        };
    }
    return plain(404, "not found\n");
}

fn kvPut(engine: *Engine, req: Request, key: []const u8) HandleError!Response {
    if (req.body_too_large) return plain(413, "payload too large\n");
    engine.set(key, req.body) catch |err| return engineError(err);
    return noContent();
}

fn kvDelete(engine: *Engine, key: []const u8) HandleError!Response {
    engine.delete(key) catch |err| return engineError(err);
    return noContent();
}

fn adminFlush(engine: *Engine) HandleError!Response {
    engine.flush() catch |err| return engineError(err);
    return noContent();
}

fn adminCompact(engine: *Engine) HandleError!Response {
    engine.compactAll() catch |err| return engineError(err);
    return noContent();
}

fn adminStats(arena: std.mem.Allocator, engine: *Engine) HandleError!Response {
    const sst = engine.sstableCount();
    const active = engine.entryCountActive();
    const failures = engine.compactionFailureCount();

    const body = try std.fmt.allocPrint(
        arena,
        "{{\"sstables\":{d},\"active_entries\":{d},\"compaction_failures\":{d}}}\n",
        .{ sst, active, failures },
    );
    return .{
        .status = 200,
        .content_type = "application/json",
        .body = body,
    };
}

// ---- helpers --------------------------------------------------------------

fn methodGuard(actual: Method, expected: Method) ?Response {
    if (actual == expected) return null;
    return plain(405, "method not allowed\n");
}

fn plain(status: u16, body: []const u8) Response {
    return .{ .status = status, .body = body };
}

fn noContent() Response {
    return .{ .status = 204, .body = "" };
}

fn engineError(err: anyerror) HandleError!Response {
    if (err == error.OutOfMemory) return plain(503, "out of memory\n");
    if (err == error.KeyTooLong) return plain(400, "bad request: key too long\n");
    return plain(500, "internal error\n");
}

const PercentDecodeError = error{ OutOfMemory, BadEncoding };

/// Decode `%XX` escapes in a URL path segment. `+` is kept literal — keys
/// are arbitrary bytes and `+`-as-space is form-data semantics, not path.
fn percentDecode(arena: std.mem.Allocator, src: []const u8) PercentDecodeError![]u8 {
    // Worst case: no escapes, output equals input.
    var out = try arena.alloc(u8, src.len);
    var n: usize = 0;
    var i: usize = 0;
    while (i < src.len) {
        const c = src[i];
        if (c == '%') {
            if (i + 2 >= src.len) return error.BadEncoding;
            const hi = hexNibble(src[i + 1]) orelse return error.BadEncoding;
            const lo = hexNibble(src[i + 2]) orelse return error.BadEncoding;
            out[n] = (hi << 4) | lo;
            n += 1;
            i += 3;
        } else {
            out[n] = c;
            n += 1;
            i += 1;
        }
    }
    return out[0..n];
}

fn hexNibble(c: u8) ?u8 {
    return switch (c) {
        '0'...'9' => c - '0',
        'a'...'f' => c - 'a' + 10,
        'A'...'F' => c - 'A' + 10,
        else => null,
    };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const testing = std.testing;

const Harness = struct {
    arena: std.heap.ArenaAllocator,
    engine: Engine,

    fn init(gpa: std.mem.Allocator) !Harness {
        return .{
            .arena = std.heap.ArenaAllocator.init(gpa),
            .engine = try Engine.init(gpa, .{}),
        };
    }

    fn deinit(self: *Harness) void {
        self.engine.deinit();
        self.arena.deinit();
    }

    fn call(self: *Harness, req: Request) !Response {
        return handle(self.arena.allocator(), &self.engine, req);
    }
};

test "GET /healthz returns 200 ok" {
    var h = try Harness.init(testing.allocator);
    defer h.deinit();

    const r = try h.call(.{ .method = .GET, .target = "/healthz", .body = "" });
    try testing.expectEqual(@as(u16, 200), r.status);
    try testing.expectEqualStrings("ok\n", r.body);
}

test "PUT then GET on /v1/kv/{key} round-trips" {
    var h = try Harness.init(testing.allocator);
    defer h.deinit();

    const put = try h.call(.{ .method = .PUT, .target = "/v1/kv/foo", .body = "bar" });
    try testing.expectEqual(@as(u16, 204), put.status);

    const get = try h.call(.{ .method = .GET, .target = "/v1/kv/foo", .body = "" });
    try testing.expectEqual(@as(u16, 200), get.status);
    try testing.expectEqualStrings("bar", get.body);
    try testing.expectEqualStrings("application/octet-stream", get.content_type);
}

test "GET /v1/kv/missing returns 404" {
    var h = try Harness.init(testing.allocator);
    defer h.deinit();

    const r = try h.call(.{ .method = .GET, .target = "/v1/kv/missing", .body = "" });
    try testing.expectEqual(@as(u16, 404), r.status);
}

test "DELETE removes a previously set key" {
    var h = try Harness.init(testing.allocator);
    defer h.deinit();

    _ = try h.call(.{ .method = .PUT, .target = "/v1/kv/foo", .body = "bar" });

    const del = try h.call(.{ .method = .DELETE, .target = "/v1/kv/foo", .body = "" });
    try testing.expectEqual(@as(u16, 204), del.status);

    const get = try h.call(.{ .method = .GET, .target = "/v1/kv/foo", .body = "" });
    try testing.expectEqual(@as(u16, 404), get.status);
}

test "empty key path returns 400" {
    var h = try Harness.init(testing.allocator);
    defer h.deinit();

    const r = try h.call(.{ .method = .PUT, .target = "/v1/kv/", .body = "x" });
    try testing.expectEqual(@as(u16, 400), r.status);
}

test "key longer than MAX_KEY_LEN returns 400" {
    var h = try Harness.init(testing.allocator);
    defer h.deinit();

    var path_buf: [sstable_format.MAX_KEY_LEN + 32]u8 = undefined;
    @memcpy(path_buf[0.."/v1/kv/".len], "/v1/kv/");
    @memset(path_buf["/v1/kv/".len..], 'k');

    const r = try h.call(.{ .method = .PUT, .target = path_buf[0..], .body = "" });
    try testing.expectEqual(@as(u16, 400), r.status);
}

test "POST /admin/flush moves data to an SSTable" {
    var h = try Harness.init(testing.allocator);
    defer h.deinit();

    _ = try h.call(.{ .method = .PUT, .target = "/v1/kv/k1", .body = "v1" });
    try testing.expectEqual(@as(usize, 0), h.engine.sstableCount());

    const r = try h.call(.{ .method = .POST, .target = "/admin/flush", .body = "" });
    try testing.expectEqual(@as(u16, 204), r.status);
    try testing.expectEqual(@as(usize, 1), h.engine.sstableCount());
}

test "POST /admin/compact collapses SSTables" {
    var h = try Harness.init(testing.allocator);
    defer h.deinit();

    _ = try h.call(.{ .method = .PUT, .target = "/v1/kv/a", .body = "1" });
    _ = try h.call(.{ .method = .POST, .target = "/admin/flush", .body = "" });
    _ = try h.call(.{ .method = .PUT, .target = "/v1/kv/b", .body = "2" });
    _ = try h.call(.{ .method = .POST, .target = "/admin/flush", .body = "" });
    try testing.expectEqual(@as(usize, 2), h.engine.sstableCount());

    const r = try h.call(.{ .method = .POST, .target = "/admin/compact", .body = "" });
    try testing.expectEqual(@as(u16, 204), r.status);
    try testing.expectEqual(@as(usize, 1), h.engine.sstableCount());
}

test "GET /admin/stats returns JSON with both fields" {
    var h = try Harness.init(testing.allocator);
    defer h.deinit();

    _ = try h.call(.{ .method = .PUT, .target = "/v1/kv/a", .body = "1" });
    _ = try h.call(.{ .method = .PUT, .target = "/v1/kv/b", .body = "2" });

    const r = try h.call(.{ .method = .GET, .target = "/admin/stats", .body = "" });
    try testing.expectEqual(@as(u16, 200), r.status);
    try testing.expectEqualStrings("application/json", r.content_type);
    try testing.expect(std.mem.indexOf(u8, r.body, "\"sstables\":") != null);
    try testing.expect(std.mem.indexOf(u8, r.body, "\"active_entries\":2") != null);
}

test "method/path edge cases: 405, 404, 413, percent-decode" {
    var h = try Harness.init(testing.allocator);
    defer h.deinit();

    // OTHER (PATCH-equivalent) on /healthz → 405.
    const m = try h.call(.{ .method = .OTHER, .target = "/healthz", .body = "" });
    try testing.expectEqual(@as(u16, 405), m.status);

    // Unknown path → 404.
    const u = try h.call(.{ .method = .GET, .target = "/unknown", .body = "" });
    try testing.expectEqual(@as(u16, 404), u.status);

    // body_too_large set by adapter → 413.
    const t = try h.call(.{
        .method = .PUT,
        .target = "/v1/kv/foo",
        .body = "",
        .body_too_large = true,
    });
    try testing.expectEqual(@as(u16, 413), t.status);

    // %2F-encoded slash decodes back into the key. Round-trip set/get to
    // confirm the decoder's output is what reaches the engine.
    _ = try h.call(.{ .method = .PUT, .target = "/v1/kv/a%2Fb", .body = "encoded" });
    const got = try h.call(.{ .method = .GET, .target = "/v1/kv/a%2Fb", .body = "" });
    try testing.expectEqual(@as(u16, 200), got.status);
    try testing.expectEqualStrings("encoded", got.body);

    // Malformed percent-encoding → 400.
    const bad = try h.call(.{ .method = .GET, .target = "/v1/kv/a%ZZ", .body = "" });
    try testing.expectEqual(@as(u16, 400), bad.status);
}
