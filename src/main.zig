const std = @import("std");
const Io = std.Io;

const zero_db = @import("zero_db");
const http = zero_db.server;

const max_body_bytes: usize = 16 * 1024 * 1024;

pub fn main(init: std.process.Init) !void {
    const arena: std.mem.Allocator = init.arena.allocator();
    const args = try init.minimal.args.toSlice(arena);

    const io = init.io;
    var stderr_buffer: [1024]u8 = undefined;
    var stderr_file_writer: Io.File.Writer = .init(.stderr(), io, &stderr_buffer);
    const stderr = &stderr_file_writer.interface;

    const mode: Mode = parseArgs(args[1..]);
    switch (mode) {
        .help => try printUsage(stderr),
        .selftest => try selftest(init.gpa, stderr),
        .serve => try serve(init, stderr),
    }
    try stderr.flush();
}

const Mode = enum { serve, selftest, help };

fn parseArgs(args: []const [:0]const u8) Mode {
    for (args) |arg| {
        if (std.mem.eql(u8, arg, "--selftest")) return .selftest;
        if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) return .help;
    }
    return .serve;
}

fn printUsage(w: *Io.Writer) !void {
    try w.writeAll(
        \\zero_db — serverless KV (LSM, Cloud Run target)
        \\
        \\Usage:
        \\  zero_db                serve HTTP on $PORT (default 8080)
        \\  zero_db --selftest     run an in-process Engine smoke test
        \\  zero_db --help         show this message
        \\
        \\Routes (serve mode):
        \\  GET    /healthz
        \\  GET    /v1/kv/{key}
        \\  PUT    /v1/kv/{key}      body = value
        \\  DELETE /v1/kv/{key}
        \\  POST   /admin/flush
        \\  POST   /admin/compact
        \\  GET    /admin/stats
        \\
    );
}

// ---------------------------------------------------------------------------
// Serve mode
// ---------------------------------------------------------------------------

fn serve(init: std.process.Init, log: *Io.Writer) !void {
    const gpa = init.gpa;
    const io = init.io;

    const port = parsePort(init.environ_map.*) orelse 8080;

    // Production wiring (enabled in Phase 18, the Cloud Run packaging
    // phase). Sketch:
    //
    //     var rt = zero_db.gcs.RealTransport.init(gpa, io);
    //     defer rt.deinit();
    //     var ts = zero_db.auth.TokenSource.init(
    //         gpa,
    //         rt.transport(),
    //         zero_db.auth.DEFAULT_METADATA_URL,
    //         zero_db.auth.SystemClock.clock(),
    //     );
    //     defer ts.deinit();
    //     var client = zero_db.gcs.Client.init(gpa, rt.transport(), .{ .token_source = &ts });
    //     defer client.deinit();
    //     // engine.init takes (..., bucket, manifest_object, &client) once
    //     // Phase 12 lands the upload + manifest path.
    //
    // Until Phase 12, the engine remains in-memory only on cold start.

    var engine = try zero_db.engine.Engine.init(gpa, .{});
    defer engine.deinit();

    // Per-request arena pool. Cloud Run typical concurrency is ~80; 32
    // pooled arenas is a comfortable upper bound on simultaneous in-
    // flight requests sharing already-mmap'd buffers.
    var pool = zero_db.arena_pool.ArenaPool.init(gpa, 32);
    defer pool.deinit();

    const addr = try Io.net.IpAddress.parse("0.0.0.0", port);
    var listener = try addr.listen(io, .{ .reuse_address = true });
    defer listener.deinit(io);

    try log.print("zero-db serving on 0.0.0.0:{d}\n", .{port});
    try log.flush();

    while (true) {
        const stream = listener.accept(io) catch |err| {
            log.print("accept failed: {s}\n", .{@errorName(err)}) catch {};
            log.flush() catch {};
            continue;
        };
        defer stream.close(io);

        handleOne(gpa, &pool, io, stream, &engine, log) catch |err| {
            log.print("request failed: {s}\n", .{@errorName(err)}) catch {};
            log.flush() catch {};
        };
    }
}

fn parsePort(environ: std.process.Environ.Map) ?u16 {
    const raw = environ.get("PORT") orelse return null;
    return std.fmt.parseInt(u16, raw, 10) catch null;
}

/// Process a single accepted connection. Borrows a per-request arena
/// from the pool so allocations (decoded keys, value copies, JSON
/// bodies) reuse already-mmap'd capacity from the previous request.
fn handleOne(
    gpa: std.mem.Allocator,
    pool: *zero_db.arena_pool.ArenaPool,
    io: Io,
    stream: Io.net.Stream,
    engine: *zero_db.engine.Engine,
    log: *Io.Writer,
) !void {
    _ = gpa; // pool already carries the parent allocator
    var read_buffer: [16 * 1024]u8 = undefined;
    var write_buffer: [16 * 1024]u8 = undefined;

    var stream_reader = stream.reader(io, &read_buffer);
    var stream_writer = stream.writer(io, &write_buffer);

    var server = std.http.Server.init(&stream_reader.interface, &stream_writer.interface);
    var http_req = server.receiveHead() catch |err| {
        log.print("receiveHead: {s}\n", .{@errorName(err)}) catch {};
        return;
    };

    const arena_state = try pool.acquire();
    defer pool.release(arena_state);
    const arena = arena_state.allocator();

    const target_full = http_req.head.target;
    const target_path = stripQuery(target_full);
    const method = mapMethod(http_req.head.method);

    // Read body for methods that carry one and that signal a length. A
    // POST/PUT without Content-Length and without Transfer-Encoding has no
    // body in HTTP/1.1 — treat it as empty rather than reading until the
    // client closes (which would hang the request).
    var body: []const u8 = "";
    var body_too_large = false;
    if (http_req.head.method.requestHasBody() and bodyHasContent(http_req.head)) {
        if (http_req.head.content_length) |cl| {
            if (cl > max_body_bytes) body_too_large = true;
        }
        if (!body_too_large) {
            var body_buf: [4096]u8 = undefined;
            const body_reader = http_req.readerExpectNone(&body_buf);
            body = body_reader.allocRemaining(arena, .limited(max_body_bytes)) catch |err| switch (err) {
                error.StreamTooLong => blk: {
                    body_too_large = true;
                    break :blk "";
                },
                else => return err,
            };
        }
    }

    const req: http.Request = .{
        .method = method,
        .target = target_path,
        .body = body,
        .body_too_large = body_too_large,
    };

    const resp = try http.handle(arena, engine, req);

    const status: std.http.Status = @enumFromInt(resp.status);
    const headers = [_]std.http.Header{
        .{ .name = "content-type", .value = resp.content_type },
    };

    // For 204 No Content: HTTP forbids a content-length on these. Pass
    // transfer_encoding=.none so respond skips both content-length and
    // chunked headers.
    const opts: std.http.Server.Request.RespondOptions = if (resp.status == 204) .{
        .status = status,
        .extra_headers = &headers,
        .transfer_encoding = .none,
    } else .{
        .status = status,
        .extra_headers = &headers,
    };

    http_req.respond(resp.body, opts) catch |err| {
        log.print("respond: {s}\n", .{@errorName(err)}) catch {};
    };
}

fn bodyHasContent(head: std.http.Server.Request.Head) bool {
    return head.transfer_encoding == .chunked or
        (head.content_length orelse 0) > 0;
}

fn stripQuery(target: []const u8) []const u8 {
    const q = std.mem.findScalar(u8, target, '?') orelse return target;
    return target[0..q];
}

fn mapMethod(m: std.http.Method) http.Method {
    return switch (m) {
        .GET => .GET,
        .PUT => .PUT,
        .DELETE => .DELETE,
        .POST => .POST,
        else => .OTHER,
    };
}

// ---------------------------------------------------------------------------
// Selftest mode
// ---------------------------------------------------------------------------

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

test "parsePort handles missing/malformed/valid" {
    var map = std.process.Environ.Map.init(std.testing.allocator);
    defer map.deinit();

    try std.testing.expectEqual(@as(?u16, null), parsePort(map));

    try map.put("PORT", "abc");
    try std.testing.expectEqual(@as(?u16, null), parsePort(map));

    try map.put("PORT", "9090");
    try std.testing.expectEqual(@as(?u16, 9090), parsePort(map));
}

test "stripQuery handles plain and querystring paths" {
    try std.testing.expectEqualStrings("/foo", stripQuery("/foo"));
    try std.testing.expectEqualStrings("/foo", stripQuery("/foo?a=1"));
    try std.testing.expectEqualStrings("/", stripQuery("/?bare"));
}

test "mapMethod covers all our routes" {
    try std.testing.expectEqual(http.Method.GET, mapMethod(.GET));
    try std.testing.expectEqual(http.Method.PUT, mapMethod(.PUT));
    try std.testing.expectEqual(http.Method.DELETE, mapMethod(.DELETE));
    try std.testing.expectEqual(http.Method.POST, mapMethod(.POST));
    try std.testing.expectEqual(http.Method.OTHER, mapMethod(.PATCH));
    try std.testing.expectEqual(http.Method.OTHER, mapMethod(.HEAD));
}

test "parseArgs picks selftest, help, or default serve" {
    try std.testing.expectEqual(Mode.serve, parseArgs(&.{}));
    try std.testing.expectEqual(Mode.selftest, parseArgs(&.{"--selftest"}));
    try std.testing.expectEqual(Mode.help, parseArgs(&.{"--help"}));
    try std.testing.expectEqual(Mode.help, parseArgs(&.{"-h"}));
}
