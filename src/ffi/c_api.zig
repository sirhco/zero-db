//! C ABI surface for embedding Zero-DB in non-Zig processes.
//!
//! Errors are returned as a flat `c_int`; positive values are status
//! codes the caller maps to its own error model. The handle is opaque;
//! callers treat it as an `void*` (or `zero_db_handle_t*` with the
//! provided header).
//!
//! Memory model:
//!   - Caller-supplied byte slices (`key_ptr/len`, `value_ptr/len`)
//!     are borrowed for the call's duration. The library copies what
//!     it needs to retain.
//!   - `zero_db_get` returns a library-allocated buffer the caller
//!     frees via `zero_db_free`.
//!   - `zero_db_open` dupes every string from `Options` into the
//!     handle's allocator, so caller-supplied option pointers can
//!     vanish immediately.
//!
//! Threading:
//!   - `zero_db_open` and `zero_db_close` must run on a single thread.
//!   - Subsequent `set` / `get` / `delete` / `flush` / `compact` may
//!     interleave from any thread; the underlying engine's SpinMutex
//!     protects shared state.

const std = @import("std");

const engine_mod = @import("../engine/engine.zig");
const gcs = @import("../storage/gcs.zig");
const auth = @import("../storage/auth.zig");

// ---------------------------------------------------------------------------
// Public ABI: error codes
// ---------------------------------------------------------------------------

pub const ZERO_DB_OK: c_int = 0;
pub const ZERO_DB_ERR_OOM: c_int = 1;
pub const ZERO_DB_ERR_NOT_FOUND: c_int = 2;
pub const ZERO_DB_ERR_INVALID_ARG: c_int = 3;
pub const ZERO_DB_ERR_IO: c_int = 4;
pub const ZERO_DB_ERR_AUTH: c_int = 5;
pub const ZERO_DB_ERR_OVER_BUDGET: c_int = 6;
pub const ZERO_DB_ERR_MANIFEST_CONFLICT: c_int = 7;
pub const ZERO_DB_ERR_INTERNAL: c_int = 99;

// ---------------------------------------------------------------------------
// Public ABI: opaque handle + options + stats
// ---------------------------------------------------------------------------

pub const Handle = opaque {};

pub const Options = extern struct {
    /// Nullable. NULL → in-memory only mode.
    bucket: ?[*:0]const u8 = null,
    /// Nullable. Defaults to "manifest.json" when bucket is set.
    manifest_object: ?[*:0]const u8 = null,
    /// Nullable. When set, every set/delete is fsync'd to this path.
    wal_path: ?[*:0]const u8 = null,
    /// 0 → engine default.
    memtable_flush_bytes: usize = 0,
    /// 0 → engine default.
    block_cache_bytes: usize = 0,
    /// 0 → engine default.
    l0_compaction_threshold: usize = 0,
    /// Pad to a stable size so future fields can be appended without
    /// ABI breakage. Reserved bytes MUST be zero.
    _reserved: [64]u8 = @splat(0),
};

pub const Stats = extern struct {
    sstables: u64,
    active_entries: u64,
    compaction_failures: u64,
};

// ---------------------------------------------------------------------------
// Internal state — opaque to consumers.
// ---------------------------------------------------------------------------

const State = struct {
    gpa_state: std.heap.DebugAllocator(.{}),
    gpa: std.mem.Allocator,
    threaded: std.Io.Threaded,
    rt: ?gcs.RealTransport,
    ts: ?auth.TokenSource,
    client: ?gcs.Client,
    engine: engine_mod.Engine,
    bucket_owned: ?[]u8,
    manifest_owned: ?[]u8,
    wal_owned: ?[]u8,
};

fn handleFromState(s: *State) *Handle {
    return @ptrCast(s);
}

fn stateFromHandle(h: *Handle) *State {
    return @ptrCast(@alignCast(h));
}

fn mapEngineError(err: anyerror) c_int {
    return switch (err) {
        error.OutOfMemory => ZERO_DB_ERR_OOM,
        error.OverMemoryBudget => ZERO_DB_ERR_OVER_BUDGET,
        error.AuthFailed => ZERO_DB_ERR_AUTH,
        error.ManifestConflict => ZERO_DB_ERR_MANIFEST_CONFLICT,
        error.ManifestLoadFailed,
        error.HttpError,
        error.BadStatus,
        error.PutFailed,
        error.DeleteFailed,
        error.PreconditionFailed,
        error.EmptyRange,
        error.ShortRead,
        => ZERO_DB_ERR_IO,
        else => ZERO_DB_ERR_INTERNAL,
    };
}

// ---------------------------------------------------------------------------
// Public ABI: lifecycle
// ---------------------------------------------------------------------------

pub export fn zero_db_open(
    options: ?*const Options,
    out: *?*Handle,
) c_int {
    out.* = null;
    const opts = options orelse return ZERO_DB_ERR_INVALID_ARG;

    const allocator_for_state = std.heap.page_allocator;
    const state = allocator_for_state.create(State) catch return ZERO_DB_ERR_OOM;
    errdefer allocator_for_state.destroy(state);

    state.* = .{
        .gpa_state = .init,
        .gpa = undefined,
        .threaded = std.Io.Threaded.init(allocator_for_state, .{}),
        .rt = null,
        .ts = null,
        .client = null,
        .engine = undefined,
        .bucket_owned = null,
        .manifest_owned = null,
        .wal_owned = null,
    };
    state.gpa = state.gpa_state.allocator();
    errdefer _ = state.gpa_state.deinit();
    errdefer state.threaded.deinit();
    errdefer if (state.bucket_owned) |s| state.gpa.free(s);
    errdefer if (state.manifest_owned) |s| state.gpa.free(s);
    errdefer if (state.wal_owned) |s| state.gpa.free(s);

    var engine_opts: engine_mod.Options = .{};
    if (opts.bucket) |b_z| {
        const bucket_slice = std.mem.span(b_z);
        state.bucket_owned = state.gpa.dupe(u8, bucket_slice) catch return ZERO_DB_ERR_OOM;
        engine_opts.bucket = state.bucket_owned.?;

        if (opts.manifest_object) |m_z| {
            const m = std.mem.span(m_z);
            state.manifest_owned = state.gpa.dupe(u8, m) catch return ZERO_DB_ERR_OOM;
            engine_opts.manifest_object = state.manifest_owned.?;
        }

        const io = state.threaded.io();
        state.rt = gcs.RealTransport.init(state.gpa, io);
        errdefer if (state.rt) |*r| r.deinit();
        state.ts = auth.TokenSource.init(
            state.gpa,
            state.rt.?.transport(),
            auth.DEFAULT_METADATA_URL,
            auth.SystemClock.clock(),
        );
        errdefer if (state.ts) |*t| t.deinit();
        state.client = gcs.Client.init(
            state.gpa,
            state.rt.?.transport(),
            .{ .token_source = &state.ts.? },
        );
        errdefer if (state.client) |*c| c.deinit();
        engine_opts.gcs_client = &state.client.?;
    }
    if (opts.wal_path) |w_z| {
        const w = std.mem.span(w_z);
        state.wal_owned = state.gpa.dupe(u8, w) catch return ZERO_DB_ERR_OOM;
        engine_opts.wal_path = state.wal_owned.?;
    }
    if (opts.memtable_flush_bytes != 0) engine_opts.memtable_flush_bytes = opts.memtable_flush_bytes;
    if (opts.block_cache_bytes != 0) engine_opts.block_cache_bytes = opts.block_cache_bytes;
    if (opts.l0_compaction_threshold != 0) engine_opts.l0_compaction_threshold = opts.l0_compaction_threshold;

    state.engine = engine_mod.Engine.init(state.gpa, engine_opts) catch
        return ZERO_DB_ERR_INTERNAL;
    if (engine_opts.gcs_client != null) {
        state.engine.startCompactor() catch {
            state.engine.deinit();
            return ZERO_DB_ERR_INTERNAL;
        };
    }

    out.* = handleFromState(state);
    return ZERO_DB_OK;
}

pub export fn zero_db_close(h: ?*Handle) void {
    const handle = h orelse return;
    const state = stateFromHandle(handle);
    state.engine.deinit();
    if (state.client) |*c| c.deinit();
    if (state.ts) |*t| t.deinit();
    if (state.rt) |*r| r.deinit();
    if (state.bucket_owned) |s| state.gpa.free(s);
    if (state.manifest_owned) |s| state.gpa.free(s);
    if (state.wal_owned) |s| state.gpa.free(s);
    state.threaded.deinit();
    _ = state.gpa_state.deinit();
    std.heap.page_allocator.destroy(state);
}

// ---------------------------------------------------------------------------
// Public ABI: KV
// ---------------------------------------------------------------------------

pub export fn zero_db_set(
    h: ?*Handle,
    key_ptr: [*]const u8,
    key_len: usize,
    value_ptr: [*]const u8,
    value_len: usize,
) c_int {
    const handle = h orelse return ZERO_DB_ERR_INVALID_ARG;
    const state = stateFromHandle(handle);
    state.engine.set(key_ptr[0..key_len], value_ptr[0..value_len]) catch |err|
        return mapEngineError(err);
    return ZERO_DB_OK;
}

pub export fn zero_db_delete(
    h: ?*Handle,
    key_ptr: [*]const u8,
    key_len: usize,
) c_int {
    const handle = h orelse return ZERO_DB_ERR_INVALID_ARG;
    const state = stateFromHandle(handle);
    state.engine.delete(key_ptr[0..key_len]) catch |err| return mapEngineError(err);
    return ZERO_DB_OK;
}

pub export fn zero_db_get(
    h: ?*Handle,
    key_ptr: [*]const u8,
    key_len: usize,
    out_value: *?[*]u8,
    out_value_len: *usize,
) c_int {
    const handle = h orelse return ZERO_DB_ERR_INVALID_ARG;
    const state = stateFromHandle(handle);
    out_value.* = null;
    out_value_len.* = 0;
    const got = state.engine.get(key_ptr[0..key_len], state.gpa) catch |err|
        return mapEngineError(err);
    const v = got orelse return ZERO_DB_ERR_NOT_FOUND;
    out_value.* = v.ptr;
    out_value_len.* = v.len;
    return ZERO_DB_OK;
}

pub export fn zero_db_free(
    h: ?*Handle,
    ptr: ?[*]u8,
    len: usize,
) void {
    const handle = h orelse return;
    const p = ptr orelse return;
    const state = stateFromHandle(handle);
    state.gpa.free(p[0..len]);
}

// ---------------------------------------------------------------------------
// Public ABI: admin
// ---------------------------------------------------------------------------

pub export fn zero_db_flush(h: ?*Handle) c_int {
    const handle = h orelse return ZERO_DB_ERR_INVALID_ARG;
    const state = stateFromHandle(handle);
    state.engine.flush() catch |err| return mapEngineError(err);
    return ZERO_DB_OK;
}

pub export fn zero_db_compact(h: ?*Handle) c_int {
    const handle = h orelse return ZERO_DB_ERR_INVALID_ARG;
    const state = stateFromHandle(handle);
    state.engine.compactAll() catch |err| return mapEngineError(err);
    return ZERO_DB_OK;
}

pub export fn zero_db_stats(h: ?*Handle, out: *Stats) c_int {
    const handle = h orelse return ZERO_DB_ERR_INVALID_ARG;
    const state = stateFromHandle(handle);
    out.* = .{
        .sstables = state.engine.sstableCount(),
        .active_entries = state.engine.entryCountActive(),
        .compaction_failures = state.engine.compactionFailureCount(),
    };
    return ZERO_DB_OK;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const testing = std.testing;

test "FFI: open + close on in-memory mode" {
    var h: ?*Handle = null;
    const opts: Options = .{};
    try testing.expectEqual(ZERO_DB_OK, zero_db_open(&opts, &h));
    try testing.expect(h != null);
    zero_db_close(h);
}

test "FFI: set + delete (no read-back)" {
    var h: ?*Handle = null;
    const opts: Options = .{};
    try testing.expectEqual(ZERO_DB_OK, zero_db_open(&opts, &h));
    defer zero_db_close(h);
    try testing.expectEqual(ZERO_DB_OK, zero_db_set(h, "alpha", 5, "AAA", 3));
    try testing.expectEqual(ZERO_DB_OK, zero_db_delete(h, "alpha", 5));
}

test "FFI: set + get + free roundtrips a value" {
    var h: ?*Handle = null;
    const opts: Options = .{};
    try testing.expectEqual(ZERO_DB_OK, zero_db_open(&opts, &h));
    defer zero_db_close(h);
    try testing.expectEqual(ZERO_DB_OK, zero_db_set(h, "k", 1, "value", 5));

    var out: ?[*]u8 = null;
    var out_len: usize = 0;
    try testing.expectEqual(ZERO_DB_OK, zero_db_get(h, "k", 1, &out, &out_len));
    defer zero_db_free(h, out, out_len);

    try testing.expect(out != null);
    try testing.expectEqualStrings("value", out.?[0..out_len]);
}

test "FFI: get on missing key returns NOT_FOUND" {
    var h: ?*Handle = null;
    const opts: Options = .{};
    try testing.expectEqual(ZERO_DB_OK, zero_db_open(&opts, &h));
    defer zero_db_close(h);
    var out: ?[*]u8 = null;
    var out_len: usize = 0;
    try testing.expectEqual(ZERO_DB_ERR_NOT_FOUND, zero_db_get(h, "ghost", 5, &out, &out_len));
}

test "FFI: flush + compact + stats" {
    var h: ?*Handle = null;
    const opts: Options = .{};
    try testing.expectEqual(ZERO_DB_OK, zero_db_open(&opts, &h));
    defer zero_db_close(h);

    try testing.expectEqual(ZERO_DB_OK, zero_db_set(h, "k", 1, "v", 1));
    try testing.expectEqual(ZERO_DB_OK, zero_db_flush(h));

    var s: Stats = undefined;
    try testing.expectEqual(ZERO_DB_OK, zero_db_stats(h, &s));
    try testing.expectEqual(@as(u64, 1), s.sstables);
    try testing.expectEqual(@as(u64, 0), s.compaction_failures);
}

test "FFI: invalid handle returns INVALID_ARG, NULL options too" {
    try testing.expectEqual(ZERO_DB_ERR_INVALID_ARG, zero_db_set(null, "k", 1, "v", 1));
    try testing.expectEqual(ZERO_DB_ERR_INVALID_ARG, zero_db_delete(null, "k", 1));
    try testing.expectEqual(ZERO_DB_ERR_INVALID_ARG, zero_db_flush(null));
    try testing.expectEqual(ZERO_DB_ERR_INVALID_ARG, zero_db_compact(null));

    var h: ?*Handle = null;
    try testing.expectEqual(ZERO_DB_ERR_INVALID_ARG, zero_db_open(null, &h));
    try testing.expect(h == null);
}

test "FFI ABI: Stats struct is exactly 24 bytes (3 × u64)" {
    try testing.expectEqual(@as(usize, 24), @sizeOf(Stats));
}

test "FFI ABI: Options struct preserves headroom for future fields" {
    try testing.expect(@sizeOf(Options) >= 96);
    try testing.expect(@sizeOf(Options) <= 256);
}
