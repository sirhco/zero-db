//! Write-ahead log: a single local file that records every set/delete
//! before the entry lands in the active MemTable. Cloud Run instances
//! survive in-process crashes by replaying the WAL on the next start.
//!
//! Record layout (little-endian):
//!   - tag: u8 — TAG_SET = 0x01, TAG_DELETE = 0x02
//!   - klen: varint (LEB128, unsigned) — key length in bytes
//!   - vlen: varint — value length in bytes (always 0 for DELETE)
//!   - key bytes
//!   - value bytes (none for DELETE)
//!   - crc32c trailer over [tag .. last value byte], little-endian u32
//!
//! `replay` scans from the start, applying each well-formed record. On
//! the first corrupt record it stops cleanly and treats every byte past
//! that point as truncated tail (most commonly an interrupted write).
//!
//! v0 limitations:
//!   - No truncation or rotation. The file grows forever; replay walks
//!     the whole thing on cold start. Phase 16+ adds checkpointing in
//!     the manifest so replay can skip the prefix already absorbed by an
//!     SSTable.
//!   - Single writer assumed. Callers serialize append calls.

const std = @import("std");
const varint = @import("../util/varint.zig");
const crc_mod = @import("../util/crc.zig");

pub const Error = error{
    OpenFailed,
    WriteFailed,
    SyncFailed,
    ShortWrite,
    OutOfMemory,
};

pub const TAG_SET: u8 = 0x01;
pub const TAG_DELETE: u8 = 0x02;

pub const Wal = struct {
    gpa: std.mem.Allocator,
    fd: std.posix.fd_t,

    pub fn open(gpa: std.mem.Allocator, path: []const u8) Error!Wal {
        const path_z = gpa.dupeZ(u8, path) catch return error.OutOfMemory;
        defer gpa.free(path_z);

        const flags: std.posix.O = .{ .ACCMODE = .RDWR, .CREAT = true, .APPEND = true };
        const fd = std.posix.openatZ(std.posix.AT.FDCWD, path_z, flags, 0o644) catch
            return error.OpenFailed;
        return .{ .gpa = gpa, .fd = fd };
    }

    pub fn close(self: *Wal) void {
        _ = std.posix.system.close(self.fd);
        self.* = undefined;
    }

    pub fn appendSet(self: *Wal, key: []const u8, value: []const u8) Error!void {
        try self.appendRecord(TAG_SET, key, value);
    }

    pub fn appendDelete(self: *Wal, key: []const u8) Error!void {
        try self.appendRecord(TAG_DELETE, key, &.{});
    }

    fn appendRecord(self: *Wal, tag: u8, key: []const u8, value: []const u8) Error!void {
        var hdr: [21]u8 = undefined;
        hdr[0] = tag;
        const klen_n = varint.encodeU64(@intCast(key.len), hdr[1..]) catch
            return error.WriteFailed;
        const vlen_n = varint.encodeU64(@intCast(value.len), hdr[1 + klen_n ..]) catch
            return error.WriteFailed;
        const hdr_len: usize = 1 + klen_n + vlen_n;

        const total = hdr_len + key.len + value.len + 4;
        const buf = self.gpa.alloc(u8, total) catch return error.OutOfMemory;
        defer self.gpa.free(buf);
        @memcpy(buf[0..hdr_len], hdr[0..hdr_len]);
        @memcpy(buf[hdr_len..][0..key.len], key);
        @memcpy(buf[hdr_len + key.len ..][0..value.len], value);
        const crc = crc_mod.crc32c(buf[0 .. total - 4]);
        std.mem.writeInt(u32, buf[total - 4 ..][0..4], crc, .little);

        try writeAll(self.fd, buf);
        std.posix.fdatasync(self.fd) catch return error.SyncFailed;
    }

    /// Iterate every record in the file from the start, calling
    /// `visitor.apply(tag, key, value)` for each. Stops at EOF or the
    /// first corrupt record (treats the tail as a truncated write).
    pub fn replay(self: *Wal, visitor: anytype) Error!void {
        // Read entire file into memory. WAL files are bounded by
        // memtable_flush_bytes-ish; for v0 a single allocation is fine.
        var pos: u64 = 0;
        const sz = lseekEnd(self.fd) catch return error.OpenFailed;
        _ = lseekSet(self.fd, 0) catch return error.OpenFailed;
        const buf = self.gpa.alloc(u8, @intCast(sz)) catch return error.OutOfMemory;
        defer self.gpa.free(buf);
        var off: usize = 0;
        while (off < buf.len) {
            const n = std.posix.read(self.fd, buf[off..]) catch return error.OpenFailed;
            if (n == 0) break;
            off += n;
        }
        // Position the file at end so subsequent appends land correctly.
        _ = lseekEnd(self.fd) catch return error.OpenFailed;

        while (pos < buf.len) {
            const rec = decodeRecord(buf[@intCast(pos)..]) orelse return; // tail corrupt → stop
            switch (rec.tag) {
                TAG_SET => visitor.apply(.set, rec.key, rec.value) catch return error.WriteFailed,
                TAG_DELETE => visitor.apply(.delete, rec.key, &.{}) catch return error.WriteFailed,
                else => return,
            }
            pos += rec.consumed;
        }
    }
};

fn writeAll(fd: std.posix.fd_t, bytes: []const u8) Error!void {
    var off: usize = 0;
    while (off < bytes.len) {
        const rc = std.posix.system.write(fd, bytes[off..].ptr, bytes.len - off);
        const n: isize = @bitCast(rc);
        if (n <= 0) return error.WriteFailed;
        off += @intCast(n);
    }
}

fn lseekSet(fd: std.posix.fd_t, off: i64) !u64 {
    const rc = std.posix.system.lseek(fd, off, 0); // SEEK_SET = 0
    const r: i64 = @bitCast(@as(u64, @bitCast(rc)));
    if (r < 0) return error.WriteFailed;
    return @intCast(r);
}

fn lseekEnd(fd: std.posix.fd_t) !u64 {
    const rc = std.posix.system.lseek(fd, 0, 2); // SEEK_END = 2
    const r: i64 = @bitCast(@as(u64, @bitCast(rc)));
    if (r < 0) return error.WriteFailed;
    return @intCast(r);
}

const Record = struct {
    tag: u8,
    key: []const u8,
    value: []const u8,
    consumed: usize,
};

fn decodeRecord(buf: []const u8) ?Record {
    if (buf.len < 1) return null;
    const tag = buf[0];
    if (tag != TAG_SET and tag != TAG_DELETE) return null;
    var off: usize = 1;
    const klen_dec = varint.decodeU64(buf[off..]) catch return null;
    off += klen_dec.bytes_read;
    const vlen_dec = varint.decodeU64(buf[off..]) catch return null;
    off += vlen_dec.bytes_read;
    const klen: usize = @intCast(klen_dec.value);
    const vlen: usize = @intCast(vlen_dec.value);
    if (off + klen + vlen + 4 > buf.len) return null;

    const body_end = off + klen + vlen;
    const crc_seen = std.mem.readInt(u32, buf[body_end..][0..4], .little);
    if (crc_mod.crc32c(buf[0..body_end]) != crc_seen) return null;

    const key = buf[off .. off + klen];
    const value = buf[off + klen .. body_end];
    return .{ .tag = tag, .key = key, .value = value, .consumed = body_end + 4 };
}

const testing = std.testing;

const RecordingVisitor = struct {
    set_count: u32 = 0,
    delete_count: u32 = 0,
    last_key: [64]u8 = undefined,
    last_key_len: usize = 0,
    last_value: [64]u8 = undefined,
    last_value_len: usize = 0,

    pub const Op = enum { set, delete };

    pub fn apply(self: *RecordingVisitor, op: Op, key: []const u8, value: []const u8) !void {
        switch (op) {
            .set => self.set_count += 1,
            .delete => self.delete_count += 1,
        }
        self.last_key_len = @min(key.len, self.last_key.len);
        @memcpy(self.last_key[0..self.last_key_len], key[0..self.last_key_len]);
        self.last_value_len = @min(value.len, self.last_value.len);
        @memcpy(self.last_value[0..self.last_value_len], value[0..self.last_value_len]);
    }
};

test "WAL: append + replay roundtrips set/delete records" {
    const gpa = testing.allocator;

    // Per-test temp path. tmp.tmpDir would also work but adds an Io
    // dependency in 0.16; a fixed /tmp path with the test pid keeps it
    // self-contained.
    const pid = std.posix.system.getpid();
    var path_buf: [128]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, "/tmp/zero-db-wal-test-{d}.log", .{pid});
    defer {
        if (gpa.dupeZ(u8, path)) |path_z| {
            _ = std.posix.system.unlink(path_z.ptr);
            gpa.free(path_z);
        } else |_| {}
    }

    {
        var w = try Wal.open(gpa, path);
        defer w.close();
        try w.appendSet("alpha", "AAA");
        try w.appendSet("bravo", "BB");
        try w.appendDelete("alpha");
        try w.appendSet("charlie", "CCCC");
    }

    var w2 = try Wal.open(gpa, path);
    defer w2.close();
    var v: RecordingVisitor = .{};
    try w2.replay(&v);
    try testing.expectEqual(@as(u32, 3), v.set_count);
    try testing.expectEqual(@as(u32, 1), v.delete_count);
    try testing.expectEqualStrings("charlie", v.last_key[0..v.last_key_len]);
    try testing.expectEqualStrings("CCCC", v.last_value[0..v.last_value_len]);
}

test "WAL: replay stops cleanly at the first corrupt record" {
    const gpa = testing.allocator;
    const pid = std.posix.system.getpid();
    var path_buf: [128]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, "/tmp/zero-db-wal-test-corrupt-{d}.log", .{pid});
    defer {
        if (gpa.dupeZ(u8, path)) |path_z| {
            _ = std.posix.system.unlink(path_z.ptr);
            gpa.free(path_z);
        } else |_| {}
    }

    {
        var w = try Wal.open(gpa, path);
        defer w.close();
        try w.appendSet("alpha", "AAA");
        try w.appendSet("bravo", "BB");

        // Append a partial record (tag only — no varint, no key, no CRC).
        try writeAll(w.fd, &[_]u8{TAG_SET});
        std.posix.fdatasync(w.fd) catch return;
    }

    var w2 = try Wal.open(gpa, path);
    defer w2.close();
    var v: RecordingVisitor = .{};
    try w2.replay(&v);
    try testing.expectEqual(@as(u32, 2), v.set_count);
}

test "WAL: empty file replay is a no-op" {
    const gpa = testing.allocator;
    const pid = std.posix.system.getpid();
    var path_buf: [128]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, "/tmp/zero-db-wal-test-empty-{d}.log", .{pid});
    defer {
        if (gpa.dupeZ(u8, path)) |path_z| {
            _ = std.posix.system.unlink(path_z.ptr);
            gpa.free(path_z);
        } else |_| {}
    }

    var w = try Wal.open(gpa, path);
    defer w.close();
    var v: RecordingVisitor = .{};
    try w.replay(&v);
    try testing.expectEqual(@as(u32, 0), v.set_count);
    try testing.expectEqual(@as(u32, 0), v.delete_count);
}
