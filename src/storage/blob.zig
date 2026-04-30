//! Blob-storage abstraction.
//!
//! `Storage` is a tiny vtable interface — `rangeGet` + `size` — so the
//! SSTable Reader can be unit tested against an in-memory byte slice while
//! the production path drops in a GCS-backed implementation behind the
//! same seam. Designed to model exactly what GCS exposes: random-access
//! byte-range reads on an immutable object of known total size.

const std = @import("std");

pub const Error = error{
    OutOfRange,
    StorageError,
};

/// Implementations live elsewhere; this struct is the value-type "fat
/// pointer" callers hold. `ptr` is opaque, `vtable` carries the function
/// pointers. Both `rangeGet` and `size` return `anyerror!...` so concrete
/// impls can surface their own error sets without forcing a leaky union
/// up here. Reader-level code narrows back to `ReaderError` at the seam.
pub const Storage = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        /// Read `[start, start+len)` of the blob into `dst`. Returns the
        /// number of bytes actually written (may be < len at EOF). `len`
        /// must satisfy `len <= dst.len`.
        rangeGet: *const fn (ptr: *anyopaque, dst: []u8, start: u64, len: u32) anyerror!usize,
        /// Total byte size of the blob.
        size: *const fn (ptr: *anyopaque) anyerror!u64,
    };

    pub fn rangeGet(self: Storage, dst: []u8, start: u64, len: u32) anyerror!usize {
        std.debug.assert(len <= dst.len);
        return self.vtable.rangeGet(self.ptr, dst, start, len);
    }

    pub fn size(self: Storage) anyerror!u64 {
        return self.vtable.size(self.ptr);
    }
};

/// In-memory `Storage` over a borrowed byte slice. Used by SSTable Reader
/// tests today; also handy for the local-flush path before we ship live
/// GCS upload (write the SSTable buffer locally, mount it as memory
/// storage, serve reads from RAM).
pub const MemoryStorage = struct {
    bytes: []const u8,

    pub fn init(bytes: []const u8) MemoryStorage {
        return .{ .bytes = bytes };
    }

    pub fn storage(self: *const MemoryStorage) Storage {
        return .{
            .ptr = @ptrCast(@constCast(self)),
            .vtable = &vtable,
        };
    }

    const vtable: Storage.VTable = .{
        .rangeGet = rangeGetImpl,
        .size = sizeImpl,
    };

    fn rangeGetImpl(ptr: *anyopaque, dst: []u8, start: u64, len: u32) anyerror!usize {
        const self: *const MemoryStorage = @ptrCast(@alignCast(ptr));
        if (start >= self.bytes.len) return 0;
        const want = @min(@as(usize, len), dst.len);
        const remaining: usize = @intCast(@as(u64, self.bytes.len) - start);
        const n = @min(want, remaining);
        const s: usize = @intCast(start);
        @memcpy(dst[0..n], self.bytes[s .. s + n]);
        return n;
    }

    fn sizeImpl(ptr: *anyopaque) anyerror!u64 {
        const self: *const MemoryStorage = @ptrCast(@alignCast(ptr));
        return @as(u64, self.bytes.len);
    }
};

/// `Storage` wrapper that records call counts. Used by Reader tests to
/// prove that lazy footer/bloom/index loads only hit storage once even
/// across repeated `get()` calls.
pub const CountingStorage = struct {
    inner: Storage,
    range_get_calls: u32 = 0,
    size_calls: u32 = 0,

    pub fn init(inner: Storage) CountingStorage {
        return .{ .inner = inner };
    }

    pub fn storage(self: *CountingStorage) Storage {
        return .{
            .ptr = @ptrCast(self),
            .vtable = &vtable,
        };
    }

    const vtable: Storage.VTable = .{
        .rangeGet = rangeGetImpl,
        .size = sizeImpl,
    };

    fn rangeGetImpl(ptr: *anyopaque, dst: []u8, start: u64, len: u32) anyerror!usize {
        const self: *CountingStorage = @ptrCast(@alignCast(ptr));
        self.range_get_calls += 1;
        return self.inner.rangeGet(dst, start, len);
    }

    fn sizeImpl(ptr: *anyopaque) anyerror!u64 {
        const self: *CountingStorage = @ptrCast(@alignCast(ptr));
        self.size_calls += 1;
        return self.inner.size();
    }
};

const testing = std.testing;

test "MemoryStorage: size matches" {
    const data = [_]u8{ 1, 2, 3, 4, 5 };
    var ms = MemoryStorage.init(&data);
    const s = ms.storage();
    try testing.expectEqual(@as(u64, 5), try s.size());
}

test "MemoryStorage: rangeGet inside bounds" {
    const data = [_]u8{ 'a', 'b', 'c', 'd', 'e', 'f' };
    var ms = MemoryStorage.init(&data);
    const s = ms.storage();

    var dst: [3]u8 = undefined;
    const n = try s.rangeGet(&dst, 1, 3);
    try testing.expectEqual(@as(usize, 3), n);
    try testing.expectEqualSlices(u8, "bcd", &dst);
}

test "MemoryStorage: rangeGet truncates at EOF" {
    const data = [_]u8{ 'a', 'b', 'c' };
    var ms = MemoryStorage.init(&data);
    const s = ms.storage();

    var dst: [10]u8 = undefined;
    const n = try s.rangeGet(&dst, 1, 10);
    try testing.expectEqual(@as(usize, 2), n);
    try testing.expectEqualSlices(u8, "bc", dst[0..2]);
}

test "MemoryStorage: start past EOF returns 0" {
    const data = [_]u8{ 'a', 'b', 'c' };
    var ms = MemoryStorage.init(&data);
    const s = ms.storage();

    var dst: [4]u8 = undefined;
    const n = try s.rangeGet(&dst, 100, 4);
    try testing.expectEqual(@as(usize, 0), n);
}

test "CountingStorage: counts pass through" {
    const data = [_]u8{ 1, 2, 3 };
    var ms = MemoryStorage.init(&data);
    var counter = CountingStorage.init(ms.storage());
    const s = counter.storage();

    _ = try s.size();
    var dst: [2]u8 = undefined;
    _ = try s.rangeGet(&dst, 0, 2);
    _ = try s.rangeGet(&dst, 1, 2);

    try testing.expectEqual(@as(u32, 1), counter.size_calls);
    try testing.expectEqual(@as(u32, 2), counter.range_get_calls);
}
