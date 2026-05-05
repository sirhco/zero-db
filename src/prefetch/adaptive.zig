//! Adaptive prefetcher.
//!
//! Tracks per-request key access sequences and learns single-step
//! transitions: if key A is observed, then key B's block range is
//! recorded as "what comes after A". A subsequent observation of A
//! returns B's last-seen range as a prefetch hint. The reader can use
//! the hint to extend the next Range fetch and absorb a likely-needed
//! block in the same round trip.
//!
//! This is the simplest learner that matches a real workload pattern
//! (sequential scans, fixed-shape API responses) without paying for a
//! richer co-occurrence model. Higher-order n-grams + decay are
//! straightforward extensions when traffic exposes the need.
//!
//! Reader-side integration is intentionally out of scope here — the
//! tracker exposes a small pure API; the reader can opt into hints
//! once Phase 18 deployment exposes the hot key shapes.

const std = @import("std");

pub const Range = struct {
    offset: u64,
    len: u32,
};

pub const Observation = struct {
    key: []const u8,
    range: Range,
};

pub const Tracker = struct {
    gpa: std.mem.Allocator,
    /// Keys are copied into this arena. The map borrows the duped slices.
    arena: std.heap.ArenaAllocator,
    /// "After observing K, the next observed key's range was V."
    transitions: std.StringHashMapUnmanaged(Range) = .empty,
    /// Slice into `arena` of the most recently observed key, or null.
    last_key: ?[]const u8 = null,

    pub fn init(gpa: std.mem.Allocator) Tracker {
        return .{ .gpa = gpa, .arena = std.heap.ArenaAllocator.init(gpa) };
    }

    pub fn deinit(self: *Tracker) void {
        self.transitions.deinit(self.gpa);
        self.arena.deinit();
        self.* = undefined;
    }

    /// Record that `obs.key` was just fetched at `obs.range`. If a
    /// successor is known for this key, return its range as a prefetch
    /// hint. Caller may issue the hint as an additional Range read or
    /// extend the current fetch; tracker is purely advisory.
    pub fn observe(self: *Tracker, obs: Observation) !?Range {
        const prediction = self.transitions.get(obs.key);

        if (self.last_key) |prev| {
            if (!std.mem.eql(u8, prev, obs.key)) {
                try self.recordTransition(prev, obs.range);
            }
        }

        // Update last_key to point at an arena-owned copy of obs.key.
        // Reuse an existing transition key when available so the arena
        // does not balloon on repeated probes for the same hot key.
        const aa = self.arena.allocator();
        if (self.transitions.getKey(obs.key)) |existing_key| {
            self.last_key = existing_key;
        } else {
            self.last_key = try aa.dupe(u8, obs.key);
        }
        return prediction;
    }

    fn recordTransition(self: *Tracker, prev_key: []const u8, range: Range) !void {
        const aa = self.arena.allocator();
        const gop = try self.transitions.getOrPut(self.gpa, prev_key);
        if (!gop.found_existing) {
            // Map borrows the slot key; keep ownership of `prev_key` (it
            // already lives in our arena) by re-pointing the map's key
            // slot at it. If `prev_key` is a transient slice from the
            // caller, dupe into arena.
            gop.key_ptr.* = aa.dupe(u8, prev_key) catch |err| {
                _ = self.transitions.remove(prev_key);
                return err;
            };
        }
        gop.value_ptr.* = range;
    }

    pub fn count(self: *const Tracker) usize {
        return self.transitions.count();
    }
};

const testing = std.testing;

test "Tracker: first observation returns no prediction" {
    const gpa = testing.allocator;
    var t = Tracker.init(gpa);
    defer t.deinit();
    const r = try t.observe(.{ .key = "alpha", .range = .{ .offset = 0, .len = 100 } });
    try testing.expectEqual(@as(?Range, null), r);
}

test "Tracker: A then B then A predicts B's range" {
    const gpa = testing.allocator;
    var t = Tracker.init(gpa);
    defer t.deinit();

    _ = try t.observe(.{ .key = "alpha", .range = .{ .offset = 0, .len = 100 } });
    _ = try t.observe(.{ .key = "bravo", .range = .{ .offset = 100, .len = 50 } });
    const pred = (try t.observe(.{ .key = "alpha", .range = .{ .offset = 0, .len = 100 } })).?;

    try testing.expectEqual(@as(u64, 100), pred.offset);
    try testing.expectEqual(@as(u32, 50), pred.len);
}

test "Tracker: latest successor wins on conflicting transitions" {
    const gpa = testing.allocator;
    var t = Tracker.init(gpa);
    defer t.deinit();

    _ = try t.observe(.{ .key = "a", .range = .{ .offset = 0, .len = 10 } });
    _ = try t.observe(.{ .key = "b", .range = .{ .offset = 10, .len = 20 } });
    _ = try t.observe(.{ .key = "a", .range = .{ .offset = 0, .len = 10 } });
    _ = try t.observe(.{ .key = "c", .range = .{ .offset = 30, .len = 40 } });

    const pred = (try t.observe(.{ .key = "a", .range = .{ .offset = 0, .len = 10 } })).?;
    try testing.expectEqual(@as(u64, 30), pred.offset);
    try testing.expectEqual(@as(u32, 40), pred.len);
}

test "Tracker: same key twice in a row records no self-transition" {
    const gpa = testing.allocator;
    var t = Tracker.init(gpa);
    defer t.deinit();

    _ = try t.observe(.{ .key = "k", .range = .{ .offset = 0, .len = 1 } });
    _ = try t.observe(.{ .key = "k", .range = .{ .offset = 0, .len = 1 } });
    try testing.expectEqual(@as(usize, 0), t.count());
}

test "Tracker: caller-owned key bytes do not have to outlive observe" {
    const gpa = testing.allocator;
    var t = Tracker.init(gpa);
    defer t.deinit();

    var k_buf: [8]u8 = "alphabet".*;
    _ = try t.observe(.{ .key = k_buf[0..5], .range = .{ .offset = 0, .len = 1 } });
    @memset(&k_buf, 0); // clobber the original buffer

    var k_buf2: [4]u8 = "blue".*;
    _ = try t.observe(.{ .key = &k_buf2, .range = .{ .offset = 100, .len = 2 } });
    @memset(&k_buf2, 0);

    var probe: [5]u8 = "alpha".*;
    const pred = (try t.observe(.{ .key = &probe, .range = .{ .offset = 0, .len = 1 } })).?;
    try testing.expectEqual(@as(u64, 100), pred.offset);
    try testing.expectEqual(@as(u32, 2), pred.len);
}
