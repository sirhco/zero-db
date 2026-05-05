//! Background flush worker.
//!
//! Owns one OS thread that drains frozen MemTables off a queue, writes
//! each as an SSTable (uploading to GCS when persistence is configured),
//! and splices the new SSTable handle into the engine's read list under
//! the engine's mutex.
//!
//! Lifecycle:
//!   - `Compactor.create(gpa, *Engine)` heap-allocates a Compactor and
//!     spawns its worker thread.
//!   - `enqueue(*MemTable)` pushes a flush job and the worker picks it up.
//!   - `waitDrained()` blocks until in-flight count is zero — the
//!     synchronous-flush hook used by `Engine.flush`.
//!   - `shutdownAndJoin()` flips the shutdown flag, the worker exits its
//!     loop, the thread joins. Idempotent.
//!
//! Synchronization primitives are intentionally simple: a spin-yield
//! mutex protects the queue and shutdown flag, an atomic u32 tracks
//! in-flight jobs (used both by waiters and by the worker's idle-loop
//! sleep). Zig 0.16's `std.Io.Mutex` would require threading an `Io`
//! through every site; this leaf-level worker does not need that.

const std = @import("std");
const memtable_mod = @import("memtable.zig");
const writer = @import("../sstable/writer.zig");
const engine_mod = @import("engine.zig");

pub const Engine = engine_mod.Engine;
pub const MemTable = memtable_mod.MemTable;
pub const SpinMutex = engine_mod.SpinMutex;

pub const Error = error{
    OutOfMemory,
};

/// One unit of background work. `memtable != null` means "flush this
/// frozen MemTable". `memtable == null` means "merge level-0 SSTables".
const Job = struct {
    node: std.SinglyLinkedList.Node = .{},
    memtable: ?*MemTable,
};

/// How long the worker sleeps between queue checks when idle, in
/// nanoseconds. Trades a little extra latency on flush-trigger for zero
/// futex/condvar dependency.
const IDLE_SLEEP_NS: u64 = 1 * std.time.ns_per_ms;

fn idleSleep() void {
    var ts: std.posix.timespec = .{ .sec = 0, .nsec = @intCast(IDLE_SLEEP_NS) };
    _ = std.posix.system.nanosleep(&ts, &ts);
}

pub const Compactor = struct {
    gpa: std.mem.Allocator,
    engine: *Engine,

    mu: SpinMutex = .{},
    queue: std.SinglyLinkedList = .{},
    in_flight: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
    shutdown_requested: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    thread: ?std.Thread = null,

    pub fn create(gpa: std.mem.Allocator, engine: *Engine) !*Compactor {
        const c = try gpa.create(Compactor);
        c.* = .{ .gpa = gpa, .engine = engine };
        c.thread = try std.Thread.spawn(.{}, run, .{c});
        return c;
    }

    pub fn shutdownAndJoin(self: *Compactor) void {
        if (self.shutdown_requested.swap(true, .seq_cst)) {
            return; // already shutting down
        }
        if (self.thread) |t| {
            t.join();
            self.thread = null;
        }
    }

    pub fn enqueue(self: *Compactor, mt: *MemTable) Error!void {
        return self.enqueueJob(.{ .memtable = mt });
    }

    pub fn enqueueMerge(self: *Compactor) Error!void {
        return self.enqueueJob(.{ .memtable = null });
    }

    fn enqueueJob(self: *Compactor, init_job: Job) Error!void {
        const job = self.gpa.create(Job) catch return error.OutOfMemory;
        job.* = init_job;

        // Order matters: increment in_flight BEFORE making the job
        // visible, so a concurrent waitDrained() that observes the job
        // also observes in_flight > 0.
        _ = self.in_flight.fetchAdd(1, .seq_cst);

        self.mu.lock();
        self.queue.prepend(&job.node);
        self.mu.unlock();
    }

    pub fn waitDrained(self: *Compactor) void {
        while (self.in_flight.load(.acquire) > 0) {
            std.Thread.yield() catch std.atomic.spinLoopHint();
        }
    }

    fn run(self: *Compactor) void {
        while (true) {
            const maybe_job: ?*Job = blk: {
                self.mu.lock();
                defer self.mu.unlock();
                const node = self.queue.popFirst() orelse break :blk null;
                break :blk @fieldParentPtr("node", node);
            };

            if (maybe_job) |job| {
                self.processJob(job) catch |err| {
                    self.engine.recordCompactionFailure(err);
                };
                self.gpa.destroy(job);
                _ = self.in_flight.fetchSub(1, .release);
                continue;
            }

            // Queue empty. Either exit or sleep briefly.
            if (self.shutdown_requested.load(.acquire)) return;
            idleSleep();
        }
    }

    fn processJob(self: *Compactor, job: *Job) anyerror!void {
        if (job.memtable) |mt| {
            const sst_bytes = try writer.writeFromMemTable(self.gpa, mt, .{
                .target_block_size = self.engine.options.target_block_size,
                .expected_fp = self.engine.options.expected_fp,
            });
            try self.engine.installFlushedSSTable(mt, sst_bytes);
        } else {
            try self.engine.runMergeJob();
        }
    }
};

const testing = std.testing;

test "Compactor: type compiles" {
    _ = Compactor;
    _ = Job;
}
