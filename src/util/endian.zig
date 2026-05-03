//! Endianness utilities.
//!
//! The on-disk SSTable format is little-endian. Cloud Run targets x86_64 and
//! arm64, both little-endian, so we assert that at compile time and skip any
//! runtime byte-swap on the hot read path. A future big-endian port would
//! replace `@bitCast`-of-bytes with explicit `std.mem.readInt(.little, ...)`
//! calls; until then, native casts are sound and zero-cost.

const std = @import("std");
const builtin = @import("builtin");

comptime {
    if (builtin.cpu.arch.endian() != .little) {
        @compileError("Zero-DB on-disk format is little-endian; this target is big-endian. " ++
            "Build for a little-endian arch (x86_64 / aarch64) or port the parser to use " ++
            "explicit byte-swapped reads.");
    }
}

test "host is little-endian" {
    try std.testing.expectEqual(std.builtin.Endian.little, builtin.cpu.arch.endian());
}
