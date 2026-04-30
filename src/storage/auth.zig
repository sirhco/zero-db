//! Cloud Run / GCE bearer-token fetch from the metadata service.
//!
//! `http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token`
//! returns a JSON blob with `access_token` and `expires_in`. Cache + refresh
//! ~60s before expiry to avoid stalling a hot-path request on a token RPC.
//!
//! Stub: signatures only.

const std = @import("std");

pub const Error = error{
    NotImplemented,
};

pub const TokenSource = struct {
    gpa: std.mem.Allocator,

    pub fn init(gpa: std.mem.Allocator) TokenSource {
        return .{ .gpa = gpa };
    }

    pub fn deinit(self: *TokenSource) void {
        _ = self;
    }

    pub fn token(self: *TokenSource) Error![]const u8 {
        _ = self;
        return Error.NotImplemented;
    }
};

test "token source type compiles" {
    _ = TokenSource;
}
