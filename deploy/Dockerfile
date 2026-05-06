# syntax=docker/dockerfile:1.7
#
# Multi-stage build: a Zig 0.16 toolchain compiles a static musl binary,
# then a distroless final image carries only that binary. Cloud Run
# invokes the binary with `--mode=serve` (the default); the engine reads
# PORT from the environment.

FROM alpine:3.20 AS builder
ARG ZIG_VERSION=0.16.0
RUN apk add --no-cache curl xz tar ca-certificates
WORKDIR /workspace

# Download Zig. URL convention since ~0.14 puts arch before OS:
#   zig-x86_64-linux-VERSION.tar.xz
RUN curl -fsSL "https://ziglang.org/download/${ZIG_VERSION}/zig-x86_64-linux-${ZIG_VERSION}.tar.xz" \
      -o zig.tar.xz \
 && tar -xJf zig.tar.xz \
 && mv "zig-x86_64-linux-${ZIG_VERSION}" /opt/zig \
 && rm zig.tar.xz
ENV PATH=/opt/zig:$PATH

# Pull source after the toolchain layer so source-only edits skip the
# multi-hundred-MB toolchain download on rebuild.
COPY . .

RUN zig build -Doptimize=ReleaseFast -Dtarget=x86_64-linux-musl

# ---------------------------------------------------------------------------
# Runtime image. distroless/static is ~2 MB and ships no shell — the binary
# is the only attack surface.
# ---------------------------------------------------------------------------
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /workspace/zig-out/bin/zero_db /usr/local/bin/zero_db
# Explicitly carry the Alpine CA bundle into the runtime image. Zig's
# std.http.Client searches several standard paths; the Debian-style path
# at /etc/ssl/certs/ca-certificates.crt is the most universal.
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
USER nonroot
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/zero_db"]
