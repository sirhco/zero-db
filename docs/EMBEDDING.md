# Embedding Zero-DB via the C ABI

> Use `libzero_db.{so,dylib,a}` to embed the engine in-process. Any language with a C FFI can call it directly — no separate service, no localhost HTTP hop.

For a sidecar deployment instead (zero changes to your app), see [`docs/SIDECAR.md`](SIDECAR.md).

---

## Build the library

Requires Zig **0.16+** on the build host.

```bash
# Native host build → libzero_db.{so,dylib} + .a + zero_db.h
zig build -Dffi=true -Doptimize=ReleaseFast

# Cross-compile to Linux musl (e.g. for Cloud Run / Alpine)
zig build -Dffi=true -Doptimize=ReleaseFast -Dtarget=x86_64-linux-musl

# Output:
ls zig-out/lib zig-out/include
# zig-out/lib/libzero_db.dylib   (or .so on Linux)
# zig-out/lib/libzero_db.a       (static archive)
# zig-out/include/zero_db.h
```

Drop both files into your build:

```bash
sudo cp zig-out/lib/libzero_db.dylib /usr/local/lib/   # or .so
sudo cp zig-out/include/zero_db.h    /usr/local/include/
```

Or vendor them under your project's `vendor/` and point your build system at the local paths.

---

## API surface (recap)

```c
int  zero_db_open(const zero_db_options_t* options, zero_db_handle_t** out);
void zero_db_close(zero_db_handle_t* h);

int  zero_db_set(zero_db_handle_t* h, const uint8_t* k, size_t kl, const uint8_t* v, size_t vl);
int  zero_db_get(zero_db_handle_t* h, const uint8_t* k, size_t kl, uint8_t** out_v, size_t* out_vl);
int  zero_db_delete(zero_db_handle_t* h, const uint8_t* k, size_t kl);
void zero_db_free(zero_db_handle_t* h, uint8_t* ptr, size_t len);

int  zero_db_flush(zero_db_handle_t* h);
int  zero_db_compact(zero_db_handle_t* h);
int  zero_db_stats(zero_db_handle_t* h, zero_db_stats_t* out);
```

Status codes:

```c
ZERO_DB_OK                    0
ZERO_DB_ERR_OOM               1
ZERO_DB_ERR_NOT_FOUND         2
ZERO_DB_ERR_INVALID_ARG       3
ZERO_DB_ERR_IO                4
ZERO_DB_ERR_AUTH              5
ZERO_DB_ERR_OVER_BUDGET       6
ZERO_DB_ERR_MANIFEST_CONFLICT 7
ZERO_DB_ERR_INTERNAL          99
```

Memory + threading rules (apply to every binding below):

- Caller-supplied bytes are **borrowed** for the call's duration. The library copies what it retains.
- `zero_db_get` returns a **library-allocated** buffer. Free it with `zero_db_free` using the **same handle**.
- `zero_db_open` and `zero_db_close` must run on a single thread. All other calls may interleave from any thread.

---

## C

Canonical example. All other bindings are mechanical translations.

```c
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <zero_db.h>

int main(void) {
    zero_db_options_t opts = {0};
    opts.bucket = "my-zero-db";       /* NULL → in-memory only */
    opts.wal_path = "/tmp/zero-db-wal.log";

    zero_db_handle_t* h = NULL;
    int rc = zero_db_open(&opts, &h);
    if (rc != ZERO_DB_OK) { fprintf(stderr, "open: %d\n", rc); return 1; }

    /* Write */
    const uint8_t key[] = "alpha";
    const uint8_t val[] = "AAA";
    rc = zero_db_set(h, key, sizeof(key) - 1, val, sizeof(val) - 1);
    if (rc != ZERO_DB_OK) { fprintf(stderr, "set: %d\n", rc); goto cleanup; }

    /* Read */
    uint8_t* out = NULL;
    size_t   out_len = 0;
    rc = zero_db_get(h, key, sizeof(key) - 1, &out, &out_len);
    if (rc == ZERO_DB_OK) {
        printf("got: %.*s\n", (int)out_len, (char*)out);
        zero_db_free(h, out, out_len);
    } else if (rc == ZERO_DB_ERR_NOT_FOUND) {
        printf("missing\n");
    } else {
        fprintf(stderr, "get: %d\n", rc); goto cleanup;
    }

    zero_db_flush(h);

cleanup:
    zero_db_close(h);
    return 0;
}
```

Compile:

```bash
clang -O2 -I /usr/local/include main.c -L /usr/local/lib -lzero_db -o demo
./demo
```

---

## Go (cgo)

```go
package zerodb

/*
#cgo LDFLAGS: -lzero_db
#include <stdlib.h>
#include <zero_db.h>
*/
import "C"
import (
	"errors"
	"unsafe"
)

type DB struct {
	h *C.zero_db_handle_t
}

type Options struct {
	Bucket               string
	ManifestObject       string
	WALPath              string
	MemtableFlushBytes   uint64
	BlockCacheBytes      uint64
	L0CompactionThreshold uint64
}

func Open(o Options) (*DB, error) {
	var copts C.zero_db_options_t
	copts.memtable_flush_bytes = C.size_t(o.MemtableFlushBytes)
	copts.block_cache_bytes = C.size_t(o.BlockCacheBytes)
	copts.l0_compaction_threshold = C.size_t(o.L0CompactionThreshold)

	if o.Bucket != "" {
		copts.bucket = C.CString(o.Bucket)
		defer C.free(unsafe.Pointer(copts.bucket))
	}
	if o.ManifestObject != "" {
		copts.manifest_object = C.CString(o.ManifestObject)
		defer C.free(unsafe.Pointer(copts.manifest_object))
	}
	if o.WALPath != "" {
		copts.wal_path = C.CString(o.WALPath)
		defer C.free(unsafe.Pointer(copts.wal_path))
	}

	var h *C.zero_db_handle_t
	if rc := C.zero_db_open(&copts, &h); rc != C.ZERO_DB_OK {
		return nil, errFromRC(rc)
	}
	return &DB{h: h}, nil
}

func (d *DB) Close() { C.zero_db_close(d.h) }

func (d *DB) Set(key, value []byte) error {
	rc := C.zero_db_set(d.h,
		(*C.uint8_t)(unsafe.SliceData(key)), C.size_t(len(key)),
		(*C.uint8_t)(unsafe.SliceData(value)), C.size_t(len(value)),
	)
	if rc != C.ZERO_DB_OK { return errFromRC(rc) }
	return nil
}

func (d *DB) Get(key []byte) ([]byte, error) {
	var out *C.uint8_t
	var outLen C.size_t
	rc := C.zero_db_get(d.h,
		(*C.uint8_t)(unsafe.SliceData(key)), C.size_t(len(key)),
		&out, &outLen,
	)
	if rc == C.ZERO_DB_ERR_NOT_FOUND { return nil, nil }
	if rc != C.ZERO_DB_OK { return nil, errFromRC(rc) }
	defer C.zero_db_free(d.h, out, outLen)
	return C.GoBytes(unsafe.Pointer(out), C.int(outLen)), nil
}

func (d *DB) Delete(key []byte) error {
	rc := C.zero_db_delete(d.h, (*C.uint8_t)(unsafe.SliceData(key)), C.size_t(len(key)))
	if rc != C.ZERO_DB_OK { return errFromRC(rc) }
	return nil
}

func (d *DB) Flush() error {
	if rc := C.zero_db_flush(d.h); rc != C.ZERO_DB_OK { return errFromRC(rc) }
	return nil
}

var (
	ErrAuth             = errors.New("zero-db: auth failed")
	ErrOverBudget       = errors.New("zero-db: over memory budget")
	ErrIO               = errors.New("zero-db: io")
	ErrManifestConflict = errors.New("zero-db: manifest conflict")
	ErrInternal         = errors.New("zero-db: internal")
)

func errFromRC(rc C.int) error {
	switch rc {
	case C.ZERO_DB_ERR_OOM:                return errors.New("zero-db: oom")
	case C.ZERO_DB_ERR_AUTH:               return ErrAuth
	case C.ZERO_DB_ERR_OVER_BUDGET:        return ErrOverBudget
	case C.ZERO_DB_ERR_IO:                 return ErrIO
	case C.ZERO_DB_ERR_MANIFEST_CONFLICT:  return ErrManifestConflict
	case C.ZERO_DB_ERR_INVALID_ARG:        return errors.New("zero-db: invalid arg")
	}
	return ErrInternal
}
```

Build:

```bash
CGO_LDFLAGS="-L/usr/local/lib -lzero_db" \
CGO_CFLAGS="-I/usr/local/include" \
go build ./...
```

---

## Python (ctypes — no native deps)

```python
import ctypes
import ctypes.util

# Locate the library. Path can be hardcoded if you vendor it.
_lib_path = ctypes.util.find_library("zero_db") or "/usr/local/lib/libzero_db.so"
_lib = ctypes.CDLL(_lib_path)


class _Options(ctypes.Structure):
    _fields_ = [
        ("bucket", ctypes.c_char_p),
        ("manifest_object", ctypes.c_char_p),
        ("wal_path", ctypes.c_char_p),
        ("memtable_flush_bytes", ctypes.c_size_t),
        ("block_cache_bytes", ctypes.c_size_t),
        ("l0_compaction_threshold", ctypes.c_size_t),
        ("_reserved", ctypes.c_uint8 * 64),
    ]


class _Stats(ctypes.Structure):
    _fields_ = [
        ("sstables", ctypes.c_uint64),
        ("active_entries", ctypes.c_uint64),
        ("compaction_failures", ctypes.c_uint64),
    ]


# Function signatures.
_lib.zero_db_open.argtypes = [ctypes.POINTER(_Options), ctypes.POINTER(ctypes.c_void_p)]
_lib.zero_db_open.restype = ctypes.c_int
_lib.zero_db_close.argtypes = [ctypes.c_void_p]
_lib.zero_db_close.restype = None
_lib.zero_db_set.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_size_t, ctypes.c_char_p, ctypes.c_size_t]
_lib.zero_db_set.restype = ctypes.c_int
_lib.zero_db_get.argtypes = [
    ctypes.c_void_p, ctypes.c_char_p, ctypes.c_size_t,
    ctypes.POINTER(ctypes.POINTER(ctypes.c_uint8)),
    ctypes.POINTER(ctypes.c_size_t),
]
_lib.zero_db_get.restype = ctypes.c_int
_lib.zero_db_free.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_uint8), ctypes.c_size_t]
_lib.zero_db_free.restype = None
_lib.zero_db_delete.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_size_t]
_lib.zero_db_delete.restype = ctypes.c_int
_lib.zero_db_flush.argtypes = [ctypes.c_void_p]
_lib.zero_db_flush.restype = ctypes.c_int
_lib.zero_db_compact.argtypes = [ctypes.c_void_p]
_lib.zero_db_compact.restype = ctypes.c_int
_lib.zero_db_stats.argtypes = [ctypes.c_void_p, ctypes.POINTER(_Stats)]
_lib.zero_db_stats.restype = ctypes.c_int


class ZeroDBError(Exception): pass
class NotFound(KeyError): pass


def _check(rc):
    if rc == 0: return
    if rc == 2: raise NotFound()
    raise ZeroDBError(f"zero_db rc={rc}")


class ZeroDB:
    def __init__(self, *, bucket=None, manifest_object=None, wal_path=None,
                 memtable_flush_bytes=0, block_cache_bytes=0, l0_compaction_threshold=0):
        opts = _Options(
            bucket=(bucket.encode() if bucket else None),
            manifest_object=(manifest_object.encode() if manifest_object else None),
            wal_path=(wal_path.encode() if wal_path else None),
            memtable_flush_bytes=memtable_flush_bytes,
            block_cache_bytes=block_cache_bytes,
            l0_compaction_threshold=l0_compaction_threshold,
        )
        h = ctypes.c_void_p()
        _check(_lib.zero_db_open(ctypes.byref(opts), ctypes.byref(h)))
        self._h = h

    def close(self):
        if self._h: _lib.zero_db_close(self._h); self._h = None

    def __enter__(self): return self
    def __exit__(self, *_): self.close()

    def set(self, key: bytes, value: bytes):
        _check(_lib.zero_db_set(self._h, key, len(key), value, len(value)))

    def get(self, key: bytes) -> bytes | None:
        out = ctypes.POINTER(ctypes.c_uint8)()
        out_len = ctypes.c_size_t(0)
        rc = _lib.zero_db_get(self._h, key, len(key), ctypes.byref(out), ctypes.byref(out_len))
        if rc == 2: return None
        _check(rc)
        try:
            return bytes(ctypes.string_at(out, out_len.value))
        finally:
            _lib.zero_db_free(self._h, out, out_len.value)

    def delete(self, key: bytes):
        _check(_lib.zero_db_delete(self._h, key, len(key)))

    def flush(self):   _check(_lib.zero_db_flush(self._h))
    def compact(self): _check(_lib.zero_db_compact(self._h))

    def stats(self) -> dict:
        s = _Stats()
        _check(_lib.zero_db_stats(self._h, ctypes.byref(s)))
        return {"sstables": s.sstables, "active_entries": s.active_entries, "compaction_failures": s.compaction_failures}


# Usage
if __name__ == "__main__":
    with ZeroDB(bucket="my-zero-db", wal_path="/tmp/zero-db-wal.log") as db:
        db.set(b"alpha", b"AAA")
        print(db.get(b"alpha"))
        db.flush()
        print(db.stats())
```

---

## Rust (manual FFI; bindgen optional)

`build.rs`:

```rust
fn main() {
    println!("cargo:rustc-link-lib=zero_db");
    println!("cargo:rustc-link-search=native=/usr/local/lib");
}
```

`src/lib.rs`:

```rust
use std::ffi::CString;
use std::os::raw::{c_char, c_int};
use std::ptr;

#[repr(C)]
struct CHandle { _opaque: [u8; 0] }

#[repr(C)]
#[derive(Default)]
struct COptions {
    bucket: *const c_char,
    manifest_object: *const c_char,
    wal_path: *const c_char,
    memtable_flush_bytes: usize,
    block_cache_bytes: usize,
    l0_compaction_threshold: usize,
    _reserved: [u8; 64],
}

#[repr(C)]
#[derive(Default)]
struct CStats { sstables: u64, active_entries: u64, compaction_failures: u64 }

extern "C" {
    fn zero_db_open(o: *const COptions, out: *mut *mut CHandle) -> c_int;
    fn zero_db_close(h: *mut CHandle);
    fn zero_db_set(h: *mut CHandle, k: *const u8, kl: usize, v: *const u8, vl: usize) -> c_int;
    fn zero_db_get(h: *mut CHandle, k: *const u8, kl: usize,
                   out: *mut *mut u8, out_len: *mut usize) -> c_int;
    fn zero_db_delete(h: *mut CHandle, k: *const u8, kl: usize) -> c_int;
    fn zero_db_free(h: *mut CHandle, p: *mut u8, len: usize);
    fn zero_db_flush(h: *mut CHandle) -> c_int;
    fn zero_db_compact(h: *mut CHandle) -> c_int;
    fn zero_db_stats(h: *mut CHandle, out: *mut CStats) -> c_int;
}

const OK: c_int = 0;
const NOT_FOUND: c_int = 2;

#[derive(Debug)]
pub enum Error { NotFound, Auth, OverBudget, Io, ManifestConflict, InvalidArg, Internal(c_int) }

pub struct ZeroDB { h: *mut CHandle }

unsafe impl Send for ZeroDB {}
unsafe impl Sync for ZeroDB {}

#[derive(Default)]
pub struct Options<'a> {
    pub bucket: Option<&'a str>,
    pub manifest_object: Option<&'a str>,
    pub wal_path: Option<&'a str>,
    pub memtable_flush_bytes: usize,
    pub block_cache_bytes: usize,
    pub l0_compaction_threshold: usize,
}

impl ZeroDB {
    pub fn open(opts: Options) -> Result<Self, Error> {
        let bucket  = opts.bucket.map(|s| CString::new(s).unwrap());
        let manif   = opts.manifest_object.map(|s| CString::new(s).unwrap());
        let wal     = opts.wal_path.map(|s| CString::new(s).unwrap());
        let mut copts = COptions::default();
        copts.bucket          = bucket.as_ref().map_or(ptr::null(), |s| s.as_ptr());
        copts.manifest_object = manif.as_ref().map_or(ptr::null(), |s| s.as_ptr());
        copts.wal_path        = wal.as_ref().map_or(ptr::null(), |s| s.as_ptr());
        copts.memtable_flush_bytes = opts.memtable_flush_bytes;
        copts.block_cache_bytes = opts.block_cache_bytes;
        copts.l0_compaction_threshold = opts.l0_compaction_threshold;

        let mut h: *mut CHandle = ptr::null_mut();
        let rc = unsafe { zero_db_open(&copts, &mut h) };
        check(rc)?;
        Ok(ZeroDB { h })
    }

    pub fn set(&self, k: &[u8], v: &[u8]) -> Result<(), Error> {
        check(unsafe { zero_db_set(self.h, k.as_ptr(), k.len(), v.as_ptr(), v.len()) })
    }

    pub fn get(&self, k: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        let mut out: *mut u8 = ptr::null_mut();
        let mut out_len: usize = 0;
        let rc = unsafe { zero_db_get(self.h, k.as_ptr(), k.len(), &mut out, &mut out_len) };
        if rc == NOT_FOUND { return Ok(None); }
        check(rc)?;
        let copy = unsafe { std::slice::from_raw_parts(out, out_len).to_vec() };
        unsafe { zero_db_free(self.h, out, out_len) };
        Ok(Some(copy))
    }

    pub fn delete(&self, k: &[u8]) -> Result<(), Error> {
        check(unsafe { zero_db_delete(self.h, k.as_ptr(), k.len()) })
    }

    pub fn flush(&self)   -> Result<(), Error> { check(unsafe { zero_db_flush(self.h) }) }
    pub fn compact(&self) -> Result<(), Error> { check(unsafe { zero_db_compact(self.h) }) }
}

impl Drop for ZeroDB { fn drop(&mut self) { unsafe { zero_db_close(self.h) } } }

fn check(rc: c_int) -> Result<(), Error> {
    match rc {
        0 => Ok(()),
        1 => Err(Error::Internal(1)),
        2 => Err(Error::NotFound),
        3 => Err(Error::InvalidArg),
        4 => Err(Error::Io),
        5 => Err(Error::Auth),
        6 => Err(Error::OverBudget),
        7 => Err(Error::ManifestConflict),
        x => Err(Error::Internal(x)),
    }
}
```

---

## Node.js (ffi-rs / koffi)

`koffi` is the maintained FFI binding for modern Node. Install:

```bash
npm install koffi
```

```js
const koffi = require("koffi");
const lib = koffi.load("/usr/local/lib/libzero_db.dylib"); // or .so on Linux

const Options = koffi.struct("zero_db_options_t", {
  bucket: "string",
  manifest_object: "string",
  wal_path: "string",
  memtable_flush_bytes: "size_t",
  block_cache_bytes: "size_t",
  l0_compaction_threshold: "size_t",
  _reserved: koffi.array("uint8_t", 64),
});

const Stats = koffi.struct("zero_db_stats_t", {
  sstables: "uint64",
  active_entries: "uint64",
  compaction_failures: "uint64",
});

const Handle = koffi.opaque();

const open    = lib.func("int zero_db_open(_In_ Options*, _Out_ Handle**)");
const close_  = lib.func("void zero_db_close(Handle*)");
const set     = lib.func("int zero_db_set(Handle*, _In_ uint8_t*, size_t, _In_ uint8_t*, size_t)");
const get     = lib.func("int zero_db_get(Handle*, _In_ uint8_t*, size_t, _Out_ uint8_t**, _Out_ size_t*)");
const del     = lib.func("int zero_db_delete(Handle*, _In_ uint8_t*, size_t)");
const free_   = lib.func("void zero_db_free(Handle*, uint8_t*, size_t)");
const flush   = lib.func("int zero_db_flush(Handle*)");
const compact = lib.func("int zero_db_compact(Handle*)");
const stats   = lib.func("int zero_db_stats(Handle*, _Out_ Stats*)");

const OK = 0, NOT_FOUND = 2;

class ZeroDB {
  constructor(opts) {
    const o = { ...opts, _reserved: new Array(64).fill(0) };
    const out = [null];
    const rc = open(o, out);
    if (rc !== OK) throw new Error(`zero_db_open rc=${rc}`);
    this.h = out[0];
  }
  close() { if (this.h) { close_(this.h); this.h = null; } }
  set(k, v) {
    const kk = Buffer.from(k); const vv = Buffer.from(v);
    const rc = set(this.h, kk, kk.length, vv, vv.length);
    if (rc !== OK) throw new Error(`set rc=${rc}`);
  }
  get(k) {
    const kk = Buffer.from(k);
    const outPtr = [null]; const outLen = [0];
    const rc = get(this.h, kk, kk.length, outPtr, outLen);
    if (rc === NOT_FOUND) return null;
    if (rc !== OK) throw new Error(`get rc=${rc}`);
    const buf = koffi.decode(outPtr[0], `uint8_t [${outLen[0]}]`);
    free_(this.h, outPtr[0], outLen[0]);
    return Buffer.from(buf);
  }
  delete(k) {
    const kk = Buffer.from(k);
    const rc = del(this.h, kk, kk.length);
    if (rc !== OK) throw new Error(`delete rc=${rc}`);
  }
  flush()   { if (flush(this.h) !== OK) throw new Error("flush failed"); }
  compact() { if (compact(this.h) !== OK) throw new Error("compact failed"); }
  stats()   { const s = {}; const rc = stats(this.h, s); if (rc !== OK) throw new Error(`stats rc=${rc}`); return s; }
}

// Usage
const db = new ZeroDB({ bucket: "my-zero-db", wal_path: "/tmp/zero-db-wal.log" });
db.set("alpha", "AAA");
console.log(db.get("alpha")?.toString());
db.flush();
console.log(db.stats());
db.close();
```

---

## Memory + ownership cheat sheet

| Operation | Caller responsibility | Library responsibility |
|---|---|---|
| `zero_db_open` | Pass `Options` pointing at C strings the library may copy. Free your copies after the call. | Dupes every string into the handle's allocator. |
| `zero_db_set` / `zero_db_delete` | Pass key/value pointers; they may go out of scope after the call. | Copies into the active MemTable. |
| `zero_db_get` (OK) | After use, call `zero_db_free(h, out, out_len)` with the **same handle**. | Allocates the output buffer from the handle's allocator. |
| `zero_db_get` (NOT_FOUND) | Nothing to free — `*out_value` is NULL. | No allocation. |
| `zero_db_close` | Stop calling any other entry point on this handle. | Tears down engine + GCS client + WAL + threaded I/O + handle. |

---

## Threading

- `zero_db_open` and `zero_db_close` must each run on a single thread, and not concurrently with any other entry point.
- All other functions are safe to call from any thread on a long-lived handle. The engine's internal `SpinMutex` protects shared state.
- The library spawns one background compactor thread per handle when GCS persistence is configured. It runs without your involvement.

---

## When to choose this over the sidecar

| Path | Latency | Lift | Trade |
|---|---|---|---|
| **Sidecar** (`docs/SIDECAR.md`) | ~0.1 ms (localhost) | Zero changes to either side | Two containers per revision |
| **C ABI** (this doc) | sub-µs in-process | Bindings to maintain | Tighter coupling — Zero-DB updates require recompiling consumers |

For an internal-only KV with one consumer, the sidecar is almost always the right call. Reach for the C ABI when you've measured the localhost hop and it shows up in your latency budget.

---

## Versioning

The `zero_db.h` header pins the C ABI for a given Zig source revision. Re-vendor both the `.h` and the binary together when updating Zero-DB. The `Options` struct carries 64 reserved bytes of padding so additions don't break existing consumers, but new fields cannot rely on the old default value.
