/* zero_db.h — C ABI for embedding Zero-DB in another process.
 *
 * Build the matching binary with `zig build -Dffi=true`. The shared
 * library lands at zig-out/lib/libzero_db.{so,dylib} and a static
 * archive at zig-out/lib/libzero_db.a.
 *
 * Memory model:
 *   - Caller-supplied byte slices are borrowed for the call's
 *     duration. The library copies whatever it needs to retain.
 *   - On ZERO_DB_OK from zero_db_get, *out_value points to a
 *     library-allocated buffer the caller frees via zero_db_free.
 *   - zero_db_open dupes every string from zero_db_options_t into
 *     the handle's allocator, so caller-supplied option pointers
 *     can be freed immediately.
 *
 * Threading:
 *   - zero_db_open and zero_db_close must run on a single thread.
 *   - All other calls may interleave from any thread.
 */

#ifndef ZERO_DB_H
#define ZERO_DB_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Status codes returned by every entry point. */
#define ZERO_DB_OK                    0
#define ZERO_DB_ERR_OOM               1
#define ZERO_DB_ERR_NOT_FOUND         2
#define ZERO_DB_ERR_INVALID_ARG       3
#define ZERO_DB_ERR_IO                4
#define ZERO_DB_ERR_AUTH              5
#define ZERO_DB_ERR_OVER_BUDGET       6
#define ZERO_DB_ERR_MANIFEST_CONFLICT 7
#define ZERO_DB_ERR_INTERNAL          99

/* Opaque engine handle. */
typedef struct zero_db_handle zero_db_handle_t;

/* Configuration for zero_db_open. Field-by-field defaults:
 *   bucket          NULL → in-memory only mode (no GCS persistence)
 *   manifest_object NULL → "manifest.json" when bucket is set
 *   wal_path        NULL → no WAL; engine is in-memory only across
 *                          process restarts. Set to a writable path
 *                          like "/tmp/zero-db-wal.log" on Cloud Run.
 *   memtable_flush_bytes        0 → engine default (~4 MiB)
 *   block_cache_bytes           0 → engine default (~64 MiB)
 *   l0_compaction_threshold     0 → engine default (4)
 *   _reserved[64]   MUST be zero. Reserved for future fields.
 */
typedef struct {
  const char* bucket;
  const char* manifest_object;
  const char* wal_path;
  size_t      memtable_flush_bytes;
  size_t      block_cache_bytes;
  size_t      l0_compaction_threshold;
  uint8_t     _reserved[64];
} zero_db_options_t;

/* Engine telemetry snapshot. */
typedef struct {
  uint64_t sstables;
  uint64_t active_entries;
  uint64_t compaction_failures;
} zero_db_stats_t;

/* Lifecycle. */
int  zero_db_open(const zero_db_options_t* options, zero_db_handle_t** out);
void zero_db_close(zero_db_handle_t* h);

/* KV operations. Pointer + length pairs avoid C-string ambiguity for
 * binary keys / values. */
int zero_db_set(zero_db_handle_t* h,
                const uint8_t* key, size_t key_len,
                const uint8_t* value, size_t value_len);

int zero_db_delete(zero_db_handle_t* h,
                   const uint8_t* key, size_t key_len);

/* On ZERO_DB_OK, *out_value is library-allocated; free via zero_db_free.
 * On ZERO_DB_ERR_NOT_FOUND, *out_value is NULL and *out_value_len is 0. */
int zero_db_get(zero_db_handle_t* h,
                const uint8_t* key, size_t key_len,
                uint8_t** out_value, size_t* out_value_len);

/* Frees a buffer returned by zero_db_get. Pass the same handle that
 * issued the get. NULL ptr is a no-op. */
void zero_db_free(zero_db_handle_t* h, uint8_t* ptr, size_t len);

/* Admin operations. flush blocks until the active MemTable is durable;
 * compact runs a full SSTable merge. */
int zero_db_flush(zero_db_handle_t* h);
int zero_db_compact(zero_db_handle_t* h);
int zero_db_stats(zero_db_handle_t* h, zero_db_stats_t* out);

#ifdef __cplusplus
}
#endif

#endif /* ZERO_DB_H */
