# read_json: compression and full-JSON support

## Summary

Extend the `read_json` table-valued function to read compressed JSON (gzip and
zip archives) and full-JSON top-level arrays (`[{...},{...}]`), in addition to
the newline-delimited JSON (NDJSON) it reads today. Nested objects and arrays
stay as Arrow struct/list columns, queried with standard SQL (field access,
`UNNEST`).

## Motivation

`read_json` today wraps DataFusion's `JsonFormat` + `ListingTable`. That path
reads only NDJSON / whitespace-separated JSON *values* and has no compression
support at all. Two common real-world shapes are unreachable:

- **Compressed JSON.** `.json.gz` (streaming codec) and `.json.zip` (archive).
- **Full JSON arrays.** A top-level `[{...},{...}]` document. Confirmed
  empirically (arrow 58): arrow-json's reader rejects a top-level array
  (`Expected JSON record to be an object, found Array`). NDJSON is the only
  framing it accepts.

The sibling `read_csv` already carries a `compression` arg
(`auto`/`gzip`/`bz2`/`xz`/`zstd`) with extension-strip logic. `read_json` should
reach parity and add the two JSON-specific gaps (zip, array framing).

## What changes

1. **`compression` named arg** on `read_json`, mirroring `read_csv`, plus a new
   `zip` value: `auto | none | gzip | zip | zstd | bz2 | xz`. Default `auto`
   (derived from the file-extension chain: `.json.gz`, `.json.zip`).
2. **`format` named arg**, DuckDB-aligned: `auto | newline_delimited | array`.
   Default `auto` (peek the first non-whitespace byte; `[` implies `array`).
   The existing `newline_delimited` bool is kept as an alias mapping onto
   `format`.
3. **Route by framing, then compression.** NDJSON with a streaming codec keeps
   the existing `ListingTable` path. `format = array` OR `compression = zip`
   takes a new buffer path.
4. **Buffer path** (`read_json_buffer.rs`): enforce the TVF path policy, fetch
   bytes through the object store, decompress, reshape arrays to NDJSON via
   `serde_json`, infer + decode with `arrow::json`, expose the result as a
   `MemTable`.

## Non-goals

- No auto-flatten of nested JSON. Nested fields remain Arrow struct/list
  columns; callers use SQL field access / `UNNEST`.
- No streaming for zip or full-array inputs. Those are buffered in memory and
  are assumed small (see "Scale assumption"). NDJSON/gzip stay streaming.
- No `format = unstructured` (DuckDB's raw-text mode). Out of scope.

## Architecture

### Routing

Resolve `format` and `compression` (auto-detect when `auto`), then:

```
if format == array  OR  compression == zip:
        -> buffer path (read_json_buffer.rs)
else:
        -> streaming ListingTable path (existing + FileCompressionType)
```

Framing is the primary key. A `.json.gz` that contains a top-level array must
take the buffer path (decompress gzip there, then reshape), because the
streaming path would decompress the gzip cleanly and then arrow-json would choke
on the leading `[`.

### Streaming path (unchanged shape, adds compression)

Port `read_csv`'s `parse_compression`, `strip_compression_ext`, and
`compression_from_extension` helpers (or lift them into `file_tvf_common` for
reuse). Apply `ListingOptions::with_file_compression_type(...)` on the existing
`JsonFormat` listing table. `zip` is rejected on this path (it routes away
before reaching here).

### Buffer path (new)

```
enforce_tvf_path_policy(FN_NAME, &args, &storage, &caller)   # SECURITY GATE, first
store, location = resolve_object_store(path)                 # local | s3 | azure | gcs | http
raw = store.get(location).await?.bytes().await?              # size-capped (see below)
inner = decompress(raw, codec)                               # zip -> entries; gzip -> flate2; none -> raw
ndjson = match format {
        array           => serde_reshape_to_ndjson(inner),      # proven: parse Value::Array, re-emit line-per-element
        newline_delimited => inner,
}
schema = arrow::json::reader::infer_json_schema(ndjson)
batches = arrow::json::ReaderBuilder::new(schema).build(ndjson)
-> MemTable::try_new(schema, vec![batches])                  # TableProvider
```

The `zip` crate is synchronous. Because bytes are already buffered, wrap the
archive in a `std::io::Cursor` inside the existing `block_on_compat` boundary.

### Zip entry semantics

A zip may hold several files. Read **every entry whose name matches a JSON
extension** (`.json`, `.jsonl`, `.ndjson`), decompress each, apply the same
array-reshape per entry, concatenate all resulting NDJSON byte streams, then run
a **single** infer + decode over the concatenation. Concatenating before
inference lets arrow-json union differing per-entry schemas. Non-JSON entries
(READMEs, checksums) are ignored. Zero matching entries is a typed error.

## Named-arg schema

| Arg | Values | Default | Notes |
|---|---|---|---|
| `format` | `auto`, `newline_delimited`, `array` | `auto` | auto = peek first non-ws byte |
| `compression` (alias `compress`) | `auto`, `none`, `gzip`, `zip`, `zstd`, `bz2`, `xz` | `auto` | auto from extension chain |
| `newline_delimited` | bool | (unset) | legacy alias -> `format` |
| `file_extension` | string | `.json` | listing extension (streaming path) |

Storage args (`access_key`, `secret_key`, endpoint, region, path-style, ...) are
inherited unchanged from `parse_file_tvf_args` / `file_tvf_common`.

## Security

The buffer path fetches bytes directly via `object_store.get()`, bypassing
`ListingTable`. It **must** call `enforce_tvf_path_policy` before the fetch. This
is the exact surface closed in MR !190 (`read_parquet('/etc/shadow')`,
`read_parquet('http://169.254.169.254/...')` IMDS SSRF). The object-store
abstraction hides the trust boundary, so the gate is enforced in code, first,
not left to the listing layer. This is a hard requirement, not later hardening.

## Error handling

Typed `DataFusionError::Plan`, each naming the path/arg:

- Unknown `format` / `compression` value.
- `zip` archive with zero JSON-extension entries.
- Malformed array document (surface the `serde_json` error + byte offset).
- Buffer exceeds `max_buffer_bytes` (a guard so a mislabeled multi-GB "array"
  fails with a clear message instead of OOM). Default cap is generous
  (e.g. 512 MiB) and overridable via a named arg or config.
- No tokio runtime available (existing `block_on_compat` failure).

## Scale assumption

Zip and full-array inputs are buffered in memory and assumed "small enough"
(hundreds of MB, guarded by `max_buffer_bytes`). NDJSON and gzip of any size
keep the streaming `ListingTable` path with no memory ceiling. This matches the
"mixed" workload: huge NDJSON/gzip streams, small zip/array documents.

## Testing (all local, no Polaris/S3 stack)

Unit tests using `tempfile` local paths and in-memory bytes:

- NDJSON (regression), NDJSON `.gz`.
- Full array `.json`, full array `.json.gz` (routes to buffer + gzip decompress).
- Zip of one NDJSON entry; zip of one array entry.
- Zip of multiple JSON entries (concatenation + schema union).
- Zip with no JSON entry -> typed error.
- Malformed array -> typed error with offset.
- `format = auto` correctly detects `[` vs object.
- `compression = auto` correctly reads the extension chain.
- Security gate rejects `/etc/shadow` and `169.254.169.254` on the buffer path.
- MemTable schema equals the struct-column schema arrow-json infers (nested
  objects become struct columns).

The arrow-json array-rejection and serde-reshape behavior were verified in an
isolated probe against arrow 58 before this design (see Motivation).

## Files

- `crates/sqe-catalog/src/read_json.rs` — add `format`/`compression` parsing;
  the router; keep `build_json_listing_table` for the streaming branch.
- `crates/sqe-catalog/src/read_json_buffer.rs` (new) — buffer path: fetch,
  decompress, reshape, decode, MemTable.
- `crates/sqe-catalog/src/file_tvf_common.rs` — optionally lift the shared
  compression helpers here so `read_csv` and `read_json` share one copy.
- `crates/sqe-catalog/Cargo.toml` + workspace `Cargo.toml` — add the `zip`
  dependency. `serde_json`, `flate2`, `arrow`, `object_store`, `bytes` are
  already present.

## Rollback

The feature is additive. `read_json` with no new args behaves exactly as today
(NDJSON streaming). Reverting the crate changes and dropping the `zip` dep
restores prior behavior with no data-format migration.
