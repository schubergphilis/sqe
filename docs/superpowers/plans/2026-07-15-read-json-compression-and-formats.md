# read_json Compression and Full-JSON Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend the `read_json` TVF to read gzip/zip-compressed JSON and full-JSON top-level arrays, in addition to the NDJSON it reads today.

**Architecture:** Resolve `format` and `compression` from named args, then route: NDJSON + a streaming codec stays on the existing `ListingTable` path (adding `FileCompressionType`); `format=array` OR `compression=zip` takes a new buffer path that fetches bytes through the object store, decompresses, reshapes arrays to NDJSON via `serde_json`, decodes with `arrow::json`, and serves a `MemTable`.

**Tech Stack:** Rust, DataFusion 54, arrow 58 (`arrow::json`), `serde_json`, `flate2` (gzip), `zip` (archives), `object_store`.

## Global Constraints

- The buffer path MUST call `enforce_tvf_path_policy` BEFORE any `object_store.get()`. This is the `/etc/shadow` / IMDS-SSRF surface closed in MR !190. Non-negotiable.
- Framing is the primary routing key: a `.json.gz` containing a top-level array routes to the buffer path, not the streaming path.
- DuckDB-aligned arg names: `format => auto|newline_delimited|array`, `compression => auto|none|gzip|zip|zstd|bz2|xz`.
- Nested JSON stays as Arrow struct/list columns. No auto-flatten.
- All tests in this plan are local (tempfile / in-memory bytes). No Polaris/S3 stack required.
- The feature is additive: `read_json('file.jsonl')` with no new args must behave exactly as today.
- Empirically verified (arrow 58): arrow-json rejects top-level `[...]`; `serde_json` parse + re-emit line-per-element produces valid NDJSON that decodes.
- `block_on_compat<F>(fut) -> Option<F::Output>`; `None` means "no tokio runtime" and is a hard `DataFusionError::Plan`.
- `MemTable` import path is `datafusion::datasource::MemTable`.

---

### Task 1: Add `zip` dependency and format/compression arg types

**Files:**
- Modify: `Cargo.toml` (workspace `[workspace.dependencies]`)
- Modify: `crates/sqe-catalog/Cargo.toml` (`[dependencies]`)
- Modify: `crates/sqe-catalog/src/read_json.rs` (types + parsers)
- Test: `crates/sqe-catalog/src/read_json.rs` (`#[cfg(test)] mod tests`)

**Interfaces:**
- Produces: `enum JsonFraming { Auto, NewlineDelimited, Array }`; `fn parse_framing(&str) -> DFResult<JsonFraming>`; `fn parse_json_compression(&str) -> DFResult<Option<JsonCompression>>` where `enum JsonCompression { None, Gzip, Zip, Zstd, Bz2, Xz }`; extended `JsonOpts` with `framing: Option<JsonFraming>` and `compression: Option<JsonCompression>`.
- Note: we use a local `JsonCompression` enum (not DataFusion's `FileCompressionType`) because it must also represent `Zip`, which `FileCompressionType` cannot. The streaming path maps the non-zip variants onto `FileCompressionType` in Task 2.

- [ ] **Step 1: Add the `zip` dependency**

In the workspace root `Cargo.toml` under `[workspace.dependencies]`, add:

```toml
zip = { version = "2", default-features = false, features = ["deflate"] }
```

In `crates/sqe-catalog/Cargo.toml` under `[dependencies]`, add:

```toml
zip = { workspace = true }
```

- [ ] **Step 2: Run cargo to fetch the dep and confirm it resolves**

Run: `cargo tree -p sqe-catalog -i zip`
Expected: prints a `zip v2.x` node with `sqe-catalog` as a dependent (no resolution error).

- [ ] **Step 3: Write failing tests for the two parsers**

Add to the `tests` module in `read_json.rs`:

```rust
#[test]
fn parse_framing_accepts_known_values() {
    assert!(matches!(parse_framing("auto").unwrap(), JsonFraming::Auto));
    assert!(matches!(parse_framing("array").unwrap(), JsonFraming::Array));
    assert!(matches!(
        parse_framing("newline_delimited").unwrap(),
        JsonFraming::NewlineDelimited
    ));
    assert!(matches!(parse_framing("ARRAY").unwrap(), JsonFraming::Array));
    assert!(parse_framing("bogus").is_err());
}

#[test]
fn parse_json_compression_accepts_known_values() {
    assert!(parse_json_compression("auto").unwrap().is_none());
    assert!(matches!(
        parse_json_compression("gzip").unwrap(),
        Some(JsonCompression::Gzip)
    ));
    assert!(matches!(
        parse_json_compression("zip").unwrap(),
        Some(JsonCompression::Zip)
    ));
    assert!(matches!(
        parse_json_compression("none").unwrap(),
        Some(JsonCompression::None)
    ));
    assert!(parse_json_compression("rar").is_err());
}
```

- [ ] **Step 4: Run to verify failure**

Run: `cargo test -p sqe-catalog read_json 2>&1 | tail -20`
Expected: FAIL to compile — `parse_framing`, `parse_json_compression`, `JsonFraming`, `JsonCompression` not found.

- [ ] **Step 5: Add the enums, extend `JsonOpts`, add the parsers**

Replace the existing `JsonOpts` struct in `read_json.rs` with:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JsonFraming {
    Auto,
    NewlineDelimited,
    Array,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JsonCompression {
    None,
    Gzip,
    Zip,
    Zstd,
    Bz2,
    Xz,
}

#[derive(Debug, Default)]
struct JsonOpts {
    framing: Option<JsonFraming>,
    compression: Option<JsonCompression>,
    file_extension: Option<String>,
}

fn parse_framing(value: &str) -> DFResult<JsonFraming> {
    match value.to_ascii_lowercase().as_str() {
        "auto" | "" => Ok(JsonFraming::Auto),
        "newline_delimited" | "ndjson" | "nd" => Ok(JsonFraming::NewlineDelimited),
        "array" | "json" => Ok(JsonFraming::Array),
        other => Err(DataFusionError::Plan(format!(
            "{FN_NAME}: 'format' must be one of auto, newline_delimited, array; got '{other}'"
        ))),
    }
}

fn parse_json_compression(value: &str) -> DFResult<Option<JsonCompression>> {
    match value.to_ascii_lowercase().as_str() {
        "auto" | "" => Ok(None),
        "none" | "uncompressed" | "off" => Ok(Some(JsonCompression::None)),
        "gz" | "gzip" => Ok(Some(JsonCompression::Gzip)),
        "zip" => Ok(Some(JsonCompression::Zip)),
        "zst" | "zstd" => Ok(Some(JsonCompression::Zstd)),
        "bz2" | "bzip2" => Ok(Some(JsonCompression::Bz2)),
        "xz" => Ok(Some(JsonCompression::Xz)),
        other => Err(DataFusionError::Plan(format!(
            "{FN_NAME}: 'compression' must be one of auto, none, gzip, zip, zstd, bz2, xz; got '{other}'"
        ))),
    }
}
```

Keep the existing `parse_bool` function (still used for the `newline_delimited` legacy alias in Task 3).

- [ ] **Step 6: Run to verify the parser tests pass**

Run: `cargo test -p sqe-catalog read_json 2>&1 | tail -20`
Expected: the two new tests PASS. (The pre-existing `parse_bool_accepts_common_forms` and `new_can_be_created` still pass. The `call()` still references the old `JsonOpts` fields — those are updated in Task 3; if compilation breaks there, temporarily leave `call()` using `newline_delimited`/`file_extension` by mapping through the new fields, or proceed straight to Task 3. Prefer proceeding to Task 3 in the same session.)

- [ ] **Step 7: Commit**

```bash
git add Cargo.toml crates/sqe-catalog/Cargo.toml crates/sqe-catalog/src/read_json.rs
git commit -m "feat(read_json): add zip dep, format/compression arg enums and parsers"
```

---

### Task 2: Extension-based auto-detection helpers

**Files:**
- Modify: `crates/sqe-catalog/src/read_json.rs`
- Test: `crates/sqe-catalog/src/read_json.rs`

**Interfaces:**
- Consumes: `JsonCompression`, `JsonFraming` (Task 1).
- Produces: `fn compression_from_extension(path: &str) -> JsonCompression`; `fn strip_compression_ext(path: &str) -> &str`; `fn detect_framing_from_bytes(bytes: &[u8]) -> JsonFraming` (peek first non-whitespace byte: `[` => Array, else NewlineDelimited).

- [ ] **Step 1: Write failing tests**

```rust
#[test]
fn compression_from_extension_maps_suffixes() {
    assert!(matches!(compression_from_extension("a.json.gz"), JsonCompression::Gzip));
    assert!(matches!(compression_from_extension("a.json.zip"), JsonCompression::Zip));
    assert!(matches!(compression_from_extension("a.json.zst"), JsonCompression::Zstd));
    assert!(matches!(compression_from_extension("a.json"), JsonCompression::None));
}

#[test]
fn detect_framing_peeks_first_nonws_byte() {
    assert!(matches!(detect_framing_from_bytes(b"   [ {\"a\":1} ]"), JsonFraming::Array));
    assert!(matches!(detect_framing_from_bytes(b"\n\t[1,2]"), JsonFraming::Array));
    assert!(matches!(detect_framing_from_bytes(b"{\"a\":1}\n"), JsonFraming::NewlineDelimited));
    assert!(matches!(detect_framing_from_bytes(b""), JsonFraming::NewlineDelimited));
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p sqe-catalog read_json 2>&1 | tail -20`
Expected: FAIL — functions not found.

- [ ] **Step 3: Implement the helpers**

```rust
/// Strip a compression suffix so extension logic sees the format ext.
/// `data.json.gz` -> `data.json`.
fn strip_compression_ext(path: &str) -> &str {
    for ext in [".gz", ".gzip", ".zip", ".bz2", ".bzip2", ".xz", ".zst", ".zstd"] {
        if path.to_ascii_lowercase().ends_with(ext) {
            return &path[..path.len() - ext.len()];
        }
    }
    path
}

/// Map a path's trailing codec extension to a [`JsonCompression`].
fn compression_from_extension(path: &str) -> JsonCompression {
    let lower = path.to_ascii_lowercase();
    if lower.ends_with(".gz") || lower.ends_with(".gzip") {
        JsonCompression::Gzip
    } else if lower.ends_with(".zip") {
        JsonCompression::Zip
    } else if lower.ends_with(".bz2") || lower.ends_with(".bzip2") {
        JsonCompression::Bz2
    } else if lower.ends_with(".xz") {
        JsonCompression::Xz
    } else if lower.ends_with(".zst") || lower.ends_with(".zstd") {
        JsonCompression::Zstd
    } else {
        JsonCompression::None
    }
}

/// Peek the first non-whitespace byte to decide framing. `[` => array.
fn detect_framing_from_bytes(bytes: &[u8]) -> JsonFraming {
    for &b in bytes {
        match b {
            b' ' | b'\t' | b'\r' | b'\n' => continue,
            b'[' => return JsonFraming::Array,
            _ => return JsonFraming::NewlineDelimited,
        }
    }
    JsonFraming::NewlineDelimited
}
```

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p sqe-catalog read_json 2>&1 | tail -20`
Expected: both new tests PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-catalog/src/read_json.rs
git commit -m "feat(read_json): extension-based compression + framing auto-detection"
```

---

### Task 3: Parse the new named args and add the router (streaming branch only)

**Files:**
- Modify: `crates/sqe-catalog/src/read_json.rs`
- Test: `crates/sqe-catalog/src/read_json.rs`

**Interfaces:**
- Consumes: everything from Tasks 1-2; `parse_file_tvf_args`, `enforce_tvf_path_policy`, the `register_*_store_if_needed` fns, `block_on_compat`, `JsonFormat`, `ListingTable`.
- Produces: an updated `TableFunctionImpl::call` that parses `format`/`compression`/`newline_delimited`/`file_extension`, resolves them, and (for now) always calls the streaming builder — with the buffer branch added in Task 6. `build_json_listing_table` gains a `FileCompressionType` argument.

- [ ] **Step 1: Extend the arg-parse closure in `call()`**

Replace the closure body in `call()` (the `parse_file_tvf_args(FN_NAME, exprs, |key, value| { ... })` call) with:

```rust
let args = parse_file_tvf_args(FN_NAME, exprs, |key, value| {
    match key {
        "format" => match parse_framing(value) {
            Ok(f) => json_opts.framing = Some(f),
            Err(e) => parse_err = Some(e),
        },
        // Legacy alias: newline_delimited => format.
        "newline_delimited" => match parse_bool("newline_delimited", value) {
            Ok(true) => json_opts.framing = Some(JsonFraming::NewlineDelimited),
            Ok(false) => json_opts.framing = Some(JsonFraming::Array),
            Err(e) => parse_err = Some(e),
        },
        "compression" | "compress" => match parse_json_compression(value) {
            Ok(c) => json_opts.compression = c,
            Err(e) => parse_err = Some(e),
        },
        "file_extension" => json_opts.file_extension = Some(value.to_string()),
        _ => return false,
    }
    true
})?;
```

Note: `newline_delimited=false` historically meant "not NDJSON"; we map it to `Array` framing, which the buffer path handles. This is a behavior refinement, documented in Task 7.

- [ ] **Step 2: Add the `FileCompressionType` import and map non-zip codecs**

Add near the top imports:

```rust
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
```

Add this helper to the file:

```rust
/// Map the streaming-capable codecs onto DataFusion's `FileCompressionType`.
/// `Zip` never reaches here (it routes to the buffer path); treat it as a bug.
fn streaming_compression(c: JsonCompression) -> FileCompressionType {
    match c {
        JsonCompression::None => FileCompressionType::UNCOMPRESSED,
        JsonCompression::Gzip => FileCompressionType::GZIP,
        JsonCompression::Bz2 => FileCompressionType::BZIP2,
        JsonCompression::Xz => FileCompressionType::XZ,
        JsonCompression::Zstd => FileCompressionType::ZSTD,
        JsonCompression::Zip => FileCompressionType::UNCOMPRESSED, // unreachable via router
    }
}
```

- [ ] **Step 3: Thread compression into `build_json_listing_table`**

Change the signature and body of `build_json_listing_table` so it applies the codec and strips the codec extension for the listing extension:

```rust
async fn build_json_listing_table(
    args: &FileTvfArgs,
    json_opts: &JsonOpts,
    compression: FileCompressionType,
    storage: &StorageConfig,
    runtime_env: Option<&datafusion::execution::runtime_env::RuntimeEnv>,
) -> DFResult<Arc<dyn TableProvider>> {
    let mut args = args.clone();
    rewrite_hf_path_in_place(FN_NAME, &mut args)?;

    let listing_url = ListingTableUrl::parse(&args.path)?;

    let tmp_ctx = SessionContext::new();
    register_s3_store_if_needed(FN_NAME, &tmp_ctx, &args, storage, runtime_env)?;
    register_azure_store_if_needed(FN_NAME, &tmp_ctx, &args, storage)?;
    register_gcs_store_if_needed(FN_NAME, &tmp_ctx, &args, storage)?;
    register_http_store_if_needed(FN_NAME, &tmp_ctx, &args.path)?;

    let mut format = JsonFormat::default();
    // NDJSON is the only framing the streaming path serves.
    format = format.with_file_compression_type(compression);

    // Listing extension ignores the codec suffix: `.json.gz` lists as `.json`.
    let default_ext = strip_compression_ext(&args.path);
    let extension = json_opts
        .file_extension
        .as_deref()
        .unwrap_or_else(|| extension_of(default_ext));
    let listing_options =
        ListingOptions::new(Arc::new(format)).with_file_extension(extension);

    let state = tmp_ctx.state();
    crate::file_tvf_common::ensure_local_files_exist(
        FN_NAME, &state, &listing_url, extension, &args.path,
    )
    .await?;
    let schema = listing_options.infer_schema(&state, &listing_url).await?;

    let config = ListingTableConfig::new(listing_url)
        .with_listing_options(listing_options)
        .with_schema(schema);

    let table = ListingTable::try_new(config)?;
    debug!(path = %args.path, "read_json: built ListingTable");
    Ok(Arc::new(table))
}

/// Return the file extension (with leading dot) of a path, or ".json".
fn extension_of(path: &str) -> &str {
    match path.rfind('.') {
        Some(idx) => &path[idx..],
        None => ".json",
    }
}
```

- [ ] **Step 4: Update `call()` to resolve compression and dispatch (streaming for now)**

After the `enforce_tvf_path_policy(...)` call in `call()`, replace the `block_on_compat` invocation with:

```rust
let compression = json_opts
    .compression
    .unwrap_or_else(|| compression_from_extension(&args.path));

// Router (buffer branch added in Task 6): framing first, then zip.
let storage = self.storage.clone();
let runtime_env = self.runtime_env.clone();
let df_compression = streaming_compression(compression);
crate::runtime_bridge::block_on_compat(async move {
    build_json_listing_table(&args, &json_opts, df_compression, &storage, runtime_env.as_deref())
        .await
})
.ok_or_else(|| DataFusionError::Plan(format!("{FN_NAME}: no tokio runtime available")))?
```

- [ ] **Step 5: Write a regression test through the TVF for plain NDJSON**

Add a test that writes a temp `.jsonl` and reads it via the function. Use the crate's existing test-context helper if present; otherwise register the function on a local `SessionContext`. Example:

```rust
#[tokio::test]
async fn reads_plain_ndjson_local() {
    use datafusion::prelude::SessionContext;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.jsonl");
    std::fs::write(&path, b"{\"a\":1}\n{\"a\":2}\n").unwrap();

    let ctx = SessionContext::new();
    ctx.register_udtf("read_json", Arc::new(ReadJsonFunction::new(StorageConfig::default())));
    let df = ctx
        .sql(&format!("SELECT count(*) AS n FROM read_json('{}')", path.display()))
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    // 2 rows expected.
    let n = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(n, 2);
}
```

Add `tempfile = { workspace = true }` to `crates/sqe-catalog/Cargo.toml` `[dev-dependencies]` if not already present (check with `grep tempfile crates/sqe-catalog/Cargo.toml`).

- [ ] **Step 6: Run to verify pass**

Run: `cargo test -p sqe-catalog read_json 2>&1 | tail -30`
Expected: `reads_plain_ndjson_local` PASSES; all earlier tests still pass.

- [ ] **Step 7: Commit**

```bash
git add crates/sqe-catalog/src/read_json.rs crates/sqe-catalog/Cargo.toml
git commit -m "feat(read_json): parse format/compression args, thread codec into streaming path"
```

---

### Task 4: Buffer-path decode core (bytes -> MemTable), local files only

**Files:**
- Create: `crates/sqe-catalog/src/read_json_buffer.rs`
- Modify: `crates/sqe-catalog/src/lib.rs` (add `mod read_json_buffer;`)
- Test: `crates/sqe-catalog/src/read_json_buffer.rs`

**Interfaces:**
- Produces:
  - `pub(crate) fn reshape_array_to_ndjson(bytes: &[u8]) -> DFResult<Vec<u8>>` — parse a top-level JSON array (or single object) and re-emit one compact JSON value per line.
  - `pub(crate) fn decode_ndjson_to_memtable(ndjson: &[u8]) -> DFResult<Arc<dyn TableProvider>>` — infer schema + decode into a `MemTable`.
  - `pub(crate) fn decompress(bytes: Vec<u8>, codec: JsonCompression) -> DFResult<Vec<u8>>` for `None`/`Gzip` here; `Zip` added in Task 5 (return a "not yet" error for `Zip` in this task to keep it compiling).

- [ ] **Step 1: Create the module with imports and a failing reshape test**

Create `crates/sqe-catalog/src/read_json_buffer.rs`:

```rust
//! Buffer path for `read_json`: reads a whole JSON document into memory,
//! decompresses (gzip/zip), reshapes a top-level array into NDJSON, decodes
//! with `arrow::json`, and serves the rows as a `MemTable`. Used for
//! `format=array` and `compression=zip`, which DataFusion's streaming JSON
//! listing path cannot handle.

use std::io::Read;
use std::sync::Arc;

use datafusion::catalog::TableProvider;
use datafusion::datasource::MemTable;
use datafusion::error::{DataFusionError, Result as DFResult};

use crate::read_json::{JsonCompression, JsonFraming};

const FN_NAME: &str = "read_json";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reshape_array_yields_one_line_per_element() {
        let out = reshape_array_to_ndjson(b"[{\"a\":1},{\"a\":2}]").unwrap();
        let s = String::from_utf8(out).unwrap();
        let lines: Vec<&str> = s.lines().collect();
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0], "{\"a\":1}");
        assert_eq!(lines[1], "{\"a\":2}");
    }

    #[test]
    fn reshape_accepts_single_object() {
        let out = reshape_array_to_ndjson(b"{\"a\":1}").unwrap();
        assert_eq!(String::from_utf8(out).unwrap().lines().count(), 1);
    }

    #[test]
    fn reshape_rejects_malformed() {
        assert!(reshape_array_to_ndjson(b"[not json").is_err());
    }
}
```

This requires `JsonCompression`/`JsonFraming` to be `pub(crate)` in `read_json.rs` (they are per Task 1) and the enums re-exported. Add `pub(crate) mod read_json;`-visibility as needed; confirm `read_json` module is declared in `lib.rs` (it is).

- [ ] **Step 2: Register the module and run to verify failure**

In `crates/sqe-catalog/src/lib.rs`, add alongside the other `mod` lines:

```rust
mod read_json_buffer;
```

Run: `cargo test -p sqe-catalog read_json_buffer 2>&1 | tail -20`
Expected: FAIL — `reshape_array_to_ndjson` not found.

- [ ] **Step 3: Implement `reshape_array_to_ndjson`**

```rust
/// Parse a top-level JSON value. An array becomes one line per element; a
/// bare object becomes a single line. Anything else is an error.
pub(crate) fn reshape_array_to_ndjson(bytes: &[u8]) -> DFResult<Vec<u8>> {
    let value: serde_json::Value = serde_json::from_slice(bytes).map_err(|e| {
        DataFusionError::Plan(format!(
            "{FN_NAME}: failed to parse JSON document (line {}, col {}): {e}",
            e.line(),
            e.column()
        ))
    })?;

    let mut out = Vec::with_capacity(bytes.len());
    match value {
        serde_json::Value::Array(items) => {
            for item in items {
                serde_json::to_writer(&mut out, &item).map_err(|e| {
                    DataFusionError::Plan(format!("{FN_NAME}: re-serialize failed: {e}"))
                })?;
                out.push(b'\n');
            }
        }
        obj @ serde_json::Value::Object(_) => {
            serde_json::to_writer(&mut out, &obj).map_err(|e| {
                DataFusionError::Plan(format!("{FN_NAME}: re-serialize failed: {e}"))
            })?;
            out.push(b'\n');
        }
        _ => {
            return Err(DataFusionError::Plan(format!(
                "{FN_NAME}: expected a JSON object or array of objects at top level"
            )))
        }
    }
    Ok(out)
}
```

- [ ] **Step 4: Run to verify reshape tests pass**

Run: `cargo test -p sqe-catalog read_json_buffer 2>&1 | tail -20`
Expected: the three reshape tests PASS.

- [ ] **Step 5: Add `decompress` (None/Gzip; Zip stubbed) and `decode_ndjson_to_memtable` with tests**

Add tests first:

```rust
    #[test]
    fn decompress_gzip_roundtrips() {
        use flate2::write::GzEncoder;
        use flate2::Compression;
        use std::io::Write;
        let mut enc = GzEncoder::new(Vec::new(), Compression::default());
        enc.write_all(b"{\"a\":1}\n").unwrap();
        let gz = enc.finish().unwrap();
        let out = decompress(gz, JsonCompression::Gzip).unwrap();
        assert_eq!(out, b"{\"a\":1}\n");
    }

    #[test]
    fn decode_ndjson_builds_two_row_table() {
        let table = decode_ndjson_to_memtable(b"{\"a\":1}\n{\"a\":2}\n").unwrap();
        assert_eq!(table.schema().fields().len(), 1);
    }
```

Then the implementations:

```rust
pub(crate) fn decompress(bytes: Vec<u8>, codec: JsonCompression) -> DFResult<Vec<u8>> {
    match codec {
        JsonCompression::None => Ok(bytes),
        JsonCompression::Gzip => {
            let mut dec = flate2::read::GzDecoder::new(&bytes[..]);
            let mut out = Vec::new();
            dec.read_to_end(&mut out).map_err(|e| {
                DataFusionError::Plan(format!("{FN_NAME}: gzip decode failed: {e}"))
            })?;
            Ok(out)
        }
        JsonCompression::Zip => Err(DataFusionError::Plan(format!(
            "{FN_NAME}: zip handled by decompress_zip (see Task 5)"
        ))),
        other => Err(DataFusionError::Plan(format!(
            "{FN_NAME}: codec {other:?} is not valid on the buffer path"
        ))),
    }
}

pub(crate) fn decode_ndjson_to_memtable(ndjson: &[u8]) -> DFResult<Arc<dyn TableProvider>> {
    use arrow::json::reader::infer_json_schema;
    use arrow::json::ReaderBuilder;
    use std::io::Cursor;

    let (schema, _) = infer_json_schema(&mut Cursor::new(ndjson), None).map_err(|e| {
        DataFusionError::Plan(format!("{FN_NAME}: JSON schema inference failed: {e}"))
    })?;
    let schema = Arc::new(schema);

    let reader = ReaderBuilder::new(schema.clone())
        .build(Cursor::new(ndjson))
        .map_err(|e| DataFusionError::Plan(format!("{FN_NAME}: JSON reader build failed: {e}")))?;

    let mut batches = Vec::new();
    for b in reader {
        batches.push(b.map_err(|e| {
            DataFusionError::Plan(format!("{FN_NAME}: JSON decode failed: {e}"))
        })?);
    }

    let table = MemTable::try_new(schema, vec![batches])?;
    Ok(Arc::new(table))
}
```

Ensure `flate2` is a dependency of `sqe-catalog` (add `flate2 = { workspace = true }` to `[dependencies]` if `grep flate2 crates/sqe-catalog/Cargo.toml` shows nothing; it is already in the workspace lock).

- [ ] **Step 6: Run to verify pass**

Run: `cargo test -p sqe-catalog read_json_buffer 2>&1 | tail -30`
Expected: all buffer-module tests PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/sqe-catalog/src/read_json_buffer.rs crates/sqe-catalog/src/lib.rs crates/sqe-catalog/Cargo.toml
git commit -m "feat(read_json): buffer-path decode core (reshape, gzip, ndjson->MemTable)"
```

---

### Task 5: Zip archive support (multi-entry concatenation)

**Files:**
- Modify: `crates/sqe-catalog/src/read_json_buffer.rs`
- Test: `crates/sqe-catalog/src/read_json_buffer.rs`

**Interfaces:**
- Produces: `pub(crate) fn decompress_zip_to_ndjson(bytes: Vec<u8>, framing: JsonFraming) -> DFResult<Vec<u8>>` — read every JSON-extension entry, per-entry reshape when the (resolved) framing is `Array`, concatenate all NDJSON. Returns a typed error if no JSON entry is found.

- [ ] **Step 1: Write a failing test that builds a zip in memory**

```rust
    #[test]
    fn zip_concatenates_json_entries() {
        use std::io::Write;
        use zip::write::SimpleFileOptions;
        let mut buf = Vec::new();
        {
            let mut zw = zip::ZipWriter::new(std::io::Cursor::new(&mut buf));
            let opts = SimpleFileOptions::default();
            zw.start_file("a.jsonl", opts).unwrap();
            zw.write_all(b"{\"a\":1}\n").unwrap();
            zw.start_file("README.txt", opts).unwrap(); // ignored (non-json)
            zw.write_all(b"ignore me").unwrap();
            zw.start_file("b.jsonl", opts).unwrap();
            zw.write_all(b"{\"a\":2}\n").unwrap();
            zw.finish().unwrap();
        }
        let nd = decompress_zip_to_ndjson(buf, JsonFraming::NewlineDelimited).unwrap();
        assert_eq!(String::from_utf8(nd).unwrap().lines().count(), 2);
    }

    #[test]
    fn zip_with_no_json_entry_errors() {
        use std::io::Write;
        use zip::write::SimpleFileOptions;
        let mut buf = Vec::new();
        {
            let mut zw = zip::ZipWriter::new(std::io::Cursor::new(&mut buf));
            zw.start_file("README.txt", SimpleFileOptions::default()).unwrap();
            zw.write_all(b"nope").unwrap();
            zw.finish().unwrap();
        }
        assert!(decompress_zip_to_ndjson(buf, JsonFraming::Auto).is_err());
    }
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p sqe-catalog read_json_buffer 2>&1 | tail -20`
Expected: FAIL — `decompress_zip_to_ndjson` not found.

- [ ] **Step 3: Implement zip extraction**

```rust
/// Read every JSON-extension entry from a zip archive, reshape each per the
/// resolved framing, and concatenate into one NDJSON byte stream. Entries
/// whose name does not end in .json/.jsonl/.ndjson are ignored. Zero JSON
/// entries is an error.
pub(crate) fn decompress_zip_to_ndjson(
    bytes: Vec<u8>,
    framing: JsonFraming,
) -> DFResult<Vec<u8>> {
    use std::io::Cursor;
    let mut archive = zip::ZipArchive::new(Cursor::new(bytes))
        .map_err(|e| DataFusionError::Plan(format!("{FN_NAME}: not a valid zip: {e}")))?;

    let mut out = Vec::new();
    let mut json_entries = 0usize;
    for i in 0..archive.len() {
        let mut entry = archive
            .by_index(i)
            .map_err(|e| DataFusionError::Plan(format!("{FN_NAME}: zip entry read failed: {e}")))?;
        if entry.is_dir() {
            continue;
        }
        let name = entry.name().to_ascii_lowercase();
        if !(name.ends_with(".json") || name.ends_with(".jsonl") || name.ends_with(".ndjson")) {
            continue;
        }
        let mut raw = Vec::new();
        entry
            .read_to_end(&mut raw)
            .map_err(|e| DataFusionError::Plan(format!("{FN_NAME}: zip entry decompress failed: {e}")))?;

        // Resolve framing per entry when Auto.
        let entry_framing = match framing {
            JsonFraming::Auto => crate::read_json::detect_framing_from_bytes(&raw),
            other => other,
        };
        let nd = match entry_framing {
            JsonFraming::Array => reshape_array_to_ndjson(&raw)?,
            _ => {
                // Ensure trailing newline so concatenation stays line-safe.
                let mut v = raw;
                if !v.ends_with(b"\n") {
                    v.push(b'\n');
                }
                v
            }
        };
        out.extend_from_slice(&nd);
        json_entries += 1;
    }

    if json_entries == 0 {
        return Err(DataFusionError::Plan(format!(
            "{FN_NAME}: zip archive contains no .json/.jsonl/.ndjson entries"
        )));
    }
    Ok(out)
}
```

This calls `crate::read_json::detect_framing_from_bytes`, which must be `pub(crate)` — confirm/adjust visibility in `read_json.rs`.

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p sqe-catalog read_json_buffer 2>&1 | tail -30`
Expected: both zip tests PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-catalog/src/read_json_buffer.rs crates/sqe-catalog/src/read_json.rs
git commit -m "feat(read_json): zip archive support with multi-entry concatenation"
```

---

### Task 6: Wire the buffer path into the router with the security gate and size guard

**Files:**
- Modify: `crates/sqe-catalog/src/read_json_buffer.rs` (add the orchestrating entry point)
- Modify: `crates/sqe-catalog/src/read_json.rs` (router dispatch in `call()`)
- Test: `crates/sqe-catalog/src/read_json.rs` (TVF-level tests) and `read_json_buffer.rs`

**Interfaces:**
- Consumes: `enforce_tvf_path_policy`, `parse_file_tvf_args` result (`FileTvfArgs`), the `register_*_store_if_needed` fns, `block_on_compat`, `ListingTableUrl`, `object_store` API.
- Produces: `pub(crate) async fn build_buffer_table(args: &FileTvfArgs, framing: JsonFraming, codec: JsonCompression, max_bytes: usize, storage: &StorageConfig, runtime_env: Option<&RuntimeEnv>) -> DFResult<Arc<dyn TableProvider>>`.

- [ ] **Step 1: Implement `build_buffer_table` (fetch -> decompress -> reshape -> decode)**

Add to `read_json_buffer.rs`:

```rust
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::execution::context::SessionContext;
use datafusion::execution::runtime_env::RuntimeEnv;
use object_store::ObjectStore;
use sqe_core::config::StorageConfig;

use crate::file_tvf_common::{
    register_azure_store_if_needed, register_gcs_store_if_needed, register_http_store_if_needed,
    register_s3_store_if_needed, FileTvfArgs,
};

/// Fetch the whole object, decompress, reshape to NDJSON, decode to a MemTable.
/// The caller MUST have already run `enforce_tvf_path_policy` on `args`.
pub(crate) async fn build_buffer_table(
    args: &FileTvfArgs,
    framing: JsonFraming,
    codec: JsonCompression,
    max_bytes: usize,
    storage: &StorageConfig,
    runtime_env: Option<&RuntimeEnv>,
) -> DFResult<Arc<dyn TableProvider>> {
    let listing_url = ListingTableUrl::parse(&args.path)?;

    let tmp_ctx = SessionContext::new();
    register_s3_store_if_needed(FN_NAME, &tmp_ctx, args, storage, runtime_env)?;
    register_azure_store_if_needed(FN_NAME, &tmp_ctx, args, storage)?;
    register_gcs_store_if_needed(FN_NAME, &tmp_ctx, args, storage)?;
    register_http_store_if_needed(FN_NAME, &tmp_ctx, &args.path)?;

    let store: Arc<dyn ObjectStore> =
        tmp_ctx.state().runtime_env().object_store(&listing_url)?;
    let path = listing_url.prefix().clone();

    let meta = store
        .head(&path)
        .await
        .map_err(|e| DataFusionError::Plan(format!("{FN_NAME}: cannot stat '{}': {e}", args.path)))?;
    if meta.size as usize > max_bytes {
        return Err(DataFusionError::Plan(format!(
            "{FN_NAME}: '{}' is {} bytes, exceeds max_buffer_bytes {} (raise max_buffer_bytes to read it)",
            args.path, meta.size, max_bytes
        )));
    }

    let raw = store
        .get(&path)
        .await
        .map_err(|e| DataFusionError::Plan(format!("{FN_NAME}: fetch '{}' failed: {e}", args.path)))?
        .bytes()
        .await
        .map_err(|e| DataFusionError::Plan(format!("{FN_NAME}: read '{}' failed: {e}", args.path)))?
        .to_vec();

    let ndjson = match codec {
        JsonCompression::Zip => decompress_zip_to_ndjson(raw, framing)?,
        other => {
            let inner = decompress(raw, other)?;
            let resolved = match framing {
                JsonFraming::Auto => detect_framing(&inner),
                f => f,
            };
            match resolved {
                JsonFraming::Array => reshape_array_to_ndjson(&inner)?,
                _ => inner,
            }
        }
    };

    decode_ndjson_to_memtable(&ndjson)
}

fn detect_framing(bytes: &[u8]) -> JsonFraming {
    crate::read_json::detect_framing_from_bytes(bytes)
}
```

Note: `ListingTableUrl::prefix()` returns `&object_store::path::Path`; `meta.size` is `u64` (or `usize` depending on `object_store` version — adjust the cast at compile time). If `head()` is unsupported by a store, fall back to fetching then checking `raw.len()` against `max_bytes` before decode; keep the `head` guard as the primary check.

- [ ] **Step 2: Add the router dispatch in `read_json.rs` `call()`**

Replace the Task-3 streaming-only dispatch with the full router. After computing `compression` and before the `block_on_compat`:

```rust
let framing = json_opts.framing.unwrap_or(JsonFraming::Auto);
let compression = json_opts
    .compression
    .unwrap_or_else(|| compression_from_extension(&args.path));

let use_buffer = matches!(framing, JsonFraming::Array)
    || matches!(compression, JsonCompression::Zip);

let storage = self.storage.clone();
let runtime_env = self.runtime_env.clone();

if use_buffer {
    let max_bytes = DEFAULT_MAX_BUFFER_BYTES;
    crate::runtime_bridge::block_on_compat(async move {
        crate::read_json_buffer::build_buffer_table(
            &args, framing, compression, max_bytes, &storage, runtime_env.as_deref(),
        )
        .await
    })
    .ok_or_else(|| DataFusionError::Plan(format!("{FN_NAME}: no tokio runtime available")))?
} else {
    let df_compression = streaming_compression(compression);
    crate::runtime_bridge::block_on_compat(async move {
        build_json_listing_table(&args, &json_opts, df_compression, &storage, runtime_env.as_deref())
            .await
    })
    .ok_or_else(|| DataFusionError::Plan(format!("{FN_NAME}: no tokio runtime available")))?
}
```

Add the constant near the top of `read_json.rs`:

```rust
/// Default cap for the buffer path (zip / full-array). Mislabeled huge inputs
/// fail with a clear message instead of OOM. Overridable later via a named arg.
const DEFAULT_MAX_BUFFER_BYTES: usize = 512 * 1024 * 1024;
```

Confirm `enforce_tvf_path_policy(...)` is still called in `call()` before this dispatch (it is, from the current code). Both branches rely on it.

- [ ] **Step 3: Write TVF-level tests for array through the function**

**Auto-detect decision (locked):** the router decides buffer-vs-streaming BEFORE fetching bytes, so it cannot peek file content. Therefore: `format=auto` on a non-zip file resolves to NDJSON and routes to the streaming path; a top-level array is served only when the caller passes `format => 'array'` OR the extension is `.zip` (zip always buffers, and its entries auto-detect framing per-entry). We do NOT string-match the arrow "found Array" error (fragile across arrow versions). This is documented in Task 7.

Add to `read_json.rs` tests:

```rust
#[tokio::test]
async fn reads_full_array_local() {
    use datafusion::prelude::SessionContext;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.json");
    std::fs::write(&path, b"[{\"a\":1},{\"a\":2},{\"a\":3}]").unwrap();

    let ctx = SessionContext::new();
    ctx.register_udtf("read_json", Arc::new(ReadJsonFunction::new(StorageConfig::default())));
    let df = ctx
        .sql(&format!(
            "SELECT count(*) AS n FROM read_json('{}', format => 'array')",
            path.display()
        ))
        .await
        .unwrap();
    let n = df.collect().await.unwrap()[0]
        .column(0).as_any().downcast_ref::<arrow_array::Int64Array>().unwrap().value(0);
    assert_eq!(n, 3);
}
```

- [ ] **Step 4: (no fallback path — decision locked in Step 3)**

Nothing to implement here. The buffer path is reached only via explicit `format => 'array'` or `compression = zip` / `.zip` extension. This step is a placeholder to keep task-step numbering stable with the security tests below.

- [ ] **Step 5: Run all read_json tests**

Run: `cargo test -p sqe-catalog read_json 2>&1 | tail -40`
Expected: NDJSON, array (explicit), and (if implemented) auto-detected array PASS.

- [ ] **Step 6: Add the security-gate tests**

```rust
#[tokio::test]
async fn buffer_path_rejects_local_secret() {
    use datafusion::prelude::SessionContext;
    let ctx = SessionContext::new();
    ctx.register_udtf("read_json", Arc::new(ReadJsonFunction::new(StorageConfig::default())));
    // Anonymous caller + a sensitive absolute path must be denied by policy,
    // BEFORE any fetch. format=array forces the buffer path.
    let res = ctx
        .sql("SELECT * FROM read_json('/etc/shadow', format => 'array')")
        .await;
    // Either plan-time or collect-time error; assert it errors and never reads.
    let err = match res {
        Err(e) => e.to_string(),
        Ok(df) => format!("{:?}", df.collect().await.err()),
    };
    assert!(err.to_lowercase().contains("denied") || err.to_lowercase().contains("not allowed")
        || err.to_lowercase().contains("policy") || err.contains("read_json"),
        "expected a policy denial, got: {err}");
}
```

Adjust the assertion substring to match the actual `enforce_tvf_path_policy` denial message (grep the function body for its `DataFusionError` text and match it exactly).

- [ ] **Step 7: Run and commit**

Run: `cargo test -p sqe-catalog read_json 2>&1 | tail -40`
Expected: all pass.

```bash
git add crates/sqe-catalog/src/read_json.rs crates/sqe-catalog/src/read_json_buffer.rs
git commit -m "feat(read_json): route array/zip to buffer path with security gate + size guard"
```

---

### Task 7: gzip/zip end-to-end tests, clippy, and docs

**Files:**
- Test: `crates/sqe-catalog/src/read_json.rs`
- Modify: `docs/site/book/src/sql-reference/array-map.md` or the read_json reference page (grep for the existing read_json doc page)
- Modify: `crates/sqe-catalog/src/read_json.rs` module doc comment

- [ ] **Step 1: Add gzip-array and zip end-to-end TVF tests**

```rust
#[tokio::test]
async fn reads_gzipped_array() {
    use datafusion::prelude::SessionContext;
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use std::io::Write;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.json.gz");
    let mut enc = GzEncoder::new(Vec::new(), Compression::default());
    enc.write_all(b"[{\"a\":1},{\"a\":2}]").unwrap();
    std::fs::write(&path, enc.finish().unwrap()).unwrap();

    let ctx = SessionContext::new();
    ctx.register_udtf("read_json", Arc::new(ReadJsonFunction::new(StorageConfig::default())));
    // .gz auto-detects gzip; format=array forces the buffer path (array can't stream).
    let n = ctx
        .sql(&format!("SELECT count(*) AS n FROM read_json('{}', format => 'array')", path.display()))
        .await.unwrap().collect().await.unwrap()[0]
        .column(0).as_any().downcast_ref::<arrow_array::Int64Array>().unwrap().value(0);
    assert_eq!(n, 2);
}

#[tokio::test]
async fn reads_zip_of_ndjson() {
    use datafusion::prelude::SessionContext;
    use std::io::Write;
    use zip::write::SimpleFileOptions;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.json.zip");
    let mut buf = Vec::new();
    {
        let mut zw = zip::ZipWriter::new(std::io::Cursor::new(&mut buf));
        zw.start_file("events.jsonl", SimpleFileOptions::default()).unwrap();
        zw.write_all(b"{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n").unwrap();
        zw.finish().unwrap();
    }
    std::fs::write(&path, buf).unwrap();

    let ctx = SessionContext::new();
    ctx.register_udtf("read_json", Arc::new(ReadJsonFunction::new(StorageConfig::default())));
    let n = ctx
        .sql(&format!("SELECT count(*) AS n FROM read_json('{}')", path.display()))
        .await.unwrap().collect().await.unwrap()[0]
        .column(0).as_any().downcast_ref::<arrow_array::Int64Array>().unwrap().value(0);
    assert_eq!(n, 3);
}
```

- [ ] **Step 2: Run the full read_json suite**

Run: `cargo test -p sqe-catalog read_json 2>&1 | tail -40`
Expected: all pass (ndjson, ndjson.gz via streaming, array, array.gz, zip-ndjson, security, parsers, detection).

- [ ] **Step 3: Clippy clean**

Run: `cargo clippy -p sqe-catalog --all-targets -- -D warnings 2>&1 | tail -20`
Expected: no warnings.

- [ ] **Step 4: Update the module doc + user docs**

Rewrite the `read_json.rs` top module doc to describe `format` and `compression` and the two paths. Then update the read_json reference in the book (grep `grep -rl "read_json" docs/site/book/src` to find the page) with a table of the new args and examples:

```sql
-- gzip NDJSON (streaming)
SELECT * FROM read_json('s3://bucket/events.jsonl.gz');

-- full JSON array
SELECT * FROM read_json('/data/events.json', format => 'array');

-- zip archive (all JSON entries concatenated)
SELECT * FROM read_json('/data/export.json.zip');
```

Follow `voice.md`: no emdash/endash/unicode arrows in prose. Record the auto-detect decision from Task 6 (either "arrays auto-detected via streaming fallback" or "arrays require `format => 'array'`").

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-catalog/src/read_json.rs docs/site/book/src
git commit -m "test+docs(read_json): gzip/zip e2e tests, clippy, reference docs"
```

---

## Self-Review

**Spec coverage:**
- gzip streaming -> Task 3 (`streaming_compression` + `build_json_listing_table`). Covered.
- zip -> Tasks 5-6. Covered.
- full array -> Tasks 4, 6. Covered.
- DuckDB arg names -> Tasks 1, 3. Covered.
- framing-first routing -> Task 6. Covered.
- security gate before fetch -> Task 6 (relies on existing `enforce_tvf_path_policy` in `call()`, asserted in Step 6). Covered.
- zip multi-entry concatenation -> Task 5. Covered.
- max_buffer_bytes guard -> Task 6. Covered.
- struct columns / no flatten -> inherent in `decode_ndjson_to_memtable` (arrow-json infers structs). Covered.
- all-local tests -> every task. Covered.

**Auto-detect decision (locked, Task 6 Step 3):** a top-level array is served only via explicit `format => 'array'` or a `.zip` archive (whose entries auto-detect per-entry). `format=auto` on a plain `.json` resolves to NDJSON. No arrow-error string-matching. Recorded in Task 7 docs.

**Type consistency:** `JsonFraming`/`JsonCompression` defined in Task 1, used consistently in Tasks 2-6. `build_buffer_table` / `build_json_listing_table` / `decode_ndjson_to_memtable` / `reshape_array_to_ndjson` / `decompress` / `decompress_zip_to_ndjson` signatures are stable across the tasks that consume them.

**API drift note:** `ListingTableUrl::prefix()`, `object_store` `head().size` type, and `GetResult::bytes()` are pinned to arrow/object_store versions in the lock; the TDD loop (test fails -> fix) catches any minor signature drift at compile time.
