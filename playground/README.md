# SQE Playground

DataFusion compiled to WebAssembly, running SQL in a browser tab with no server
behind it. Register a parquet or CSV file by URL, write SQL, get a table.

```bash
rustup target add wasm32-unknown-unknown
cargo install wasm-bindgen-cli --version 0.2.127

./build.sh
python3 www/serve.py          # http://127.0.0.1:8799
```

## What this is not

It is not the SQE engine. There is no Iceberg, no catalog, no policy
enforcement, no distributed execution. What runs here is DataFusion 55 reading
parquet and CSV over `object_store`, which is a slice of the engine's read path
and nothing more.

The reason is specific. `iceberg-rust` does FileIO through opendal, and
opendal's `Access` trait uses boxed futures declared without a `Send` bound.
On `wasm32-unknown-unknown` reqwest's futures are `!Send`, so the crate fails to
compile with 39 variants of "future cannot be sent between threads safely". No
feature flag fixes it; it needs an object_store FileIO backend upstream, or a
metadata reader written against the Iceberg spec directly.

`object_store` escapes the same trap only because wasm-bindgen carries
`unsafe impl Send for JsValue` on non-atomics wasm, which satisfies its
`#[async_trait]` bounds. That difference is the whole reason this exists at all.

## Constraints worth knowing before you debug something

**CORS, and specifically ExposeHeaders.** The browser talks to your data host
directly, so any bucket or web server it reads must send CORS headers or the
request dies before the engine sees it. There is a second half that costs more
time than the first: the host must also *expose* `content-range` to the page.

Allowing the origin is not enough. A host can answer the range request
correctly with a `206` and a `Content-Range`, and the browser will still hide
that header from JavaScript unless it appears in
`Access-Control-Expose-Headers`. The parquet reader then cannot tell how much of
the file it received, and you get:

    Generic S3 error: Content-Range header not present in partial response

This was measured, not guessed. RustFS allows the origin and sends
`Content-Range`, but its expose list is `x-request-id, content-type,
content-length, etag`, so a browser read fails until something widens it. On AWS
S3 the fix is `ExposeHeaders` in the bucket CORS rule:

```json
[{
  "AllowedOrigins": ["http://127.0.0.1:8799"],
  "AllowedMethods": ["GET", "HEAD"],
  "AllowedHeaders": ["*"],
  "ExposeHeaders": ["content-range", "content-length", "etag", "last-modified", "accept-ranges"]
}]
```

`www/serve.py` gets this right, which is why HTTP sources work out of the box.

**HTTP Range.** Parquet readers fetch the footer first, then column chunks. The
host must answer a range request with `206` and `Content-Range`. `python3 -m
http.server` answers `200` with the whole file, and `object_store` correctly
refuses it. `www/serve.py` handles ranges, including the `bytes=-N` suffix form
that parquet uses to find the footer.

**WebDAV, for directory sources.** A URL ending in `/` makes DataFusion list
instead of stat, and `object_store` lists an HTTP source over WebDAV `PROPFIND`.
A server that answers `501` there looks retryable, so the source hangs for the
whole retry budget rather than failing. `www/serve.py` implements PROPFIND,
which is what makes the directory form work over plain HTTP. Note that
`object_store` wants `getlastmodified` on every response including the
collection itself, or it rejects the document outright.

**Retries are capped at 12 seconds here.** `object_store` defaults to retrying
for three minutes, which is right for a CLI and wrong for a UI: one mistyped URL
locks the page behind a spinner with nothing to read. `src/lib.rs` sets a
`RetryConfig` so a bad source fails in milliseconds and the error is the
feedback.

**Single-threaded.** Browser wasm has no threads without `SharedArrayBuffer`,
which needs COOP/COEP headers and a rebuild with atomics. Expect single-core
query performance.

**What has actually been tested.** HTTP and HTTPS sources: parquet and CSV,
file and directory form, verified in Chrome. S3: verified against RustFS through
a header-widening proxy, since RustFS's own expose list makes a direct browser
read impossible. The directory form passed 10 of 10 runs, the single-file form 9
of 10, with the tenth returning a truncated read. That was only ever observed
through the throwaway proxy, so it is not attributed to the engine, but
`register` now refuses a zero-column table outright rather than registering an
empty one that fails later as "No field named x".

**AWS Glue and S3 Tables do not work here**, and cannot without a proxy. The AWS
SDK does not target browser wasm, and AWS API endpoints serve no CORS headers.
Both already work natively: `sqe-cli --embedded --catalog-backend glue|s3tables`.

## Layout

| Path | Purpose |
|---|---|
| `src/lib.rs` | `#[wasm_bindgen]` surface: `start`, `register`, `run_sql`, `explain`, `configure_s3` |
| `www/index.html` | The whole UI. Hand-written SQL tokenizer, no framework, no CDN |
| `www/serve.py` | Static server with Range, CORS and WebDAV PROPFIND |
| `build.sh` | cargo build, wasm-bindgen, optional wasm-opt |

The crate is excluded from the cargo workspace on purpose. As a member it would
pull a wasm target and its own DataFusion feature set into every
`cargo build --all`, the same reasoning that keeps `sqe-cli`'s `aws` feature
default-off.

`build.sh` asks `cargo metadata` where the target directory is rather than
assuming `./target`, because the repo's root `.cargo/config.toml` pins a shared
one and that setting wins over this directory's.

## Gotchas that cost time

`chrono` lands in the dependency graph without its default `wasmbind` feature,
so `Utc::now()` falls through to `std::time::SystemTime::now()`, which panics at
runtime on wasm32 with "time not implemented on this platform". It compiles
fine. The crate names `chrono` directly with `features = ["wasmbind"]` purely to
turn that back on, since cargo features are additive.

`getrandom` appears at three major versions at once. Each needs its own
browser-entropy opt-in, and 0.3+ additionally refuse to pick a backend unless
`--cfg getrandom_backend="wasm_js"` is set, which `.cargo/config.toml` does.

The wasm-bindgen CLI version must match the `wasm-bindgen` crate version exactly
or the generated JS glue will not line up with the wasm exports.

## Accessibility

The palette is measured rather than eyeballed, and `--faint` in the first
version failed WCAG outright at 2.64:1 while carrying every micro-label, type
name, NULL and row number. Now every colour used for text clears AA (4.5:1)
against every surface it sits on, and seven of eight clear AAA.

The four syntax colours were picked by searching candidate sets for the best
worst-case separation under simulated deuteranopia, protanopia and tritanopia.
The old set collapsed to 36.5 between functions and numbers, which were both
blue-green and effectively identical without colour vision. The current set
holds 113.9 in the worst case across all three. Colour is never the only
channel either: keywords carry weight, comments are italic, numbers are
right-aligned, so the interface still parses with no colour at all.

## Size

25 MB raw, 6.7 MB gzip, roughly 5 MB brotli. `opt-level = "z"`, LTO, one codegen
unit, `panic = "abort"`, stripped. Installing binaryen (`brew install binaryen`)
lets `build.sh` run `wasm-opt -Oz` for another 15 to 25 percent. Boot to ready is
30 to 150 ms once the bundle is cached.

## Where this could go

Reading Iceberg without iceberg-rust is the obvious next step: `metadata.json`
is JSON, manifest lists and manifests are Avro, and `apache-avro` is pure Rust.
Snapshot to manifest list to manifests to data file paths, then hand the paths to
DataFusion. Read-only, no position deletes. That would make it Iceberg in a
browser tab.

After that, browsing an Iceberg REST catalog is plain HTTPS and JSON over fetch,
with no need for `iceberg-catalog-rest` (whose non-optional `gcp_auth` dependency
drags in hyper and mio and will not build for wasm either). Gated entirely on
whether the catalog sends CORS headers.
