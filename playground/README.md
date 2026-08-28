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

**CORS.** The browser talks to your data host directly. Any bucket or web server
it reads must send CORS headers, or the request dies before the engine sees it.
`www/serve.py` sends them; your S3 bucket probably does not until you configure it.

**HTTP Range.** Parquet readers fetch the footer first, then column chunks. The
host must answer a range request with `206` and `Content-Range`. `python3 -m
http.server` answers `200` with the whole file, and `object_store` correctly
refuses it. `www/serve.py` handles ranges, including the `bytes=-N` suffix form
that parquet uses to find the footer.

**Single-threaded.** Browser wasm has no threads without `SharedArrayBuffer`,
which needs COOP/COEP headers and a rebuild with atomics. Expect single-core
query performance.

**AWS Glue and S3 Tables do not work here**, and cannot without a proxy. The AWS
SDK does not target browser wasm, and AWS API endpoints serve no CORS headers.
Both already work natively: `sqe-cli --embedded --catalog-backend glue|s3tables`.

## Layout

| Path | Purpose |
|---|---|
| `src/lib.rs` | `#[wasm_bindgen]` surface: `start`, `register`, `run_sql`, `explain`, `configure_s3` |
| `www/index.html` | The whole UI. Hand-written SQL tokenizer, no framework, no CDN |
| `www/serve.py` | Static server with Range + CORS |
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
