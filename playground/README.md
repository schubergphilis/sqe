# SQE Playground

DataFusion compiled to WebAssembly, running SQL in a browser tab with no server
behind it. Register a parquet or CSV file by URL, write SQL, get a table.

```bash
rustup target add wasm32-unknown-unknown
cargo install wasm-bindgen-cli --version 0.2.127

./build.sh
python3 www/serve.py          # http://127.0.0.1:8799
```

## Reading Iceberg without iceberg-rust

Point it at a table's `metadata.json` and it reads the table:

    /iceberg/demo/events/metadata/00002-....metadata.json

`iceberg-rust` cannot compile for `wasm32-unknown-unknown`. Its FileIO goes
through opendal, whose `Access` trait uses boxed futures declared without a
`Send` bound, and reqwest's futures are `!Send` on browser wasm: 39 variants of
"future cannot be sent between threads safely", and no feature flag fixes it.
(`object_store` escapes the identical trap only because wasm-bindgen carries
`unsafe impl Send for JsValue` on non-atomics wasm.)

So `src/iceberg.rs` implements the read path against the spec instead, which is
smaller than it sounds because Iceberg's on-disk formats are ordinary: JSON and
Avro, both reachable over fetch.

    metadata.json  ->  current-snapshot-id  ->  manifest list (Avro)
                   ->  manifests (Avro)     ->  parquet data files

Three things it gets right, each of which is silently wrong if you skip it:

- **The snapshot governs.** Every path is driven from `current-snapshot-id`.
  Registering the data *directory* would appear to work on any test table and be
  wrong in general, because the directory holds files from every snapshot
  including replaced ones. Point the reader at an older `metadata.json` and you
  get that version of the table, which is time travel for free.
- **Paths are rebased.** Iceberg records absolute locations, and they are almost
  never the URL you fetched from. The bundled fixture records `file:///...`
  because SQE wrote it locally, while the browser reads it over HTTP; an S3
  table records `s3://`. Each path is re-rooted by stripping the table's own
  `location` prefix.
- **Delete files are refused, not ignored.** A merge-on-read table with position
  or equality deletes returns an error. Skipping them silently would return rows
  the table says are deleted, which looks exactly like success.

Verified against a table written by `sqe-cli` itself: two appends, two
snapshots, two data files. `count(*)` returns 6 and the aggregate matches what
the CLI prints. The v1 metadata returns 3 rows from one file; the v0 metadata
errors as empty.

Still not the SQE engine: read-only, copy-on-write only, no catalog
integration yet, no policy layer, no distributed execution.

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
content-length, etag`, so a browser read fails until something widens it. MinIO gets it right by default. On AWS S3 you set `ExposeHeaders` in the bucket
CORS rule:

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

## Public data, and which hosts actually work

Being public is not the requirement. Exposing `content-range` is. Measured
2026-08-28, all through a real browser:

| Host | Origin allowed | content-range exposed | Works |
|---|---|---|---|
| HuggingFace CDN | `*` | `*` | **yes** |
| `shell.duckdb.org` | `*` | nothing | no |
| `raw.githubusercontent.com` | `*` | nothing | no |
| NYC TLC CloudFront | none | nothing | no |
| MinIO (default) | configurable | `Content-Range, ... , *` | **yes** |
| RustFS | yes | `x-request-id, content-type, content-length, etag` | no |

The three failures are instructive. Two of them serve a perfectly good `206`
with a `Content-Range` header, and curl sees it; the browser hides it from the
page because it is not on the CORS safelist and was not exposed, and the parquet
reader cannot tell how much of the file it received.

So the built-in "Public data" list points at HuggingFace, which hosts a large
amount of open parquet and gets the headers right. The 2 GB fineweb shard is the
one worth clicking: a filtered `LIMIT` returns in about 670 ms because only the
footer and a couple of row groups cross the network. An aggregate over the
whole file, or a filter that matches almost nothing, has to pull all 2 GB
through one wasm thread, which takes minutes. Same engine, same file; the
difference is whether parquet gets to skip.

**Results stop at 2,000 rows.** `run_sql` streams the plan and drops the
stream once one row past the cap arrives, which cancels the scan. So
`SELECT *` without a `LIMIT` behaves as if it had one: a 100-million-row
`generate_series` returns its first 2,000 rows in 3 ms. The status line then
reads `first 2000`, because a scan that stopped early cannot know its total.

**What has actually been tested.** HTTP and HTTPS: parquet and CSV, file and
directory form, local and remote, verified in Chrome. Remote parquet at 0.2 MB,
2 MB, 20 MB and 2 GB. S3: verified against MinIO with no proxy in the path, 30
of 30 runs clean across both the file and directory forms.

An earlier round measured 9 of 10 on the S3 single-file form, but that ran
through a throwaway CORS-widening proxy needed to work around RustFS. Against
MinIO directly the flake does not reproduce, so it was the proxy. `register`
refuses a zero-column table regardless, rather than registering an empty one
that fails later as "No field named x".

**AWS Glue and S3 Tables do not work here yet.** The AWS SDK does not target
browser wasm, so reaching them needs a hand-written SigV4 client; the endpoints
themselves do answer CORS (see "Where this could go"). Both already work
natively: `sqe-cli --embedded --catalog-backend glue|s3tables`.

## Layout

| Path | Purpose |
|---|---|
| `src/lib.rs` | `#[wasm_bindgen]` surface: `start`, `register`, `register_iceberg`, `run_sql`, `explain`, `configure_s3` |
| `src/iceberg.rs` | Iceberg read path: metadata.json, manifest list and manifests |
| `www/iceberg/` | A real Iceberg table written by `sqe-cli`, used as the fixture |
| `www/index.html` | The whole UI. Hand-written SQL tokenizer, no framework, no CDN |
| `www/serve.py` | Static server with Range, CORS and WebDAV PROPFIND |
| `build.sh` | cargo build, wasm-bindgen, optional wasm-opt |

The crate is excluded from the cargo workspace on purpose. As a member it would
pull a wasm target and its own DataFusion feature set into every
`cargo build --all`, the same reasoning that keeps `sqe-cli`'s `aws` feature
default-off. Being outside the workspace, it does not inherit the root
`[patch.crates-io]`, so `Cargo.toml` repeats the `object_store` patch that keeps
quick-xml off the RUSTSEC-2026-0194/0195 versions. No CI job builds this crate:
build it and run the tests below before merging a change here.

## Tests

The Iceberg reader has host-side tests, including an end-to-end read of the
bundled fixture (6 rows from 2 files at the current snapshot, 3 from 1 at the
previous one). `.cargo/config.toml` defaults the build to wasm32, where tests
cannot run, so name the host target:

```bash
cargo test --target "$(rustc -vV | sed -n 's/^host: //p')"
```

The wasm surface (`run_sql`, `register`, the UI) is checked in a browser:
`./build.sh`, `python3 www/serve.py`, then register the fixture's newest
`metadata.json` and expect `count(*)` = 6.

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

25 MB raw, 6.8 MB gzip, roughly 5 MB brotli. `opt-level = "z"`, LTO, one codegen
unit, `panic = "abort"`, stripped. Installing binaryen (`brew install binaryen`)
lets `build.sh` run `wasm-opt -Oz` for another 15 to 25 percent. Boot to ready is
30 to 150 ms once the bundle is cached.

## Where this could go

An Iceberg REST catalog client, so you point at a catalog rather than at a
`metadata.json`. It is plain HTTPS and JSON over fetch and needs none of
`iceberg-catalog-rest` (whose non-optional `gcp_auth` drags in hyper and mio and
will not build for wasm). `loadTable` returns the table metadata inline, so it
feeds straight into the reader that already works.

Measured 2026-08-28: **AWS's service endpoints support CORS**.
`s3tables.us-east-1.amazonaws.com` and `glue.us-east-1.amazonaws.com` both answer
the preflight with `Access-Control-Allow-Origin: *`, reflect back the signed
headers (`authorization`, `x-amz-date`, `x-amz-content-sha256`,
`x-amz-security-token`) and cache it for 48 hours. So no proxy is needed to
reach the catalog; SigV4 is HMAC-SHA256 and does not need the AWS SDK.

What is still unverified for S3 Tables is the data files: they live in an
AWS-managed table bucket, which must also expose `content-range`. That needs an
account to settle. Snowflake Open Catalog is managed Polaris, so it is the same
REST client with a bearer token instead of SigV4, and testable with no AWS at
all.

Beyond that: merge-on-read (applying position deletes), and partition pruning
from the manifest statistics that are already being parsed and currently
discarded.
