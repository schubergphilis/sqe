### Added

- SQE Playground: DataFusion compiled to WebAssembly, running SQL in a browser tab with no server

    `playground/` reads parquet and CSV over HTTP or S3 and copy-on-write Iceberg tables from a `metadata.json` URL, with a hand-written Iceberg read path because iceberg-rust cannot build for wasm32. Results stop at 2,000 rows and the scan is cancelled there, so `SELECT *` without a `LIMIT` on a large remote file returns at once. The crate is outside the cargo workspace and no CI job builds it, though its lockfile is in the SBOM and the license gate (its license comes from `playground/Cargo.toml` via `AIKIDO_SBOM_LICENSE_MANIFESTS`); `playground/README.md` has the build, the tests and the CORS requirements a data host must meet.
