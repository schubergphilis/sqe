**STATUS 2026-11-03: engine-bench reads Polaris from DuckDB, Polars and embedded SQE (`feat/engine-bench-polaris-readers`).**

`POLARIS_READERS=1 ./run.sh` adds `sqe-embedded-polaris` (`sqe-cli --embedded`
with `ATTACH ... TYPE iceberg_rest`), `duckdb-polaris` (DuckDB `iceberg`
extension) and `polars-polaris` (pyiceberg + `pl.scan_iceberg` planned by
`polars-iceberg`) to the existing `sqe-polaris` (coordinator over Flight SQL).
All four read the same `polaris.bench_<size>` snapshot on RustFS, and their
answers are checked against DuckDB's local answer like every other engine.

Not measured yet: no `POLARIS_READERS=1` run has been recorded, so there is no `OUTPUT-polaris.md`. The branch was rebased onto main on 2026-11-13 without a rerun.

Gotchas found on the way: Polars' native reader ignores the pyiceberg FileIO
`s3.*` props and needs `storage_options` (else it goes to AWS); pyiceberg asks
for vended credentials by default, which Polaris without STS cannot give
(`header.X-Iceberg-Access-Delegation: ""`); embedded SQE's ATTACH ignored
`SQE_STORAGE__S3_*` (fixed separately in `fix/embedded-attach-storage-fallback`,
!1628), so `polaris_attach()` still spells the S3 options out.

**NEXT:** run `POLARIS_READERS=1 ./run.sh --check` once and commit
`OUTPUT-polaris.md`; then drop the inline `S3_*` options from
`polaris_attach()` (!1628 has merged).
