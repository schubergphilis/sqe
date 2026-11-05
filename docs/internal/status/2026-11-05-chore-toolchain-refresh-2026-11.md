**STATUS 2026-11-05: toolchain refresh before v0.53.0 (`chore/toolchain-refresh-2026-11`).**

Rust 1.99.1 (pinned in `rust-toolchain.toml`) is the newest stable; the `Dockerfile` and
`Dockerfile.bench` builder images were still on 1.99.0 and now match it. `make lock-maintenance`
refreshed 65 crates within semver bounds; hakari reported no workspace-hack change. The vendored
`datafusion-physical-plan` TopK used `AtomicUsize::fetch_update`, deprecated in 1.99, and was the only
warning in `make release`; it now calls `try_update`.

Held back: arrow/parquet 60 and sqlparser 0.63 (DataFusion 55.2.0 is the newest release and is built
on arrow 59 / sqlparser 0.62), reqwest 0.13, jsonwebtoken 11, rand 0.10. iceberg-rust 0.11.0 was
released on 2026-11-03; taking it means re-vendoring and is separate work.

Gate: `make bench-smoke` (release build + engine-bench 10M, every run and answer checked). The
150M engine-bench on main before this change: all answers match across all six engines; SQE
q3 window 4.72 -> 3.94 s and q4 high-cardinality aggregate 4.11 -> 3.34 s against the 2026-11-02
capture; Polars still exceeds the 4.5 GiB watchdog on q3/q4, Spark failed one q3 rep on memory.

**NEXT:** cut v0.53.0 once this merges.
