### Changed

- `make dev-fast`: opt-in rebuild loop that is ~2x faster than `make dev`

    `scripts/dev-fast.sh` (also `make dev-fast`) builds `sqe-server` on the
    nightly parallel frontend (`-Zthreads=8`) and, for the workspace crates,
    the cranelift codegen backend. Measured on an M3 Pro / 36 GB (2026-10-19,
    DataFusion 55.2), warm cache, `CARGO_INCREMENTAL=0`: a `sqe-core` edit
    takes 31-36s instead of 1m18s, a `sqe-coordinator` edit 20s, and a cold build
    2m41s instead of 4m11s. `sqe-core`, `opendal` and `ctor` stay on LLVM
    because they use `#[ctor]`, which cranelift cannot compile (`asm!` sym
    operands, plus the `section$start$`/`section$end$` Mach-O symbols it does
    not emit, which break the final link). Artifacts go to `target/fast`, so
    the stable cache in `target/debug` is untouched, and the profile overrides
    live in `.cargo/fast.toml`, which only this script loads: stable builds,
    CI and the Docker build never see the unstable `codegen-backend` key. The
    binaries are a compile-time optimisation for the inner loop only - never
    ship, release or benchmark them; `make dev` stays the reference build.

### Fixed

- `sqe-core` no longer dev-depends on `sqe-trino-functions`

    The `variant_json_args` test moved to
    `crates/sqe-trino-functions/tests/it/variant_json_args_test.rs`. The
    dependency pointed from the base crate back up at a crate that depends on
    it, so every test build of `sqe-core` also compiled `sqe-trino-functions`
    and `datafusion-functions-json`. Only the dev edge existed, so this does
    not change build times; it removes a cycle from the graph that made
    `cargo tree` and dependency tooling lie about layering.
