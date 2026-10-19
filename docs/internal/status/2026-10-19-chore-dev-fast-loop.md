**STATUS 2026-10-19: Opt-in fast dev loop, 36s instead of 1m18 for a `sqe-core` edit (`chore/dev-fast-loop`).**

Follow-up to `chore/build-speed` (MR !1482). That branch removed the two largest
avoidable costs (incremental compilation, feature-set churn); this one attacks
what is left of the rebuild itself.

Measured on an M3 Pro / 36 GB (2026-10-19, DataFusion 55.2), warm cache,
`CARGO_INCREMENTAL=0`, same feature set as `make dev`:

| variant | `sqe-core` edit | `sqe-coordinator` edit | cold build |
| --- | --- | --- | --- |
| stable, `jobs = 8` (reference) | 1m18 | - | 4m11 |
| nightly + `-Zthreads=8` | 1m07 | - | 3m50 |
| nightly + `-Zthreads=8` + cranelift | **36s** | **20s** | **2m41** |

`-Zthreads` alone is a 14% win, so it is not worth a separate toolchain on its
own. Cranelift is the real one: on a per-crate basis it compiles
`sqe-coordinator` in 12.5s versus 20.7s on LLVM and `sqe-planner` in 1.7s
versus 4.5s. Per-crate codegen backend selection is supported as an unstable
profile key, so `.cargo/fast.toml` turns cranelift on for `[profile.dev]` and
back off for the three crates it cannot compile:

- `sqe-core` (`#[ctor]` in `tls.rs`) and `opendal` (the same, via
  `auto-register-services`) fail on `asm!`/`global_asm!` sym operands.
- `ctor` itself compiles but emits references to Mach-O
  `section$start$`/`section$end$` symbols that cranelift never emits, so the
  final link of the binary fails with `undefined symbol:
  section$end$__DATA$_CTOR0_ISIZE_FN`. With those three on LLVM the whole
  `sqe-server` graph builds and the binary runs.

Limits: nightly-only, `target-cpu=native`, artifacts in `target/fast` (toggling
the script does not invalidate the stable cache), and the binaries must never
be shipped, released or benchmarked. `make dev` remains the reference build and
the one CI reproduces.

Also in this branch, from the same investigation: the `variant_json_args` test
moved out of `sqe-core` into `crates/sqe-trino-functions/tests/it/`, removing
the inverted dev-dependency edge `sqe-core -> sqe-trino-functions`. It was
dev-only, so no build-time change; it just stops the base crate from appearing
to depend on a crate above it.

Not pursued, with reasons recorded in the MR: the cranelift backend for
`sqe-core` itself (needs `#[ctor]` to go away, and that is a load-bearing
pre-main guarantee for the rustls provider), `RUSTC_BOOTSTRAP=1` on stable
instead of nightly (measured 2m13 warm / 4m48 cold - much worse, the 1.99
frontend is not the reworked one), `jobs = 4/6` (3m09 / 1m55, worse), and
`debug = 0` for first-party crates (no gain).

**NEXT:** unchanged. CI-side follow-ups still open: sccache in the CI image,
Dockerfile layer caching, and evaluating `cargo nextest` for the test loop.
