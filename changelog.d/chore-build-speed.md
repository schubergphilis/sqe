### Changed

- Debug builds are ~2x faster: incremental compilation is off and there is one canonical dev command

    `incremental = false` in `[profile.dev]` and `[profile.test]`. An edit in `sqe-core` recompiles ~15 crates from scratch, so the incremental cache never amortises its own bookkeeping: measured on an M3 Pro (2026-10-19, DataFusion 55.2), `sqe-core` -> `sqe-server` is 1m21 instead of 2m50, and a leaf edit in `sqe-server` is 23s instead of 32s. It also removes the 21 GB `target/debug/incremental` tree. `CARGO_INCREMENTAL=1` still turns it back on per invocation.

    Adds `cargo dev` (identical to `make dev`), `cargo dev-server`, `cargo check-server` and `make check-server`. Cargo keys artifacts on the exact feature set, so alternating between `cargo build`, `make dev` and `cargo clippy --all-features` kept a separate copy of the whole dependency graph each (86 fingerprints for `sqe-core` alone, 128 GB of `target/` on one machine). `CLAUDE.md` now documents the single dev loop.
