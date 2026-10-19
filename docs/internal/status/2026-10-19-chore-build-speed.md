**STATUS 2026-10-19: Debug builds ~2x faster, one canonical dev loop (`chore/build-speed`).**

Local builds were taking minutes per iteration and `target/` had grown to 128 GB on a 97%-full disk. Measured on an M3 Pro / 36 GB (2026-10-19, DataFusion 55.2), no-op build 3.7s:

| action | before | after |
| --- | --- | --- |
| edit `sqe-core` -> `sqe-server` | 2m50 | 1m19 |
| edit `sqe-server` only | 31.8s | 22.8s |
| `cargo check --bin sqe-server` after a `sqe-core` edit | 1m27 | 1m27 |

The rebuild cost is structural: `sqe-core` is imported by ~15 crates, so an edit near the bottom recompiles the chain from scratch and incremental compilation only adds bookkeeping (it was slower even for a leaf edit). `incremental = false` now applies to `[profile.dev]`/`[profile.test]`; `CARGO_INCREMENTAL=1` restores it per invocation.

The other half was feature-set churn. Cargo keys artifacts on the exact feature set, so `cargo build` (default features), `make dev` (`--no-default-features`, two binaries) and `cargo clippy --all-features` each kept a separate copy of the whole graph: 86 distinct fingerprints for `sqe-core`, 7498 fingerprint dirs, 6830 dep-info files, 128 GB. New aliases `cargo dev` (byte-identical to `make dev`), `cargo dev-server`, `cargo check-server`, plus `make check-server`, and a `CLAUDE.md` "Build loop" section that says not to mix them in one tree.

Not changed: sccache stays off locally. It misses precisely on the workspace crates that actually change (3% hit rate measured 2026-06-12) and doubles the write surface on Defender-scanned Macs. It only pays off in CI.

Also observed while measuring, not fixed by this branch: a dependency refresh landing in a working tree another session is building in invalidates every fingerprint (three full rebuilds in ten minutes), and concurrent cargo runs serialise on the build lock for ~100s. `CLAUDE.md` now notes one builder per tree.

**NEXT:** unchanged. Optional follow-ups: drop `incremental = true` from `[profile.dev-release]` for consistency, and evaluate sccache in CI using the existing Dockerfile.ci.
