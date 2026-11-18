**STATUS 2026-11-18: MR pipelines green again (`fix/ci-deps-pin-and-advisories`).**

Four jobs failed on every MR pipeline since !1843, and `deps-check` failed on main:

| Job | Cause | Fix |
|---|---|---|
| `aikido-test-rust`, `aikido-rust-extra`, `catalog-live-tests` | the pinned `ci-deps:main@sha256:db44c576...` digest was gone from the registry | pin `sha256:1496ad27...` (`ci-deps:lock-a3137de30f85`), built by `ci-deps-image` on main at 552cccbe |
| `ci-config-sanity` | `check-advisory-ignores.py` wants `# review-by YYYY-MM-DD owner=<name>` right after the advisory id; the three entries added for RUSTSEC-2026-0542/0543/0555 put the crate name first | crate name moved to the end of the comment |
| `deps-check` (main) | `sqe-cli` still declared `sqe-policy` after its last use went away | dependency and its `Cargo.lock` line removed |

With the image back, `aikido-rust-extra` reached `xtask budgets --check`, which main already failed: the Trino parity MRs merged with the budgets hook skipped. `.config/budgets.json` drops the removed `sqe-cli -> sqe-policy` edge and raises four ceilings by hand: `sqe-sql/src/trino_compat.rs` gets its own 3500 (3119 lines), `write_handler.rs` 16000 to 16500 (16029), `sqe-catalog` 64600 to 67000 (65348), `sqe-trino-functions` 21600 to 24000 (22496, the new Trino functions). Splitting `trino_compat.rs` is the follow-up rather than more headroom.

`Cargo.lock`, `rust-toolchain.toml` and `vendor/` are identical between 552cccbe and the branch base, so the pinned image matches. Removing the `sqe-cli` edge changes `Cargo.lock`, which triggers `ci-deps-image` on main after merge; the pin still works (the image just bakes one extra edge) but should be refreshed from that job's output.

Checked locally: `check-advisory-ignores.py` ok, `make leak-scan` 0, `cargo machete crates/sqe-cli` clean, `cargo metadata --locked --offline` 0, `cargo xtask budgets --check` 0.

**NEXT:** after merge, take the digest that `ci-deps-image` on main prints and refresh the pin in `.gitlab-ci.yml`.
