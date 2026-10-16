**STATUS 2026-10-16: pre-commit hooks cover Rust style, the supply chain, shell scripts and clippy (`fix/pre-commit-hooks`).**

Commits had been failing with `InvalidManifestError`: pre-commit's cache database still pointed at two hook checkouts that no longer existed on disk (a local cache problem, not the config). Removing those rows made pre-commit re-clone them. The installed hook also pointed at a removed Python 3.9, and `core.hooksPath` was set (to the default path), which made `pre-commit install` refuse; both are local fixes.

The config now adds:

- `rustfmt-ratchet` (`scripts/pre-commit-hook.sh rustfmt`): about 275 files on main carry rustfmt drift, so a plain `cargo fmt --check` would block every commit. The ratchet fails a staged file only for a rustfmt hunk its HEAD version lacks. It compares hunk bodies, not counts: an edit inside an already-drifted hunk changes that hunk and fails, which a count would miss (it did, on `iceberg_scan.rs`).
- Supply chain on dependency changes: `cargo metadata --locked`, `cargo hakari generate --diff`, `cargo deny check advisories bans sources`, `cargo audit`, and `cargo machete` for crate manifests. Each takes 5-30 s and needs no compile.
- shellcheck (pinned shellcheck-py) at error severity: 0 scripts have errors, 27 have warnings.
- clippy at pre-push, workspace plus `sqe-cli --no-default-features`, warnings denied. The ten lints the 1.99 toolchain added are fixed, so it starts green. About 2 min after an edit; `SKIP=clippy git push` skips it.
- `check-case-conflict`.

`pre-commit run --all-files` is documented as off limits: the existing whitespace fixers would rewrite ~1500 files that predate them. `make pre-commit-repo` runs the repo-wide hooks instead (about 45 s warm, all green).

Verified: `make pre-commit-repo` passes; `pre-commit run` on the staged change passes; the ratchet flagged the two unformatted `clippy --fix` edits before they were fixed; puffin_carry, pruning_stats and ticket_signing unit tests pass.

**NEXT:** shellcheck warnings (27 scripts) and whitespace drift could be cleaned file by file to tighten the gates; #812 or #815.
