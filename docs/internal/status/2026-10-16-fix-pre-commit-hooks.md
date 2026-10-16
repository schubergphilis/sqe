**STATUS 2026-10-16: pre-commit hooks cover Rust style, the supply chain, shell scripts and clippy (`fix/pre-commit-hooks`).**

Commits had been failing with `InvalidManifestError`: pre-commit's cache database still pointed at two hook checkouts that no longer existed on disk (a local cache problem, not the config). Removing those rows made pre-commit re-clone them. The installed hook also pointed at a removed Python 3.9, and `core.hooksPath` was set (to the default path), which made `pre-commit install` refuse; both are local fixes.

The config now adds:

- `rustfmt-ratchet` (`scripts/pre-commit-hook.sh rustfmt`): about 275 files on main carry rustfmt drift, so a plain `cargo fmt --check` would block every commit. The ratchet fails a staged file only for a rustfmt hunk its HEAD version lacks. It compares hunk bodies, not counts: an edit inside an already-drifted hunk changes that hunk and fails, which a count would miss (it did, on `iceberg_scan.rs`).
- Supply chain on dependency changes: `cargo metadata --locked`, `cargo hakari generate --diff`, `cargo deny check advisories bans sources`, `cargo audit`, and `cargo machete` for crate manifests. Each takes 5-30 s and needs no compile.
- shellcheck (pinned shellcheck-py) at warning severity. All 81 scripts pass after fixing 29 warnings (unchecked `cd`, dead variables, `local x=$(...)` masking). Under `set -e` a split capture would abort where the old one continued, so trino-compat keeps its behaviour with `|| true`. The AWS quickstarts now stop on a failed `aws sts get-caller-identity` rather than deploy with an empty account.
- clippy at pre-push, workspace plus `sqe-cli --no-default-features`, warnings denied. The ten lints the 1.99 toolchain added are fixed, so it starts green. About 2 min after an edit; `SKIP=clippy git push` skips it.
- `check-case-conflict`.

The whole tree now passes every hook, so `make pre-commit-all` (`pre-commit run --all-files` for both stages) is a usable gate. Of the ~1540 files the whitespace fixers wanted to rewrite, almost all were generated: 1482 benchmark reports (the writer emits no final newline), captured EXPLAIN plans under `docs/evidence/`, rendered ebook diagrams, test data, and the vendored mermaid bundle. Those paths are excluded from the fixers (a shared `&generated` anchor), so new benchmark reports never trip them. About 20 hand-written files (internal docs, five Rust files, a few specs and configs) were fixed. The five Rust files had trailing spaces inside rustfmt-drifted hunks, so they were rustfmt'd in full (about 200 lines, formatting only); `sqe-sql` (544) and `sqe-policy` grants (86) unit tests pass.

Verified: `pre-commit run --all-files` and `--hook-stage pre-push` pass and leave the tree unchanged; the ratchet flagged the two unformatted `clippy --fix` edits and the whitespace-only edits inside drifted hunks; puffin_carry, pruning_stats and ticket_signing unit tests pass.

**NEXT:** rustfmt drift (~270 files) is the one gate still ratcheted rather than enforced; formatting it crate by crate in small MRs would let the hook become a plain `cargo fmt --check`. Then #812 or #815.
