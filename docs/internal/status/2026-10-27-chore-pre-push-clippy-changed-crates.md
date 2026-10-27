**STATUS 2026-10-27: pre-push clippy scoped to changed crates (`chore/pre-push-clippy-changed-crates`).**

The pre-push hook ran `cargo clippy --workspace --all-targets` on every push (~3 min, holding the GitLab ssh connection open). MR pipelines run the `minimal` CI profile, which skips clippy, so the hook is the only lint before merge; it stays on, but narrower.

- `scripts/pre-commit-hook.sh clippy` lints the crates under `crates/` and `xtask/` changed between `git merge-base HEAD origin/main` and `HEAD` (`CLIPPY_BASE` overrides). `sqe-cli` keeps `--no-default-features`. A change to `Cargo.toml`, `Cargo.lock`, `.cargo/`, `clippy.toml` or `rust-toolchain.toml`, or no merge base, lints the whole workspace. No crate changed: no lint.
- `clippy-all` subcommand = the old behaviour; `make lint` runs it, `make pre-commit-all` calls `make lint` instead of the pre-push stage.
- The hook triggers on `.rs`, `Cargo.toml`, `Cargo.lock`, `clippy.toml`, `rust-toolchain.toml` (was `types: [rust]`, so a manifest-only push skipped clippy).
- Dependents of a changed crate are not linted. The MR pipeline compiles the whole workspace, so an API break still fails before merge; a lint that only fires in a dependent shows up on main's full pipeline or with `ci::full`. The workspace-hack keeps feature sets unified, so `-p` runs reuse the workspace build.

Measured: `CLIPPY_BASE=3f74359a` (sqe-catalog, sqe-coordinator, sqe-server, sqe-worker) 1m58 after a branch switch, 18 s warm. On a branch with no crate change: returns at once. shellcheck clean.

Also this session: the shared object store had 70,649 loose objects (2.07 GiB) and no gc/maintenance configured, which made `git rev-list` in the pre-push hook take ~4 min and `git fetch` > 2 min; `git repack -d && git prune-packed` run on the main checkout.

**NEXT:** none for the hook; consider `git maintenance register` on each clone.
