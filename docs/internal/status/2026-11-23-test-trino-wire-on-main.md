**STATUS 2026-11-23: main runs the Trino wire goldens (`test/trino-wire-on-main`).**

Owner decision 23 Nov: main must run the `sqe-trino-compat` wire test. `deda8f7d5` broke `tests/wire.rs` on main and main stayed green, because main pipelines run no test jobs (see the `.gitlab-ci.yml` header); the fix `703daebd4` is on main.

New job `sqe-trino-compat-wire` in `.gitlab-ci.yml`: it extends `aikido-rust-extra` (deps image, warm target dir, build env) and runs only `cargo test -p sqe-trino-compat --test wire`. Rules: never on schedules; on a main push when Rust sources, manifests, `Cargo.lock` or the toolchain change; on an MR when `.gitlab-ci.yml` changes (proves the job). The rest of main stays test-free.

Gates: `glab ci lint` valid; `cargo test -p sqe-trino-compat --test wire` on main 400bdce2d exit 0 (15/15); `scripts/changelog-collect.py --check`.

**NEXT:** confirm the job's first main run after merge is green and note its duration.
