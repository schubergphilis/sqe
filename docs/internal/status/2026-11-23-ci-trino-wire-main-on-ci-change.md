**STATUS 2026-11-23: the Trino wire job also runs on main after a CI-only merge (`ci/trino-wire-main-on-ci-change`).**

The first main pipeline after !1947 (1730596, `d3ca277a1`) had no `sqe-trino-compat-wire` job. On main the job ran only when Rust sources, manifests, `Cargo.lock` or the toolchain changed, and that merge changed only `.gitlab-ci.yml` and docs. A second main-push rule arm now also starts the job when `.gitlab-ci.yml` changes, so a CI change proves itself on main. Schedules still skip it.

Gates: `glab ci lint`; `scripts/changelog-collect.py --check`; `make leaks`.

**NEXT:** after the merge, confirm the job appears in the main pipeline and passes.
