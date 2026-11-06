### Fixed

- GitHub release binaries build again: the workflow no longer asks for the removed `sqe-worker` binary

    `.github/workflows/release-binaries.yml` still built and packaged `--bin sqe-worker`, which was
    deleted on 2026-09-18 (a worker is `sqe-server --mode worker`). Cargo stopped with "no bin target
    named `sqe-worker`" on all three targets, so v0.50.0 through v0.53.1 got no GitHub Release. The
    archives now hold `sqe-server`, `sqe-cli` and `sqe-bench`.
