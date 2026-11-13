### Changed

- Image build: `sqe-bench` links only for the `bench-runtime` target

    The `builder` stage of `Dockerfile` compiled and linked `sqe-bench` for every server image. A `bench-builder` stage now does that, on the same target cache, so `docker build .` and the quickstart Compose builds skip it. `.dockerignore` lists the directories no image build reads (`benchmarks/`, `quickstart/`, `deploy/`, `playground/`, `adapters/`, `dist/`, `sbom/`, `testing/`, `changelog.d/`).

- Image build: no sccache, no cmake

    `Dockerfile` no longer downloads and wraps rustc with sccache. Beside the `target/` cache mount it had nothing to add: a rebuild after a `sqe-core` edit recompiled 15 workspace crates with 0 cache hits, and the build step took 495 s with it and 481 s without. The builder stage no longer installs `cmake`, which no crate needs (a cold build of all 603 crates passes without it), nor `libssl-dev`, `pkg-config` and `curl`, which the Rust base image already ships.

### Removed

- `Dockerfile.bench`

    Nothing built it. The bench image is `docker build --target bench-runtime .`; the separate file had no dependency caching and an unpinned builder base.

### Fixed

- `docker build --build-arg CARGO_PROFILE=dev` failed after compiling

    The builder copied the binaries from `target/dev`, but cargo writes the `dev` profile to `target/debug`.
