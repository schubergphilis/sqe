**STATUS 2026-11-13: image build audit; leaner `Dockerfile`, one worktree rebuild bug fixed (`refactor/docker-build`).**

An audit of the image build paths, measured on an Apple silicon workstation (native linux/arm64 builds).

Changed:

- `sqe-bench` moved from `builder` to a `bench-builder` stage. The server image no longer links it; the bench stage compiled exactly one crate (`sqe-bench`, 53 s) on the shared target cache.
- sccache removed from `Dockerfile`. On a warm rebuild after a `sqe-core` edit it had 0 hits of 15 and the build step took 495 s with it, 481 s without.
- `cmake` dropped from the builder: a cold `dev` build of all 603 crates passed without it. `libssl-dev`, `pkg-config` and `curl` dropped from the install line: the Rust base image already ships them.
- `CARGO_PROFILE=dev` copies from `target/debug`. It could never work before.
- `Dockerfile.bench` deleted (no callers, no caching, unpinned base). `.dockerignore` lists every directory no image build reads.

Looked at and left alone:

- `COPY --link`, zstd layers and a buildx registry cache: small or no gain now that CI and `make latest` compile outside the image build, and kaniko and scanner compatibility is unverified.
- One shared definition of the runtime stages for `Dockerfile` and `Dockerfile.ci`: the CI path goes through the shared template, so the two stay separate and reviewed together.
- Release profile settings. A warm rebuild after one `sqe-core` edit still takes 8 minutes in `release` (thin LTO, 15 crates). Local image builds that do not need release speed should pass `CARGO_PROFILE=dev-release`.

Checked beyond the three builds (`dev` runtime, `dev` bench-runtime, `release` runtime, all linux/arm64, all passed and the binaries run):

- Nothing in the repository passed sccache build arguments to `Dockerfile`; CI images are packaged with `Dockerfile.ci`, which compiles nothing.
- `sqe-catalog/full-backends` adds no native (`-sys`) dependency over the default feature set, so the package list holds for `make push`.
- Every Compose and quickstart build uses the root `Dockerfile`, which copies only `Cargo.toml`, `Cargo.lock`, `crates/`, `vendor/` and `xtask/`. None of those is ignored. The one `include_str!` that reaches into `benchmarks/` is test code.
- The native C dependencies (`aws-lc-sys`, `zstd-sys`, `liblzma-sys`) compile on linux/amd64 in the builder base image without `cmake`.

Not run: a full linux/amd64 image build, a full-backends image build, and a kaniko build of `Dockerfile`.

**NEXT:** the release tag pipeline still compiles inside kaniko on amd64, 38 to 67 minutes with no cache mounts. Move it to the cross-compiled binaries plus `Dockerfile.ci` per architecture (the same flow as `make latest`), which also makes `:vX.Y.Z` multi-arch. `make push` still compiles amd64 under QEMU; route it through `scripts/push-latest.sh` once `sqe-catalog/full-backends` is proven to cross-compile.
