### Added

- `make latest`: a multi-arch (linux/amd64 + linux/arm64) `:latest` image

    `scripts/push-latest.sh` cross-compiles `sqe-server` and `sqe-cli` for both architectures with cargo-zigbuild (glibc 2.28 floor, no QEMU), packages each with `Dockerfile.ci`, and pushes one manifest list as `:latest` and `:<short-sha>` to `chameleon/sqlengine/sqe` and `chameleon/sqe`. It builds HEAD of `main` only (`ALLOW_BRANCH=1` overrides), refuses a modified tree, and fails unless every tag lists both platforms. `DRY_RUN=1` builds without pushing. Host paths are remapped out of the binaries, the image is stamped with the commit date and carries no attestations, so the same commit gives the same digest.

### Changed

- Release tags no longer write `:latest`

    `sqe-publish-latest` copied the amd64-only release image over `:latest`, which would replace a multi-arch `:latest` on every release. It now mirrors `:vX.Y.Z` and `:stable` only.

- `make push` no longer tags `:latest`

    It pushed `chameleon/sqe:latest` from any branch and feature set. It now pushes only `:<short-sha>`; `:latest` has one writer, `make latest`.
