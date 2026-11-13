# feat/make-latest-multiarch

`make latest` (`scripts/push-latest.sh`) builds a multi-arch `:latest` from a workstation: cargo-zigbuild for `x86_64-unknown-linux-gnu.2.28` and `aarch64-unknown-linux-gnu.2.28`, `Dockerfile.ci` per platform pushed by digest, `buildx imagetools create` into `chameleon/sqlengine/sqe` and `chameleon/sqe`, then a check that each tag lists both platforms. CI's `sqe-publish-latest` stops writing `:latest`, so a release no longer overwrites it with the amd64 image.

Basis: the aarch64 cross-build spike (2026-11-13). Both binaries cross-compiled on the first try; release profile 20 min cold on a Mac; max glibc symbol 2.28; `--version` runs in an arm64 container. `aws-lc-sys` (via `jsonwebtoken` in `sqe-auth`) builds without cmake. The x86_64 cross-build clears `.cargo/config.toml`'s clang + mold rustflags for that run.

Three dry runs shaped the script. The first cross-built binaries held the build machine's home path in about 1,950 strings each; `--remap-path-prefix` (and `-ffile-prefix-map` for the C build scripts) removes them, and the script fails if one is left. With the commit date as build date, `rewrite-timestamp` and no provenance or SBOM attestation, two runs at one commit gave identical binaries and identical image digests. Warm, a run takes about 9 minutes, nearly all of it `sqe-server` relinking twice because of a build-script bug in git worktrees (fixed on `refactor/docker-build`); cold it is about 27 minutes for both architectures. `make push` stops tagging `:latest`, which leaves `make latest` as its only writer.

Before this change Harbor's `:latest` (both paths) was the v0.54.0 amd64 image.

**NEXT:** ai-portal-data moves `SQE_IMAGE` to `:latest` with `SQE_IMAGE_PLATFORMS=linux/amd64,linux/arm64`. Then a CI multi-arch release build (cross-compile job, kaniko `--custom-platform` per arch, manifest list) and `--all` in aikido's `deploy_mirror.py` and here, so `:vX.Y.Z` is multi-arch too.
