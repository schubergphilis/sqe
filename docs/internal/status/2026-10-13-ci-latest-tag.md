**STATUS 2026-10-13: Harbor `:latest` follows the newest release (`ci/latest-tag`).**

`:latest` was never set by CI: `repo.sovereign-data.org/chameleon/sqlengine/sqe:latest` was a 2026-09-22 image, and `chameleon/sqe` (the `make push` repo) did not exist. New job `sqe-publish-latest` runs on `vX.Y.Z` tags after `aikido-publish-release-image` and `skopeo copy`s the release image to `chameleon/sqlengine/sqe:latest` and `chameleon/sqe:{vX.Y.Z,stable,latest}`. Fail-closed, prints `mirrored ->` per tag. A second `.asset` registry could not do this, because `deploy_mirror.py` applies one `AIKIDO_IMAGE_NAME` to every registry.

Done by hand for v0.51.0 the same day (`docker buildx imagetools create`), so both repos already serve v0.51.0 as `:latest`.

amd64 only, like `:stable`: all CI runners are amd64 and kaniko builds one arch. arm64 still needs a local `make push`. In CI it needs an aarch64 cross-compile of the release binaries, a second kaniko `--custom-platform linux/arm64` build (Dockerfile.ci is COPY-only, so no emulation), a manifest list, and signing/scanning on the index: an aikido template change.

Not verifiable before merge: release-tag jobs don't run on MR pipelines. `glab ci lint` passes; check the `mirrored ->` lines on the next release.

**NEXT:** decide on CI arm64; dependency/CVE batch (#800, !1133, !1107, !1389).
