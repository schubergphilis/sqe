**STATUS 2026-11-16: multi-arch release image in CI (`feat/multiarch-release-image-898`, #898 ask 1).**

A release tag pipeline now builds linux/amd64 and linux/arm64 and publishes one
manifest list under `:vX.Y.Z` and `:stable`:

- `sqe-release-binaries` (matrix amd64/arm64): `scripts/ci-release-binaries.sh`
  cross-compiles with cargo-zigbuild (zig 0.14.1, cargo-zigbuild 0.24.1, both
  sha256-pinned), glibc 2.28 floor, on the amd64 runner.
- `sqe-release-image-arch` (matrix): kaniko `--custom-platform` over
  `Dockerfile.ci`, pushed as `:<ref>-<arch>`.
- `aikido-release-image` (override): `scripts/ci-release-index.sh` checks each
  input's config arch, joins them with `crane index append` (no Docker daemon,
  so no buildx), tags, reads back, writes `image-digest.txt`.
- `aikido-release-image-scan` pinned to `GRYPE_PLATFORM=linux/amd64`; new
  `sqe-release-image-scan-arm64`.
- `aikido-publish-release-image` and `sqe-publish-latest` (overrides):
  `scripts/ci-mirror-release.sh` copies with `skopeo copy --all
  --preserve-digests` and fails unless each tag reads back with both platforms
  and the index digest. The template's `deploy_mirror.py` has no `--all` and
  would flatten the list to amd64 under a new digest.
- `gitlab-release` names the index digest.
- `ci::multiarch` MR label: dry run that pushes `:mr-<iid>` to the project
  registry only.

Verified locally: index script against a local registry (happy path plus
wrong-arch input, malformed digest, single-platform, amd64-only and
wrong-digest cases all refused); mirror script with skopeo 1.18 (`--all` keeps
the digest, a plain copy is caught); `GRYPE_PLATFORM` selects the platform with
the pinned grype; `glab ci lint` on a tag ref and on `main` (no new job on
`main`). Not verified: the CI path itself and the zigbuild compile on the
runner; the first `ci::multiarch` dry run or the next tag is the proof.

Left open: `deploy_mirror.py` upstream in aikido could take `--all`, which
would let the two mirror overrides go. Dev images from `main` stay amd64.

**NEXT:** run the `ci::multiarch` dry run on this MR before merging; then #901
system-catalog split.
