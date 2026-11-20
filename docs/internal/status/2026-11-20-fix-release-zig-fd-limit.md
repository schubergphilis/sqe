**STATUS 2026-11-20: release binaries raise the open-file limit before the zig link (`fix/release-zig-fd-limit`).**

The v0.55.0 tag pipeline was the first release to run the zigbuild job from
#898, and both `sqe-release-binaries` jobs failed at link time with
`ProcessFdQuotaExceeded`: zig's lld keeps every input object open (about 1560
for sqe-cli, 1640 for sqe-server) and the runner's soft limit is 1024.
`scripts/ci-release-binaries.sh` now sets the soft limit to the hard one and
fails before the build when that is still below 4096. The image, scan and
GitLab release jobs of v0.55.0 were skipped, so v0.55.0 has no image or release
page.

**NEXT:** tag v0.55.1 from main once this merges and check that both arches link.
