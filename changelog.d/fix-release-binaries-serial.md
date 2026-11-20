### Fixed

- Release pipeline: the amd64 and arm64 `sqe-release-binaries` jobs run one after the other, and each builds `sqe-cli` and `sqe-server` in separate cargo runs. In the v0.55.1 tag pipeline both arches landed on the same runner host twice, and the host died during the thin-LTO link both times. The job now logs its memory use every minute.

### Added

- CI: the `ci::multiarch-release` MR label runs the multi-arch image dry run with the release profile, so the tag build can be proven before tagging.
