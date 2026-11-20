### Fixed

- Release pipeline: the `sqe-release-binaries` jobs get 180 minutes instead of 90. The thin-LTO link of `sqe-server` on a busy shared runner outlasted the old limit, so the v0.55.1 tag pipeline timed out before it built any binary.
