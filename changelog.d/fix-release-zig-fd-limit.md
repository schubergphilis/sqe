### Fixed

- Release binaries link again: the zigbuild job raises its open-file limit

    zig's linker keeps every input object open, about 1650 for sqe-server,
    and the release runner's soft limit of 1024 failed the v0.55.0 tag
    pipeline with ProcessFdQuotaExceeded on both architectures.
    `scripts/ci-release-binaries.sh` now raises the soft limit to the hard one
    and stops early when that is still below 4096.
