### Changed

- CI: the main-branch pipeline no longer re-runs the test suite, clippy matrix, catalog live tests or config checks after a merge

    The merge request pipeline already ran them on the same tree. Main keeps the security and supply-chain scans and the dependency advisory check. The release build and the `:main` image are on demand: press play on `release-binaries`, or run a main pipeline with `SQE_BUILD_IMAGE=1`. Release tags still build their own image.
