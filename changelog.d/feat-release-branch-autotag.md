### Added

- Releases: merging a `release/vX.Y.Z` merge request tags its merge commit `vX.Y.Z`

    `scripts/release.sh X.Y.Z` on a `release/vX.Y.Z` branch prepares the version bump, Helm chart and changelog, and no longer tags locally. When the merge request merges, the `sqe-tag-release` job on `main` checks that the workspace version matches the branch name and creates the tag through the GitLab API; the tag pipeline then builds and publishes the release. Hotfix and pre-release tags are still pushed by hand.
