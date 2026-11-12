# feat/release-branch-autotag

Releases are cut from a `release/vX.Y.Z` branch. `scripts/release.sh` prepares the branch without tagging; on merge, `sqe-tag-release` (`scripts/ci-tag-release.py`, `.pre` on `main`) verifies `[workspace.package] version` against the branch name and creates `vX.Y.Z` on the merge commit through the API with `AIKIDO_GROUP_TOKEN`. Refuses on a version mismatch or a tag on another commit; a retry on the same commit passes. `release.md` documents the flow and that `AIKIDO_RELEASE` stays unset.

Verified locally: the script's dry-run, mismatch, non-release and missing-token paths; `bash -n scripts/release.sh`; CI lint. Not verified: whether `AIKIDO_GROUP_TOKEN` may create protected `v*` tags. The first real release merge shows it; `release.md` gives the manual fallback.

Note: `[workspace.package] version` on main is 0.53.1 while the newest tag is v0.53.0.

**NEXT:** cut the next release through `release/v0.54.0` and watch `sqe-tag-release` and the tag pipeline.
