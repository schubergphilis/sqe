### Changed

- CHANGELOG entries are written as `changelog.d/` fragments and folded in at release

    Merge requests no longer edit `CHANGELOG.md`; each adds
    `changelog.d/<branch-slug>.md`, and `scripts/release.sh` folds the
    fragments into `[Unreleased]` with `scripts/changelog-collect.py`. Open
    merge requests no longer conflict on the top of the `[Unreleased]` lists.
