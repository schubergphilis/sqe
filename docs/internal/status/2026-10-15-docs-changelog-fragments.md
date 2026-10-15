**STATUS 2026-10-15: CHANGELOG entries go in `changelog.d/<branch-slug>.md`, and `scripts/release.sh` folds them into `CHANGELOG.md` (`docs/changelog-fragments`).**

Every merge request added its entry at the top of the same `### Fixed` (or `### Added`) list under `[Unreleased]`, so each merge made the next open merge request conflict. Three MRs (!1426, !1427, !1428) had to be rebased for it on 2026-10-14, and !1427 again the next day. This is the same fix the status log got in !1391.

- `changelog.d/README.md`: naming and format. Each fragment holds one or more `### <Section>` headings with the entry text.
- `scripts/changelog-collect.py`: moves each entry to the top of its section under `[Unreleased]`, creates missing sections in Keep-a-Changelog order, and deletes the fragments. `--check` validates without writing. Tested on a copy: entries land in the right sections, and a malformed fragment exits 1.
- `scripts/release.sh`: runs the collector before renaming `[Unreleased]`, skips it for pre-releases, and stages `changelog.d`.
- `CLAUDE.md` "After Completing Work" and `RELEASING.md`: updated.

**NEXT:** unchanged
