# Changelog fragments

One file per merge request, named after the branch (`fix/foo-123` ->
`fix-foo-123.md`). Do not edit `CHANGELOG.md` in a merge request: every entry
there sits at the top of the same `[Unreleased]` section, so each merge made
the next open merge request conflict.

`scripts/release.sh` runs `scripts/changelog-collect.py`. It moves every
fragment to the top of its section under `[Unreleased]` in `CHANGELOG.md` and
deletes the fragment files. `scripts/changelog-collect.py --check` validates
the fragments without changing anything.

## Format

One or more `### <Section>` headings (`Added`, `Changed`, `Deprecated`,
`Removed`, `Fixed`, `Security`), each followed by the entry exactly as it
should appear in `CHANGELOG.md`:

```markdown
### Fixed

- One-line summary of the change (#123)

    What changed, what it fixes, limits.
```
