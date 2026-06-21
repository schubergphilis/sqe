# Releasing SQE

SQE versions track [Semantic Versioning](https://semver.org/) on the workspace
crates. A release is one annotated git tag (`v<x>.<y>.<z>`) pointing at a
commit where every `crates/*/Cargo.toml` has been bumped to the matching
version.

## The script

Always cut a release through `scripts/release.sh`:

```bash
# Patch bump (bug fixes only)
scripts/release.sh 0.31.4

# Push when ready
scripts/release.sh 0.31.4 --push
```

What it does:

1. Bumps `version = "..."` in every `crates/*/Cargo.toml` to the requested
   version. Skips `xtask/Cargo.toml` (internal tooling, pinned at `0.0.0`).
2. Updates `Cargo.lock` via `cargo check --workspace`.
3. Creates a `chore(release): <version>` commit.
4. Creates an annotated tag `v<version>`.
5. Optionally pushes both to `origin`.

The script refuses to run if the working tree is dirty or the tag already
exists.

## Why the script exists

Between v0.16 and v0.31 the git tags moved but `Cargo.toml` versions never
did — every crate stayed at `0.15.0` for fourteen releases. `cargo --version`
on a `v0.31.0` binary reported `0.15.0`. The script makes the bump atomic
with the tag so the two cannot drift again.

If you need to bump versions outside a release (testing a pre-release
artifact, etc.), do it by hand and remember to either land it as a real
release or revert before merging. Never merge a `chore(release):` commit
that isn't tagged.

## Choosing the version

| Change type | Bump |
|---|---|
| Bug fix, perf improvement, internal refactor | patch (`x.y.Z+1`) |
| New SQL feature, new public API, new backend | minor (`x.Y+1.0`) |
| Breaking change to config, SQL surface, or wire protocol | major (`X+1.0.0`) |

In practice every catalog mount backend (Glue, S3 Tables, HMS, etc.) and
every new SQL primitive (ATTACH, CREATE SECRET, CALL system.*) has been a
minor bump. Operational fixes (auth recovery, error messages, dockerfile
build context) are patches.

## What happens after the tag is pushed

`.gitlab-ci.yml` has a release pipeline that fires on any `v*` tag:

1. `changelog` job runs `git-cliff --latest` against `cliff.toml` to
   generate `release-notes.md` from conventional commits since the previous
   tag.
2. `release` job uses `release-cli` to publish a GitLab Release named
   `SQE v<x>.<y>.<z>` with those notes attached.

No human action needed past `git push origin v<version>`.

## Backfilling release notes for an older tag

If you forgot to write a `chore(release):` commit at the time, you can still
add release notes after the fact: edit `CHANGELOG.md` and put the entry under
the right `[<x>.<y>.<z>]` header. git-cliff regenerates from commits, not
from the file, so this is purely human-readable history. The GitLab Release
description is fixed at tag-push time; to update it, edit on the GitLab UI.

If you forgot to bump `Cargo.toml`, the tag stays at whatever `Cargo.toml`
said. Don't move the tag — historical tags should match the bytes that were
actually shipped at that point. Just make sure the next release bumps to a
higher version than the drift.

## Pre-release versions

For testing flow before cutting a release:

```bash
scripts/release.sh 0.32.0-rc.1
```

CI publishes a GitLab Release for any `v*` tag including pre-releases. Use
the `-rc.N` suffix for release candidates, `-alpha.N` for early test builds.

## Emergency hotfix on a previous minor

To patch `v0.30.x` while `main` is on `v0.31.x`:

```bash
git checkout -b release/0.30.x v0.30.1
# ... cherry-pick the fix commits ...
scripts/release.sh 0.30.2 --push
```

The release pipeline fires on the tag push regardless of branch.

## Retrospective tags (v0.31.5 .. v0.35.0)

Between 2026-05-11 (v0.31.4) and 2026-06-12 no releases were cut while 150+
MRs merged. On 2026-06-12 we tagged the milestone merge points
retrospectively:

| Tag | Commit | Wave |
|---|---|---|
| v0.31.5 | 882a662 | Security/hardening audit campaign (May 13-15) |
| v0.32.0 | f9398c8 | Dynamic predicate pushdown, system.register_table |
| v0.33.0 | ccf42b4 | DuckDB quack protocol, iceberg rebase + int96 |
| v0.34.0 | 199f550 | Ballista wind-down, catalog discovery, S3 TVFs |
| v0.35.0 | b7059a3 | Web UI, embedded cloud catalogs, quickstarts |

These tags point at trees whose crates still read `0.31.4` -- the known
drift exception. They were pushed with `-o ci.skip` so no release pipeline
ran for them. v0.36.0 is the first normal release after the gap. Don't
repeat this: cut a release when a feature wave merges, not a month later.
