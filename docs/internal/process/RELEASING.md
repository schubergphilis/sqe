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

## Retrospective tags, second gap (v0.38.0 .. v0.47.0)

It happened again. Between 2026-06-24 (v0.37.0) and 2026-09-14 no release was
cut while 521 MRs merged: the aikido `aikido-release` job described in
`release.md` only dry-runs unless the project variable `AIKIDO_RELEASE` is set,
and it never was. On 2026-09-14 the wave endpoints were tagged retrospectively:

| Tag | Commit | Wave |
|---|---|---|
| v0.38.0 | c8b1a60b | Trino BI-wire compat, mixed Basic-auth listener, write-path pool tracking (to Jul 2) |
| v0.39.0 | 5a26cca8 | Parallel scan on by default, stranded-LIMIT fixes, COUNT(*) fast-path (to Jul 8) |
| v0.40.0 | 9d1e807d | Production audit remediation, MERGE SCD2 surface, bloom row-group pruning, observability (to Jul 13) |
| v0.41.0 | 128a9117 | Compaction phases 1-4, INSERT OVERWRITE, unified bench harness, array lambdas (to Jul 22) |
| v0.42.0 | d1e2b119 | Bounded-memory and spill program, event-driven cache invalidation, aikido CI, Renovate (to Jul 31) |
| v0.43.0 | 590bac79 | Access-control wave: grants as caller, DENY, grant-profile v4/v5, Spark parity, masking policies (to Aug 11) |
| v0.44.0 | 9f942a08 | Hardening batch: ATTACH jdbc, byte budgets, circuit breaker, worker row-group pruning (to Aug 19) |
| v0.45.0 | 88af57a5 | Hive external tables cycles 1/2/4/5, DataFusion 55 / arrow 59, Rust 1.98 (to Aug 27) |
| v0.46.0 | dda731d2 | MCP server, Glue catalog over MCP, iceberg vendored fixes, Puffin NDV, cargo-hakari (to Sep 4) |
| v0.47.0 | f04b2b31 | Review work packages #652-#658, crate split, async-path fixes, sqlite-rs default (to Sep 10) |

As before, these trees self-report `0.37.0` and were pushed with `-o ci.skip`,
so no image or GitLab Release exists for them. v0.48.0 is the first normal
release after this gap. To stop this from recurring, set `AIKIDO_RELEASE=1` on
the project (see `release.md`, "What you need once") or cut with
`scripts/release.sh` when a wave merges.
