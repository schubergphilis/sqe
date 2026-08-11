# Releasing SQE

SQE follows [Semantic Versioning](https://semver.org/) on the workspace crates.
The single source of truth for the version is `[workspace.package] version` in
the root `Cargo.toml`; all 17 crates inherit it with `version.workspace = true`.
A release is one annotated git tag `vX.Y.Z` pointing at a commit where that
version (and `Cargo.lock`) has been bumped to match.

There are two ways to cut a release. The **branch-name** flow is the default and
runs entirely in CI (aikido owns it). The **local script** is a fallback for
hotfixes, pre-releases, or when you want full local control.

## Where images go

Since the 2026-07-22 migration to the aikido templates, this repo no longer
pushes to Harbor itself. `aikido-build` pushes to the **GitLab registry** (the
primary), and `aikido-publish-image` / `aikido-publish-release-image` mirror from
there to every `[[registry]]` declared in `.asset` — Harbor today. So a Harbor
tag exists only if the mirror job ran and succeeded.

## Dev versions (no release)

Every merge to `main` that touches the engine builds and mirrors a development
image:

```
repo.sovereign-data.org/chameleon/sqlengine/sqe:<short-sha>
```

There is **no `:latest`**. The old in-repo pipeline published one; the aikido
mirror tags dev builds by short SHA only. Pin a `:<short-sha>` when you need an
exact dev build, or `:stable` for the newest release.

The mirror job on the merge path is `allow_failure: true`, so a merge can land
with no Harbor tag and a green pipeline. If a short SHA you expect is missing,
check the `aikido-publish-image` job on that pipeline before assuming the build
failed.

The in-repo workspace version does not move on a normal merge: it stays at the
last released version until a release bumps it. So a binary built from `main`
between releases **reports the previous release version** — a `<short-sha>` image
from today still identifies itself as v0.37.0. That is expected, and it is why
`:<short-sha>` is a poor thing to pin in a deployment.

## Cutting a release: the branch-name flow (default)

Owned by aikido's `aikido-release` job (`semver_release.py`), not by this repo.
On every merge to `main` it bumps the version, commits it back, and tags it.

The **bump level comes from the merged MR's source branch name**, not from the
MR title:

| Source branch | Bump  | Example: from `v0.37.0` |
|---------------|-------|-------------------------|
| `release/*`   | major | `v1.0.0`                |
| `feat/*`      | minor | `v0.38.0`               |
| anything else | patch | `v0.37.1`               |

Note that `release/*` means **major** here. Under this repo's pre-aikido flow it
merely marked a branch as releasable and the level came from the MR title; a
`release/*` MR merged today cuts a major version.

The job is a dry-run — it prints the plan and stops — unless the project CI/CD
variable `AIKIDO_RELEASE=1` is set. If releases are not happening on merge, check
that variable first.

Merge it. CI then, on the `main` push:

1. Finds the merged MR for the commit and reads its source branch.
2. Computes the next version from the highest existing `vX.Y.Z` tag.
3. Writes the new version into `Cargo.toml`, syncs `Cargo.lock`, and commits
   `chore(release): vX.Y.Z [skip ci]` to `main`.
4. Creates the tag `vX.Y.Z`.

The tag pipeline then builds and pushes to the GitLab registry
(`aikido-release-image`) and mirrors to Harbor (`aikido-publish-release-image`):

```
repo.sovereign-data.org/chameleon/sqlengine/sqe:vX.Y.Z   (immutable)
repo.sovereign-data.org/chameleon/sqlengine/sqe:stable   (newest release)
```

and runs the `changelog` + GitLab Release + SBOM jobs. No further human action is
needed after the merge.

The release mirror is fail-closed, so a Harbor outage fails the tag pipeline
rather than shipping a release that only exists in the GitLab registry.

`aikido-publish-release-image` is newer than the `ref:` this repo pins in
`.gitlab-ci.yml` (v0.9.5). Until that ref is bumped to an aikido release
containing it — renovate does this automatically — **a tag pipeline mirrors
nothing**. That is why Harbor served `:stable` = `v0.37.0` from 2026-06-24 onward
while `main` moved 1110 commits.

There is no "non-release" MR any more: every merge to `main` goes through
`aikido-release`, and a branch that matches nothing still cuts a patch. Use the
branch name to say what you mean.

## Cutting a release: the local script (fallback / hotfix)

`scripts/release.sh` does the same version bump locally and pushes the tag
directly. Use it for emergency hotfixes on an older minor, or when you cannot go
through an MR.

```bash
# Bump, commit, tag locally (does not push)
scripts/release.sh 0.36.1

# Also push the branch + tag (fires the tag pipeline)
scripts/release.sh 0.36.1 --push
```

It bumps `[workspace.package] version`, refreshes `Cargo.lock`, creates a
`chore(release): <version>` commit and an annotated `v<version>` tag, and
refuses to run on a dirty tree or re-tag an existing version. The tag pipeline
publishes the same `:vX.Y.Z` + `:stable` images regardless of how the tag was
created.

### Choosing the version by hand

| Change type | Bump |
|---|---|
| Bug fix, perf improvement, internal refactor | patch (`x.y.Z+1`) |
| New SQL feature, new public API, new backend | minor (`x.Y+1.0`) |
| Breaking change to config, SQL surface, or wire protocol | major (`X+1.0.0`) |

### Hotfix on a previous minor

```bash
git checkout -b release/0.35.x v0.35.0
# ... cherry-pick the fix ...
scripts/release.sh 0.35.1 --push
```

The tag pipeline fires on the tag push regardless of branch.

### Pre-releases

```bash
scripts/release.sh 0.37.0-rc.1 --push
```

CI publishes a GitLab Release for any `v*` tag, including `-rc.N` / `-alpha.N`
pre-releases. The automated release-branch flow only produces clean `vX.Y.Z`
versions, so use the script for pre-releases.

## What you need once (ops)

All of this is aikido-owned now; the pre-aikido `RELEASE_TOKEN` is no longer read.

- `AIKIDO_RELEASE=1` — without it `aikido-release` only prints its plan and no
  tag is ever created.
- `AIKIDO_GROUP_TOKEN` — used by `semver_release.py` to commit the version bump
  and create the tag through the GitLab API on the protected `main` branch.
- `REGISTRY_USERNAME` / `REGISTRY_PASSWORD` — the Harbor credentials, named (not
  stored) by the `[[registry]]` block in `.asset` and inherited from the group.

## See also

- `docs/internal/process/RELEASING.md` - history of the version-drift problem the
  script fixes, and the retrospective-tag record.
- `docs/superpowers/specs/2026-06-24-harbor-push-and-semver-releases-design.md` -
  design of the Harbor push + release-branch flow.
