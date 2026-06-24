# Releasing SQE

SQE follows [Semantic Versioning](https://semver.org/) on the workspace crates.
The single source of truth for the version is `[workspace.package] version` in
the root `Cargo.toml`; all 17 crates inherit it with `version.workspace = true`.
A release is one annotated git tag `vX.Y.Z` pointing at a commit where that
version (and `Cargo.lock`) has been bumped to match.

There are two ways to cut a release. The **release branch** flow is the default
and runs entirely in CI. The **local script** is a fallback for hotfixes or when
you want full local control.

## Dev versions (no release)

Every merge to `main` that touches the engine builds and pushes a development
image to Harbor:

```
repo.sovereign-data.org/chameleon/sqlengine/sqe:<short-sha>
repo.sovereign-data.org/chameleon/sqlengine/sqe:latest
```

`:latest` always points at the newest `main`. The in-repo workspace version does
not move on a normal merge: it stays at the last released version until a release
bumps it. So a binary built from `main` between releases reports the previous
release version. That is expected. Pin to a `:<short-sha>` tag when you need to
reference an exact dev build.

## Cutting a release: the release branch flow (default)

Open an MR from a branch named `release/*`. When it merges to `main`, CI reads
the merged MR, bumps the version, commits it back to `main`, tags it, and the tag
pipeline publishes the release images.

The **bump level comes from the MR title**:

| MR title contains | Bump  | Example: from `v0.36.0` |
|-------------------|-------|-------------------------|
| `#major`          | major | `v1.0.0`                |
| `#minor`          | minor | `v0.37.0`               |
| neither (default) | patch | `v0.36.1`               |

### Steps

```bash
git checkout -b release/q3-write-path
# ... your changes (or an empty branch to release what is already on main) ...
git push -u origin release/q3-write-path
```

Open the MR. Title it for the bump you want:

- `Write path improvements` -> patch
- `#minor: add MERGE INTO and CREATE SECRET` -> minor
- `#major: change the Flight SQL wire contract` -> major

Merge it. CI then, on the `main` push:

1. Finds the merged MR for the commit and confirms its source branch is
   `release/*`.
2. Computes the next version from the highest existing `vX.Y.Z` tag.
3. Writes the new version into `Cargo.toml`, syncs `Cargo.lock`, and commits
   `chore(release): vX.Y.Z` to `main`.
4. Creates the tag `vX.Y.Z`.

The tag pipeline then builds and pushes:

```
repo.sovereign-data.org/chameleon/sqlengine/sqe:vX.Y.Z   (immutable)
repo.sovereign-data.org/chameleon/sqlengine/sqe:stable   (newest release)
```

and runs the existing `changelog` + GitLab Release + SBOM jobs. No further human
action is needed after the merge.

A normal (non-`release/*`) MR is unaffected: it just produces the `:latest` +
`:<short-sha>` dev images.

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

The release-branch flow needs a project CI/CD variable `RELEASE_TOKEN`: a project
access token with `api` + `write_repository` scopes and the Maintainer role, so
CI can commit the bump and create the tag on the protected `main` branch. The
Harbor push uses the shared `robot$vpf-ci-pusher` credentials inherited from the
group; nothing per-project is needed for the push.

## See also

- `docs/internal/process/RELEASING.md` - history of the version-drift problem the
  script fixes, and the retrospective-tag record.
- `docs/superpowers/specs/2026-06-24-harbor-push-and-semver-releases-design.md` -
  design of the Harbor push + release-branch flow.
