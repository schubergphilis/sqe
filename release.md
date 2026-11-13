# Releasing SQE

SQE follows [Semantic Versioning](https://semver.org/) on the workspace crates.
The single source of truth for the version is `[workspace.package] version` in
the root `Cargo.toml`; all 17 crates inherit it with `version.workspace = true`.
A release is one annotated git tag `vX.Y.Z` pointing at a commit where that
version (and `Cargo.lock`) has been bumped to match.

Releases are cut by hand, never on an ordinary merge. The default is a
**release branch**: `scripts/release.sh` prepares `release/vX.Y.Z`, the merge
request is reviewed and merged, and CI tags the merge commit. The **local
script** with a pushed tag stays for hotfixes on an older minor and for
pre-releases.

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

## Cutting a release: the release branch (default)

```bash
git switch -c release/v0.54.0 origin/main
scripts/release.sh 0.54.0 --push
glab mr create --source-branch release/v0.54.0 --target-branch main \
  --title "chore(release): 0.54.0"
```

On a `release/vX.Y.Z` branch `release.sh` bumps `[workspace.package] version`,
refreshes `Cargo.lock`, updates the Helm chart, folds `changelog.d/` into
`CHANGELOG.md` and commits `chore(release): X.Y.Z`. It does **not** tag: with
`--push` it pushes only the branch. The version is yours to choose (see
"Choosing the version by hand" below); the branch name must carry the same
version.

The merge request runs the normal MR pipeline. Review the changelog section in
it: that text becomes the GitLab Release notes.

Merge it. The `main` pipeline for the merge commit runs `sqe-tag-release`
(`scripts/ci-tag-release.py`, stage `.pre`, not interruptible). The job runs
only when the merge commit message is `Merge branch 'release/vX.Y.Z' into ...`,
so keep GitLab's default merge commit message. It:

1. Reads `X.Y.Z` from the merge commit message.
2. Refuses (job fails, no tag) when `[workspace.package] version` on `main` is
   not `X.Y.Z`. Rerun `release.sh` on the branch and merge again.
3. Passes without change when `vX.Y.Z` already points at this commit (a retry),
   and fails when it points anywhere else.
4. Creates the annotated tag `vX.Y.Z` on the merge commit through the GitLab
   API, with `AIKIDO_GROUP_TOKEN`.

If the job fails on the token (HTTP 401/403), create the tag by hand on the
merge commit: `git tag -a vX.Y.Z -m "Release vX.Y.Z" <merge-sha> && git push
origin vX.Y.Z`. Then fix the token's rights on protected `v*` tags.

The tag pipeline then builds and pushes to the GitLab registry
(`aikido-release-image`) and mirrors to Harbor (`aikido-publish-release-image`):

```
repo.sovereign-data.org/chameleon/sqlengine/sqe:vX.Y.Z   (immutable)
repo.sovereign-data.org/chameleon/sqlengine/sqe:stable   (newest release)
```

and publishes a GitLab Release (`gitlab-release`) whose notes are that version's
`CHANGELOG.md` section. `scripts/release.sh` creates the section by renaming
`[Unreleased]` to the version and opening a fresh `[Unreleased]`, so curate the
notes under `[Unreleased]` as you merge. No further human action is needed after
the merge.

The release mirror is fail-closed, so a Harbor outage fails the tag pipeline
rather than shipping a release that only exists in the GitLab registry.

`aikido-publish-release-image` is newer than the `ref:` this repo pins in
`.gitlab-ci.yml` (v0.9.5). Until that ref is bumped to an aikido release
containing it — renovate does this automatically — **a tag pipeline mirrors
nothing**. That is why Harbor served `:stable` = `v0.37.0` from 2026-06-24 onward
while `main` moved 1110 commits.

### aikido's automatic release stays off

The aikido template also ships `aikido-release` (`semver_release.py`), which on
every merge to `main` would bump the version from the source branch name
(`release/*` major, `feat/*` minor, anything else patch), commit it back and
tag. It runs as a dry-run, printing its plan, unless the CI/CD variable
`AIKIDO_RELEASE=1` is set. Keep that variable unset: with it, every merge cuts a
release, and a merged `release/vX.Y.Z` branch would get a major bump on top of
the version `release.sh` already set.

## Cutting a release: the local script (fallback / hotfix)

Off a `release/vX.Y.Z` branch, `scripts/release.sh` does the same version bump,
tags locally and pushes the tag directly. Use it for hotfixes on an older minor,
for pre-releases, or when you cannot go through an MR.

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

The tag pipeline does not reuse anything from the `main` pipeline: `release-binaries`
excludes tags, so `aikido-release-image` runs kaniko against the root `Dockerfile`
and compiles from source a second time. The `:vX.Y.Z` / `:stable` image is
therefore a separate build of the same commit; the tested binaries (`out/`) are
the ones packaged into the short-SHA image by `aikido-build`. See #736.

### Pre-releases

```bash
scripts/release.sh 0.37.0-rc.1 --push
```

CI publishes a GitLab Release for clean `vX.Y.Z` tags only. Pre-release tags
(`-rc.N` / `-alpha.N`) get an image but no Release, and `release.sh` leaves
`[Unreleased]` untouched for them so the final release keeps its notes. The
automated release-branch flow only produces clean `vX.Y.Z` versions, so use the
script for pre-releases.

## Tag pipelines need one job outside `.post`

Both aikido release jobs (`aikido-release-image`, `aikido-publish-release-image`)
run in the `.post` stage. GitLab does not run a pipeline whose jobs are all in
`.pre`/`.post`; a tag push with nothing else matching fails silently with "The
resulting pipeline would have been empty" (visible only if you POST
`/projects/:id/pipeline?ref=vX.Y.Z` by hand). `ci-config-sanity` therefore also
runs on `$CI_COMMIT_TAG`. The v0.48.0 tag predates that rule and has no
pipeline, no image and no GitLab Release; v0.48.1 is the first tag after the
fix. Do not move v0.48.0; the config is read from the tagged commit.

## `v*` tags must be protected, or Harbor gets nothing

`REGISTRY_USERNAME` / `REGISTRY_PASSWORD` are **protected** group variables.
GitLab exposes protected variables only to pipelines on protected branches or
tags. Until 2026-09-14 this project protected `main` but no tags, so every tag
pipeline ran `aikido-publish-release-image` without credentials and it logged
`skip harbor repo.sovereign-data.org/chameleon: repo or credential vars unset`
and exited green. The image existed in the GitLab registry; Harbor `:stable`
never moved. `v*` is a protected tag now (create: Maintainers). If the Harbor
mirror ever logs that skip line again, check Settings > Repository >
Protected tags first, then retry the job; no rebuild is needed.

## Release images are not signed yet

`AIKIDO_SIGN` is `"0"`. With `"1"`, the aikido job `aikido-sign-image` runs
`cosign sign` keyless: it asks the public Sigstore Fulcio for a certificate with
the job's GitLab ID token. Fulcio does not trust this self-hosted GitLab as an
identity issuer and answers HTTP 400 ("error processing the identity token").
v0.54.0 was the first tag where the job ran, and it failed after the image and
the Release were already published. No SQE image has a signature.

Keyless signing against public Sigstore would also write the image reference
to the public Rekor transparency log. The planned fix signs with a cosign key
held in CI (or a KMS) and skips the public log; turn `AIKIDO_SIGN` back on
with that change, not before.

## Images are amd64 only

The CI images (`:vX.Y.Z`, `:stable`, `:latest`, `:<short-sha>`) are built by
kaniko for linux/amd64. `make push` builds amd64 and arm64 with `docker
buildx` from a workstation. A CI multi-arch build (cross-compiling aarch64
binaries and packaging `Dockerfile.ci` per platform) is being evaluated.

## Binaries on the GitHub mirror

`github.com/schubergphilis/sqe` is a read-only push mirror of this repo and
receives every `v*` tag. `.github/workflows/release-binaries.yml` runs on that
tag push: it builds `sqe-server`, `sqe-worker`, `sqe-cli` and `sqe-bench` for
`aarch64-apple-darwin`, `x86_64-apple-darwin` and `x86_64-unknown-linux-gnu`
with the same `cargo build --locked --no-default-features --profile release`
line as the GitLab `release-binaries` job, and attaches one
`sqe-<version>-<target>.tar.gz` plus a `.sha256` per target to a GitHub Release
named `SQE vX.Y.Z` with generated notes.

Two things to know:

- The workflow runs from the tree at the tagged commit, so tags cut before the
  file existed have no run. Backfill from the Actions tab with **Run workflow**
  and the tag name as input (v0.48.0 is the first candidate).
- The dispatch form also takes a `profile`. Hosted macOS runners have 7 GB of
  RAM; if the thin-LTO `release` link dies there, rerun with `dev-release`.

Harbor images stay the GitLab tag pipeline's job; GitHub only gets binaries.

## What you need once (ops)

All of this is aikido-owned now; the pre-aikido `RELEASE_TOKEN` is no longer read.

- `AIKIDO_RELEASE` stays **unset**, so `aikido-release` only prints its plan
  (see "aikido's automatic release stays off").
- `AIKIDO_GROUP_TOKEN`: `sqe-tag-release` creates the `vX.Y.Z` tag with it
  through the GitLab API. It needs `api` scope and the right to create protected
  `v*` tags (Maintainer).
- `REGISTRY_USERNAME` / `REGISTRY_PASSWORD` — the Harbor credentials, named (not
  stored) by the `[[registry]]` block in `.asset` and inherited from the group.

## See also

- `docs/internal/process/RELEASING.md` - history of the version-drift problem the
  script fixes, and the retrospective-tag record.
- `docs/superpowers/specs/2026-06-24-harbor-push-and-semver-releases-design.md` -
  design of the Harbor push + release-branch flow.
