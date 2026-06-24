# Harbor Image Push + MR-Keyword Semver Releases (sqlengine) — Design

**Date:** 2026-06-24
**Status:** Approved design -> implementation
**Scope:** Point sqlengine's CI image build at Harbor via the shared robot, and add an opt-in
semantic-version release flow. Mirrors the data-platform design
(`../data-platform/docs/superpowers/specs/2026-06-24-harbor-robot-and-semver-releases-design.md`),
adapted for a Rust/Cargo workspace and for sqlengine's already-richer release pipeline.

## Problem

Established empirically against the current `.gitlab-ci.yml`:

1. **Image build pushes to the wrong registry.** `build-sqe` extends `.kaniko-build` from the
   shared template but does not set `REGISTRY_URL`/`REGISTRY_PROJECT`, so the template falls back
   to the GitLab project registry (`$CI_REGISTRY` / `$CI_PROJECT_PATH`). The Harbor robot
   (`robot$vpf-ci-pusher`, push+pull, all projects, expires 2027-06-24) and its inherited group
   variables `REGISTRY_USERNAME`/`REGISTRY_PASSWORD` already exist; the build is simply not aimed
   at Harbor. The ask is to publish to `repo.sovereign-data.org`.

2. **No opt-in semver release flow.** The repo already cuts a GitLab Release object, a git-cliff
   changelog, and an SBOM when a `v*` tag is pushed by hand, but there is no path that turns a
   merged MR into a version bump + tag automatically. We want the data-platform pattern: merge an
   MR from `release/*`, get a versioned + `:stable` image and the version committed back to the
   tree.

## Constraints discovered (load-bearing)

- **The shared `.kaniko-build` template** (`vpf-data-ai/libraries/ci-cd-pipelines/build-publish-docker-images`):
  - auth: `REGISTRY_USERNAME:-$CI_REGISTRY_USER` / `REGISTRY_PASSWORD:-$CI_REGISTRY_PASSWORD`;
  - image path: `${REGISTRY_PROJECT}/${IMAGE_NAME}` when `REGISTRY_PROJECT` is set, else
    `${CI_PROJECT_PATH}/${IMAGE_NAME}`; registry host = `${REGISTRY_URL:-$CI_REGISTRY}`;
  - tags: honors a pre-set space-separated `DOCKER_TAGS`; otherwise main push -> `<sha7> latest`,
    MR -> `mr-<iid>-<sha7>`, and **on a tag pipeline its auto-tag path hits "not an MR or main"
    and exits 0 (skips)**. A tag build MUST pass `DOCKER_TAGS` explicitly.
  - A job defining its own `rules:` (as `build-sqe` does) fully overrides the template's rules.
- **Cargo workspace versioning.** All 17 crates use `version.workspace = true`; the single source
  is `[workspace.package] version` in the root `Cargo.toml` (currently `0.36.0`). `Cargo.lock`
  carries a per-crate `version = "0.36.0"` entry for each local crate that must be synced or the
  next build fails on a lock mismatch.
- **Existing git tags.** Highest is `v0.36.0`, matching `Cargo.toml`. The bump-from-highest-tag
  logic finds these, so the seed value is only a fallback.
- `CI_JOB_TOKEN` cannot create commits or tags on the repo; a write-scoped `RELEASE_TOKEN` is
  required. Pushing a tag triggers a tag pipeline (loop risk if bump logic re-runs there).
- GitLab masked-variable charset excludes `$`, so the robot username (`robot$...`) is unmasked;
  only the password is masked. (Already configured at group level; not touched here.)
- **No external consumer pulls the current GitLab-registry path.** A grep of `deploy/`,
  `docker-compose*.yml`, `quickstart/`, and docs shows only unqualified refs (`image: sqe:latest`,
  Helm `repository: sqe`); the only `$CI_REGISTRY_IMAGE/sqe` references are the SBOM jobs inside
  `.gitlab-ci.yml` itself. Switching registries is therefore safe and self-contained.

## Decisions

| # | Decision |
|---|----------|
| Registry host | `REGISTRY_URL: repo.sovereign-data.org`, `REGISTRY_PROJECT: chameleon` (top-level vars). |
| Image path | `IMAGE_NAME: sqlengine/sqe` -> `repo.sovereign-data.org/chameleon/sqlengine/sqe` (mirrors `chameleon/data-platform/backend`). |
| Auth | Inherited group `REGISTRY_USERNAME`/`REGISTRY_PASSWORD` (robot). No robot or group-var change. |
| Release trigger | Merge to `main` whose MR source branch matches `release/*`. Regular merges unaffected. |
| Bump level | MR title: `#major` -> major, else `#minor` -> minor, else patch (default). |
| Version truth | Git tags `vX.Y.Z`, seed `v0.36.0` if none. |
| Version files | Commit the bump into `Cargo.toml` (`[workspace.package] version`) and sync `Cargo.lock`. |
| Lock sync | `version-bump` runs in `rust:bookworm` and runs `cargo update --workspace --offline` (single command, never misses a future crate). |
| Release image tags | `:<version>` (immutable) + `:stable` (mutable pointer). `:latest`/`:<sha>` still produced by ordinary main builds. |
| Existing tag pipeline | Keep changelog, SBOM, and the GitLab Release object. data-platform listed these as out-of-scope; here they exist, so we keep and integrate. |

## Part A — Point image build + SBOM at Harbor

1. Add top-level variables:
   ```yaml
   variables:
     REGISTRY_URL: "repo.sovereign-data.org"
     REGISTRY_PROJECT: "chameleon"
   ```
2. `build-sqe`: change `IMAGE_NAME: 'sqe'` -> `IMAGE_NAME: 'sqlengine/sqe'`. (Rules unchanged:
   still main-push + Dockerfile/crate changes.)
3. **SBOM jobs must follow the image to Harbor.** `.sbom-sign-base`, `sbom-sign`, and
   `sbom-sign-release` currently `docker login $CI_REGISTRY` and pull `$CI_REGISTRY_IMAGE/sqe:...`.
   Change all three to:
   - log in to `$REGISTRY_URL` with `$REGISTRY_USERNAME` / `$REGISTRY_PASSWORD`;
   - set `SBOM_IMAGE` to `${REGISTRY_URL}/${REGISTRY_PROJECT}/sqlengine/sqe:<tag>`.
   The `sbom-sign` (main) job uses `:${CI_COMMIT_SHORT_SHA}`; `sbom-sign-release` uses
   `:${CI_COMMIT_TAG}`.

**Rollback:** remove the two variables and revert `IMAGE_NAME`/SBOM refs; the next build returns
to the GitLab project registry.

## Part B — Semver release flow

Add a `release` stage after `build`.

### `version-bump` (main push only)

```yaml
version-bump:
  stage: release
  image: rust:bookworm
  rules:
    - if: '$CI_COMMIT_BRANCH == "main" && $CI_PIPELINE_SOURCE == "push"'
  needs:
    - { job: build-sqe, optional: true }
  before_script:
    - apt-get update -qq && apt-get install -y -qq jq git > /dev/null
  script:
    - bash scripts/ci/version-bump.sh
```

`scripts/ci/version-bump.sh`:
1. `git fetch --tags` (CI checkout is shallow/tagless).
2. Look up the merged MR for `$CI_COMMIT_SHA`:
   `GET /projects/$CI_PROJECT_ID/repository/commits/$CI_COMMIT_SHA/merge_requests`
   (`PRIVATE-TOKEN: $RELEASE_TOKEN`). Take the merged MR. If none, or its `source_branch` does not
   match `^release/` -> **exit 0**.
3. Bump level from MR `title`: `#major` -> major; else `#minor` -> minor; else **patch**.
4. Current version = highest `vMAJOR.MINOR.PATCH` git tag, or seed `v0.36.0` if none. Compute next.
5. Rewrite `[workspace.package] version = "X.Y.Z"` in `Cargo.toml`, then
   `cargo update --workspace --offline` to sync `Cargo.lock` (only the workspace-member version
   entries change; no dependency churn, no network).
6. Commit `Cargo.toml` + `Cargo.lock` to `main` via the API
   (`POST /projects/:id/repository/commits`, branch `main`, message
   `chore(release): vX.Y.Z`) using `$RELEASE_TOKEN`. **No `[skip ci]`** — `[skip ci]` is read from
   a tag's target commit and would suppress the tag pipeline that builds the images.
7. Create tag `vX.Y.Z` (`POST /projects/:id/repository/tags`, `ref` = the new commit SHA) using
   `$RELEASE_TOKEN`.

### `release-sqe` (version tag only)

```yaml
release-sqe:
  extends: .kaniko-build
  stage: build
  rules:
    - if: '$CI_COMMIT_TAG =~ /^v\d+\.\d+\.\d+$/'
  variables:
    DOCKERFILE_PATH: 'Dockerfile'
    BUILD_CONTEXT: '.'
    IMAGE_NAME: 'sqlengine/sqe'
    DOCKER_TAGS: "$CI_COMMIT_TAG stable"
    BUILD_ARGS: >-
      VERSION=${CI_COMMIT_TAG}
      BUILD_DATE=${CI_PIPELINE_CREATED_AT}
      GIT_REVISION=${CI_COMMIT_SHA}
```

No lint/test `needs:`. The tagged commit is the merged (already green) tree plus a mechanical
version-string bump; re-running the full gate on that is deliberately skipped.

### Tag-pipeline ordering (fixes the currently-broken release SBOM)

Today `sbom-sign-release` is `stage: test`, which runs **before** `build`, so on a tag pipeline it
tries to pull a tag image that nothing has built yet (and `build-sqe` does not run on tags). To make
the SBOM cover the real release image:

- `release-sqe` stays in `stage: build`.
- Move `sbom-sign-release` to `stage: release` with `needs: [{ job: release-sqe }]` (keep
  `allow_failure: true`).
- Move the GitLab `release` object job to `stage: release`; it keeps `needs: changelog` +
  `needs: sbom-sign-release (optional)`. `changelog` stays in `stage: test` (earlier, fine).

Resulting tag pipeline order: `changelog` (test) -> `release-sqe` (build) -> `sbom-sign-release`
then `release` object (release).

### Loop and redundant-rebuild avoidance

- `version-bump` is main-push only, so it never runs in a tag pipeline -> no bump loop from the tag.
- The bump commit it pushes to `main` has no `release/*` MR, so if its own main pipeline runs
  `version-bump`, step 2 exits 0.
- The bump commit changes `Cargo.toml` + `Cargo.lock`, which matches the `changes:` filter of both
  `build-sqe` **and** `sbom-sign` (whose `needs: build-sqe` is **non-optional**). Suppress both with
  an identical first rule so the bump commit's main pipeline does not rebuild/pull a redundant
  image:
  ```yaml
  rules:
    - if: '$CI_COMMIT_TITLE =~ /^chore\(release\)/'
      when: never
    - ... existing rules ...
  ```
  `cargo-check` / `cargo-test` still run on the bump commit (intended).

## Part C — Execution / ops

- **`RELEASE_TOKEN`:** create a project access token (scopes `api` + `write_repository`, role
  Maintainer, ~1yr expiry); store as a **masked** project CI variable `RELEASE_TOKEN`. Per-project
  — the data-platform token does not apply here.
- **Protected branch/tags:** confirm `main` allows the token's bot user to push (Maintainer = 40).
  If `v*` tags are protected, the bot also needs create permission on protected **tags**, not only
  the branch.
- **Robot + group vars:** already in place; untouched.

## Verification plan

1. **Lock-sync dry run (local, before trusting CI):** in a clean checkout, edit the workspace
   version, run `cargo update --workspace --offline`, and confirm `git diff Cargo.lock` shows
   **only** the local-crate version bumps (no dependency churn, no network fetch, vendored
   `iceberg-rust` path dep untouched). This is the most CI-annoying piece to debug, so prove it
   offline first.
2. **CI lint:** validate `.gitlab-ci.yml` (`glab ci lint` or the project CI lint endpoint).
3. **Registry push:** after merge to main, confirm the kaniko log shows a push to
   `repo.sovereign-data.org/chameleon/sqlengine/sqe` under the robot, and that `sbom-sign` pulls
   the same path.
4. **Live release (confirm with user before triggering):** cut a real `release/*` MR. Confirm:
   bump commit `chore(release): vX.Y.Z` on main, tag `vX.Y.Z`, tag pipeline builds
   `:<version>` + `:stable` to Harbor, `sbom-sign-release` succeeds against the built image, and the
   GitLab Release object is created. This creates a permanent tag + version bump + images.

## Out of scope (YAGNI)

- Changing the Harbor robot or group variables (already correct).
- Multi-arch in CI (kaniko is single-arch).
- Updating Helm `values.yaml` `repository: sqe` to the Harbor path — deployments override the image
  ref and nothing in CI depends on it; can be a later follow-up.
- Rotating any exposed admin credential (a data-platform concern; not applicable here).

## Robot expiry / rotation (record)

- Robot `robot$vpf-ci-pusher` **expires 2027-06-24**. Before then, recreate it and update the two
  group variables (no code change). Consider a reminder ~2027-05-24. (Owned at the group level,
  shared with data-platform.)
