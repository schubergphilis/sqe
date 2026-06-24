#!/usr/bin/env bash
# CI release-branch version bump.
#
# Runs on a `main` push (see .gitlab-ci.yml `version-bump`). If the just-merged
# MR came from a `release/*` branch, bump the workspace version, sync Cargo.lock,
# commit `chore(release): vX.Y.Z` back to `main` via the API, and create the
# `vX.Y.Z` tag. The tag pipeline then builds and publishes the release images.
#
# Bump level comes from the MR title: `#major` -> major, `#minor` -> minor,
# else patch. Current version is the highest `vX.Y.Z` git tag (seed v0.36.0).
#
# Requires: git, jq, curl, cargo (rust image). Auth: $RELEASE_TOKEN (project
# access token, scopes api + write_repository, Maintainer).
#
# Deliberately NO `[skip ci]` on the bump commit: `[skip ci]` is read from a
# tag's target commit and would suppress the tag pipeline that builds the
# images. The redundant image rebuild on the bump commit is instead skipped by
# the `chore(release)` `when: never` rule on build-sqe / sbom-sign.

set -euo pipefail

SEED_VERSION="0.36.0"
API="${CI_API_V4_URL}/projects/${CI_PROJECT_ID}"
AUTH=(--header "PRIVATE-TOKEN: ${RELEASE_TOKEN:?RELEASE_TOKEN must be set}")

echo "==> Looking up the merged MR for ${CI_COMMIT_SHA}"
# This job runs on every main push, so a transient API hiccup on the lookup
# must not redden an unrelated feature merge. Tolerate a failed lookup: warn
# and exit 0 (the job is retryable, and the merge has already landed). The
# commit/tag steps below stay fail-loud once we know this is a release.
if ! MRS=$(curl -sf "${AUTH[@]}" \
  "${API}/repository/commits/${CI_COMMIT_SHA}/merge_requests"); then
  echo "WARNING: MR lookup failed (API hiccup?). Skipping; retry this job if a release was expected."
  exit 0
fi

# The merge commit maps to its MR. Pick the merged one.
SOURCE_BRANCH=$(echo "$MRS" | jq -r '[.[] | select(.state == "merged")] | .[0].source_branch // empty')
MR_TITLE=$(echo "$MRS" | jq -r '[.[] | select(.state == "merged")] | .[0].title // empty')
MR_REF=$(echo "$MRS" | jq -r '[.[] | select(.state == "merged")] | .[0].references.full // empty')

if [[ -z "$SOURCE_BRANCH" ]]; then
  echo "No merged MR found for this commit; not a release. Exiting 0."
  exit 0
fi
if [[ ! "$SOURCE_BRANCH" =~ ^release/ ]]; then
  echo "MR source branch '${SOURCE_BRANCH}' is not release/*; not a release. Exiting 0."
  exit 0
fi
echo "    Release MR: ${MR_REF} (source ${SOURCE_BRANCH})"
echo "    Title: ${MR_TITLE}"

# Bump level from the MR title.
if [[ "$MR_TITLE" == *"#major"* ]]; then
  LEVEL="major"
elif [[ "$MR_TITLE" == *"#minor"* ]]; then
  LEVEL="minor"
else
  LEVEL="patch"
fi

echo "==> Determining current version"
git fetch --tags --quiet origin
CURRENT=$(git tag -l 'v*' | sed -n 's/^v\([0-9]\+\.[0-9]\+\.[0-9]\+\)$/\1/p' | sort -V | tail -1)
CURRENT="${CURRENT:-$SEED_VERSION}"
IFS='.' read -r MAJOR MINOR PATCH <<<"$CURRENT"

case "$LEVEL" in
  major) MAJOR=$((MAJOR + 1)); MINOR=0; PATCH=0 ;;
  minor) MINOR=$((MINOR + 1)); PATCH=0 ;;
  patch) PATCH=$((PATCH + 1)) ;;
esac
NEXT="${MAJOR}.${MINOR}.${PATCH}"
TAG="v${NEXT}"
echo "    ${CURRENT} -> ${NEXT} (${LEVEL} bump from MR title)"

if git rev-parse -q --verify "refs/tags/${TAG}" >/dev/null; then
  echo "ERROR: tag ${TAG} already exists" >&2
  exit 1
fi

echo "==> Writing version into Cargo.toml and syncing Cargo.lock"
# Single source of truth: [workspace.package] version. The root is a virtual
# manifest, so that is its only `version = ` line under the section.
perl -i -0pe 's/(\[workspace\.package\]\n(?:[^\[]*?))version = ".*?"/${1}version = "'"$NEXT"'"/s' Cargo.toml
if ! grep -A1 '\[workspace.package\]' Cargo.toml | grep -q "version = \"${NEXT}\""; then
  echo "ERROR: failed to bump [workspace.package] version in Cargo.toml" >&2
  exit 1
fi
# Offline: only the 17 workspace-member version entries change, no dep churn.
cargo update --workspace --offline

echo "==> Committing ${TAG} to main via the API"
COMMIT_PAYLOAD=$(jq -n \
  --arg branch "main" \
  --arg msg "chore(release): ${TAG}" \
  --arg toml "$(cat Cargo.toml)" \
  --arg lock "$(cat Cargo.lock)" \
  '{
    branch: $branch,
    commit_message: $msg,
    actions: [
      { action: "update", file_path: "Cargo.toml", content: $toml },
      { action: "update", file_path: "Cargo.lock", content: $lock }
    ]
  }')
NEW_SHA=$(curl -sf "${AUTH[@]}" \
  --header "Content-Type: application/json" \
  --data "$COMMIT_PAYLOAD" \
  "${API}/repository/commits" | jq -r '.id')
if [[ -z "$NEW_SHA" || "$NEW_SHA" == "null" ]]; then
  echo "ERROR: commit creation failed" >&2
  exit 1
fi
echo "    Committed ${NEW_SHA}"

echo "==> Creating tag ${TAG} on ${NEW_SHA}"
curl -sf "${AUTH[@]}" \
  --data-urlencode "tag_name=${TAG}" \
  --data-urlencode "ref=${NEW_SHA}" \
  --data-urlencode "message=Release ${TAG} (${MR_REF})" \
  "${API}/repository/tags" >/dev/null
echo "    Tagged ${TAG}. The tag pipeline will publish the release images."
