#!/usr/bin/env bash
# Cut a new SQE release.
#
# Usage:
#   scripts/release.sh <version>          (creates commit + tag, does not push)
#   scripts/release.sh <version> --push   (also pushes branch + tag)
#
# What it does:
#   1. Bumps `version = "..."` in every workspace crate's Cargo.toml to <version>.
#      Skips `xtask/Cargo.toml` (internal tooling pinned at 0.0.0).
#   2. Updates Cargo.lock via `cargo check --offline` (no network).
#   3. Creates a chore(release) commit and an annotated tag `v<version>`.
#   4. Optionally pushes both to `origin`.
#
# Why this script:
#   Tags and Cargo.toml drifted across the v0.16..v0.31 history (all 14 crates
#   stayed at 0.15.0 while tags moved to 0.31.x). This script makes the bump
#   atomic with the tag so it can never drift again. See docs/RELEASING.md.

set -euo pipefail

if [[ $# -lt 1 ]]; then
    echo "usage: $0 <version> [--push]" >&2
    echo "  e.g. $0 0.31.4" >&2
    exit 2
fi

VERSION="$1"
PUSH=false
if [[ "${2:-}" == "--push" ]]; then
    PUSH=true
fi

# Sanity-check the version format. Accept x.y.z plus optional -prerelease.
if [[ ! "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[a-zA-Z0-9.-]+)?$ ]]; then
    echo "error: '$VERSION' is not a valid semver. Expected x.y.z or x.y.z-prerelease" >&2
    exit 2
fi

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

# Refuse to run on a dirty tree — version bumps must land in a clean commit.
if [[ -n "$(git status --porcelain)" ]]; then
    echo "error: working tree is dirty. Commit or stash first." >&2
    exit 1
fi

# Refuse to tag the same version twice.
if git rev-parse -q --verify "refs/tags/v${VERSION}" >/dev/null; then
    echo "error: tag v${VERSION} already exists" >&2
    exit 1
fi

echo "==> Bumping crate versions to ${VERSION}"
# Since f7a7e81 every crate inherits its version from [workspace.package] in
# the root Cargo.toml (`version.workspace = true`), so the single source of
# truth is the root manifest. Bump only the line inside [workspace.package];
# the root is a virtual manifest, so that is its only `version = ` line.
perl -i -0pe 's/(\[workspace\.package\]\n(?:[^\[]*?))version = ".*?"/${1}version = "'"$VERSION"'"/s' Cargo.toml
echo "  Cargo.toml [workspace.package] -> $VERSION"

# Belt and braces: bump any crate that still carries its own version line
# instead of inheriting from the workspace.
for crate_toml in crates/*/Cargo.toml; do
    if ! grep -q '^version = ' "$crate_toml"; then
        continue
    fi
    perl -i -pe 's/^version = ".*"$/version = "'"$VERSION"'"/' "$crate_toml"
    echo "  $crate_toml -> $VERSION"
done

if ! grep -A1 '\[workspace.package\]' Cargo.toml | grep -q "version = \"${VERSION}\""; then
    echo "error: failed to bump [workspace.package] version in Cargo.toml" >&2
    exit 1
fi

echo "==> Refreshing Cargo.lock"
cargo check --workspace --offline --quiet 2>&1 | tail -3 || true
cargo check --workspace --quiet 2>&1 | tail -3

if [[ -z "$(git status --porcelain)" ]]; then
    echo "error: nothing changed. Were the crate versions already at ${VERSION}?" >&2
    exit 1
fi

echo "==> Committing"
git add Cargo.toml crates/*/Cargo.toml Cargo.lock
git commit -m "chore(release): ${VERSION}

Bump all workspace crate versions to ${VERSION} ahead of the v${VERSION}
tag so \`cargo --version\` matches the git tag. Tooling: scripts/release.sh."

echo "==> Tagging v${VERSION}"
git tag -a "v${VERSION}" -m "Release v${VERSION}"

if $PUSH; then
    BRANCH="$(git symbolic-ref --short HEAD)"
    echo "==> Pushing ${BRANCH} + v${VERSION}"
    git push origin "$BRANCH"
    git push origin "v${VERSION}"
else
    echo ""
    echo "Done. Review:  git show v${VERSION}"
    echo "Push when ready:  git push origin HEAD && git push origin v${VERSION}"
fi
