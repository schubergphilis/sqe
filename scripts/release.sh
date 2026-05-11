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
# Match exactly `version = "x.y.z"` on a line by itself (not inside [dependencies]).
# `crates/*/Cargo.toml` covers every published crate; the root and xtask are
# excluded intentionally.
for crate_toml in crates/*/Cargo.toml; do
    if ! grep -q '^version = ' "$crate_toml"; then
        echo "  skip $crate_toml (no version line)"
        continue
    fi
    # In-place replace the first version line (the [package] one).
    # Use a portable sed pattern that works on macOS and Linux.
    perl -i -pe 's/^version = ".*"$/version = "'"$VERSION"'"/' "$crate_toml"
    echo "  $crate_toml -> $VERSION"
done

echo "==> Refreshing Cargo.lock"
cargo check --workspace --offline --quiet 2>&1 | tail -3 || true
cargo check --workspace --quiet 2>&1 | tail -3

if [[ -z "$(git status --porcelain)" ]]; then
    echo "error: nothing changed. Were the crate versions already at ${VERSION}?" >&2
    exit 1
fi

echo "==> Committing"
git add crates/*/Cargo.toml Cargo.lock
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
