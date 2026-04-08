#!/usr/bin/env bash
# scripts/retro-tag.sh — Create annotated git tags for historical MRs.
#
# Reads the MR-to-version mapping, resolves each MR's merge commit SHA
# via glab API, and creates annotated tags. Idempotent (skips existing tags).
#
# Requires: Bash 4+ (associative arrays), glab CLI authenticated with GitLab.
#
# Usage: ./scripts/retro-tag.sh [--dry-run] [--push]

# Bash 4+ required for associative arrays
if ((BASH_VERSINFO[0] < 4)); then
    # Try homebrew bash on macOS
    if [[ -x /opt/homebrew/bin/bash ]]; then
        exec /opt/homebrew/bin/bash "$0" "$@"
    fi
    echo "ERROR: Bash 4+ required. Install with: brew install bash" >&2
    exit 1
fi

set -euo pipefail

DRY_RUN=false
PUSH=false
for arg in "$@"; do
    case "$arg" in
        --dry-run) DRY_RUN=true ;;
        --push)    PUSH=true ;;
        *)         echo "Usage: $0 [--dry-run] [--push]"; exit 1 ;;
    esac
done

# MR IID → version mapping (only merged MRs)
declare -A MR_VERSION=(
    [1]="v0.1.0"   [2]="v0.1.1"   [3]="v0.2.0"   [4]="v0.3.0"
    [5]="v0.4.0"   [6]="v0.5.0"   [7]="v0.6.0"   [8]="v0.6.1"
    [9]="v0.6.2"   [10]="v0.6.3"  [11]="v0.6.4"  [12]="v0.7.0"
    [13]="v0.7.1"  [14]="v0.7.2"  [15]="v0.8.0"  [16]="v0.8.1"
    [17]="v0.9.0"  [18]="v0.10.0" [19]="v0.11.0" [20]="v0.12.0"
    [21]="v0.13.0" [22]="v0.13.1" [23]="v0.14.0" [24]="v0.29.0"
    [25]="v0.15.1" [26]="v0.15.2" [27]="v0.16.0" [28]="v0.16.1"
    [29]="v0.16.2" [30]="v0.16.3" [31]="v0.16.4" [32]="v0.17.0"
    [33]="v0.18.0" [34]="v0.19.0" [35]="v0.20.0" [36]="v0.21.0"
    [37]="v0.22.0" [38]="v0.23.0" [39]="v0.24.0" [42]="v0.24.1"
    [43]="v0.25.0" [45]="v0.26.0" [46]="v0.27.0" [47]="v0.28.0"
)

CREATED=0
SKIPPED=0
FAILED=0

# Sort MR IIDs numerically for deterministic ordering
for mr_iid in $(printf '%s\n' "${!MR_VERSION[@]}" | sort -n); do
    version="${MR_VERSION[$mr_iid]}"

    # Skip if tag already exists
    if git rev-parse "$version" &>/dev/null; then
        echo "SKIP  $version (tag exists)"
        SKIPPED=$((SKIPPED + 1))
        continue
    fi

    # Resolve merge commit SHA via glab API (parse JSON with python3)
    sha=$(glab api "projects/:fullpath/merge_requests/$mr_iid" 2>/dev/null \
        | python3 -c "import sys,json; print(json.load(sys.stdin).get('merge_commit_sha',''))" 2>/dev/null || true)

    if [[ -z "$sha" || "$sha" == "null" ]]; then
        echo "FAIL  $version (!$mr_iid) — could not resolve merge commit"
        FAILED=$((FAILED + 1))
        continue
    fi

    # Verify the commit exists locally
    if ! git cat-file -e "$sha" 2>/dev/null; then
        echo "FAIL  $version (!$mr_iid) — commit $sha not found locally (run git fetch)"
        FAILED=$((FAILED + 1))
        continue
    fi

    if $DRY_RUN; then
        echo "DRY   $version → $sha (!$mr_iid)"
    else
        git tag -a "$version" "$sha" -m "Release $version (MR !$mr_iid)"
        echo "TAG   $version → $sha (!$mr_iid)"
    fi
    CREATED=$((CREATED + 1))
done

echo ""
echo "Summary: created=$CREATED skipped=$SKIPPED failed=$FAILED"

if $PUSH && ! $DRY_RUN && [[ $CREATED -gt 0 ]]; then
    echo "Pushing tags to origin..."
    git push origin --tags
    echo "Done."
fi
