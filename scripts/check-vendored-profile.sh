#!/usr/bin/env bash
# Are SQE's vendored grant-profile assets identical to data-platform's?
#
# The fixture tests in crates/sqe-policy/src/grants/profile.rs prove SQE agrees
# with the profile it HOLDS. They cannot notice that the profile it holds is
# stale. This proves the second half. Without both, SQE can be perfectly
# self-consistent against a dead contract, which is exactly how the two drifted
# the first time.
#
# Needs a data-platform checkout. Set DATA_PLATFORM_DIR, or keep the repos as
# siblings (the default).
#
#   scripts/check-vendored-profile.sh            # compare
#   scripts/check-vendored-profile.sh --update    # copy platform -> SQE, then show the diff
#
# Exit codes: 0 identical, 1 drifted, 2 cannot compare (missing checkout).
#
# 2 is deliberately distinct from 1. A CI job that cannot find the sibling repo
# must say so loudly rather than pass, because a silent skip on a drift gate is
# indistinguishable from "no drift" and that is the failure this file exists to
# prevent.
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DP_DIR="${DATA_PLATFORM_DIR:-$ROOT_DIR/../data-platform}"
SRC_DIR="$DP_DIR/quickstart/assets/ranger"
DST_DIR="$ROOT_DIR/crates/sqe-policy/assets"

# One file since profile v5, which folded the access-type implication graph in.
# `servicedef-polaris.json` is NOT listed: it is still the Ranger service
# DEFINITION the quickstarts register with Ranger Admin, but it is no longer an
# input to planning, so the platform is free to move or drop its shared copy
# without this gate having an opinion.
FILES=(grant-profile.json)

if [ ! -d "$SRC_DIR" ]; then
    echo "check-vendored-profile: CANNOT COMPARE" >&2
    echo "  no data-platform checkout at: $DP_DIR" >&2
    echo "  set DATA_PLATFORM_DIR=/path/to/data-platform, or clone it beside this repo." >&2
    echo "  NOT treating this as a pass: the vendored profile may be stale." >&2
    exit 2
fi

if [ "${1:-}" = "--update" ]; then
    for f in "${FILES[@]}"; do
        cp "$SRC_DIR/$f" "$DST_DIR/$f"
    done
    echo "updated. Review the diff, re-run the fixture tests, and bump the"
    echo "version_pinned assertion in crates/sqe-policy/src/grants/profile.rs if the"
    echo "profile version changed:"
    echo "  cargo test -p sqe-policy --lib grants::profile"
    git -C "$ROOT_DIR" --no-pager diff --stat -- "crates/sqe-policy/assets" || true
    exit 0
fi

drift=0
for f in "${FILES[@]}"; do
    if [ ! -f "$SRC_DIR/$f" ]; then
        echo "check-vendored-profile: CANNOT COMPARE: $SRC_DIR/$f is missing" >&2
        exit 2
    fi
    if diff -q "$SRC_DIR/$f" "$DST_DIR/$f" >/dev/null 2>&1; then
        echo "  ok       $f"
    else
        echo "  DRIFTED  $f"
        diff -u "$DST_DIR/$f" "$SRC_DIR/$f" | head -40
        drift=1
    fi
done

if [ "$drift" -ne 0 ]; then
    cat >&2 <<'EOF'

check-vendored-profile: the vendored profile is not the platform's current one.

SQE and the control plane write Ranger policies to the same `polaris` service. If
they plan differently, the same GRANT means different things depending on which
tool issued it.

  scripts/check-vendored-profile.sh --update

then re-run `cargo test -p sqe-policy --lib grants::profile`. Expect the golden
fixtures to move; that is the point of them.
EOF
    exit 1
fi

echo "check-vendored-profile: vendored assets match $DP_DIR"
