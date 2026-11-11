#!/usr/bin/env bash
# Seed a fresh checkout's target/ from an already-built one.
#
# A second checkout (a git worktree, a second clone) starts with an empty
# target/ and pays the full cold build. Most of that build is registry crates
# whose fingerprints do not depend on where the workspace lives, so cargo
# accepts a copy of them as fresh. The copy is a copy-on-write clone (APFS
# clonefile, reflink on btrfs/XFS): it takes seconds and uses no extra disk
# until a file is rewritten.
#
# Each checkout keeps its own target/ and its own build lock, so two checkouts
# build at the same time. A shared target or build directory would not allow that.
#
# Usage: scripts/seed-target.sh <source-checkout> [profile ...]
#   run from the checkout to seed; profiles default to "debug".
set -euo pipefail

if [ "$#" -lt 1 ]; then
    echo "usage: $0 <source-checkout> [profile ...]" >&2
    exit 2
fi

src_root=$(cd "$1" && pwd)
shift
dst_root=$(git rev-parse --show-toplevel)
if [ "$#" -eq 0 ]; then
    set -- debug
fi

if [ "$src_root" = "$dst_root" ]; then
    echo "seed-target: source and destination are the same checkout" >&2
    exit 2
fi

case "$(uname -s)" in
    Darwin) clone=(cp -c -R) ;;
    *) clone=(cp -R --reflink=auto) ;;
esac

for profile in "$@"; do
    src="$src_root/target/$profile"
    dst="$dst_root/target/$profile"
    if [ ! -d "$src" ]; then
        echo "seed-target: $src does not exist, skipping" >&2
        continue
    fi
    if [ -e "$dst" ]; then
        echo "seed-target: $dst already exists, leaving it alone" >&2
        continue
    fi
    mkdir -p "$dst"
    # incremental/ is keyed on the workspace path and is off in this repo;
    # the top-level binaries are relinked anyway.
    for part in .fingerprint build deps; do
        if [ -d "$src/$part" ]; then
            "${clone[@]}" "$src/$part" "$dst/$part"
        fi
    done
    echo "seed-target: seeded $dst from $src"
done
