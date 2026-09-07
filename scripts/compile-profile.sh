#!/usr/bin/env bash
# Profile where rustc spends its time compiling ONE workspace crate, and
# attribute the trait-solving time to the functions that cause it.
#
#   scripts/compile-profile.sh sqe-coordinator
#   scripts/compile-profile.sh sqe-catalog /tmp/prof-catalog
#
# Runs on the pinned stable toolchain: RUSTC_BOOTSTRAP=1 unlocks the -Z flags
# for this one invocation, and `cargo rustc -p <crate> --lib` passes them to
# that crate's rustc only, so the dependency tree stays warm. Needs measureme's
# tools once:
#
#   cargo install --git https://github.com/rust-lang/measureme summarize crox
#
# Output: pass timings (-Ztime-passes), the per-query summary, and the
# attribution table from scripts/compile-profile-attribute.py. Read the
# attribution first: on 2026-09-07 it showed that 58 of the coordinator's
# 68 s were `evaluate_obligation` (auto-trait proofs over async state
# machines) rooted in a handful of `async_trait` methods; boxing four futures
# took the crate to 18 s. See docs/internal/plans/2026-09-07-compile-time-async-hubs.md.
set -euo pipefail

crate="${1:?usage: compile-profile.sh <crate> [out-dir]}"
out="${2:-target/compile-profile/$crate}"
here="$(cd "$(dirname "$0")" && pwd)"

for tool in summarize crox; do
  command -v "$tool" >/dev/null || {
    echo "missing $tool: cargo install --git https://github.com/rust-lang/measureme summarize crox" >&2
    exit 1
  }
done

rm -rf "$out" && mkdir -p "$out"
export CARGO_INCREMENTAL=0 RUSTC_BOOTSTRAP=1

# Force this crate (only) to recompile.
lib_rs="$(cargo metadata --no-deps --format-version 1 \
  | python3 -c 'import json,sys; n=sys.argv[1]; m=json.load(sys.stdin)
for p in m["packages"]:
    if p["name"]==n:
        for t in p["targets"]:
            if "lib" in t["kind"]: print(t["src_path"])' "$crate")"
touch "$lib_rs"

echo "== compiling $crate with -Zself-profile (output in $out)"
cargo rustc -p "$crate" --lib -- \
  -Zself-profile="$out" -Zself-profile-events=default,args -Ztime-passes \
  2> "$out/rustc.log" || { tail -40 "$out/rustc.log"; exit 1; }

echo "== passes over 1 s"
grep -E '^time:' "$out/rustc.log" | sort -t: -k2 -rn | awk '$2+0 > 1.0' | head -12

prefix="$(ls "$out"/*.mm_profdata | head -1 | sed 's/\.mm_profdata$//')"
echo "== top queries by self time"
summarize summarize "$prefix" | sed -n 1,12p

echo "== attribution of evaluate_obligation to root functions"
( cd "$out" && crox --minimum-duration 1 "$(basename "$prefix")" >/dev/null )
python3 "$here/compile-profile-attribute.py" "$out/chrome_profiler.json"
