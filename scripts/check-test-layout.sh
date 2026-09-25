#!/usr/bin/env bash
# One integration-test binary per crate: tests/it/main.rs, or a single
# tests/<name>.rs. Every extra top-level tests/*.rs is its own binary that links
# DataFusion and the engine again. 97 of them cost ~500 CPU-s of a cold
# `cargo test --workspace --no-run` before they were folded into `it`
# (2026-09-25); this check keeps them from growing back, as they did once.
set -euo pipefail
cd "$(dirname "$0")/.."

bad=""
for d in crates/*/tests; do
  # Runs by SIGABRT on regression and needs a process per case; see its docs.
  n=$(find "$d" -maxdepth 1 -name '*.rs' ! -name in_subquery_or_stack_overflow.rs | wc -l)
  if { [ -d "$d/it" ] && [ "$n" -gt 0 ]; } || [ "$n" -gt 1 ]; then
    bad="$bad $d"
  fi
done

if [ -n "$bad" ]; then
  echo "add integration tests as modules of tests/it/main.rs, not new tests/*.rs files:"
  for d in $bad; do find "$d" -maxdepth 1 -name '*.rs' | sed 's/^/  /'; done
  exit 1
fi
echo "==> test layout ok (one integration-test binary per crate)"
