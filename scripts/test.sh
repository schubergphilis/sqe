#!/usr/bin/env bash
# Unified SQE test entry point. Two tiers:
#   engine    Tier 1 - cargo integration tests (delegates to integration-test.sh)
#   scenario  Tier 2 - quickstart run.sh --check scenarios
#   ci        Tier 1 + the self-contained Tier-2 scenarios (what CI runs)
#
#   scripts/test.sh engine
#   scripts/test.sh scenario <name>|all
#   scripts/test.sh ci
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

SELF_CONTAINED=(polaris-keycloak-client-id polaris-keycloak-user-token \
  polaris-ranger-keycloak nessie unity-oss embedded-files \
  embedded-sqlite-catalog attach-catalogs quack observability benchmark)

# run_scenario is generic: any quickstart/<name>/run.sh --check works here,
# including the heavy `distributed` scenario (coordinator + 2 workers). That one
# is deliberately absent from SELF_CONTAINED, so it never runs under `all`/`ci`;
# invoke it explicitly with `scripts/test.sh scenario distributed`.
run_scenario() {
  local name=$1 dir="$ROOT_DIR/quickstart/$1"
  [ -d "$dir" ] || { echo "no such scenario: $name" >&2; return 2; }
  echo "━━━ scenario: $name ━━━"
  ( cd "$dir" && ./run.sh --check )
}

cmd=${1:-ci}
case "$cmd" in
  engine)
    "$ROOT_DIR/scripts/integration-test.sh" "${@:2}" ;;
  scenario)
    target=${2:-all}
    if [ "$target" = all ]; then
      rc=0
      for s in "${SELF_CONTAINED[@]}"; do run_scenario "$s" || rc=1; done
      exit $rc
    else
      run_scenario "$target"
    fi ;;
  ci)
    "$ROOT_DIR/scripts/integration-test.sh"
    rc=0
    for s in "${SELF_CONTAINED[@]}"; do run_scenario "$s" || rc=1; done
    exit $rc ;;
  *) echo "usage: test.sh {engine|scenario <name>|all|ci}" >&2; exit 2 ;;
esac
