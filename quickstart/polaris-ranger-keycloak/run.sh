#!/usr/bin/env bash
# Bring up the Polaris + Ranger + Keycloak stack and run the access-control test.
#
#   ./run.sh          # up (build SQE first run) -> wait -> ./test.sh
#   ./run.sh --down   # tear everything down
#   ./run.sh --check  # same as default: test.sh IS the assertion harness
set -euo pipefail
cd "$(dirname "$0")"

# Note: this scenario does not source ../_shared/lib.sh. Its assertion harness is
# test.sh (masking, deny precedence, grant/revoke, SHOW GRANTS), which keeps its
# own PASS/FAIL count, prints a "RESULT: N passed, M failed" line, and exits
# non-zero on any failure. --check just runs that harness and propagates its exit
# code -- there is no separate OUTPUT.md to assert against here.
for a in "$@"; do
  case "$a" in
    --down) docker compose down -v; echo "torn down"; exit 0 ;;
    --check) ;; # handled below -- the default flow already runs test.sh
    *) echo "unknown arg: $a (use --down or --check)"; exit 1 ;;
  esac
done

[ -f .env ] || { echo "creating .env from .env.example"; cp .env.example .env; }
set -a; . ./.env; set +a

echo "bringing the stack up (Ranger Admin first-boot takes 2-4 min; SQE builds on first run)"
docker compose up -d --build --wait
echo "all services healthy and bootstrap completed"

./test.sh
