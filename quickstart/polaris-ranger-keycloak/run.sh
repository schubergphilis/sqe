#!/usr/bin/env bash
# Bring up the Polaris + Ranger + Keycloak stack and run the access-control test.
#
#   ./run.sh          # up (build SQE first run) -> wait -> ./test.sh
#   ./run.sh --down   # tear everything down
set -euo pipefail
cd "$(dirname "$0")"

for a in "$@"; do
  case "$a" in
    --down) docker compose down -v; echo "torn down"; exit 0 ;;
    *) echo "unknown arg: $a (use --down)"; exit 1 ;;
  esac
done

[ -f .env ] || { echo "creating .env from .env.example"; cp .env.example .env; }
set -a; . ./.env; set +a

echo "bringing the stack up (Ranger Admin first-boot takes 2-4 min; SQE builds on first run)"
docker compose up -d --build --wait
echo "all services healthy and bootstrap completed"

./test.sh
