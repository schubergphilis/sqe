#!/usr/bin/env bash
# Bring up the spark-parity stack and run the parity test.
#
# Proves byte-exact parity of Ranger MASK_SHOW_LAST_4 between SQE and
# Spark 3.5 (Kyuubi Authz extension). Both engines query the same Iceberg
# table in Polaris and must return identical masked ssn values.
#
#   ./run.sh          # up (build SQE + Spark first run) -> wait -> ./parity-test.sh
#   ./run.sh --down   # tear everything down
#
# First run builds SQE (~5 min on cold Rust cache) and downloads ~300 MB of
# Spark/Iceberg/Kyuubi jars. Ranger Admin first-boot takes 2-4 min.
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

echo "bringing the stack up (Ranger Admin first-boot: 2-4 min; SQE build: ~5 min; Spark jars: ~300 MB)"
docker compose up -d --build --wait
echo "all services healthy and bootstraps completed"

./parity-test.sh
