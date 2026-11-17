#!/usr/bin/env bash
set -euo pipefail
# SQE-vs-Trino result parity on shared Polaris/Iceberg tables.
#
# Brings up SQE + a real Trino baseline, both pointed at the same Polaris REST
# catalog, loads TPC-H demo data once (CTAS via Trino's built-in tpch connector
# into the shared iceberg catalog so BOTH engines see identical tables), then
# runs tests/parity/parity_compare.py and exits non-zero on any divergence.
#
# Usage:
#   scripts/parity-test.sh                 # up stack, load demo data, compare
#   scripts/parity-test.sh --no-build      # skip the SQE image rebuild
#   scripts/parity-test.sh --reload        # drop + reload the demo schema first
#   scripts/parity-test.sh --protocol both # SQE over Trino HTTP and Flight SQL
#   scripts/parity-test.sh --all           # also Iceberg scenarios + function coverage
#
# Requires: Docker, python3. The TPC-H scale is tiny (tpch.tiny ~= SF0.01).
# --protocol flight|both needs `pip install -r tests/parity/requirements.txt`
# (set PARITY_PYTHON to a venv's python). Reports land in $PARITY_OUT
# (default target/parity).
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT_DIR"

COMPOSE=(docker compose -f docker-compose.test.yml -f docker-compose.compare.yml -f docker-compose.parity.yml)
SCHEMA="tpch_demo"
TABLES=(nation region customer orders lineitem part supplier partsupp)
BUILD_FLAG="--build"
RELOAD=0
ALL=0
PROTOCOL=http
PY="${PARITY_PYTHON:-python3}"
OUT="${PARITY_OUT:-target/parity}"
while [ $# -gt 0 ]; do
  case "$1" in
    --no-build) BUILD_FLAG="" ;;
    --reload)   RELOAD=1 ;;
    --all)      ALL=1 ;;
    --protocol) PROTOCOL="${2:?--protocol needs http, flight or both}"; shift ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
  shift
done

echo "=== SQE-vs-Trino parity: shared Polaris catalog ==="
# rustfs (S3) is not in any depends_on chain; start it explicitly.
"${COMPOSE[@]}" up -d $BUILD_FLAG rustfs polaris sqe trino

echo "Bootstrapping (creates bucket + Polaris warehouse, idempotent)..."
"$SCRIPT_DIR/bootstrap-test.sh"

echo "Waiting for SQE and Trino..."
timeout 120 bash -c 'until curl -sf http://localhost:28080/v1/info >/dev/null; do sleep 2; done'
timeout 180 bash -c 'until curl -sf http://localhost:38080/v1/info | grep -q "\"starting\":false"; do sleep 2; done'

# ── Load demo data once (CTAS via Trino's tpch connector) ──────────────
texec() {
  "${COMPOSE[@]}" exec -T trino \
    trino --server http://localhost:8080 --catalog iceberg --user root --execute "$1"
}
SCHEMA_EXISTS=$(texec "SHOW SCHEMAS FROM iceberg LIKE '$SCHEMA'" 2>/dev/null | grep -c "$SCHEMA" || true)
if [ "$RELOAD" = "1" ] || [ "$SCHEMA_EXISTS" = "0" ]; then
  echo "Loading TPC-H tiny demo data into iceberg.$SCHEMA via Trino..."
  [ "$RELOAD" = "1" ] && texec "DROP SCHEMA IF EXISTS iceberg.$SCHEMA CASCADE" >/dev/null 2>&1 || true
  texec "CREATE SCHEMA IF NOT EXISTS iceberg.$SCHEMA" >/dev/null 2>&1 || true
  for t in "${TABLES[@]}"; do
    texec "CREATE TABLE IF NOT EXISTS iceberg.$SCHEMA.$t AS SELECT * FROM tpch.tiny.$t" >/dev/null 2>&1 \
      && echo "  loaded $t" || echo "  $t already present"
  done
else
  echo "Demo schema iceberg.$SCHEMA already present (use --reload to recreate)."
fi

# ── Compare ────────────────────────────────────────────────────────────
echo ""
mkdir -p "$OUT"
if [ "$ALL" = "0" ]; then
  "$PY" tests/parity/parity_compare.py --schema "$SCHEMA" --protocol "$PROTOCOL" \
    --report "$OUT/queries.json"
  exit $?
fi
# --all: run every suite, then fail if any failed.
rc=0
"$PY" tests/parity/parity_compare.py --schema "$SCHEMA" --protocol "$PROTOCOL" \
  --report "$OUT/queries.json" || rc=1
"$PY" tests/parity/iceberg_parity.py --protocol "$PROTOCOL" || rc=1
"$PY" tests/parity/function_coverage.py --schema "$SCHEMA" --protocol "$PROTOCOL" \
  --report "$OUT/functions.json" --markdown "$OUT/functions.md" || rc=1
exit $rc
