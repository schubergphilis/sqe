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
#
# Requires: Docker, python3. The TPC-H scale is tiny (tpch.tiny ~= SF0.01).
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT_DIR"

COMPOSE=(docker compose -f docker-compose.test.yml -f docker-compose.compare.yml -f docker-compose.parity.yml)
SCHEMA="tpch_demo"
TABLES=(nation region customer orders lineitem part supplier partsupp)
BUILD_FLAG="--build"
RELOAD=0
for arg in "$@"; do
  case "$arg" in
    --no-build) BUILD_FLAG="" ;;
    --reload)   RELOAD=1 ;;
    *) echo "unknown arg: $arg" >&2; exit 2 ;;
  esac
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
python3 tests/parity/parity_compare.py --schema "$SCHEMA"
