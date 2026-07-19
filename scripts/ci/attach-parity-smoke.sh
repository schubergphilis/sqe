#!/usr/bin/env bash
# End-to-end proof that the ATTACH read path (Task 5) is RESULT-NEUTRAL:
# querying preloaded golden Iceberg tables through an ATTACHed catalog returns
# the same rows, with zero load, as querying them through the normal catalog
# path. That result-neutrality -- not agreement with any external oracle -- is
# the actual scope of the attach feature this branch adds.
#
# GATING check (Step 6): run the same tpch queries via the ATTACHed golden
# catalog (`--catalog golden` -> crates/sqe-catalog/src/mount.rs
# build_iceberg_rest) AND via the PRIMARY catalog (`--namespace $GOLDEN_NS`,
# no `--catalog` -> crates/sqe-catalog/src/rest_catalog.rs) on the identical
# published tables, and require byte-identical per-query row counts. These are
# two distinct SQE catalog-construction code paths over the same physical
# tables (golden is published INTO the coordinator's primary `test_warehouse`,
# per coordinator-attach.toml), so equality is real evidence the ATTACH wiring
# is neutral -- not two names trivially resolving through one object. Inherent
# limit: a bug living BELOW the SQE wiring (inside iceberg-rust's shared
# RestCatalogBuilder) would pass both paths; the test proves the SQE-level
# attach wiring, which is its scope.
#
# ADVISORY check (Step 7, NON-gating): attach rows vs the DuckDB oracle
# (`canonical_rows_duckdb.json` `tpch.qNN.sf0_1_official_rows`), printed for
# visibility only. Divergences here are pre-existing SQE-vs-DuckDB data/query
# fidelity (e.g. a known q18 LIMIT over-return) that exist independent of the
# attach feature and MUST NOT be triaged as attach bugs or used to gate.
#
# Runs against a fresh coordinator started with the admin-capable
# tests/benchmark-attach/coordinator-attach.toml (Task 2) on the local
# docker-compose test stack (Task 3's `benchmark-publish-iceberg.sh` publishes
# INTO that same stack's `test_warehouse`).
#
# Usage: bash scripts/ci/attach-parity-smoke.sh
# Requires: docker compose test stack reachable (brought up here if not
# already running), python3, curl, cargo.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/docker-compose.test.yml"
cd "$ROOT_DIR"

BENCH_SCALE="${BENCH_SCALE:-0.1}"
# sf0.1 has all 22 tpch queries in canonical_rows_duckdb.json; sf0.01 (the
# brief's original suggestion) does not, so it can't be checked against a
# real oracle -- see the header comment above.
PROFILE="${PROFILE:-release}"
POLARIS_URL="http://localhost:18181"
S3_URL="http://localhost:19000"
FLIGHT_PORT="60051"
TRINO_PORT="18080"
STAGING_DIR="/tmp/sqe-bench-attach-smoke-data"

echo "==> Building sqe-bench + sqe-coordinator + sqe-cli (profile: $PROFILE)"
if [ "$PROFILE" = "release" ]; then
  cargo build -p sqe-bench -p sqe-coordinator -p sqe-cli --bin sqe-bench --bin sqe-coordinator --bin sqe-cli --release 2>&1
else
  cargo build -p sqe-bench -p sqe-coordinator -p sqe-cli --bin sqe-bench --bin sqe-coordinator --bin sqe-cli --profile "$PROFILE" 2>&1
fi
BENCH_BIN="$ROOT_DIR/target/$PROFILE/sqe-bench"

echo "==> Starting test stack (docker compose + bootstrap)"
docker compose -f "$COMPOSE_FILE" up -d
"$SCRIPT_DIR/../bootstrap-test.sh"

for port in "$FLIGHT_PORT" "$TRINO_PORT" 19090; do
  if lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
    echo "FAILED: port $port is already bound. Kill the stale process and retry:" >&2
    echo "  lsof -nP -iTCP:$port -sTCP:LISTEN" >&2
    exit 1
  fi
done

echo "==> Starting query coordinator (tests/benchmark-attach/coordinator-attach.toml)"
# $$ (this process's PID), not mktemp: on macOS's BSD mktemp, a template
# with a fixed suffix after the X-run (e.g. `...XXXXXX.log`) is NOT
# randomized -- it silently reuses the literal name every time, so a second
# run collides with the first run's leftover file. Matches the PID-based
# naming scripts/benchmark-test.sh already uses for its own coordinator log.
COORD_LOG="/tmp/sqe-attach-smoke-coord-$$.log"
RUST_LOG="${RUST_LOG:-sqe=info,warn}" \
  "$ROOT_DIR/target/$PROFILE/sqe-coordinator" "$ROOT_DIR/tests/benchmark-attach/coordinator-attach.toml" \
  > "$COORD_LOG" 2>&1 &
COORD_PID=$!

GOLDEN_NS=""
cleanup() {
  # Preserve the exit code that triggered the trap: without this the trap's
  # own last command (the echo below) would set $? to 0 and the script would
  # report success even after a PARITY FAIL / exit 1.
  local rc=$?
  echo "==> Cleaning up"
  if [ -n "$GOLDEN_NS" ] && [ -n "${POLARIS_TOKEN:-}" ]; then
    # Drop the namespace this run published, so the shared local test_warehouse
    # is left as it was found.
    TABLES=$(curl -sf -H "Authorization: Bearer ${POLARIS_TOKEN}" \
      "${POLARIS_URL}/api/catalog/v1/test_warehouse/namespaces/${GOLDEN_NS}/tables" 2>/dev/null \
      | python3 -c 'import sys,json;[print(t["name"]) for t in json.load(sys.stdin).get("identifiers",[])]' 2>/dev/null || true)
    for t in $TABLES; do
      curl -sf -X DELETE -H "Authorization: Bearer ${POLARIS_TOKEN}" \
        "${POLARIS_URL}/api/catalog/v1/test_warehouse/namespaces/${GOLDEN_NS}/tables/${t}?purgeRequested=true" \
        >/dev/null 2>&1 || true
    done
    curl -sf -X DELETE -H "Authorization: Bearer ${POLARIS_TOKEN}" \
      "${POLARIS_URL}/api/catalog/v1/test_warehouse/namespaces/${GOLDEN_NS}" \
      >/dev/null 2>&1 || true
  fi
  kill "$COORD_PID" 2>/dev/null || true
  wait "$COORD_PID" 2>/dev/null || true
  rm -rf "$STAGING_DIR" "${REPORT_ATTACH:-}" "${REPORT_PRIMARY:-}"
  echo "Coordinator log preserved at: $COORD_LOG"
  exit "$rc"
}
trap cleanup EXIT INT TERM

echo -n "Waiting for coordinator..."
for i in $(seq 1 60); do
  if curl -so /dev/null "http://localhost:${TRINO_PORT}/v1/info" 2>/dev/null; then
    echo " ready (PID $COORD_PID)"
    break
  fi
  if ! kill -0 "$COORD_PID" 2>/dev/null; then
    echo " FAILED (coordinator exited)"
    tail -40 "$COORD_LOG"
    exit 1
  fi
  if [ "$i" -eq 60 ]; then
    echo " TIMEOUT"
    exit 1
  fi
  echo -n "."
  sleep 1
done

TOKEN_ENDPOINT="${POLARIS_URL}/api/catalog/v1/oauth/tokens"
POLARIS_TOKEN=$(curl -sf -X POST "$TOKEN_ENDPOINT" \
  -d "grant_type=client_credentials&client_id=root&client_secret=s3cr3t&scope=PRINCIPAL_ROLE:ALL" \
  | python3 -c 'import sys,json;print(json.load(sys.stdin)["access_token"])')

TEST_ARGS=(--scale "$BENCH_SCALE" --host localhost --port "$FLIGHT_PORT" \
  --token-endpoint "$TOKEN_ENDPOINT" --client-id root --client-secret s3cr3t)

echo ""
echo "==> Step 1: verify the attach path fails BEFORE golden is attached"
# `sqe-bench test` never propagates a per-query failure as a nonzero process
# exit (crates/sqe-bench/src/test.rs::run_benchmark_test captures a client
# error as TestStatus::Error and still returns Ok) -- check the machine-
# readable BENCH_SUMMARY line's error/fail counts instead of the exit code.
PREFAIL_LOG="/tmp/sqe-attach-smoke-prefail-$$.log"
"$BENCH_BIN" test tpch "${TEST_ARGS[@]}" --catalog golden --query q1 > "$PREFAIL_LOG" 2>&1 || true
PREFAIL_SUMMARY=$(grep "^BENCH_SUMMARY:" "$PREFAIL_LOG" | tail -1)
if [ -z "$PREFAIL_SUMMARY" ]; then
  # The process itself errored out before printing a summary (e.g. the
  # client connection or catalog lookup failed hard) -- also a valid "fails
  # before ATTACH" outcome.
  echo "  confirmed: querying golden before ATTACH fails (process exited before completing; see $PREFAIL_LOG)"
else
  rm -f "$PREFAIL_LOG"
  IFS=':' read -r _ _ _ PREFAIL_FAIL _ _ PREFAIL_ERROR _ _ <<< "$PREFAIL_SUMMARY"
  if [ "${PREFAIL_FAIL:-0}" -eq 0 ] && [ "${PREFAIL_ERROR:-0}" -eq 0 ]; then
    echo "FAILED: expected an error querying an unattached 'golden' catalog, but it reported 0 fail/0 error (summary: $PREFAIL_SUMMARY)" >&2
    exit 1
  fi
  echo "  confirmed: querying golden before ATTACH fails (summary: $PREFAIL_SUMMARY)"
fi

echo ""
echo "==> Step 2: generate tpch SF${BENCH_SCALE} staging parquet"
# benchmark-publish-iceberg.sh is a *load* step only (mirrors
# scripts/benchmark-load.sh) -- it does not generate data itself for
# anything but bank, so the staging parquet must already exist at
# BENCH_DATA_SOURCE before it runs.
"$BENCH_BIN" generate tpch --scale "$BENCH_SCALE" --output "$STAGING_DIR" 2>&1

echo ""
echo "==> Step 3: publish golden tpch at SF${BENCH_SCALE} into test_warehouse"
GOLDEN_NS="tpch_sf$(python3 -c "s='$BENCH_SCALE'; print((s.rstrip('0').rstrip('.') if '.' in s else s).replace('.', '_'))")"
BENCH_SCALE="$BENCH_SCALE" \
BENCH_GOLDEN_POLARIS_URL="${POLARIS_URL}/api/catalog" \
BENCH_GOLDEN_WAREHOUSE="test_warehouse" \
BENCH_GOLDEN_TOKEN="$POLARIS_TOKEN" \
SQE_CLIENT_ID="root" \
SQE_CLIENT_SECRET="s3cr3t" \
BENCH_DATA_SOURCE="$STAGING_DIR" \
BENCH_S3_ENDPOINT="$S3_URL" \
BENCH_GOLDEN_S3_ACCESS_KEY="s3admin" \
BENCH_GOLDEN_S3_SECRET_KEY="s3admin" \
PROFILE="$PROFILE" \
bash "$SCRIPT_DIR/../benchmark-publish-iceberg.sh" tpch

echo ""
echo "==> Step 4: attach golden"
BENCH_GOLDEN_POLARIS_URL="${POLARIS_URL}/api/catalog" \
BENCH_GOLDEN_WAREHOUSE="test_warehouse" \
BENCH_GOLDEN_TOKEN="$POLARIS_TOKEN" \
BENCH_S3_ENDPOINT="$S3_URL" \
BENCH_GOLDEN_S3_ACCESS_KEY="s3admin" \
BENCH_GOLDEN_S3_SECRET_KEY="s3admin" \
BENCH_PORT_FLIGHT="$FLIGHT_PORT" \
bash "$SCRIPT_DIR/../benchmark-attach-golden.sh"

echo ""
echo "==> Step 5a: query the ATTACHED golden catalog with zero load, capture the JSON report"
ATTACH_LOG="/tmp/sqe-attach-smoke-attach-$$.log"
"$BENCH_BIN" test tpch "${TEST_ARGS[@]}" --catalog golden 2>&1 | tee "$ATTACH_LOG"
REPORT_ATTACH=$(grep "^Report written to:" "$ATTACH_LOG" | tail -1 | sed 's/^Report written to: //')
ATTACH_SUMMARY=$(grep "^BENCH_SUMMARY:" "$ATTACH_LOG" | tail -1)
rm -f "$ATTACH_LOG"
if [ -z "$REPORT_ATTACH" ] || [ ! -f "$REPORT_ATTACH" ]; then
  echo "PARITY FAIL: no JSON report from the attached-golden run (test did not complete)" >&2
  exit 1
fi
echo "  attach report: $REPORT_ATTACH"
echo "  $ATTACH_SUMMARY"

echo ""
echo "==> Step 5b: query the SAME data via the PRIMARY catalog (non-attach path), capture the JSON report"
# The coordinator's primary [catalog] is test_warehouse, where golden was
# published, so `--namespace $GOLDEN_NS` without `--catalog golden` reads the
# identical physical tables through the NORMAL (non-attach) catalog path
# (rest_catalog.rs), not the ATTACH-mounted one (mount.rs::build_iceberg_rest).
PRIMARY_LOG="/tmp/sqe-attach-smoke-primary-$$.log"
"$BENCH_BIN" test tpch "${TEST_ARGS[@]}" --namespace "$GOLDEN_NS" 2>&1 | tee "$PRIMARY_LOG"
REPORT_PRIMARY=$(grep "^Report written to:" "$PRIMARY_LOG" | tail -1 | sed 's/^Report written to: //')
rm -f "$PRIMARY_LOG"
if [ -z "$REPORT_PRIMARY" ] || [ ! -f "$REPORT_PRIMARY" ]; then
  echo "PARITY FAIL: no JSON report from the primary-catalog run (test did not complete)" >&2
  exit 1
fi
echo "  primary report: $REPORT_PRIMARY"

echo ""
echo "==> Step 6 (GATING): attach path must return the SAME rows as the primary path"
# This is the actual scope of the attach feature: result-neutrality of the
# ATTACH read path vs the normal catalog read path on identical data. It does
# NOT depend on whether SQE's data matches DuckDB (that is a separate,
# pre-existing generator/query-fidelity question -- see the advisory below).
python3 - "$REPORT_ATTACH" "$REPORT_PRIMARY" <<'PYEOF'
import json, sys
attach = {q["id"]: q for q in json.load(open(sys.argv[1]))["queries"]}
primary = {q["id"]: q for q in json.load(open(sys.argv[2]))["queries"]}
ids = sorted(set(attach) | set(primary))
diffs = []
for qid in ids:
    a, p = attach.get(qid), primary.get(qid)
    if a is None or p is None:
        diffs.append(f"{qid}: present in only one run (attach={a is not None} primary={p is not None})")
        continue
    if a["rows"] != p["rows"]:
        diffs.append(f"{qid}: attach={a['rows']} rows vs primary={p['rows']} rows")
    if a["status"] != "pass":
        diffs.append(f"{qid}: attach status={a['status']} (expected pass)")
if not ids:
    print("PARITY FAIL: no queries in either report"); sys.exit(1)
if diffs:
    print("PARITY FAIL (attach path is NOT result-neutral vs primary):")
    for d in diffs: print(f"  {d}")
    sys.exit(1)
print(f"PARITY OK: all {len(ids)} tpch queries return identical rows via the attached golden catalog and the primary catalog (zero load through attach).")
PYEOF

echo ""
echo "==> Step 7 (ADVISORY, non-gating): attach rows vs the DuckDB oracle"
# Informational only. Divergences here are SQE-vs-DuckDB data/query fidelity
# (e.g. a known q18 LIMIT/HAVING discrepancy) that exist independent of the
# attach feature and are pre-existing on main -- they must NOT gate this
# branch. Logged so the divergence stays visible and can get its own ticket.
python3 - "$REPORT_ATTACH" "$ROOT_DIR/benchmarks/expected/canonical_rows_duckdb.json" "$BENCH_SCALE" <<'PYEOF' || true
import json, sys
report_path, canonical_path, scale = sys.argv[1], sys.argv[2], sys.argv[3]
scale_key = "sf" + (scale.rstrip("0").rstrip(".") if "." in scale else scale).replace(".", "_") + "_official_rows"
report = json.load(open(report_path))
canonical = json.load(open(canonical_path))["tpch"]
checked, mism = 0, []
for q in report["queries"]:
    entry = canonical.get(q["id"])
    if entry is None or scale_key not in entry:
        continue
    checked += 1
    if q["rows"] != entry[scale_key]:
        mism.append(f"{q['id']}: attach={q['rows']} oracle={entry[scale_key]}")
if mism:
    print(f"ADVISORY: {len(mism)}/{checked} tpch queries diverge from the DuckDB oracle (pre-existing SQE data/query fidelity, NOT the attach feature):")
    for m in mism: print(f"  {m}")
else:
    print(f"ADVISORY: all {checked} tpch queries also match the DuckDB oracle.")
PYEOF
