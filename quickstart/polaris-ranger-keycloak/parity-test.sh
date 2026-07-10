#!/usr/bin/env bash
# SQE <-> Spark parity test for a Ranger `hive` MASK_SHOW_LAST_4 column mask.
#
# Both engines share ONE Polaris 1.5 Iceberg REST catalog and ONE Ranger `hive`
# service. The same policy (mask sales.orders.ssn -> show-last-4 for role
# engineer) is read by SQE's PlanRewriter and by Kyuubi's RangerSparkExtension.
# Running the same SELECT as bob (engineer in Ranger) MUST produce identical ssn
# output in both engines.
#
#   Query:    SELECT id, ssn FROM sales_wh.sales.orders ORDER BY id
#   Identity: bob (Ranger role=engineer -> ssn mask applies)
#   Expected: id=1 ssn=xxx-xx-1111 ; id=2 ssn=xxx-xx-2222   (BOTH engines)
#
# SQE runs via sqe-cli (ROPC, user=bob). Spark runs via spark-sql with
# HADOOP_USER_NAME=bob (Kyuubi Authz picks up the OS user for Ranger role
# resolution). Spark connects to Polaris as the `root` service account; the
# fine-grained ssn mask is Kyuubi's job, keyed on bob.
#
# Raw output of both engines is printed for inspection; the ssn column is
# normalized (sorted) from each and diffed. Any mismatch FAILS.
#
# Row-filter parity is deliberately out of scope: Kyuubi Spark 3.5 throws
# MISSING_ATTRIBUTES (#6889) on a row filter over an unprojected column, so the
# Ranger bootstrap seeds NO row filter (see ranger/bootstrap-ranger.sh).
set -uo pipefail
cd "$(dirname "$0")"

green() { printf '\033[32m%s\033[0m\n' "$*"; }
red()   { printf '\033[31m%s\033[0m\n' "$*"; }

QUERY="SELECT id, ssn FROM sales_wh.sales.orders ORDER BY id"

# ── SQE query (ROPC as bob) ───────────────────────────────────────────────────
echo "== Running query in SQE as bob =="
SQE_OUT=""
for i in $(seq 1 6); do
  SQE_OUT=$(docker compose exec -T \
    -e SQE_PASSWORD=bob123 \
    sqe \
    sqe-cli --port 50051 --user bob -e "$QUERY" 2>&1) || true
  echo "$SQE_OUT" | grep -qE 'xxx-xx-[0-9]{4}' && break
  echo "  attempt $i/6: SQE not ready or mask not applied yet, retrying in 10s..."
  sleep 10
done
echo "SQE raw output:"
echo "$SQE_OUT"
echo

# ── Spark query (as bob via HADOOP_USER_NAME) ─────────────────────────────────
# The Ranger plugin downloads policies on first query; allow up to 60s for that.
echo "== Running query in Spark as bob (HADOOP_USER_NAME=bob) =="
echo "(waiting up to 60s for Ranger policy download + first spark-sql start...)"
# The shared CUSTOM mask expr is portable standard SQL
# (concat('xxx-xx-', substr({col},8,4))). concat + substr are built-ins in
# Spark, so Kyuubi injects the expr verbatim and Spark renders the same
# xxx-xx-1111 as SQE -- no Hive UDF registration, no hive catalog impl, no
# default-catalog gymnastics needed. The Iceberg catalog is referenced
# fully-qualified (sales_wh.sales.orders) so no USE is required either.
SPARK_CONF='--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.apache.kyuubi.plugin.spark.authz.ranger.RangerSparkExtension'
SPARK_OUT=""
for i in $(seq 1 6); do
  SPARK_OUT=$(docker compose exec -T \
    -e HADOOP_USER_NAME=bob \
    spark \
    /opt/spark/bin/spark-sql $SPARK_CONF -e "$QUERY" 2>&1) || true
  # spark-sql prints "Fetched N row(s)" once the query actually executes.
  echo "$SPARK_OUT" | grep -qE 'Fetched [0-9]+ row' && break
  echo "  attempt $i/6: Spark not ready yet (policy download / first start), retrying in 10s..."
  sleep 10
done
echo "Spark raw output:"
echo "$SPARK_OUT"
echo

# ── Normalize: pull the ssn token from each engine's rows ─────────────────────
# Both engines render the ssn as an 11-char token: 3 mask chars, a separator, 2
# mask chars, a separator, then the 4 visible digits. Match exactly that shape
# (canonical xxx-xx-NNNN, Kyuubi's built-in nnnUnnUNNNN, or a raw NNN-NN-NNNN).
# The anchors keep stray URL/stack-trace text out of the comparison while still
# letting a genuine mask-vocabulary difference show up in the diff.
extract_ssn() {
  echo "$1" | grep -oE '[A-Za-z0-9]{3}[^A-Za-z0-9][A-Za-z0-9]{2}[^A-Za-z0-9][0-9]{4}' | sort -u
}
SQE_SSN=$(extract_ssn "$SQE_OUT")
SPARK_SSN=$(extract_ssn "$SPARK_OUT")

echo "== Normalized ssn values (sorted) =="
echo "SQE   : $(echo "$SQE_SSN" | tr '\n' ' ')"
echo "Spark : $(echo "$SPARK_SSN" | tr '\n' ' ')"
echo

# ── Assertions ────────────────────────────────────────────────────────────────
PASS=0; FAIL=0
green_pass() { green "PASS  $1"; PASS=$((PASS+1)); }
red_fail()   { red   "FAIL  $1"; FAIL=$((FAIL+1)); }

# Raw-SSN leak guard: the full 9-digit dashed value must not appear in either.
has_raw() { echo "$1" | grep -qE '[0-9]{3}-[0-9]{2}-[0-9]{4}'; }
# Last-4 visible (show-last-4 actually applied, regardless of mask chars).
shows_last4() { echo "$1" | grep -q '1111' && echo "$1" | grep -q '2222'; }

# SEMANTIC checks (pass on this stack): both engines enforce the SAME Ranger
# `hive` MASK_SHOW_LAST_4 policy -- raw hidden, last 4 visible.
if ! has_raw "$SQE_SSN" && shows_last4 "$SQE_SSN"; then
  green_pass "SQE: ssn masked show-last-4, raw hidden"
else red_fail "SQE: ssn not masked as expected (got: $SQE_SSN)"; fi

if ! has_raw "$SPARK_SSN" && shows_last4 "$SPARK_SSN"; then
  green_pass "Spark: ssn masked show-last-4, raw hidden"
else red_fail "Spark: ssn not masked as expected (got: $SPARK_SSN)"; fi

# BYTE-EXACT PARITY (the headline assertion). This is deliberately NOT loosened:
# if the two engines render the same Ranger mask type with different mask
# characters, this FAILS and the diff shows exactly how -- that is the result,
# not a bug in the test.
if [ -n "$SQE_SSN" ] && [ "$SQE_SSN" = "$SPARK_SSN" ]; then
  green_pass "BYTE-EXACT PARITY: SQE and Spark ssn output is identical"
else
  red_fail "BYTE-EXACT PARITY: SQE and Spark ssn output DIFFERS"
  echo "  SQE  : $(echo "$SQE_SSN"  | tr '\n' ' ')"
  echo "  Spark: $(echo "$SPARK_SSN" | tr '\n' ' ')"
  cat <<'NOTE'

  Expected BOTH engines to render xxx-xx-1111 / xxx-xx-2222 from the shared
  Ranger `hive` CUSTOM mask (mask_show_last_n({col},4,'x','x','x',-1,'1')).
  If Spark shows nnnUnnU1111 the ssn policy reverted to the NAMED
  MASK_SHOW_LAST_4 type -- Kyuubi does not honor that type's servicedef
  transformer and applies its own mask chars. If Spark errors with
  "Cannot load function mask_show_last_n", the temp-function registration or
  the spark_catalog default-catalog scope did not take effect.
  See OVERVIEW.md "SQE <-> Spark mask cross-compare" for the why.
NOTE
fi

echo
echo "================ PARITY RESULT: ${PASS} passed, ${FAIL} failed ================"
[ "$FAIL" -eq 0 ] || exit 1
