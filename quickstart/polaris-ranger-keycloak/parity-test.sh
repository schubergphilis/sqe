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
SPARK_OUT=""
for i in $(seq 1 6); do
  SPARK_OUT=$(docker compose exec -T \
    -e HADOOP_USER_NAME=bob \
    spark \
    /opt/spark/bin/spark-sql \
      --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.apache.kyuubi.plugin.spark.authz.ranger.RangerSparkExtension" \
      --conf "spark.driver.extraClassPath=/opt/spark/jars/*" \
      -e "USE sales_wh.sales; $QUERY" 2>&1) || true
  echo "$SPARK_OUT" | grep -qE 'xxx-xx-[0-9]{4}' && break
  echo "  attempt $i/6: Spark not ready or mask not applied yet, retrying in 10s..."
  sleep 10
done
echo "Spark raw output:"
echo "$SPARK_OUT"
echo

# ── Normalize: extract the ssn column from each output, sorted ────────────────
extract_ssn() {
  echo "$1" | grep -oE '(xxx-xx-[0-9]{4}|[0-9]{3}-[0-9]{2}-[0-9]{4})' | sort
}
SQE_SSN=$(extract_ssn "$SQE_OUT")
SPARK_SSN=$(extract_ssn "$SPARK_OUT")

echo "== Normalized ssn values (sorted) =="
echo "SQE   : $(echo "$SQE_SSN" | tr '\n' ' ')"
echo "Spark : $(echo "$SPARK_SSN" | tr '\n' ' ')"
echo

# ── Assertions ────────────────────────────────────────────────────────────────
PASS=0; FAIL=0
EXPECTED=$'xxx-xx-1111\nxxx-xx-2222'

check() { # cond desc
  if eval "$1"; then green "PASS  $2"; PASS=$((PASS+1)); else red "FAIL  $2"; FAIL=$((FAIL+1)); fi
}

# 1. SQE shows the masked pattern and not the raw ssn.
check '[ "$SQE_SSN" = "$EXPECTED" ]' \
  "SQE ssn = xxx-xx-1111 / xxx-xx-2222 (mask applied, raw hidden)"
# 2. Spark shows the masked pattern and not the raw ssn.
check '[ "$SPARK_SSN" = "$EXPECTED" ]' \
  "Spark ssn = xxx-xx-1111 / xxx-xx-2222 (mask applied, raw hidden)"
# 3. Neither engine leaks a raw 9-digit ssn.
check '! echo "$SQE_SSN$SPARK_SSN" | grep -qE "^[0-9]{3}-[0-9]{2}-[0-9]{4}$"' \
  "no raw SSN leaked by either engine"
# 4. PARITY: the two engines produce byte-identical ssn output.
if [ -n "$SQE_SSN" ] && [ "$SQE_SSN" = "$SPARK_SSN" ]; then
  green "PASS  PARITY: SQE and Spark ssn output is byte-identical"; PASS=$((PASS+1))
else
  red "FAIL  PARITY: SQE and Spark ssn output DIFFERS"
  diff <(echo "$SQE_SSN") <(echo "$SPARK_SSN") || true
  FAIL=$((FAIL+1))
fi

echo
echo "================ PARITY RESULT: ${PASS} passed, ${FAIL} failed ================"
[ "$FAIL" -eq 0 ] || exit 1
