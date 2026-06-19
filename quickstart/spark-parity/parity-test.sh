#!/usr/bin/env bash
# Parity test: SQE vs Spark/Kyuubi-Authz for Ranger MASK_SHOW_LAST_4.
#
# Both engines run: SELECT id, ssn FROM sales_wh.sales.orders ORDER BY id
# as user `bob` (Ranger role=engineer -> ssn mask applies).
#
# Expected output per engine (same data, same mask):
#   id=1  ssn=xxx-xx-1111
#   id=2  ssn=xxx-xx-2222
#   id=3  ssn=xxx-xx-3333
#
# The test:
#   1. Runs the query in SQE via sqe-cli (ROPC, user=bob).
#   2. Runs the query in Spark via spark-sql (HADOOP_USER_NAME=bob).
#   3. Extracts just the ssn column from each, sorted by id.
#   4. Diffs them. Any difference = FAIL.
#
# Both raw outputs are printed even on success for manual inspection.
set -euo pipefail
cd "$(dirname "$0")"

green() { printf '\033[32m%s\033[0m\n' "$*"; }
red()   { printf '\033[31m%s\033[0m\n' "$*"; }

QUERY="SELECT id, ssn FROM sales_wh.sales.orders ORDER BY id"

# ── SQE query (ROPC as bob) ───────────────────────────────────────────────────
echo "== Running query in SQE as bob =="
SQE_OUT=$(docker compose exec -T \
  -e SQE_PASSWORD=bob123 \
  sqe \
  sqe-cli --port 50051 --user bob -e "$QUERY" 2>&1)

echo "SQE raw output:"
echo "$SQE_OUT"
echo

# ── Spark query (as bob via HADOOP_USER_NAME) ─────────────────────────────────
# Wait for Spark to be ready and Ranger policy to be downloaded.
echo "== Running query in Spark as bob (HADOOP_USER_NAME=bob) =="
echo "(waiting up to 60s for Ranger policy download...)"

# The Ranger plugin downloads policies on first query; allow 60s for that.
SPARK_OUT=""
for i in $(seq 1 6); do
  SPARK_OUT=$(docker compose exec -T \
    -e HADOOP_USER_NAME=bob \
    spark \
    spark-sql \
      --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.apache.kyuubi.plugin.spark.authz.ranger.RangerSparkExtension" \
      --conf "spark.driver.extraClassPath=/opt/spark/jars/*" \
      -e "USE sales_wh.sales; $QUERY" 2>&1) || true
  # If we get actual output rows (contains a digit), we're done
  if echo "$SPARK_OUT" | grep -qE '^\|?\s*[0-9]'; then
    break
  fi
  echo "  attempt $i/6: Spark not ready or no output yet, retrying in 10s..."
  sleep 10
done

echo "Spark raw output:"
echo "$SPARK_OUT"
echo

# ── Extract ssn column from each output ──────────────────────────────────────
# SQE output format: table with | separators or CSV, rows like:
#   1 | xxx-xx-1111
# Spark output format: similar table or TSV.
# Strategy: grep for lines containing digits-dashes (ssn-like), extract last
# whitespace-separated field that matches xxx-xx-NNNN or NNN-NN-NNNN.

extract_ssn() {
  echo "$1" | grep -oE '(xxx-xx-[0-9]{4}|[0-9]{3}-[0-9]{2}-[0-9]{4})' | sort
}

SQE_SSN=$(extract_ssn "$SQE_OUT")
SPARK_SSN=$(extract_ssn "$SPARK_OUT")

echo "== Extracted ssn values =="
echo "SQE  ssns: $(echo "$SQE_SSN" | tr '\n' ' ')"
echo "Spark ssns: $(echo "$SPARK_SSN" | tr '\n' ' ')"
echo

# ── Assertions ────────────────────────────────────────────────────────────────
PASS=0; FAIL=0

# 1. SQE produces masked output (xxx-xx-NNNN pattern)
if echo "$SQE_SSN" | grep -qE 'xxx-xx-[0-9]{4}'; then
  green "PASS  SQE ssn is masked (xxx-xx-NNNN pattern found)"
  PASS=$((PASS+1))
else
  red "FAIL  SQE ssn NOT masked (expected xxx-xx-NNNN, got: $SQE_SSN)"
  FAIL=$((FAIL+1))
fi

# 2. SQE does NOT show raw SSNs
if echo "$SQE_SSN" | grep -qE '^[0-9]{3}-[0-9]{2}-[0-9]{4}$'; then
  red "FAIL  SQE ssn shows raw value (mask not applied)"
  FAIL=$((FAIL+1))
else
  green "PASS  SQE ssn does not leak raw value"
  PASS=$((PASS+1))
fi

# 3. Spark produces masked output
if echo "$SPARK_SSN" | grep -qE 'xxx-xx-[0-9]{4}'; then
  green "PASS  Spark ssn is masked (xxx-xx-NNNN pattern found)"
  PASS=$((PASS+1))
else
  red "FAIL  Spark ssn NOT masked (expected xxx-xx-NNNN, got: $SPARK_SSN)"
  FAIL=$((FAIL+1))
fi

# 4. Spark does NOT show raw SSNs
if echo "$SPARK_SSN" | grep -qE '^[0-9]{3}-[0-9]{2}-[0-9]{4}$'; then
  red "FAIL  Spark ssn shows raw value (mask not applied)"
  FAIL=$((FAIL+1))
else
  green "PASS  Spark ssn does not leak raw value"
  PASS=$((PASS+1))
fi

# 5. PARITY: both engines produce the same ssn values (byte-exact)
if [ -n "$SQE_SSN" ] && [ -n "$SPARK_SSN" ] && [ "$SQE_SSN" = "$SPARK_SSN" ]; then
  green "PASS  PARITY: SQE and Spark ssn outputs are identical"
  PASS=$((PASS+1))
elif [ -z "$SQE_SSN" ] || [ -z "$SPARK_SSN" ]; then
  red "FAIL  PARITY: one or both engines returned no ssn values (query failed?)"
  FAIL=$((FAIL+1))
else
  red "FAIL  PARITY: SQE and Spark ssn outputs DIFFER"
  echo "  SQE:   $SQE_SSN"
  echo "  Spark: $SPARK_SSN"
  diff <(echo "$SQE_SSN") <(echo "$SPARK_SSN") || true
  FAIL=$((FAIL+1))
fi

echo
echo "================ PARITY RESULT: ${PASS} passed, ${FAIL} failed ================"
[ "$FAIL" -eq 0 ] || exit 1
