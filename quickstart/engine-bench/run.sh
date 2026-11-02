#!/usr/bin/env bash
# engine-bench quickstart: SQE (embedded on Parquet, embedded on Iceberg, and
# a native coordinator on Polaris + S3) next to DuckDB and Polars on the
# engine-bench workload (github.com/firfircelik/engine-bench).
#
#   ./run.sh                    # 10M rows, 3 reps, sqe-* + duckdb + polars
#   SIZE=50 REPS=5 ./run.sh     # 50M rows (~3 GB of Parquet), 5 reps
#   ENGINES=sqe-polaris,duckdb ./run.sh
#   WITH_SPARK=1 ./run.sh       # add Spark (needs Java 17+; slow to start)
#   STORAGE=s3 ./run.sh         # every engine reads the Parquet from RustFS (S3)
#   ./run.sh --check            # run, then assert every run finished and every answer matches
#   ./run.sh --down             # stop the Polaris stack and drop its volume
#   ./run.sh --clean            # --down, and delete .work/ (data, warehouses, results)
set -euo pipefail
cd "$(dirname "$0")"
. ../_shared/lib.sh

CHECK=0
for a in "$@"; do
  case "$a" in
    --down) require docker; step "tearing down"; docker compose --profile setup down -v; ok "done"; exit 0 ;;
    --clean) require docker; step "tearing down + deleting .work/"; docker compose --profile setup down -v; rm -rf .work; ok "done"; exit 0 ;;
    --check) CHECK=1 ;;
    *) die "unknown arg: $a (use --check, --down or --clean)" ;;
  esac
done

[ -f .env ] || cp .env.example .env
# shellcheck disable=SC1091
set -a; . ./.env; set +a

SIZE="${SIZE:-10}"                 # millions of sales rows
REPS="${REPS:-3}"
MEM="${MEM:-3g}"                   # per-engine memory setting, upstream's default
MAX_BUSY_PCT="${MAX_BUSY_PCT:-30}" # a run starts only when the CPU is at most this busy
STORAGE="${STORAGE:-local}"        # local: data/ on disk; s3: the same files in RustFS
case "$STORAGE" in
  local) ENGINES="${ENGINES:-sqe-parquet,sqe-iceberg,sqe-polaris,duckdb,polars}" ;;
  # sqe-iceberg's embedded catalog is local-only; sqe-polaris is the Iceberg leg.
  s3) ENGINES="${ENGINES:-sqe-parquet,sqe-polaris,duckdb,polars}" ;;
  *) die "STORAGE must be local or s3, not $STORAGE" ;;
esac
[ "${WITH_SPARK:-0}" = 1 ] && ENGINES="$ENGINES,spark"
UPSTREAM_REPO="https://github.com/firfircelik/engine-bench.git"
UPSTREAM_SHA="920658085cd911f253908ac23c0f606379f5f472"
REPO_ROOT="$(cd ../.. && pwd)"
WORK="$PWD/.work"
export EB_WORK="$WORK" EB_STORAGE="$STORAGE"
export EB_S3_ENDPOINT="http://127.0.0.1:${RUSTFS_PORT:-39000}" EB_S3_PREFIX="s3://warehouse/engine-bench/data"
mkdir -p "$WORK"

require git
require uv
require docker

# ── SQE binaries: a release build, never a debug or dev-fast one ────────────
export SQE_CLI="${SQE_CLI:-$REPO_ROOT/target/release/sqe-cli}"
export SQE_SERVER="${SQE_SERVER:-$REPO_ROOT/target/release/sqe-server}"
if [ ! -x "$SQE_CLI" ] || [ ! -x "$SQE_SERVER" ]; then
  step "building release sqe-cli + sqe-server (make release; ~10 min cold)"
  make -C "$REPO_ROOT" release
fi
ok "sqe: $("$SQE_CLI" --version)"

# ── the workload, pinned ────────────────────────────────────────────────────
UP="$WORK/engine-bench"
if [ "$(git -C "$UP" rev-parse HEAD 2>/dev/null || true)" != "$UPSTREAM_SHA" ]; then
  step "cloning engine-bench at ${UPSTREAM_SHA:0:12}"
  rm -rf "$UP"
  git clone -q "$UPSTREAM_REPO" "$UP"
  git -C "$UP" -c advice.detachedHead=false checkout -q "$UPSTREAM_SHA"
fi
ok "workload: engine-bench@${UPSTREAM_SHA:0:12}"

# Spark 4.x runs on Java 17 or 21 (the write-up used OpenJDK 21); a newer
# default JVM fails at session start. SPARK_JAVA_HOME overrides the pick.
if [ "${WITH_SPARK:-0}" = 1 ]; then
  if [ -z "${SPARK_JAVA_HOME:-}" ] && [ -x /usr/libexec/java_home ]; then
    SPARK_JAVA_HOME="$(/usr/libexec/java_home -v 21 2>/dev/null || /usr/libexec/java_home -v 17 2>/dev/null || true)"
  fi
  if [ -n "${SPARK_JAVA_HOME:-}" ]; then
    export JAVA_HOME="$SPARK_JAVA_HOME" PATH="$SPARK_JAVA_HOME/bin:$PATH"
  fi
  ok "spark java: $(java -version 2>&1 | head -1)"
fi

step "python env (.work/venv)"
[ -d "$WORK/venv" ] || uv venv -q --python 3.12 "$WORK/venv"
PY="$WORK/venv/bin/python"
uv pip install -q --python "$PY" -r requirements.txt
[ "${WITH_SPARK:-0}" = 1 ] && uv pip install -q --python "$PY" pyspark==4.2.0
ok "duckdb $("$PY" -c 'import duckdb; print(duckdb.__version__)'), polars $("$PY" -c 'import polars; print(polars.__version__)')"

S="${SIZE}m"
if [ ! -f "$UP/data/sales_$S/_DONE" ]; then
  step "generating $SIZE M sales rows (seeded, deterministic)"
  (cd "$UP" && "$PY" gen_data.py "$SIZE")
  touch "$UP/data/sales_$S/_DONE"
fi
ok "data: $(du -sh "$UP/data/sales_$S" | cut -f1) of Parquet in .work/engine-bench/data/sales_$S"

# ── Polaris stack + sqe.toml ────────────────────────────────────────────────
if [[ ",$ENGINES," == *",sqe-polaris,"* ]] || [ "$STORAGE" = s3 ]; then
  step "bringing up RustFS + Polaris"
  # --wait would count the one-shot setup's exit as a failure: wait for
  # Polaris, then run the (idempotent) setup in the foreground.
  docker compose up -d --wait polaris
  docker compose run --rm polaris-setup
  ok "polaris on :$POLARIS_PORT, rustfs on :$RUSTFS_PORT"
  sed -e "s#@WORK@#$WORK#g" \
      -e "s#@MEM_LIMIT@#$(echo "$MEM" | tr '[:lower:]' '[:upper:]')B#g" \
      -e "s#@SQE_FLIGHT_PORT@#$SQE_FLIGHT_PORT#g" \
      -e "s#@SQE_TRINO_PORT@#$SQE_TRINO_PORT#g" \
      -e "s#@SQE_METRICS_PORT@#$SQE_METRICS_PORT#g" \
      -e "s#@POLARIS_PORT@#$POLARIS_PORT#g" \
      -e "s#@POLARIS_ROOT_SECRET@#$POLARIS_ROOT_SECRET#g" \
      -e "s#@RUSTFS_PORT@#$RUSTFS_PORT#g" \
      -e "s#@S3_ACCESS_KEY@#$S3_ACCESS_KEY#g" \
      -e "s#@S3_SECRET_KEY@#$S3_SECRET_KEY#g" \
      sqe.toml.tmpl > "$WORK/sqe.toml"
fi

if [ "$STORAGE" = s3 ]; then
  step "copying sales_$S + products into RustFS ($EB_S3_PREFIX), skipped when already there"
  docker compose run --rm data-upload "sales_$S"
  if [[ ",$ENGINES," == *",spark,"* ]]; then
    step "resolving Spark's S3A jars (hadoop-aws + AWS SDK bundle; first run downloads ~600 MB)"
    "$PY" run_engine_s3.py spark-jars
  fi
fi

LOAD_ENGINES=$(echo "$ENGINES" | tr ',' '\n' | grep -E '^sqe-(iceberg|polaris)$' | paste -sd, - || true)
if [ -n "$LOAD_ENGINES" ]; then
  step "loading $S into Iceberg ($LOAD_ENGINES), skipped when already loaded"
  "$PY" bench.py load --size "$S" --engines "$LOAD_ENGINES"
fi

# ── run, validate, summarize ────────────────────────────────────────────────
TAG="$S$([ "$STORAGE" = s3 ] && echo -s3)-$(date -u +%Y%m%dT%H%M%SZ)"
step "running $REPS reps x 4 queries x {$ENGINES} at $S from $STORAGE storage (fresh process per run)"
"$PY" bench.py run --sizes "$S" --reps "$REPS" --mem "$MEM" --engines "$ENGINES" --tag "$TAG" \
  --max-busy-pct "$MAX_BUSY_PCT"

step "validating answers (row counts + checksums against duckdb)"
set +e
VALIDATION="$("$PY" bench.py validate --size "$S" --engines "$ENGINES")"
VALID_RC=$?
set -e
echo "$VALIDATION"

OUTPUT=OUTPUT.md
[ "$STORAGE" = s3 ] && OUTPUT=OUTPUT-s3.md
{
  echo "# Captured output"
  echo
  echo "_Generated by \`SIZE=$SIZE REPS=$REPS$([ "$STORAGE" = s3 ] && echo " STORAGE=s3")$([ "${WITH_SPARK:-0}" = 1 ] && echo " WITH_SPARK=1") ./run.sh\` on $(date -u +%Y-%m-%d)._"
  echo
  echo "- Machine: $(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -m), $("$PY" -c 'import psutil; print(round(psutil.virtual_memory().total / 2**30))') GiB RAM, $(uname -s)"
  echo "- SQE: $("$SQE_CLI" --version | sed 's/^sqe-cli //') (release build); $("$PY" -c 'import duckdb, polars; print(f"DuckDB {duckdb.__version__}, Polars {polars.__version__}")')"
  echo "- Workload: engine-bench@${UPSTREAM_SHA:0:12}, $S sales rows read from $STORAGE storage, $REPS reps, memory setting $MEM, 8 threads, watchdog 4.5 GiB, CPU gate ${MAX_BUSY_PCT}%"
  echo
  echo "## Answers"
  echo
  echo "$VALIDATION"
  echo
  echo "## Timings"
  "$PY" bench.py summary --tag "$TAG"
} > "$OUTPUT"
ok "captured to $OUTPUT (raw runs in .work/results/$TAG.jsonl)"

if [ "$CHECK" -eq 1 ]; then
  step "checking invariants"
  assert_eq "every engine's answers match" "0" "$VALID_RC"
  OK_RUNS=$(grep -c '"status": "ok"' "$WORK/results/$TAG.jsonl" || true)
  TOTAL=$(wc -l < "$WORK/results/$TAG.jsonl" | tr -d ' ')
  assert_eq "every run finished ($OK_RUNS/$TOTAL ok)" "$OK_RUNS" "$TOTAL"
  for e in ${ENGINES//,/ }; do
    assert_contains "the summary has $e" "$(cat "$OUTPUT")" "$e"
  done
  check_summary
fi
