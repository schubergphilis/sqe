#!/usr/bin/env bash
# Hive external tables over S3-compatible storage, end to end.
#
# Brings up an S3-compatible store (RustFS), generates random Hive-layout data
# in four formats with a Python script, uploads it, creates one external table
# per format with Athena DDL, queries them, then STOPS the engine and starts a
# fresh process that re-attaches the same warehouse root and reads the tables
# back from the sidecar manifests in the bucket.
#
#   ./run.sh                    # full run, captures OUTPUT.md
#   ./run.sh --check            # full run, then assert the invariants
#   ./run.sh --server           # same SQL, same assertions, through a real
#                               # sqe-server over Flight SQL instead of the
#                               # in-process engine
#   ./run.sh --size-mb 1024     # bigger data set (5120 = the 5GB ceiling)
#   ./run.sh --regen            # regenerate + re-upload even if data is present
#   ./run.sh --down             # stop the store, keep the bucket volume
#   ./run.sh --clean            # stop and delete the bucket volume + ./data
set -euo pipefail
cd "$(dirname "$0")"
. ../_shared/lib.sh

CHECK=0
REGEN=0
MODE=embedded
SIZE_MB="${SIZE_MB:-64}"
while [ $# -gt 0 ]; do
  case "$1" in
    --check) CHECK=1 ;;
    --regen) REGEN=1 ;;
    --server) MODE=server ;;
    --size-mb) shift; SIZE_MB="${1:-}" ;;
    --down) docker compose down; ok "stopped (volume kept)"; exit 0 ;;
    --clean) docker compose down -v; rm -rf ./data ./.venv; ok "stopped, volume and ./data removed"; exit 0 ;;
    *) die "unknown arg: $1 (use --check, --server, --regen, --size-mb N, --down, --clean)" ;;
  esac
  shift
done
[ "$SIZE_MB" -ge 1 ] 2>/dev/null || die "--size-mb must be a positive integer"

# .env is optional; defaults below match docker-compose.yml.
[ -f .env ] && . ./.env
S3_PORT="${S3_PORT:-19100}"
S3_ACCESS_KEY="${S3_ACCESS_KEY:-s3admin}"
S3_SECRET_KEY="${S3_SECRET_KEY:-s3admin}"
S3_REGION="${S3_REGION:-us-east-1}"
S3_URL="http://localhost:${S3_PORT}"
FLIGHT_PORT="${FLIGHT_PORT:-50151}"
TRINO_PORT="${TRINO_PORT:-18081}"
BUCKET=lake                      # fixed: create-tables.sql names it literally
ROOT="s3://${BUCKET}/wh/"        # manifests
DATA="s3://${BUCKET}/data/"      # table data, deliberately not under the root

require docker python3

# --- the engine ------------------------------------------------------------
# A host binary, because it has to reach the store on localhost. Release is
# preferred; dev-release is the fast-rebuild profile and works identically here.
SQE="${SQE_CLI:-}"
if [ -z "$SQE" ]; then
  for cand in ../../target/release/sqe-cli ../../target/dev-release/sqe-cli; do
    [ -x "$cand" ] && SQE="$cand" && break
  done
fi
[ -z "$SQE" ] && command -v sqe-cli >/dev/null 2>&1 && SQE="$(command -v sqe-cli)"
[ -n "$SQE" ] || die "sqe-cli not found. Build it: (cd ../.. && cargo build --release -p sqe-cli), or set SQE_CLI."
# Print the binary and its build date. `target/release` is preferred over
# `target/dev-release`, so a stale release build silently wins over a fresh
# dev one; seeing the date is what turns that from a mystery into a one-line
# diagnosis. It has already cost this quickstart two debugging detours.
ok "engine: $SQE (built $(date -r "$SQE" '+%Y-%m-%d %H:%M' 2>/dev/null || echo unknown))"

SQE_SERVER="${SQE_SERVER:-}"
if [ "$MODE" = server ] && [ -z "$SQE_SERVER" ]; then
  for cand in ../../target/release/sqe-server ../../target/dev-release/sqe-server; do
    [ -x "$cand" ] && SQE_SERVER="$cand" && break
  done
  [ -z "$SQE_SERVER" ] && command -v sqe-server >/dev/null 2>&1 && SQE_SERVER="$(command -v sqe-server)"
  [ -n "$SQE_SERVER" ] || die "sqe-server not found. Build it: (cd ../.. && cargo build --release -p sqe-coordinator --bin sqe-server), or set SQE_SERVER."
  ok "server: $SQE_SERVER (built $(date -r "$SQE_SERVER" '+%Y-%m-%d %H:%M' 2>/dev/null || echo unknown))"
fi

# The secret goes in the environment, not on the command line, where `ps` and
# shell history would both keep a copy. The endpoint is a flag to show the
# precedence: flag first, then SQE_STORAGE__S3_*, then AWS_*.
export SQE_STORAGE__S3_ACCESS_KEY="$S3_ACCESS_KEY"
export SQE_STORAGE__S3_SECRET_KEY="$S3_SECRET_KEY"
export SQE_STORAGE__S3_REGION="$S3_REGION"

# sqe(): how a statement reaches the engine, and the ONLY thing that differs
# between the two modes. Everything downstream (the DDL, the queries, the
# assertions) is byte-identical, which is the point of --server: the same SQL
# and the same manifests, one in-process engine and one over the wire.
#
# embedded: one engine process per call, a cold start with nothing but the
# warehouse root, which is what makes the re-attach step below mean something.
# server: a client call against a long-lived sqe-server that attached the root
# from `[[hive.roots]]` in sqe.toml at startup.
sqe() {
  if [ "$MODE" = server ]; then
    SQE_PASSWORD=anonymous "$SQE" --host localhost --port "$FLIGHT_PORT" --user anonymous "$@"
  else
    "$SQE" --embedded --memory --s3-endpoint "$S3_URL" --hive-root "lake=${ROOT}" "$@"
  fi
}

# list_tables(): the statement that lists a Hive root's tables is NOT the same
# in both modes, and neither mode can use the other's.
#   embedded: `SHOW TABLES FROM <root>.<db>` is not parsed at all (a
#             pre-existing embedded gap, against any catalog kind), so
#             information_schema is the only way.
#   server:   `SHOW TABLES` works and is policy-gated, while
#             `information_schema.tables` enumerates EVERY registered catalog,
#             including the deliberately unreachable Iceberg one in sqe.toml,
#             and fails on it before it ever reaches the WHERE clause.
# Measured both ways; see the README.
list_tables() {
  if [ "$MODE" = server ]; then
    sqe -e "SHOW TABLES FROM lake.demo"
  else
    sqe -e "SELECT table_name FROM information_schema.tables WHERE table_catalog = 'lake' AND table_schema = 'demo' ORDER BY table_name"
  fi
}

SERVER_PID=""
SERVER_LOG="./server.log"

# start_server: boots sqe-server on sqe.toml. RUST_LOG defaults to info, not
# warn: readiness and the "did the root actually attach" assertion both read
# INFO lines out of the log, so a quieter default would make the wait time out
# on a perfectly healthy server. The TOML stays static and
# readable; anything .env can change is passed as an SQE_* env override, which
# is the same mechanism a container deployment uses.
start_server() {
  # Preflight both ports. Without this, a coordinator left over from a previous
  # run (or anything else on 18081) fails the bind deep inside startup and the
  # user sees "sqe-server exited during startup" with no cause.
  local p
  for p in "$FLIGHT_PORT" "$TRINO_PORT"; do
    if lsof -nP -iTCP:"$p" -sTCP:LISTEN >/dev/null 2>&1; then
      die "port $p is already in use (a coordinator from a previous run?). Free it, or set FLIGHT_PORT / TRINO_PORT in .env"
    fi
  done
  step "starting sqe-server on :${FLIGHT_PORT} (config: sqe.toml)"
  SQE_CONFIG=./sqe.toml \
  SQE_COORDINATOR__FLIGHT_SQL_PORT="$FLIGHT_PORT" \
  SQE_COORDINATOR__TRINO_HTTP_PORT="$TRINO_PORT" \
  SQE_STORAGE__S3_ENDPOINT="$S3_URL" \
  RUST_LOG="${RUST_LOG:-info}" \
    "$SQE_SERVER" > "$SERVER_LOG" 2>&1 &
  SERVER_PID=$!
  printf '    waiting for the coordinator '
  local i
  for i in $(seq 1 60); do
    if grep -q "coordinator listening" "$SERVER_LOG" 2>/dev/null \
       || lsof -nP -iTCP:"$FLIGHT_PORT" -sTCP:LISTEN >/dev/null 2>&1; then
      echo " ready"
      # The root has to appear as an attached catalog, or every later failure
      # would be a confusing "unknown catalog" instead of this one.
      # A server that boots but attaches nothing is almost always a binary
      # older than the Hive support itself: `[[hive.roots]]` is simply unknown
      # config to it, so it starts perfectly and serves no Hive catalog. That
      # is invisible unless something checks, which is what this is.
      grep -q '"kind":"hive_external"' "$SERVER_LOG" \
        || die "sqe-server started but attached no hive_external root.
       Binary: $SQE_SERVER (built $(date -r "$SQE_SERVER" '+%Y-%m-%d %H:%M' 2>/dev/null || echo unknown))
       Most likely that binary predates Hive external tables and ignores
       [[hive.roots]] entirely. Rebuild it, or point SQE_SERVER at a current
       build. Full log: $SERVER_LOG"
      ok "hive root attached from [[hive.roots]] at startup"
      return 0
    fi
    kill -0 "$SERVER_PID" 2>/dev/null || { echo; tail -20 "$SERVER_LOG"; die "sqe-server exited during startup"; }
    printf '.'; sleep 1
  done
  echo; tail -20 "$SERVER_LOG"; die "sqe-server did not report listening"
}

stop_server() {
  [ -n "$SERVER_PID" ] || return 0
  kill "$SERVER_PID" 2>/dev/null || true
  wait "$SERVER_PID" 2>/dev/null || true
  SERVER_PID=""
}
trap stop_server EXIT

# --- the S3 client ---------------------------------------------------------
# Host `aws` if present, otherwise the aws-cli container on the compose
# network. The container reaches the store as http://rustfs:9000; the host
# reaches the same bucket on localhost. Two network paths, one bucket.
export AWS_ACCESS_KEY_ID="$S3_ACCESS_KEY"
export AWS_SECRET_ACCESS_KEY="$S3_SECRET_KEY"
export AWS_DEFAULT_REGION="$S3_REGION"
# S3_CLIENT=host|container|auto (default auto) forces which client to use.
# `container` is the path a machine without the aws CLI takes, and naming it
# explicitly is the only way to exercise that path on a machine that has one.
S3_CLIENT="${S3_CLIENT:-auto}"
if [ "$S3_CLIENT" != "container" ] && { [ "$S3_CLIENT" = "host" ] || command -v aws >/dev/null 2>&1; }; then
  command -v aws >/dev/null 2>&1 || die "S3_CLIENT=host but the aws CLI is not on PATH"
  S3_MODE=host
  s3() { aws --endpoint-url "$S3_URL" "$@"; }
  s3_upload() { s3 s3 cp --recursive --quiet ./data "$DATA"; }
else
  S3_MODE=container
  s3() { docker compose run --rm -T awscli --endpoint-url http://rustfs:9000 "$@"; }
  s3_upload() { s3 s3 cp --recursive --quiet /work/data "$DATA"; }
fi

wait_for_store() {
  printf '    waiting for the store '
  local i code=000
  for i in $(seq 1 60); do
    # Any HTTP answer means the listener is up; an unauthenticated request to
    # the root legitimately returns 403 or 503, which is still "listening".
    code=$(curl -s -o /dev/null -w '%{http_code}' "$S3_URL" 2>/dev/null || echo 000)
    case "$code" in 000) printf '.'; sleep 1 ;; *) echo " ready (HTTP $code)"; return 0 ;; esac
  done
  echo " timeout"
  return 1
}

step "starting the S3-compatible store (RustFS on :${S3_PORT}, client: ${S3_MODE})"
docker compose up -d rustfs >/dev/null
wait_for_store || die "store did not answer on $S3_URL"
s3 s3 mb "s3://${BUCKET}" >/dev/null 2>&1 || true
ok "bucket s3://${BUCKET} present"

# --- data ------------------------------------------------------------------
# Regenerating and re-uploading 5GB on every run would make the quickstart
# unusable at size, so the local marker records what was generated and the run
# skips both steps when it already matches.
MARKER="./data/generated.json"
NEED_DATA=1
if [ "$REGEN" -eq 0 ] && [ -f "$MARKER" ] && python3 -c "
import json,sys
m=json.load(open('$MARKER'))
sys.exit(0 if m.get('size_mb_requested')==$SIZE_MB else 1)
" 2>/dev/null; then
  NEED_DATA=0
fi

if [ "$NEED_DATA" -eq 1 ]; then
  step "generating ~${SIZE_MB}MB of Hive-layout data in four formats"
  # pyarrow writes the Parquet table; CSV and JSON are stdlib. The venv is
  # local to this directory and ./run.sh --clean removes it.
  if ! python3 -c "import pyarrow" >/dev/null 2>&1; then
    [ -d .venv ] || python3 -m venv .venv
    ./.venv/bin/pip install -q --disable-pip-version-check pyarrow \
      || die "could not install pyarrow into ./.venv (needs network)"
    PY=./.venv/bin/python
  else
    PY=python3
  fi
  "$PY" gen_data.py --out ./data --size-mb "$SIZE_MB"

  step "uploading to s3://${BUCKET}/data/"
  s3 s3 rm --recursive --quiet "$DATA" >/dev/null 2>&1 || true
  s3_upload
  ok "uploaded $(s3 s3 ls --recursive "$DATA" | wc -l | tr -d ' ') objects"

  # RustFS caches object metadata in process, and MEASURED on
  # rustfs/rustfs:latest (2026-08-20) that cache is not invalidated by an
  # overwrite, by a delete-then-write, or even by deleting and recreating the
  # bucket: LIST keeps reporting the previous object's size and mtime while
  # GET serves the new bytes. A reader plans its byte ranges from the LISTED
  # size, so a stale size makes the last range end mid-line and the query
  # fails with "Csv error: incorrect number of fields", or worse, silently
  # returns the row count of the object that used to be there. A restart
  # clears the cache; nothing short of it did. This is a store bug, not an
  # engine one (plain `aws s3 ls` reports the same stale size), and it only
  # bites a re-upload, which is exactly what this quickstart does.
  step "restarting the store so its metadata cache matches what was uploaded"
  docker compose restart rustfs >/dev/null
  wait_for_store || die "store did not come back after the restart"

  # Belt and braces: compare every object's LISTED size against the local file
  # it came from. A mismatch here would mean wrong answers downstream, so it
  # fails loudly rather than producing an OUTPUT.md nobody can trust.
  step "verifying the store agrees with the local files, byte for byte"
  listed=$(s3 s3 ls --recursive "$DATA" | awk '{print $3" "$4}' | sort)
  mismatch=0
  while read -r size key; do
    [ -z "${key:-}" ] && continue
    local_file="./data/${key#data/}"
    [ -f "$local_file" ] || continue
    local_size=$(wc -c < "$local_file" | tr -d ' ')
    if [ "$size" != "$local_size" ]; then
      warn "size mismatch: $key listed $size, local $local_size"
      mismatch=1
    fi
  done <<< "$listed"
  [ "$mismatch" -eq 0 ] || die "the store's listing disagrees with the uploaded files; \
re-run with ./run.sh --clean first (the store's metadata cache is stale)"
  ok "all $(printf '%s\n' "$listed" | grep -c . ) objects match their local size"
else
  ok "data for ${SIZE_MB}MB already present (--regen to rebuild)"
fi

# --- process 1: define and query ------------------------------------------
# Two committed captures, one per deployment shape, so neither run overwrites
# the other's evidence.
if [ "$MODE" = server ]; then OUT=OUTPUT-server.md; else OUT=OUTPUT.md; fi
capture_begin "$OUT"
{
  echo "# Captured output"
  echo
  echo "_Generated by \`./run.sh --size-mb ${SIZE_MB}$([ "$MODE" = server ] && echo ' --server')\` on $(date -u +%Y-%m-%dT%H:%M:%SZ)._"
  echo
  if [ "$MODE" = server ]; then
    echo "Mode: **server**. Every statement below crossed a Flight SQL connection to a real"
    echo "\`sqe-server\` process that attached the warehouse root from \`[[hive.roots]]\` in"
    echo "\`sqe.toml\`. The SQL is byte-identical to the embedded run."
    echo
    echo "Two statements in \`queries.sql\` are EXPECTED to fail here, and the failure is the"
    echo "interesting part: \`information_schema.tables\` and \`DESCRIBE\` enumerate every"
    echo "registered catalog, so they trip over this config's deliberately unreachable Iceberg"
    echo "catalog before they ever reach the Hive root. \`SHOW TABLES FROM lake.demo\` works in"
    echo "server mode and is what the re-attach section uses; embedded mode is the reverse."
  else
    echo "Mode: **embedded**. Every statement below ran in an in-process engine started for"
    echo "that one call."
  fi
  echo
} > "$OUT"

[ "$MODE" = server ] && start_server

step "process 1: CREATE EXTERNAL TABLE x4 (Athena DDL), then query"
# --stop-on-error: a half-created set of tables would make every later step
# report a confusing failure instead of this one.
sqe --stop-on-error --file create-tables.sql >/dev/null || die "DDL failed; re-run without >/dev/null to see it"
{
  echo '## The data in the bucket'
  echo '```'
  s3 s3 ls --recursive "$DATA" | head -12
  echo '```'
  echo
  echo '## Process 1: create the tables, then query them'
  echo
  echo 'The DDL is Athena syntax, unchanged (`create-tables.sql`):'
  echo
  echo '```sql'
  # The comments in the file explain each clause; strip them here so the
  # captured DDL reads as DDL.
  grep -v '^--' create-tables.sql | grep -v '^$'
  echo '```'
  echo
  echo '```'
  sqe --file queries.sql 2>&1
  echo '```'
} >> "$OUT"
ok "tables created and queried"

# --- the manifest ----------------------------------------------------------
step "the sidecar manifest, straight out of the bucket"
MANIFEST_KEY="s3://${BUCKET}/wh/demo/events_parquet/_sqe/table.json"
{
  echo
  echo '## The manifest: the whole metastore, one JSON object per table'
  echo
  echo "No Polaris, no Nessie, no Hive Metastore. \`$MANIFEST_KEY\`:"
  echo
  echo '```json'
  s3 s3 cp "$MANIFEST_KEY" - 2>/dev/null
  echo '```'
} >> "$OUT"
ok "manifest read from $MANIFEST_KEY"

# --- process 2: stop, re-attach, read it back -----------------------------
# Process 1 is already gone: every sqe() call above was its own process, with
# an ephemeral --memory engine that persisted nothing locally. This step proves
# the definitions live in the bucket, not in the engine.
if [ "$MODE" = server ]; then
  # The embedded path gets its process boundary for free (every sqe() call is
  # its own engine). A server does not: it is long-lived, so the only honest
  # way to prove the definitions live in the bucket is to kill it and boot a
  # new one that re-reads [[hive.roots]] and the manifests from scratch.
  step "restarting the coordinator, so process 2 really is a new process"
  stop_server
  start_server
fi

step "process 2: a fresh engine re-attaches the same root"
{
  echo
  echo '## Process 2: a new engine, nothing local, same tables'
  echo
  echo 'The engine from process 1 has exited. This one attaches the same root and'
  echo 'nothing else: no DDL, no schema, no file paths.'
  echo
  echo '```'
  list_tables 2>&1
  sqe -e "SELECT COUNT(*) AS parquet_rows FROM lake.demo.events_parquet" 2>&1
  sqe -e "SHOW CREATE TABLE lake.demo.events_parquet" 2>&1
  echo '```'
} >> "$OUT"
ok "captured to $OUT"

if [ "$CHECK" -eq 0 ]; then
  echo
  ok "done. ./run.sh --down keeps the bucket, ./run.sh --clean removes everything."
  exit 0
fi

# --- checks ----------------------------------------------------------------
# The invariant that matters is not "a query returned rows" but "process 2
# agrees with process 1", which is the only thing that distinguishes reading
# the manifest from having recreated the table.
step "checking invariants"

count_of() { sqe -e "SELECT COUNT(*) AS n FROM $1" 2>&1 | grep -Eo '^\| [0-9]+' | tr -d '| ' | head -1; }

p1_parquet=$(count_of lake.demo.events_parquet)
p1_csv=$(count_of lake.demo.events_csv)
p1_json=$(count_of lake.demo.events_json)
p1_ndjson=$(count_of lake.demo.events_ndjson)

assert_not_empty "parquet table returns a count" "$p1_parquet"
assert_not_empty "csv table returns a count"     "$p1_csv"
assert_not_empty "json table returns a count"    "$p1_json"
assert_not_empty "ndjson table returns a count"  "$p1_ndjson"

# The generator recorded what it wrote, so the engine's counts are checked
# against an independent source rather than against themselves.
gen_rows() { python3 -c "import json;print(json.load(open('$MARKER'))['row_counts']['$1'])"; }
assert_eq "parquet row count matches the generator" "$p1_parquet" "$(gen_rows events_parquet)"
assert_eq "csv row count matches the generator"     "$p1_csv"     "$(gen_rows events_csv)"
assert_eq "json row count matches the generator"    "$p1_json"    "$(gen_rows events_json)"
assert_eq "ndjson row count matches the generator"  "$p1_ndjson"  "$(gen_rows events_ndjson)"

# The CSV header must not arrive as a data row: without
# skip.header.line.count the count would be 4 higher (one per partition file)
# and `event_id` would appear as a value.
header_leak=$(sqe -e "SELECT COUNT(*) AS n FROM lake.demo.events_csv WHERE kind = 'kind'" 2>&1)
assert_contains "csv header is skipped, not read as data" "$header_leak" "| 0 "

# Partition columns come from the path, land last, and filter like columns.
pruned=$(sqe -e "SELECT DISTINCT dt, region FROM lake.demo.events_parquet WHERE dt = '2026-01-02' AND region = 'eu'" 2>&1)
assert_contains "partition filter returns the pinned partition" "$pruned" "2026-01-02"
assert_not_contains "partition filter excludes other days"      "$pruned" "2026-01-03"
assert_not_contains "partition filter excludes other regions"   "$pruned" "| us "

# A second, independent process reading the same manifests must agree
# exactly. This is the stop-and-re-attach invariant.
p2_parquet=$(count_of lake.demo.events_parquet)
assert_eq "process 2 agrees with process 1 on the row count" "$p2_parquet" "$p1_parquet"

# ... and must render back the DDL that created the table.
ddl=$(sqe -e "SHOW CREATE TABLE lake.demo.events_parquet" 2>&1)
assert_contains "re-attached DDL keeps the location"       "$ddl" "s3://lake/data/events_parquet/"
assert_contains "re-attached DDL keeps both partition keys" "$ddl" "PARTITIONED BY (dt string, region string)"
assert_contains "re-attached DDL keeps the Parquet SerDe"  "$ddl" "ParquetHiveSerDe"

# The manifest is a Glue-shaped TableInput, so a future Glue backend is a
# field mapping rather than a rewrite.
manifest=$(s3 s3 cp "$MANIFEST_KEY" - 2>/dev/null)
assert_contains "manifest is an EXTERNAL_TABLE"        "$manifest" '"table_type": "EXTERNAL_TABLE"'
assert_contains "manifest carries a storage_descriptor" "$manifest" '"storage_descriptor"'
assert_contains "manifest carries partition_keys"       "$manifest" '"partition_keys"'

# DROP removes the definition and leaves every byte of data in place.
sqe -e "DROP TABLE lake.demo.events_ndjson" >/dev/null 2>&1
still_there=$(s3 s3 ls --recursive "s3://${BUCKET}/data/events_ndjson/" 2>&1)
assert_contains "DROP TABLE left the data untouched" "$still_there" ".ndjson"
gone=$(s3 s3 ls "s3://${BUCKET}/wh/demo/events_ndjson/_sqe/" 2>&1 || true)
assert_not_contains "DROP TABLE removed the manifest" "$gone" "table.json"
# Put it back, so a --check run leaves the same state a plain run does.
sqe --file create-tables.sql >/dev/null 2>&1

check_summary
