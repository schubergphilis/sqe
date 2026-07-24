#!/usr/bin/env bash
set -euo pipefail

# Publish golden Iceberg tables once into a persistent Polaris. Load-many
# afterwards attaches them read-only (see benchmark-attach-golden.sh) instead
# of re-loading. Read-only suites only; write suites (tpcc/tpce) are cloned
# at run time, not published here.
#
# Unlike benchmark-publish-data.sh (Tier 1: raw parquet -> S3), this tier
# needs a live SQE coordinator: `sqe-bench load` is a Flight/Trino *client*
# of a running coordinator (--host/--port), not a direct Polaris client --
# `--catalog-uri`/`--warehouse` only exist on `sqe-bench generate --sink
# iceberg` (bank's direct-to-Iceberg path; see crates/sqe-bench/src/cli.rs).
# So for tpch/ssb/tpcds/clickbench this script stands up its own throwaway
# coordinator configured with the golden Polaris as its PRIMARY catalog,
# runs the loads against it, and tears it down; bank instead calls
# `generate --sink iceberg` straight at the golden Polaris, exactly as
# scripts/benchmark-load.sh already does for the local test stack.
#
# Usage:
#   BENCH_GOLDEN_POLARIS_URL=https://polaris.example.com/api/catalog \
#   BENCH_GOLDEN_WAREHOUSE=golden_warehouse \
#   BENCH_GOLDEN_TOKEN=<bearer> \
#   SQE_CLIENT_ID=... SQE_CLIENT_SECRET=... \
#   BENCH_S3_ENDPOINT=https://s3.example.com \
#   BENCH_GOLDEN_S3_ACCESS_KEY=... BENCH_GOLDEN_S3_SECRET_KEY=... \
#   BENCH_DATA_SOURCE=s3://sqe-benchmark BENCH_S3_PROFILE=storagegrid \
#   BENCH_SCALE=1 ./scripts/benchmark-publish-iceberg.sh            # all read suites
#   ... ./scripts/benchmark-publish-iceberg.sh tpch ssb              # only these
#
# Environment:
#   BENCH_GOLDEN_POLARIS_URL   golden Polaris REST catalog URI (required)
#   BENCH_GOLDEN_WAREHOUSE     warehouse name in the golden Polaris (required)
#   BENCH_GOLDEN_TOKEN         bearer token for the golden REST skip-check
#                              (required); also the default bank bearer if
#                              ICEBERG_BEARER_TOKEN is not set separately
#   BENCH_SCALE                scale factor (default: 0.1)
#   BENCH_DATA_SOURCE          Tier-1 parquet source: s3://<bucket> or a
#                              local dir (default: /tmp/sqe-bench-data);
#                              not used by bank, which generates in-process
#   BENCH_S3_ENDPOINT          S3 endpoint shared by the Tier-1 source and
#                              the golden warehouse bucket (required)
#   BENCH_S3_PROFILE           aws CLI profile for the Tier-1 source creds
#                              (default: default; only used when
#                              BENCH_DATA_SOURCE is s3://)
#   BENCH_S3_REGION            S3 region (default: us-east-1)
#   BENCH_S3_PATH_STYLE        path-style S3 addressing, MinIO/RustFS/
#                              StorageGRID style (default: true)
#   BENCH_GOLDEN_S3_ACCESS_KEY / BENCH_GOLDEN_S3_SECRET_KEY
#                              credentials for the golden warehouse bucket
#                              (required) -- this is where the throwaway
#                              coordinator's [storage] and bank's direct
#                              write both land table data
#   SQE_TOKEN_ENDPOINT / SQE_CLIENT_ID / SQE_CLIENT_SECRET
#                              OAuth2 client_credentials for the golden
#                              Polaris (required for tpch/ssb/tpcds/
#                              clickbench, which need a throwaway coordinator
#                              authenticated against the golden catalog;
#                              SQE_TOKEN_ENDPOINT defaults to
#                              <polaris_url>/v1/oauth/tokens)
#   ICEBERG_BEARER_TOKEN       alternative to SQE_CLIENT_ID/SECRET for bank's
#                              direct `generate --sink iceberg` catalog auth
#                              (default: BENCH_GOLDEN_TOKEN)
#   PROFILE                    cargo build profile: release (default) |
#                              dev-release | debug
#   BENCH_FORCE=1              re-publish even if the golden namespace
#                              already has tables

ALL_READ_SUITES=(tpch ssb tpcds tpcbb clickbench bank)

BENCH_SCALE="${BENCH_SCALE:-0.1}"
BENCH_GOLDEN_POLARIS_URL="${BENCH_GOLDEN_POLARIS_URL:-}"
BENCH_GOLDEN_WAREHOUSE="${BENCH_GOLDEN_WAREHOUSE:-}"
BENCH_FORCE="${BENCH_FORCE:-0}"

if [ -z "$BENCH_GOLDEN_POLARIS_URL" ] || [ -z "$BENCH_GOLDEN_WAREHOUSE" ]; then
  echo "ERROR: BENCH_GOLDEN_POLARIS_URL and BENCH_GOLDEN_WAREHOUSE must be set." >&2
  exit 1
fi

BENCH_GOLDEN_TOKEN="${BENCH_GOLDEN_TOKEN:-}"
BENCH_DATA_SOURCE="${BENCH_DATA_SOURCE:-/tmp/sqe-bench-data}"
BENCH_S3_ENDPOINT="${BENCH_S3_ENDPOINT:-}"
BENCH_S3_PROFILE="${BENCH_S3_PROFILE:-default}"
BENCH_S3_REGION="${BENCH_S3_REGION:-us-east-1}"
BENCH_S3_PATH_STYLE="${BENCH_S3_PATH_STYLE:-true}"
BENCH_GOLDEN_S3_ACCESS_KEY="${BENCH_GOLDEN_S3_ACCESS_KEY:-}"
BENCH_GOLDEN_S3_SECRET_KEY="${BENCH_GOLDEN_S3_SECRET_KEY:-}"
SQE_TOKEN_ENDPOINT="${SQE_TOKEN_ENDPOINT:-${BENCH_GOLDEN_POLARIS_URL%/}/v1/oauth/tokens}"
SQE_CLIENT_ID="${SQE_CLIENT_ID:-}"
SQE_CLIENT_SECRET="${SQE_CLIENT_SECRET:-}"
ICEBERG_BEARER_TOKEN="${ICEBERG_BEARER_TOKEN:-$BENCH_GOLDEN_TOKEN}"
# Deliberately NOT exported: `sqe-bench load`'s --client-id/--client-secret/
# --token-endpoint flags fall back to these exact env var names via clap,
# and would silently override the --username/--password handshake the load
# path below relies on (verified empirically -- exporting them reproduces
# "Invalid or expired bearer token" even with explicit --username/--password,
# because the coordinator's ClientCredentials authenticator mints its own
# token on handshake and doesn't accept a client-pre-fetched one). These
# values only need plain shell-variable interpolation into the coordinator
# config below and explicit CLI flags for bank's `generate`, neither of
# which requires export.

if [ -z "$BENCH_GOLDEN_TOKEN" ]; then
  echo "ERROR: BENCH_GOLDEN_TOKEN must be set (bearer for the golden Polaris REST skip-check)." >&2
  exit 1
fi
if [ -z "$BENCH_S3_ENDPOINT" ] || [ -z "$BENCH_GOLDEN_S3_ACCESS_KEY" ] || [ -z "$BENCH_GOLDEN_S3_SECRET_KEY" ]; then
  echo "ERROR: BENCH_S3_ENDPOINT, BENCH_GOLDEN_S3_ACCESS_KEY and BENCH_GOLDEN_S3_SECRET_KEY must be set (golden warehouse bucket)." >&2
  exit 1
fi

# Normalize the path-style toggle to a bare TOML boolean.
case "$(printf '%s' "$BENCH_S3_PATH_STYLE" | tr '[:upper:]' '[:lower:]')" in
  1|true|yes) BENCH_S3_PATH_STYLE=true ;;
  *) BENCH_S3_PATH_STYLE=false ;;
esac

SUITES=("$@"); [ $# -eq 0 ] && SUITES=("${ALL_READ_SUITES[@]}")

# Which of the requested suites need the throwaway coordinator (everything
# except bank, which writes straight to Iceberg, and tpcbb, which is skipped
# below since it shares the tpcds namespace).
NEEDS_COORDINATOR=0
for BENCH in "${SUITES[@]}"; do
  case "$BENCH" in
    bank|tpcbb) ;;
    *) NEEDS_COORDINATOR=1 ;;
  esac
done

if [ "$NEEDS_COORDINATOR" = "1" ] && { [ -z "$SQE_CLIENT_ID" ] || [ -z "$SQE_CLIENT_SECRET" ]; }; then
  echo "ERROR: SQE_CLIENT_ID and SQE_CLIENT_SECRET must be set for tpch/ssb/tpcds/clickbench (they authenticate the throwaway coordinator against the golden Polaris; bank alone can use ICEBERG_BEARER_TOKEN instead)." >&2
  exit 1
fi

# Mirrors crates/sqe-bench/src/main.rs::format_scale exactly (integral
# scales print without a decimal point; fractional scales replace '.' with
# '_'), so the namespace computed here for the REST skip-check always
# matches the namespace `sqe-bench load`/`generate` computes internally.
# Same formula scripts/benchmark-load.sh already uses for bank's namespace.
scale_fmt() {
  python3 -c "s='$1'; print((s.rstrip('0').rstrip('.') if '.' in s else s).replace('.', '_'))"
}

# Tier-1 source data resolution (only needed for the coordinator/load path;
# bank generates its own data in-process). Mirrors benchmark-load.sh's
# EXTERNAL_DATA branch.
DATA_S3_ACCESS_KEY=""
DATA_S3_SECRET_KEY=""
if [ "$NEEDS_COORDINATOR" = "1" ]; then
  case "$BENCH_DATA_SOURCE" in
    s3://*)
      DATA_S3_ACCESS_KEY="${DATA_S3_ACCESS_KEY:-$(aws configure get aws_access_key_id --profile "$BENCH_S3_PROFILE" 2>/dev/null || true)}"
      DATA_S3_SECRET_KEY="${DATA_S3_SECRET_KEY:-$(aws configure get aws_secret_access_key --profile "$BENCH_S3_PROFILE" 2>/dev/null || true)}"
      if [ -z "$DATA_S3_ACCESS_KEY" ] || [ -z "$DATA_S3_SECRET_KEY" ]; then
        echo "ERROR: could not resolve S3 credentials from profile '$BENCH_S3_PROFILE' for BENCH_DATA_SOURCE=$BENCH_DATA_SOURCE" >&2
        exit 1
      fi
      ;;
    *) ;;
  esac
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT_DIR"

PROFILE="${PROFILE:-release}"
case "$PROFILE" in
  release|debug|dev-release) ;;
  *) echo "ERROR: PROFILE must be 'release', 'dev-release' or 'debug', got: '$PROFILE'" >&2; exit 1 ;;
esac

if [ "$NEEDS_COORDINATOR" = "1" ]; then
  BUILD_TARGETS=(-p sqe-bench -p sqe-coordinator --bin sqe-bench --bin sqe-coordinator)
else
  BUILD_TARGETS=(-p sqe-bench --bin sqe-bench)
fi
echo "Building ${BUILD_TARGETS[*]} (profile: $PROFILE)..."
if [ "$PROFILE" = "release" ]; then
  cargo build "${BUILD_TARGETS[@]}" --release 2>&1
elif [ "$PROFILE" = "dev-release" ]; then
  cargo build "${BUILD_TARGETS[@]}" --profile dev-release 2>&1
else
  cargo build "${BUILD_TARGETS[@]}" 2>&1
fi
BENCH_BIN="$ROOT_DIR/target/$PROFILE/sqe-bench"
SQE_BIN="$ROOT_DIR/target/$PROFILE/sqe-coordinator"
echo ""

# ── Throwaway coordinator against the golden Polaris (load path only) ────
# Dedicated ports so this never collides with a coordinator the local test
# stack (scripts/benchmark-load.sh, port 60051/18080) may already have up.
GOLDEN_COORD_FLIGHT_PORT="${BENCH_GOLDEN_COORD_FLIGHT_PORT:-60052}"
GOLDEN_COORD_TRINO_PORT="${BENCH_GOLDEN_COORD_TRINO_PORT:-18082}"
GOLDEN_COORD_PROM_PORT="${BENCH_GOLDEN_COORD_PROM_PORT:-19092}"

GOLDEN_COORD_PID=""
GOLDEN_COORD_LOG=""
GOLDEN_COORD_CONFIG=""

start_golden_coordinator() {
  GOLDEN_COORD_CONFIG="$(mktemp /tmp/sqe-bench-golden-coord-XXXXXX.toml)"
  GOLDEN_COORD_LOG="$(mktemp /tmp/sqe-bench-golden-coord-XXXXXX.log)"

  # `load` ingests staged parquet via `CREATE TABLE ... AS SELECT * FROM
  # read_parquet(...)` (crates/sqe-bench/src/load.rs), which is gated by the
  # coordinator's SSRF allowlist (issue #46) when the data source carries an
  # inline `endpoint =>` override. Mirrors the same host-allowlisting
  # benchmark-load.sh does for its EXTERNAL_DATA branch.
  TVF_SECTION=""
  case "$BENCH_DATA_SOURCE" in
    s3://*)
      ENDPOINT_HOST=$(python3 -c "from urllib.parse import urlparse; import sys; print(urlparse(sys.argv[1]).hostname or '')" "$BENCH_S3_ENDPOINT")
      if [ -z "$ENDPOINT_HOST" ]; then
        echo "ERROR: could not parse host from BENCH_S3_ENDPOINT=$BENCH_S3_ENDPOINT" >&2
        exit 1
      fi
      TVF_SECTION=$'\n[storage.tvf]\nallowed_http_hosts = ["'"$ENDPOINT_HOST"'", "localhost", "127.0.0.1"]'
      ;;
    *)
      TVF_SECTION=$'\n[storage.tvf]\nallow_local_paths = true'
      ;;
  esac

  cat > "$GOLDEN_COORD_CONFIG" <<EOF
[coordinator]
flight_sql_port = $GOLDEN_COORD_FLIGHT_PORT
trino_http_port = $GOLDEN_COORD_TRINO_PORT
memory_limit = "64GB"
spill_to_disk = true
spill_dir = "/tmp/sqe-bench-golden-spill"
spill_compression = "lz4"

[metrics]
prometheus_port = $GOLDEN_COORD_PROM_PORT

[auth]
token_endpoint = "$SQE_TOKEN_ENDPOINT"
client_id = "$SQE_CLIENT_ID"
client_secret = "$SQE_CLIENT_SECRET"

[query]
max_result_rows = 10_000_000
max_query_memory = "32GB"
per_user_memory_budget = "0"
timeout_secs = 1800

[catalog]
polaris_url = "$BENCH_GOLDEN_POLARIS_URL"
warehouse = "$BENCH_GOLDEN_WAREHOUSE"

[storage]
s3_endpoint = "$BENCH_S3_ENDPOINT"
s3_access_key = "$BENCH_GOLDEN_S3_ACCESS_KEY"
s3_secret_key = "$BENCH_GOLDEN_S3_SECRET_KEY"
s3_region = "$BENCH_S3_REGION"
s3_path_style = $BENCH_S3_PATH_STYLE
$TVF_SECTION
EOF

  echo "Starting throwaway coordinator against the golden Polaris (flight :$GOLDEN_COORD_FLIGHT_PORT)..."
  RUST_LOG="${RUST_LOG:-sqe=info,warn}" "$SQE_BIN" "$GOLDEN_COORD_CONFIG" > "$GOLDEN_COORD_LOG" 2>&1 &
  GOLDEN_COORD_PID=$!

  for _ in $(seq 1 120); do
    if curl -so /dev/null "http://localhost:${GOLDEN_COORD_TRINO_PORT}/v1/info" 2>/dev/null; then
      echo "  ready (PID $GOLDEN_COORD_PID)"
      return 0
    fi
    if ! kill -0 "$GOLDEN_COORD_PID" 2>/dev/null; then
      echo "ERROR: golden coordinator exited during startup" >&2
      tail -40 "$GOLDEN_COORD_LOG" >&2
      exit 1
    fi
    sleep 1
  done
  echo "ERROR: golden coordinator did not become ready in time" >&2
  kill "$GOLDEN_COORD_PID" 2>/dev/null || true
  exit 1
}

cleanup() {
  if [ -n "$GOLDEN_COORD_PID" ]; then
    kill "$GOLDEN_COORD_PID" 2>/dev/null || true
    wait "$GOLDEN_COORD_PID" 2>/dev/null || true
  fi
  rm -f "$GOLDEN_COORD_CONFIG" "$GOLDEN_COORD_LOG"
}
trap cleanup EXIT INT TERM

if [ "$NEEDS_COORDINATOR" = "1" ]; then
  start_golden_coordinator
fi

# ── Publish loop ──────────────────────────────────────────────────────────
for BENCH in "${SUITES[@]}"; do
  # tpcbb reuses tpcds tables; publishing tpcds covers it.
  if [ "$BENCH" = "tpcbb" ]; then
    echo "SKIP tpcbb: reuses the tpcds namespace; publish tpcds instead."
    continue
  fi

  NS="${BENCH}_sf$(scale_fmt "$BENCH_SCALE")"
  echo "== publishing $BENCH -> $BENCH_GOLDEN_POLARIS_URL ns=$NS =="

  # Skip-if-present: query the golden Polaris REST catalog directly for the
  # namespace's table list. Nothing is ATTACHed at publish time, so this
  # must not go through an attached `golden` catalog -- uses the Iceberg
  # REST listTables endpoint with the golden bearer token directly (mirrors
  # crates/sqe-catalog/src/rest_catalog.rs::rest_prefix: <url>/v1/<warehouse>).
  if [ "$BENCH_FORCE" != "1" ]; then
    LIST_URL="${BENCH_GOLDEN_POLARIS_URL%/}/v1/${BENCH_GOLDEN_WAREHOUSE}/namespaces/${NS}/tables"
    COUNT=$(curl -sf -H "Authorization: Bearer ${BENCH_GOLDEN_TOKEN}" "$LIST_URL" \
              2>/dev/null | python3 -c 'import sys,json;print(len(json.load(sys.stdin).get("identifiers",[])))' 2>/dev/null || echo 0)
    if [ "${COUNT:-0}" -gt 0 ]; then
      echo "SKIP $BENCH: golden namespace $NS already has $COUNT tables."
      continue
    fi
  fi

  # ── bank: direct-to-Iceberg, no coordinator (matches benchmark-load.sh) ──
  if [ "$BENCH" = "bank" ]; then
    SCALE_FMT="$(scale_fmt "$BENCH_SCALE")"
    BANK_ROWS_PER_DAY=$(python3 -c "print(max(1, int(2_000_000 * float('$BENCH_SCALE'))))")
    BANK_CUSTOMERS=$(python3 -c "print(max(100, int(100_000 * float('$BENCH_SCALE'))))")
    BANK_AUTH_ARGS=()
    if [ -n "$SQE_CLIENT_ID" ] && [ -n "$SQE_CLIENT_SECRET" ]; then
      BANK_AUTH_ARGS=(--client-id "$SQE_CLIENT_ID" --client-secret "$SQE_CLIENT_SECRET" --oauth2-server-uri "$SQE_TOKEN_ENDPOINT")
    else
      BANK_AUTH_ARGS=(--bearer-token "$ICEBERG_BEARER_TOKEN")
    fi
    AWS_SECRET_ACCESS_KEY="$BENCH_GOLDEN_S3_SECRET_KEY" "$BENCH_BIN" generate bank \
      --sink iceberg \
      --days 12 \
      --rows-per-day "$BANK_ROWS_PER_DAY" \
      --customers "$BANK_CUSTOMERS" \
      --namespace "bank_sf${SCALE_FMT}" \
      --catalog-uri "$BENCH_GOLDEN_POLARIS_URL" \
      --warehouse "$BENCH_GOLDEN_WAREHOUSE" \
      "${BANK_AUTH_ARGS[@]}" \
      --s3-endpoint "$BENCH_S3_ENDPOINT" \
      --s3-access-key "$BENCH_GOLDEN_S3_ACCESS_KEY" \
      --s3-region "$BENCH_S3_REGION" \
      --s3-path-style \
      --clean
    continue
  fi

  # ── everything else: `sqe-bench load` against the throwaway coordinator ──
  DATA_S3_ARGS=()
  # Secret goes through the environment (AWS_SECRET_ACCESS_KEY, clap's env
  # fallback), never argv, so it is not visible in `ps`. Set only when creds
  # are present, so an absent secret stays None rather than Some("").
  DATA_S3_SECRET_ENV=()
  if [ -n "$DATA_S3_ACCESS_KEY" ]; then
    DATA_S3_ARGS=(--s3-access-key "$DATA_S3_ACCESS_KEY" --s3-endpoint "$BENCH_S3_ENDPOINT" --s3-region "$BENCH_S3_REGION")
    DATA_S3_SECRET_ENV=(AWS_SECRET_ACCESS_KEY="$DATA_S3_SECRET_KEY")
  fi
  # NOTE: the coordinator's legacy ClientCredentials authenticator mints its
  # OWN token on `handshake(user, pass)` (the config's [auth] client_id/
  # client_secret drive that, not the client's flags) -- "single service
  # token" per its own startup log line. A client that instead pre-fetches
  # a token itself (--token-endpoint/--client-id/--client-secret on `load`)
  # sends a token the coordinator's authenticator doesn't recognize
  # (`NotMyCredentials` -> "no provider accepted the credentials", verified
  # empirically against the local stack). So authenticate with a plain
  # Flight handshake instead, exactly like benchmark-load.sh does. `env -u`
  # guards against SQE_TOKEN_ENDPOINT/SQE_CLIENT_ID/SQE_CLIENT_SECRET being
  # exported ambiently in the caller's shell -- clap's env fallback on
  # `load`'s --client-id/--client-secret/--token-endpoint would otherwise
  # silently win over --username/--password even though this script never
  # exports them itself. Same trap for SQE_NAMESPACE/SQE_CATALOG: those are
  # also clap-env fallbacks on `load`, and an ambient SQE_NAMESPACE would
  # make `load` write to a different namespace than the scale_fmt-derived
  # one the REST skip-check above and `sqe-bench test` both expect.
  env -u SQE_TOKEN_ENDPOINT -u SQE_CLIENT_ID -u SQE_CLIENT_SECRET \
    -u SQE_NAMESPACE -u SQE_CATALOG \
    ${DATA_S3_SECRET_ENV[@]+"${DATA_S3_SECRET_ENV[@]}"} \
    "$BENCH_BIN" load "$BENCH" \
    --scale "$BENCH_SCALE" \
    --data "$BENCH_DATA_SOURCE" \
    --protocol flight \
    --host localhost \
    --port "$GOLDEN_COORD_FLIGHT_PORT" \
    --username root \
    --password "" \
    ${DATA_S3_ARGS[@]+"${DATA_S3_ARGS[@]}"} \
    --clean
done

echo "golden publish complete."
