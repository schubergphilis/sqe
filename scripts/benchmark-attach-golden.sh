#!/usr/bin/env bash
set -euo pipefail

# One-shot coordinator-wide ATTACH of the golden Iceberg catalog. ATTACH is
# global and persists across sessions (crates/sqe-coordinator), so a single
# `sqe-cli -e "ATTACH ..."` invocation is all that's needed -- no new binary
# subcommand. Run this once after the coordinator is up and before any
# `sqe-bench test --catalog golden` load-many run; every session afterwards
# sees the `golden` catalog.
#
# The target coordinator must run with the admin-capable config from Task 2,
# tests/benchmark-attach/coordinator-attach.toml. Its `bearer_passthrough`
# [[auth.providers]] entry assigns a fixed `service_admin` role regardless
# of the bearer token's own content, which is what satisfies ATTACH's
# `require_admin` gate (crates/sqe-coordinator/src/query_handler.rs). This
# script does not start that coordinator (see Task 5).
#
# bearer_passthrough forwards the bearer verbatim as the session's
# `catalog_token`, so BENCH_GOLDEN_TOKEN doubles as both the coordinator
# session credential (`sqe-cli --token`) and the golden catalog's own
# Polaris token (the ATTACH statement's `TOKEN '...'` clause) -- this is
# the exact pattern proven working in the Task 1 spike
# (.superpowers/sdd/task-1-report.md). Connecting without --token falls
# back to sqe-cli's interactive username/password prompt, which would hang
# a non-interactive script and, even answered, carries no admin role.
#
# S3 config travels IN the ATTACH statement (Task 2, crates/sqe-catalog/
# src/mount.rs::s3_props_from_options), so the golden catalog's FileIO
# reaches a custom endpoint (RustFS locally, StorageGRID in prod) without
# depending on ambient AWS_* env vars on the coordinator process.
#
# Usage:
#   BENCH_GOLDEN_POLARIS_URL=http://localhost:18181/api/catalog \
#   BENCH_GOLDEN_WAREHOUSE=quickstart_catalog \
#   BENCH_GOLDEN_TOKEN=<bearer> \
#   BENCH_S3_ENDPOINT=http://localhost:19000 \
#   BENCH_GOLDEN_S3_ACCESS_KEY=... BENCH_GOLDEN_S3_SECRET_KEY=... \
#   bash scripts/benchmark-attach-golden.sh
#
# Environment:
#   BENCH_GOLDEN_POLARIS_URL   golden Polaris REST catalog URI (required)
#   BENCH_GOLDEN_WAREHOUSE     warehouse name in the golden Polaris (required)
#   BENCH_GOLDEN_TOKEN         bearer token for the golden Polaris AND for
#                              the coordinator session (required)
#   BENCH_S3_ENDPOINT          S3 endpoint for the golden warehouse bucket
#                              (required)
#   BENCH_GOLDEN_S3_ACCESS_KEY / BENCH_GOLDEN_S3_SECRET_KEY
#                              credentials for the golden warehouse bucket
#                              (required)
#   BENCH_S3_REGION            S3 region (default: us-east-1)
#   BENCH_S3_PATH_STYLE        path-style S3 addressing, MinIO/RustFS/
#                              StorageGRID style (default: true)
#   BENCH_HOST                 coordinator host (default: localhost)
#   BENCH_PORT_FLIGHT          coordinator Flight SQL port (default: 60051,
#                              matches tests/benchmark-attach/coordinator-attach.toml)

: "${BENCH_GOLDEN_POLARIS_URL:?set BENCH_GOLDEN_POLARIS_URL}"
: "${BENCH_GOLDEN_WAREHOUSE:?set BENCH_GOLDEN_WAREHOUSE}"
: "${BENCH_GOLDEN_TOKEN:?set BENCH_GOLDEN_TOKEN}"

HOST="${BENCH_HOST:-localhost}"
PORT="${BENCH_PORT_FLIGHT:-60051}"
# S3 config travels IN the ATTACH statement (Task 2 added these options), so
# the golden catalog's FileIO reaches a custom endpoint without relying on
# ambient AWS_* env vars on the coordinator.
: "${BENCH_S3_ENDPOINT:?set BENCH_S3_ENDPOINT}"
: "${BENCH_GOLDEN_S3_ACCESS_KEY:?set BENCH_GOLDEN_S3_ACCESS_KEY}"
: "${BENCH_GOLDEN_S3_SECRET_KEY:?set BENCH_GOLDEN_S3_SECRET_KEY}"
S3_REGION="${BENCH_S3_REGION:-us-east-1}"
S3_PATH_STYLE="${BENCH_S3_PATH_STYLE:-true}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT_DIR"

SQL="ATTACH '${BENCH_GOLDEN_POLARIS_URL}' AS golden (TYPE iceberg_rest, WAREHOUSE '${BENCH_GOLDEN_WAREHOUSE}', TOKEN '${BENCH_GOLDEN_TOKEN}', S3_ENDPOINT '${BENCH_S3_ENDPOINT}', S3_REGION '${S3_REGION}', S3_ACCESS_KEY '${BENCH_GOLDEN_S3_ACCESS_KEY}', S3_SECRET_KEY '${BENCH_GOLDEN_S3_SECRET_KEY}', S3_PATH_STYLE '${S3_PATH_STYLE}')"

cargo run -q -p sqe-cli -- --host "$HOST" --port "$PORT" --token "$BENCH_GOLDEN_TOKEN" -e "$SQL"

echo "attached golden: ${BENCH_GOLDEN_POLARIS_URL}"
