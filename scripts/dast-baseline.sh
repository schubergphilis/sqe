#!/usr/bin/env bash
# DAST baseline: start a local sqe-server and run the OWASP ZAP baseline scan
# (spider + passive rules) against its HTTP front door. Any finding not
# reviewed in .zap/baseline.conf fails the run. Local gate, like
# `make test-integration`: needs Docker, and the CI runners have none.
#
# Scope: the unauthenticated Trino HTTP / web UI listener as the flag-only
# default config serves it. Authenticated routes, MCP and Flight SQL are not
# covered by a baseline scan.
#
#   make dast                     # builds sqe-server if needed
#   SQE_BIN=path/to/sqe-server make dast
set -euo pipefail

# Pinned by digest; bump deliberately.
ZAP_IMAGE="zaproxy/zap-stable@sha256:781a2bdaea47324e7bab583e2263f21d257b0aee61ed51521a5be45f5f5081ef"
PORT="${SQE_DAST_PORT:-8080}"
OUT="${SQE_DAST_OUT:-target/dast}"
BIN="${SQE_BIN:-target/debug/sqe-server}"

[[ -x "$BIN" ]] || cargo build -q -p sqe-server --bin sqe-server
mkdir -p "$OUT"
chmod 777 "$OUT"   # the ZAP container writes its reports as a non-root user
cp .zap/baseline.conf "$OUT/baseline.conf"

# No graceful drain (default 25 s): the scan is over, stop at once, and wait
# so a second run does not find the port still taken.
SQE_COORDINATOR__SHUTDOWN_DRAIN_SECS=0 "$BIN" > "$OUT/sqe-server.log" 2>&1 &
SERVER=$!
trap 'kill "$SERVER" 2>/dev/null; wait "$SERVER" 2>/dev/null || true' EXIT

for _ in $(seq 1 60); do
    curl -fsS -o /dev/null "http://127.0.0.1:${PORT}/v1/info" && break
    sleep 1
done
curl -fsS -o /dev/null "http://127.0.0.1:${PORT}/v1/info" || {
    echo "sqe-server did not come up on :${PORT}; see $OUT/sqe-server.log" >&2
    exit 1
}

# zap-baseline exits 1 on a FAIL rule, 2 on WARN only, 0 when clean.
set +e
docker run --rm --add-host=host.docker.internal:host-gateway \
    -v "$PWD/$OUT:/zap/wrk:rw" "$ZAP_IMAGE" \
    zap-baseline.py -t "http://host.docker.internal:${PORT}/" \
    -c baseline.conf -r report.html -J report.json
rc=$?
set -e
echo "==> DAST report: $OUT/report.html"
[[ $rc -eq 0 || $rc -eq 2 ]] || { echo "DAST baseline: findings above fail the gate" >&2; exit 1; }
echo "==> DAST baseline clean"
