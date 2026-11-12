#!/usr/bin/env bash
set -euo pipefail

# Bootstrap the distributed test stack: the single-node catalog setup
# (bootstrap-test.sh), then wait for the coordinator and the workers.

export POLARIS_URL="${POLARIS_URL:-http://localhost:18181}"
export S3_URL="${S3_URL:-http://localhost:19000}"
export S3_ACCESS_KEY="${S3_ACCESS_KEY:-s3admin}"
export S3_SECRET_KEY="${S3_SECRET_KEY:-s3admin}"
export CLIENT_ID="${CLIENT_ID:-root}"
export CLIENT_SECRET="${CLIENT_SECRET:-s3cr3t}"
export WAREHOUSE="${WAREHOUSE:-test_warehouse}"

echo "=== SQE Distributed Stack Bootstrap ==="
echo "Polaris:      $POLARIS_URL"
echo "S3:           $S3_URL"
echo "Coordinator:  localhost:60051 (Flight SQL) / localhost:28080 (Trino HTTP)"
echo "Workers:      localhost:60061, localhost:60062"
echo ""

# ── Catalog setup ─────────────────────────────────────────────
# The distributed overlay shares Polaris and RustFS (same ports) with the
# single-node test stack, and the suite runs the same tests, so it needs the
# same catalog state: both buckets, test_warehouse with `default` and
# `test_ns`, and discovery_test_wh with `disc_ns`. Reuse the single-node
# bootstrap (idempotent) instead of keeping a second copy that drifts; the
# old copy created only test_warehouse, so every catalog-discovery test
# failed on a fresh distributed stack.
"$(dirname "$0")/bootstrap-test.sh"

# ── Wait for coordinator ──────────────────────────────────────
echo ""
echo -n "Waiting for SQE coordinator..."
for i in $(seq 1 60); do
    if curl -so /dev/null "http://localhost:28080/v1/info" 2>/dev/null; then
        echo " ready"; break
    fi
    if [ "$i" -eq 60 ]; then echo " TIMEOUT"; exit 1; fi
    echo -n "."
    sleep 1
done

# ── Wait for workers ──────────────────────────────────────────
echo -n "Waiting for workers to register..."
sleep 10  # Give workers time to heartbeat
echo " done"

echo ""
echo "=== Distributed Stack Ready ==="
echo ""
echo "Query via Flight SQL:"
echo "  cargo run --bin sqe-cli -- --host localhost --port 60051 --username root --password ''"
echo ""
echo "Query via Trino HTTP:"
echo "  curl -X POST http://localhost:28080/v1/statement -u root: -d 'SELECT * FROM system.runtime.nodes'"
echo ""
echo "System tables to try:"
echo "  SELECT * FROM system.runtime.queries ORDER BY created DESC;"
echo "  SELECT * FROM system.runtime.nodes;"
echo "  SELECT * FROM system.runtime.tasks;"
echo "  SELECT * FROM system.metadata.catalogs;"
echo "  SELECT * FROM system.metadata.table_properties;"
