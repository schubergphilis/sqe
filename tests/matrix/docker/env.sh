# shellcheck shell=bash
# Source to point the suite at the stack started by start-sqe.sh:
#   source tests/matrix/docker/env.sh
# Upstream's variables keep their names, so the suite also runs against
# upstream's own start-polaris.sh stack (set ICEBERG_S3_ENDPOINT= empty there).
export ICEBERG_REST_URI="${ICEBERG_REST_URI:-http://127.0.0.1:${POLARIS_PORT:-48181}/api/catalog}"
export ICEBERG_REST_WAREHOUSE="${ICEBERG_REST_WAREHOUSE:-demo}"
export ICEBERG_REST_CREDENTIAL="${ICEBERG_REST_CREDENTIAL:-root:s3cr3t}"
export ICEBERG_REST_SCOPE="${ICEBERG_REST_SCOPE:-PRINCIPAL_ROLE:ALL}"
export ICEBERG_S3_KEY_ID="${ICEBERG_S3_KEY_ID:-rustfs}"
export ICEBERG_S3_SECRET="${ICEBERG_S3_SECRET:-rustfs12345}"
export ICEBERG_S3_REGION="${ICEBERG_S3_REGION:-us-east-1}"
# Host-side S3 address (the catalog advertises the in-compose http://rustfs:9000).
export ICEBERG_S3_ENDPOINT="${ICEBERG_S3_ENDPOINT:-http://127.0.0.1:${RUSTFS_PORT:-49000}}"
export SQE_HOST="${SQE_HOST:-127.0.0.1}"
export SQE_PORT="${SQE_PORT:-48080}"
export SQE_FLIGHT_PORT="${SQE_FLIGHT_PORT:-48051}"
export SQE_USER="${SQE_USER:-root}"
export SQE_PASSWORD="${SQE_PASSWORD:-s3cr3t}"
export SQE_CATALOG="${SQE_CATALOG:-iceberg}"
