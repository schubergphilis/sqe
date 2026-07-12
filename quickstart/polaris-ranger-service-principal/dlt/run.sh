#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

transport="trino"
if [ "${1:-}" = "--flight" ]; then
  transport="flight"
  shift
elif [ "${1:-}" = "--trino" ]; then
  shift
elif [ "${1:-}" = "--both" ]; then
  shift
  for selected in trino flight; do
    echo "Running dlt E2E suite with SQE transport: ${selected}"
    docker compose -f docker-compose.yml -f docker-compose.dlt.yml \
      --profile dlt run --rm --build -e "SQE_TRANSPORT=${selected}" dlt-tests \
      pytest -q -s /tests/test_dlt_load_paths.py "$@"
  done
  exit 0
fi

echo "Running dlt E2E suite with SQE transport: ${transport}"
docker compose -f docker-compose.yml -f docker-compose.dlt.yml \
  --profile dlt run --rm --build -e "SQE_TRANSPORT=${transport}" dlt-tests \
  pytest -q -s /tests/test_dlt_load_paths.py "$@"
