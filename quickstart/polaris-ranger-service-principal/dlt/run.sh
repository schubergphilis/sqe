#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

transport="trino"
run_both=false
build_image=false
pytest_args=()

while [ "$#" -gt 0 ]; do
  case "$1" in
    --flight)
      transport="flight"
      ;;
    --trino)
      transport="trino"
      ;;
    --both)
      run_both=true
      ;;
    --build)
      build_image=true
      ;;
    --)
      shift
      pytest_args+=("$@")
      break
      ;;
    *)
      pytest_args+=("$1")
      ;;
  esac
  shift
done

run_tests() {
  local selected="$1"
  local -a command=(
    docker compose -f docker-compose.yml -f docker-compose.dlt.yml
    --profile dlt run --rm
  )
  if [ "$build_image" = true ]; then
    command+=(--build)
  fi
  command+=(
    -e "SQE_TRANSPORT=${selected}" dlt-tests
    pytest -q -s /tests/test_dlt_load_paths.py
  )
  if [ "${#pytest_args[@]}" -gt 0 ]; then
    command+=("${pytest_args[@]}")
  fi

  echo "Running dlt E2E suite with SQE transport: ${selected}"
  "${command[@]}"
}

if [ "$run_both" = true ]; then
  run_tests trino
  run_tests flight
else
  run_tests "$transport"
fi
