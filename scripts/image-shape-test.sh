#!/usr/bin/env bash
# Build and start both published Docker targets without assuming a shell,
# coreutils, or a fixed runtime UID inside either image.
set -euo pipefail

RUNTIME_IMAGE="${RUNTIME_IMAGE:-sqe-image-shape-runtime:local}"
BENCH_IMAGE="${BENCH_IMAGE:-sqe-image-shape-bench:local}"
RUNTIME_CONTAINER="sqe-image-shape-runtime-$$"
RUNTIME_VOLUME="sqe-image-shape-runtime-$$"
BENCH_VOLUME="sqe-image-shape-bench-$$"

cleanup() {
  docker rm -f "$RUNTIME_CONTAINER" >/dev/null 2>&1 || true
  docker volume rm "$RUNTIME_VOLUME" "$BENCH_VOLUME" >/dev/null 2>&1 || true
}
trap cleanup EXIT

image_uid() {
  local image="$1" user uid
  user="$(docker image inspect --format '{{.Config.User}}' "$image")"
  uid="${user%%:*}"
  case "$uid" in
    ''|*[!0-9]*)
      echo "ERROR: $image must declare a numeric non-root USER; got '${user:-<empty>}'" >&2
      return 1
      ;;
    0)
      echo "ERROR: $image must not run as root" >&2
      return 1
      ;;
  esac
  printf '%s\n' "$uid"
}

prepare_volume() {
  local volume="$1" uid="$2"
  docker volume create "$volume" >/dev/null
  # The helper owns the volume setup; published SQE images remain shell-free.
  docker run --rm --user 0 -v "$volume:/data" busybox:1.38.0-uclibc \
    chown "$uid:$uid" /data
}

echo "Building runtime target"
if [ "${SKIP_BUILD:-0}" != 1 ]; then
  docker buildx build --load --target runtime --tag "$RUNTIME_IMAGE" .
fi
runtime_uid="$(image_uid "$RUNTIME_IMAGE")"
prepare_volume "$RUNTIME_VOLUME" "$runtime_uid"

healthcheck="$(docker image inspect --format '{{json .Config.Healthcheck.Test}}' "$RUNTIME_IMAGE")"
case "$healthcheck" in
  '["CMD",'*'/usr/local/bin/wget'*'/healthz"'*) ;;
  *)
    echo "ERROR: runtime healthcheck must be an exec-form wget /healthz probe; got $healthcheck" >&2
    exit 1
    ;;
esac

echo "Starting runtime target as UID $runtime_uid"
docker run -d --name "$RUNTIME_CONTAINER" \
  -e SQE_METRICS__AUDIT_LOG_PATH=/var/lib/sqe/audit/audit.jsonl \
  -e SQE_METRICS__PROMETHEUS_PORT=9090 \
  -v "$PWD/tests/sqe-test.toml:/etc/sqe/sqe.toml:ro" \
  -v "$RUNTIME_VOLUME:/var/lib/sqe/audit" \
  "$RUNTIME_IMAGE" --config /etc/sqe/sqe.toml >/dev/null

for _ in $(seq 1 60); do
  state="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$RUNTIME_CONTAINER")"
  [ "$state" = healthy ] && break
  if [ "$state" = exited ] || [ "$state" = dead ]; then
    docker logs "$RUNTIME_CONTAINER" >&2
    exit 1
  fi
  sleep 2
done
[ "${state:-}" = healthy ] || {
  docker inspect "$RUNTIME_CONTAINER" >&2
  docker logs "$RUNTIME_CONTAINER" >&2
  exit 1
}
docker run --rm -v "$RUNTIME_VOLUME:/data" busybox:1.38.0-uclibc \
  test -w /data/audit.jsonl
audit_uid="$(docker run --rm -v "$RUNTIME_VOLUME:/data" busybox:1.38.0-uclibc \
  stat -c %u /data/audit.jsonl)"
[ "$audit_uid" = "$runtime_uid" ] || {
  echo "ERROR: audit file owner $audit_uid does not match runtime UID $runtime_uid" >&2
  exit 1
}

if [ "${RUNTIME_ONLY:-0}" = 1 ]; then
  echo "PASS: runtime healthy and writable"
  exit 0
fi

echo "Building bench-runtime target"
if [ "${SKIP_BUILD:-0}" != 1 ]; then
  docker buildx build --load --target bench-runtime --tag "$BENCH_IMAGE" .
fi
bench_uid="$(image_uid "$BENCH_IMAGE")"
prepare_volume "$BENCH_VOLUME" "$bench_uid"

echo "Running bench target as UID $bench_uid"
docker run --rm -v "$BENCH_VOLUME:/output" "$BENCH_IMAGE" \
  generate ssb --scale 0.001 --threads 1 --output /output
docker run --rm -v "$BENCH_VOLUME:/data" busybox:1.38.0-uclibc \
  find /data -type f -name '*.parquet' -print -quit | grep -q .
parquet_uid="$(docker run --rm -v "$BENCH_VOLUME:/data" busybox:1.38.0-uclibc \
  find /data -type f -name '*.parquet' -exec stat -c %u '{}' ';' -quit)"
[ "$parquet_uid" = "$bench_uid" ] || {
  echo "ERROR: Parquet owner $parquet_uid does not match bench UID $bench_uid" >&2
  exit 1
}

echo "PASS: runtime healthy and writable; bench-runtime started and wrote Parquet"
