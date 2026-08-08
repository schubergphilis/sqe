#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$ROOT_DIR"

TAG="${1:-latest}"
REGISTRY="${SQE_REGISTRY:-}"

echo "Building Docker images (tag: $TAG)..."

# Single multi-binary image (sqe-server / sqe-worker / sqe-cli) on distroless.
docker build --target runtime -t "sqe:$TAG" .
# Optional one-shot benchmark generator (same builder stage, distinct entrypoint).
docker build --target bench-runtime -t "sqe-bench:$TAG" .

echo ""
echo "Images built:"
docker images | grep -E "^sqe(-bench)?\s" | head -6

if [ -n "$REGISTRY" ]; then
    echo ""
    echo "Pushing to $REGISTRY..."
    for img in sqe sqe-bench; do
        docker tag "$img:$TAG" "$REGISTRY/$img:$TAG"
        docker push "$REGISTRY/$img:$TAG"
    done
fi
