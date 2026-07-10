#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$ROOT_DIR"

TAG="${1:-latest}"
REGISTRY="${SQE_REGISTRY:-}"

echo "Building Docker images (tag: $TAG)..."

docker build --target coordinator -t "sqe-coordinator:$TAG" .
docker build --target worker -t "sqe-worker:$TAG" .
docker build --target cli -t "sqe-cli:$TAG" .

echo ""
echo "Images built:"
docker images | grep -E "^sqe-(coordinator|worker|cli)" | head -6

if [ -n "$REGISTRY" ]; then
    echo ""
    echo "Pushing to $REGISTRY..."
    for img in sqe-coordinator sqe-worker sqe-cli; do
        docker tag "$img:$TAG" "$REGISTRY/$img:$TAG"
        docker push "$REGISTRY/$img:$TAG"
    done
fi
