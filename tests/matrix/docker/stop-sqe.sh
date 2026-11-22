#!/usr/bin/env bash
# Tear the matrix stack down and drop its volumes.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
SQE_IMAGE="${SQE_IMAGE:-unused}" docker compose -f docker-compose.sqe.yml down -v --remove-orphans
