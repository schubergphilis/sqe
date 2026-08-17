#!/usr/bin/env bash
set -euo pipefail

# Preflight for the local integration suites (`make test-integration`,
# `make test-distributed`). The CI jobs that used to run these are gone
# (issue #387: no working dind on the shared runners), so these suites are the
# gate and their failures have to be readable.
#
# Two checks, both of which otherwise surface as noise:
#   1. No Docker daemon -> compose prints a connection error.
#   2. A fixed host port already bound -> compose fails ~14 lines into
#      bring-up with "port is already allocated" and does not say by what.
#      Stale rigs are the usual cause: the bench, parity and quickstart stacks
#      publish overlapping ports from differently-named compose projects.
#
# Usage: integration-preflight.sh [--distributed]

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

DISTRIBUTED=0
[ "${1:-}" = "--distributed" ] && DISTRIBUTED=1

# ── 1. Daemon ──────────────────────────────────────────────────
if ! docker info >/dev/null 2>&1; then
    echo "ERROR: no Docker daemon reachable (checked \`docker info\`)." >&2
    echo "       Start Docker Desktop / colima, then re-run." >&2
    exit 1
fi

# ── 2. Host ports ──────────────────────────────────────────────
# Host-side ports published by docker-compose.test.yml, plus the distributed
# overlay's coordinator/worker ports. Keep in sync with those files.
PORTS=(18181 19000 15432)
COMPOSE_ARGS=( -f "$ROOT_DIR/docker-compose.test.yml" )
if [ "$DISTRIBUTED" = "1" ]; then
    PORTS+=(60051 28080 29090 60061 29091 60062 29092)
    COMPOSE_ARGS+=( -f "$ROOT_DIR/docker-compose.distributed.yml" )
fi

# Containers belonging to THIS repo's compose project. A port they hold is the
# suite's own stack left up from a previous run, which is intended: bootstrap is
# idempotent, so a rerun reuses it instead of paying the bring-up again. Only a
# foreign holder is a conflict. Matching on the project's own container list
# rather than a name prefix matters: `sqlengine-rand-010-polaris-1` (a stale
# bench rig) shares the `sqlengine-` prefix but is NOT this project.
OWN_CONTAINERS="$(docker compose "${COMPOSE_ARGS[@]}" ps --all --format '{{.Name}}' 2>/dev/null || true)"

# name<TAB>ports for every running container, once.
PORT_MAP="$(docker ps --format '{{.Names}}	{{.Ports}}' 2>/dev/null || true)"

conflicts=0
for port in "${PORTS[@]}"; do
    # ":<port>->" is how `docker ps` renders a published host port.
    holder="$(printf '%s\n' "$PORT_MAP" | awk -F'\t' -v p=":${port}->" \
        'index($2, p) { print $1; exit }')"

    if [ -n "$holder" ]; then
        if printf '%s\n' "$OWN_CONTAINERS" | grep -qxF "$holder"; then
            continue  # our own stack, still up from a previous run
        fi
        echo "ERROR: host port $port is held by container '$holder'," >&2
        echo "       which is not part of this repo's compose project." >&2
        echo "       Stop it first:  docker rm -f $holder" >&2
        conflicts=$((conflicts + 1))
        continue
    fi

    # No container claims it, but a host process might.
    if command -v lsof >/dev/null 2>&1 && lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
        echo "ERROR: host port $port is bound by a non-Docker process." >&2
        echo "       Identify it with:  lsof -nP -iTCP:$port -sTCP:LISTEN" >&2
        conflicts=$((conflicts + 1))
    fi
done

if [ "$conflicts" -gt 0 ]; then
    echo "" >&2
    echo "$conflicts port conflict(s). The compose bring-up would fail on the" >&2
    echo "first one with 'port is already allocated' and not name the holder." >&2
    exit 1
fi

echo "Preflight OK (docker daemon up, ${#PORTS[@]} host ports free or ours)."
