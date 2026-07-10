#!/usr/bin/env bash
# Incremental dev-machine maintenance: trim the build caches that grow
# without bound instead of nuking them (`make clean` already does that).
#
# Found 2026-06-12: target/ had grown to 270 GB (239 GB of it the debug
# profile), Docker held 7.6 GB of stale BuildKit layers, and the machine
# was 10.7 GB into swap. Builds were paying for it.
#
# What it does, in order:
#   1. Remove legacy loose .o files from the former unpacked-debug profile,
#      then cargo-sweep artifacts not touched for SWEEP_DAYS
#      (default 14). Keeps the current toolchain's warm cache; the next
#      build stays incremental. Installs nothing; prints the install
#      hint when cargo-sweep is missing.
#   2. Docker (only when the daemon is reachable): prune dangling images
#      and cap the BuildKit cache at DOCKER_BUILD_CACHE (default 10GB).
#      Containers and volumes are NEVER touched: a live test stack and
#      its loaded benchmark data survive maintenance.
#   3. /tmp debris: sqe test logs and spill dirs older than a day.
#
# Run it manually:
#   make maintain
# Or weekly via cron (Sunday 09:00):
#   ( crontab -l; echo '0 9 * * 0 cd <repo> && ./scripts/dev-maintenance.sh' ) | crontab -
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SWEEP_DAYS="${SWEEP_DAYS:-14}"
DOCKER_BUILD_CACHE="${DOCKER_BUILD_CACHE:-10GB}"

human_size() {
    du -sh "$1" 2>/dev/null | cut -f1
}

echo "=== SQE dev maintenance ==="

# ── 1. Cargo target sweep ──────────────────────────────────────
if [ -d "$REPO_ROOT/target" ]; then
    BEFORE=$(human_size "$REPO_ROOT/target")

    # Current builds use split-debuginfo=off. Any loose objects directly in
    # deps/ are stale artifacts from the former unpacked profile. Do not run
    # maintenance concurrently with Cargo.
    LOOSE_OBJECT_DIR="$REPO_ROOT/target/debug/deps"
    if [ -d "$LOOSE_OBJECT_DIR" ]; then
        LOOSE_OBJECTS=$(find "$LOOSE_OBJECT_DIR" -maxdepth 1 -type f -name '*.o' | wc -l | tr -d ' ')
        if [ "$LOOSE_OBJECTS" -gt 0 ]; then
            echo "--- cargo: removing ${LOOSE_OBJECTS} legacy unpacked debug object files"
            find "$LOOSE_OBJECT_DIR" -maxdepth 1 -type f -name '*.o' -delete
        fi
    fi

    if command -v cargo-sweep >/dev/null 2>&1; then
        echo "--- cargo sweep: dropping target/ artifacts older than ${SWEEP_DAYS} days (was ${BEFORE})"
        (cd "$REPO_ROOT" && cargo sweep --time "$SWEEP_DAYS" 2>&1 | tail -2)
        echo "    target/ now $(human_size "$REPO_ROOT/target")"
    else
        echo "--- cargo-sweep not installed (target/ is ${BEFORE})"
        echo "    install once:  cargo install cargo-sweep"
        echo "    blunt fallback (full debug rebuild next time):  rm -rf target/debug"
    fi
else
    echo "--- no target/ directory, nothing to sweep"
fi

# ── 2. Docker caches (never containers or volumes) ─────────────
if docker info >/dev/null 2>&1; then
    echo "--- docker: pruning dangling images + capping build cache at ${DOCKER_BUILD_CACHE}"
    docker image prune -f 2>/dev/null | tail -1
    docker builder prune -f --keep-storage "$DOCKER_BUILD_CACHE" 2>/dev/null | tail -1
else
    echo "--- docker daemon not reachable, skipping"
fi

# ── 3. /tmp debris from test and benchmark runs ────────────────
echo "--- /tmp: removing sqe logs and spill dirs older than 1 day"
find /tmp -maxdepth 1 -name 'sqe-test-*.log' -mtime +1 -delete 2>/dev/null || true
find /tmp -maxdepth 1 -type d \( -name 'sqe-*-spill' -o -name 'sqe-bench-spill' \) -mtime +1 -exec rm -rf {} + 2>/dev/null || true

echo ""
echo "=== Done. Disk free: $(df -h / | awk 'NR==2 {print $4}') ==="
