#!/usr/bin/env bash
# Shared helpers for quickstart run.sh scripts.
# Source this: . ../_shared/lib.sh
set -euo pipefail

# Colours (no-op if not a tty)
if [ -t 1 ]; then BOLD=$'\033[1m'; GREEN=$'\033[32m'; RED=$'\033[31m'; YEL=$'\033[33m'; RST=$'\033[0m'; else BOLD=""; GREEN=""; RED=""; YEL=""; RST=""; fi

step() { echo "${BOLD}==>${RST} $*"; }
ok()   { echo "${GREEN}  ok${RST} $*"; }
warn() { echo "${YEL}  ! ${RST} $*"; }
die()  { echo "${RED}FAIL:${RST} $*" >&2; exit 1; }

# require <cmd> [<cmd> ...] — fail with a clear message if any is missing.
require() {
  for c in "$@"; do command -v "$c" >/dev/null 2>&1 || die "'$c' is required but not on PATH"; done
}

# wait_http <url> <expected-code-or-2xx> <retries> <label>
# Polls until the URL returns the expected status (use '2xx' to accept any 2xx).
wait_http() {
  local url=$1 want=${2:-2xx} retries=${3:-60} label=${4:-$1}
  printf '    waiting for %s ' "$label"
  local i code
  for ((i = 0; i < retries; i++)); do
    code=$(curl -s -o /dev/null -w "%{http_code}" "$url" 2>/dev/null || echo 000)
    if [ "$want" = "2xx" ]; then case "$code" in 2*) echo " ${GREEN}ready${RST} ($code)"; return 0;; esac
    elif [ "$code" = "$want" ]; then echo " ${GREEN}ready${RST}"; return 0; fi
    printf '.'; sleep 2
  done
  echo " ${RED}timeout${RST} (last HTTP $code)"; return 1
}

# capture_begin <file> — start teeing stdout/stderr into an output capture file
# so the run can be pasted into OUTPUT.md as committed evidence.
CAPTURE_FILE=""
capture_begin() { CAPTURE_FILE=$1; : > "$CAPTURE_FILE"; }
# cap <cmd...> — run a command, echo it, and append both command + output to the capture file.
cap() {
  echo "\$ $*"
  if [ -n "$CAPTURE_FILE" ]; then
    { echo "\$ $*"; "$@" 2>&1 | tee /dev/tty; } >> "$CAPTURE_FILE" 2>&1 || return $?
  else
    "$@"
  fi
}
