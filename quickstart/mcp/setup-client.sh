#!/usr/bin/env bash
# Show the checked-in, secret-free configuration for the selected MCP client.
set -euo pipefail
cd "$(dirname "$0")"
. ../_shared/lib.sh

client=${1:-}
case "$client" in
  codex)
    step "copy this into ~/.codex/config.toml or .codex/config.toml"
    cat clients/codex-config.toml
    echo
    echo 'export SQE_MCP_TOKEN="$(./mint-token.sh)"'
    echo 'codex mcp list'
    ;;
  claude)
    step "copy this to .mcp.json in the project where Claude Code runs"
    cat clients/claude.mcp.json
    echo
    echo 'export SQE_MCP_TOKEN="$(./mint-token.sh)"'
    echo 'claude mcp list'
    ;;
  *) die "usage: $0 codex|claude" ;;
esac
