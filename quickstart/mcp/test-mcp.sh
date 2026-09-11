#!/usr/bin/env bash
# Protocol and authorization smoke tests against an already-running SQE MCP.
set -euo pipefail
cd "$(dirname "$0")"
. ../_shared/lib.sh

require curl jq
: "${SQE_MCP_TOKEN:?export SQE_MCP_TOKEN or run through ./run.sh}"
: "${SQE_MCP_READER_TOKEN:?export SQE_MCP_READER_TOKEN or run through ./run.sh}"

MCP_URL=${SQE_MCP_URL:-http://localhost:${SQE_MCP_PORT:-19091}/mcp}
MCP_BASE=${MCP_URL%/mcp}
EXPECT_WRITES=${EXPECT_WRITES:-false}

rpc() {
  local token=$1 payload=$2 session=${3:-}
  local -a session_header=()
  if [ -n "$session" ]; then
    session_header=(-H "Mcp-Session-Id: $session")
  fi
  curl -fsS "$MCP_URL" \
    -H "Authorization: Bearer $token" \
    -H 'Content-Type: application/json' \
    -H 'Accept: application/json' \
    -H 'MCP-Protocol-Version: 2025-11-25' \
    "${session_header[@]}" \
    --data "$payload"
}

# The Mcp-Session-Id header SQE issues on initialize; the body is discarded.
init_session_id() {
  local token=$1 payload=$2
  curl -fsS -o /dev/null -D - "$MCP_URL" \
    -H "Authorization: Bearer $token" \
    -H 'Content-Type: application/json' \
    -H 'Accept: application/json' \
    -H 'MCP-Protocol-Version: 2025-11-25' \
    --data "$payload" \
    | tr -d '\r' | awk 'tolower($1)=="mcp-session-id:" {print $2}'
}

step "checking OAuth protected-resource metadata"
metadata=$(curl -fsS "$MCP_BASE/.well-known/oauth-protected-resource/mcp")
assert_eq "resource identifier" "$(jq -r .resource <<<"$metadata")" "$MCP_URL"
assert_contains "read scope advertised" "$metadata" 'sqe:mcp:read'

step "checking authentication challenges"
challenge=$(curl -sS -i "$MCP_URL" -H 'Content-Type: application/json' \
  --data '{"jsonrpc":"2.0","id":1,"method":"ping"}')
assert_contains "missing bearer returns 401" "$challenge" '401 Unauthorized'
assert_contains "challenge points to RFC 9728 metadata" "$challenge" 'resource_metadata='
# Deliberately malformed: the assertion is that SQE rejects it. Kept in a
# variable so no literal bearer value sits in a curl header, which is what a
# secret scanner looks for.
invalid_token='not.a.valid.jwt'
invalid_code=$(curl -sS -o /dev/null -w '%{http_code}' "$MCP_URL" \
  -H "Authorization: Bearer $invalid_token" -H 'Content-Type: application/json' \
  --data '{"jsonrpc":"2.0","id":1,"method":"ping"}')
assert_eq "invalid bearer rejected" "$invalid_code" "401"

step "checking MCP initialization and tools"
init=$(rpc "$SQE_MCP_TOKEN" '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"quickstart","version":"1"}}}')
assert_eq "protocol version" "$(jq -r .result.protocolVersion <<<"$init")" "2025-11-25"
listed=$(rpc "$SQE_MCP_TOKEN" '{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}')
tool_names=$(jq -r '.result.tools[].name' <<<"$listed")
assert_contains "read-only SQL tool" "$tool_names" "execute_sql_readonly"
assert_contains "metadata tools" "$tool_names" "describe_table"

step "checking per-user identity and governed reads"
who=$(rpc "$SQE_MCP_TOKEN" '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"whoami","arguments":{}}}')
assert_eq "admin identity reaches MCP" "$(jq -r .result.structuredContent.user <<<"$who")" "adminuser"

step "checking audit attribution of the calling client"
session_id=$(init_session_id "$SQE_MCP_TOKEN" '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"quickstart","version":"1"}}}')
assert_not_empty "initialize issues an Mcp-Session-Id" "$session_id"
who=$(rpc "$SQE_MCP_TOKEN" '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"whoami","arguments":{}}}' "$session_id")
assert_eq "calls carry the client from clientInfo" "$(jq -r .result.structuredContent.client <<<"$who")" "quickstart/1"
read_result=$(rpc "$SQE_MCP_TOKEN" '{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"execute_sql_readonly","arguments":{"sql":"SELECT COUNT(*) AS row_count FROM quickstart.demo.events"}}}')
assert_eq "source data read through SQE" "$(jq -r '.result.structuredContent.rows[0].row_count' <<<"$read_result")" "4"
blocked=$(rpc "$SQE_MCP_TOKEN" '{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"execute_sql_readonly","arguments":{"sql":"DROP TABLE quickstart.demo.events"}}}')
assert_eq "write rejected by read-only parser gate" "$(jq -r .error.code <<<"$blocked")" "-32602"

if [ "$EXPECT_WRITES" = "true" ]; then
  step "checking the opt-in write gate"
  assert_contains "write tool advertised" "$tool_names" "execute_sql_change"
  changed=$(rpc "$SQE_MCP_TOKEN" '{"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"execute_sql_change","arguments":{"sql":"INSERT INTO quickstart.demo.events VALUES (99, '\''mcp'\'', 9.99)"}}}')
  assert_eq "writer role can change source data" "$(jq -r .result.isError <<<"$changed")" "false"
  denied=$(rpc "$SQE_MCP_READER_TOKEN" '{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"execute_sql_change","arguments":{"sql":"INSERT INTO quickstart.demo.events VALUES (100, '\''denied'\'', 1.0)"}}}')
  assert_contains "reader denied despite OAuth write scope" "$(jq -r .error.message <<<"$denied")" "writer role"
else
  assert_not_contains "write tool hidden by default" "$tool_names" "execute_sql_change"
fi

check_summary
