#!/usr/bin/env bash
# Readable SQL transcript of SQE's access control: every statement, who ran it,
# and what came back.
#
# This is the demonstrable companion to `make test-access-control`. That suite
# asserts decoded Arrow values in Rust and prints nothing a human wants to read;
# this prints the SQL and the result table for each step so the behaviour can be
# shown, pasted into a review, or diffed after a change.
#
# It is NOT a replacement for the test suite. It checks each step and exits
# non-zero on unexpected output, deliberately: a script under scripts/ will
# eventually be run as a gate, and a demo that always exits 0 is a silent-skip
# trap.
#
# Covered, in order:
#   1  catalog gate  -- deny before grant, GRANT enables, REVOKE disables
#   2  catalog gate  -- role vs user grant, write separate from read
#   3  data gate     -- column masks (MASK_NULL, MASK_SHOW_LAST_4, MASK_HASH)
#   4  data gate     -- row filter
#   5  data gate     -- tag-based mask via SET TAGS
#   6  introspection -- SHOW GRANTS, CHECK ACCESS
#   7  the view gap  -- shown as the error it actually produces
#
# Usage:
#   scripts/access-control-demo.sh                          # bring the stack up, run all
#   AC_DEMO_NO_BOOTSTRAP=1 scripts/access-control-demo.sh   # reuse a running stack
#
# The stack is always left running (a rebuild costs minutes); the script prints
# the teardown command at the end.
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STACK_DIR="$ROOT_DIR/quickstart/polaris-ranger-keycloak"

cd "$STACK_DIR" || { echo "missing $STACK_DIR" >&2; exit 1; }

# Ports are not fixed. A developer whose 26080 is taken by another Ranger has
# RANGER_PORT=46080 in .env, and a hardcoded value would talk to the WRONG
# Ranger and fail with "Role name: engineer does not exist in ranger admin".
[ -f .env ] && set -a && . ./.env && set +a
RANGER_PORT="${RANGER_PORT:-26080}"
RANGER_PASS="${RANGER_ADMIN_PASSWORD:-rangerR0cks!}"
RANGER_URL="http://localhost:${RANGER_PORT}"

PASS=0; FAIL=0
STEP=0
bold()  { printf '\033[1m%s\033[0m\n' "$*"; }
green() { printf '\033[32m%s\033[0m\n' "$*"; }
red()   { printf '\033[31m%s\033[0m\n' "$*"; }
dim()   { printf '\033[2m%s\033[0m\n' "$*"; }

# ── stack ────────────────────────────────────────────────────────────────────

wait_oneshot() { # container ...
  for c in "$@"; do
    for _ in $(seq 1 60); do
      code="$(docker compose ps -a --format json "$c" 2>/dev/null \
        | python3 -c 'import json,sys
for line in sys.stdin:
    line=line.strip()
    if not line: continue
    try: d=json.loads(line)
    except Exception: continue
    print(d.get("ExitCode",""));break' 2>/dev/null)"
      [ "$code" = "0" ] && break
      [ -n "$code" ] && [ "$code" != "0" ] && { red "bootstrap container $c exited $code"; exit 1; }
      sleep 5
    done
  done
}

bootstrap() {
  bold "Bringing up the quickstart stack (Polaris + Ranger + Keycloak + RustFS + SQE)"
  dim "First Ranger boot takes 2-4 minutes."
  # Two phases: `--wait` treats an EXITED container as a failure, so the
  # one-shot bootstrap containers cannot be in the --wait set.
  docker compose up -d keycloak rustfs ranger-db ranger-admin >/dev/null 2>&1
  docker compose up -d --wait keycloak rustfs ranger-db ranger-admin || {
    red "stack failed to become healthy"; exit 1; }
  docker compose up -d keycloak-config bucket-init ranger-setup >/dev/null 2>&1
  wait_oneshot keycloak-config bucket-init ranger-setup
  docker compose up -d --wait polaris || { red "polaris failed"; exit 1; }
  docker compose up -d polaris-setup >/dev/null 2>&1
  wait_oneshot polaris-setup
  docker compose up -d --wait sqe || { red "sqe failed"; exit 1; }
  green "stack ready"
}

# ── SQL ──────────────────────────────────────────────────────────────────────

# sqe-cli prints "Error: ..." to stdout and still exits 0, so classify on text.
sqe() { # user sql
  docker compose exec -T -e "SQE_PASSWORD=${1}123" sqe \
    sqe-cli --port 50051 --user "$1" -e "$2" 2>&1
}
is_error() { echo "$1" | grep -qi 'error:'; }
# A user who cannot LOAD a table is told it does not exist: Polaris hides rather
# than 403s, so a denial and a typo look identical. Every table named in this
# script exists, which is what makes "not found" mean "denied" here. The Rust
# suite does not rely on that; it runs the same SQL as an admin first.
is_denial() { echo "$1" | grep -qiE 'not authorized|forbidden|unauthorized|403|denied|permission|not found|does not exist'; }

# Print a step: the SQL, who ran it, the output, and a verdict.
#
#   run <expect> <user> <sql> <desc> [must_match] [must_not_match]
#
# expect: ok | deny | error
#
# must_match / must_not_match are extended regexes checked against the output.
# They are what make a verdict mean anything. An earlier version of this script
# only checked "did it error", so every masking step reported PASS while proving
# nothing about the values -- and one of them was in fact returning NULL from the
# fail-closed path rather than the mask under test.
#
# When a matcher is supplied the step RETRIES for up to POLICY_BUDGET seconds,
# because a policy authored in Ranger is not visible to SQE until its cached
# bundle expires (policy.ranger cache-ttl-secs, default 30). Polling beats a
# fixed sleep: it is correct at any TTL and returns as soon as the change lands.
POLICY_BUDGET="${POLICY_BUDGET:-75}"

run() {
  local expect="$1" user="$2" sql="$3" desc="$4" want="${5:-}" avoid="${6:-}"
  local out verdict deadline
  STEP=$((STEP+1))
  echo
  bold "[$STEP] $desc"
  dim  "     user: $user   expect: $expect${want:+   must match: $want}${avoid:+   must not match: $avoid}"
  echo "     SQL: $sql"
  deadline=$(( $(date +%s) + POLICY_BUDGET ))
  while :; do
    out="$(sqe "$user" "$sql")"
    verdict=PASS
    case "$expect" in
      ok)    is_error  "$out" && verdict=FAIL ;;
      deny)  is_denial "$out" || verdict=FAIL ;;
      error) is_error  "$out" || verdict=FAIL ;;
      *)     verdict=FAIL ;;
    esac
    if [ "$verdict" = PASS ] && [ -n "$want" ]; then
      echo "$out" | grep -Eq "$want" || verdict=FAIL
    fi
    if [ "$verdict" = PASS ] && [ -n "$avoid" ]; then
      echo "$out" | grep -Eq "$avoid" && verdict=FAIL
    fi
    # Only a matcher-driven failure is worth waiting on: an outright error or a
    # wrong allow/deny will not fix itself.
    if [ "$verdict" = PASS ] || { [ -z "$want" ] && [ -z "$avoid" ]; } \
       || [ "$(date +%s)" -ge "$deadline" ]; then
      break
    fi
    sleep 5
  done
  echo "$out" | sed 's/^/     | /'
  if [ "$verdict" = PASS ]; then green "     PASS"; PASS=$((PASS+1));
  else
    red "     FAIL (expected $expect${want:+ matching /$want/}${avoid:+ without /$avoid/})"
    FAIL=$((FAIL+1))
  fi
}

# Every deny is shown together with the SAME SQL succeeding as an admin. Without
# that control, "0 rows" or "not found" teaches the reader nothing: it is also
# what a typo produces.
run_deny_with_control() { # denied_user sql desc
  run ok   carol "$2" "$3 (control: carol, who is allowed, must succeed)"
  run deny "$1"  "$2" "$3"
}

# Wait for a Polaris-side change to become visible. Polaris's Ranger plugin
# polls, so a GRANT is not instant.
settle() { # user sql want_error(0|1)
  for _ in $(seq 1 24); do
    out="$(sqe "$1" "$2")"
    if [ "$3" = "0" ]; then is_error "$out" || return 0; else is_error "$out" && return 0; fi
    sleep 5
  done
}

curlr() { curl -fsS -u "admin:${RANGER_PASS}" -H 'X-XSRF-HEADER:x' \
            -H 'Content-Type: application/json' "$@"; }

CAT=sales_wh
NS=acdemo
T="$CAT.$NS.orders"
V="$CAT.$NS.orders_eu"

cleanup_policies() {
  # Remove any acdemo- policies from a previous run so masks do not stack.
  for svc in hive "${TAG_SERVICE:-acdemo_tag}" tag; do
  ids="$(curl -fsS -u "admin:${RANGER_PASS}" \
    "${RANGER_URL}/service/public/v2/api/policy?serviceName=${svc}" 2>/dev/null \
    | python3 -c 'import json,sys
try: d=json.load(sys.stdin)
except Exception: sys.exit()
for p in (d if isinstance(d,list) else d.get("policies",[])):
    if str(p.get("name","")).startswith("acdemo-"): print(p["id"])' 2>/dev/null)"
  for id in $ids; do
    curl -fsS -u "admin:${RANGER_PASS}" -H 'X-XSRF-HEADER:x' \
      -X DELETE "${RANGER_URL}/service/public/v2/api/policy/${id}" >/dev/null 2>&1
  done
  done
}

hive_policy() { # name json_body
  printf '%s' "$2" > /tmp/acdemo-policy.json
  curlr -X POST "${RANGER_URL}/service/public/v2/api/policy" \
    -d @/tmp/acdemo-policy.json >/dev/null 2>&1 \
    && dim "     (ranger policy '$1' created)" \
    || { red "could not create ranger policy '$1'"; FAIL=$((FAIL+1)); }
}

TAG_SERVICE=""

# A tag policy lives on a service of TYPE tag, and the hive service must point at
# it via `tagService` or the download bundle carries no tagPolicies block at all.
#
# Prefer whatever hive is ALREADY linked to. Ranger rejects creating a second tag
# service in some states ("More than one result was returned from
# Query.getSingleResult()"), and re-pointing the link would mutate the shared demo
# service for no gain. Only create and link when nothing is linked yet.
ensure_tag_service() {
  local hive id existing
  hive="$(curl -fsS -u "admin:${RANGER_PASS}" \
    "${RANGER_URL}/service/public/v2/api/service/name/hive" 2>/dev/null)" || {
    red "could not read the hive service"; return 1; }
  existing="$(printf '%s' "$hive" \
    | python3 -c 'import json,sys; print(json.load(sys.stdin).get("tagService") or "")' 2>/dev/null)"
  if [ -n "$existing" ]; then
    TAG_SERVICE="$existing"
    dim "     (hive is already linked to tag service ${TAG_SERVICE})"
    return 0
  fi
  TAG_SERVICE=acdemo_tag
  curlr -X POST "${RANGER_URL}/service/public/v2/api/service" \
    -d "{\"name\":\"${TAG_SERVICE}\",\"type\":\"tag\",\"configs\":{},\"isEnabled\":true}" \
    >/dev/null 2>&1 || { red "could not create tag service ${TAG_SERVICE}"; return 1; }
  id="$(printf '%s' "$hive" | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])' 2>/dev/null)"
  printf '%s' "$hive" | python3 -c "
import json,sys
d=json.load(sys.stdin); d['tagService']='${TAG_SERVICE}'
json.dump(d,open('/tmp/acdemo-hive.json','w'))"
  curlr -X PUT "${RANGER_URL}/service/public/v2/api/service/${id}" \
    -d @/tmp/acdemo-hive.json >/dev/null 2>&1 \
    && dim "     (linked ${TAG_SERVICE} to hive)" \
    || { red "could not link ${TAG_SERVICE} to hive"; return 1; }
}

tag_policy() { # name json_body
  printf '%s' "$2" > /tmp/acdemo-tagpolicy.json
  curlr -X POST "${RANGER_URL}/service/public/v2/api/policy" \
    -d @/tmp/acdemo-tagpolicy.json >/dev/null 2>&1 \
    && dim "     (tag policy '$1' created)" \
    || { red "could not create tag policy '$1'"; FAIL=$((FAIL+1)); }
}

# Tag masking needs an SQE binary from 2026-07-31 or later. Before that,
# `map_mask` matched only bare Ranger mask names while the tag servicedef emits
# component-qualified ones (`hive:MASK`), so every tag mask fell through to the
# unsupported arm and RESTRICTED the column instead of masking it. That is
# fail-closed, not a leak, but a stale quickstart image will show the tag step
# returning NULL where XX is expected. Warn early rather than let the reader
# think tag masking is broken.
preflight_image_age() {
  local created
  created="$(docker inspect sqe-quickstart:latest --format '{{.Created}}' 2>/dev/null | cut -c1-10)"
  [ -n "$created" ] || return 0
  if [ "$created" \< "2026-07-31" ]; then
    red "WARNING: sqe-quickstart:latest was built $created, before the tag-mask fix"
    red "         (normalize_mask_type, 2026-07-31). The tag masking step will"
    red "         show a RESTRICTED column instead of a masked one."
    red "         Rebuild with: (cd $STACK_DIR && docker compose build sqe && docker compose up -d sqe)"
    echo
  fi
}

# ── fixture ──────────────────────────────────────────────────────────────────

fixture() {
  bold "Fixture: $T with three rows, owned by carol (sqe_admin)"
  cleanup_policies
  sqe carol "CREATE SCHEMA IF NOT EXISTS $CAT.$NS" >/dev/null 2>&1
  sqe carol "DROP TABLE IF EXISTS $T" >/dev/null 2>&1
  sqe carol "CREATE TABLE $T (id BIGINT, region VARCHAR, amount DOUBLE, ssn VARCHAR, email VARCHAR)" >/dev/null 2>&1
  sqe carol "INSERT INTO $T VALUES (1,'EU',10.0,'111-11-1111','a@x'),(2,'US',20.0,'222-22-2222','b@x'),(3,'EU',30.0,'333-33-3333','c@x')" >/dev/null 2>&1
  # Start from no grants so step 1 is a true denial.
  for r in analyst engineer; do
    sqe carol "REVOKE SELECT ON $T FROM ROLE \"$r\"" >/dev/null 2>&1
    sqe carol "REVOKE INSERT ON $T FROM ROLE \"$r\"" >/dev/null 2>&1
  done
  settle carol "SELECT count(*) FROM $T" 0
  # Wait for the REVOKE to actually take effect before step 1 claims "no grant
  # yet". Polaris's Ranger plugin polls, so on a second run alice's grant from
  # the previous run is still being served for a while. Without this wait the
  # first step passes or fails depending on how recently the script last ran,
  # which is the same defect as a test that never establishes its precondition.
  dim "waiting for the revoke to propagate so step 1 starts from a true denial"
  settle alice "SELECT id FROM $T" 1
  if ! is_denial "$(sqe alice "SELECT id FROM $T")"; then
    red "fixture could not reach a denied baseline for alice; aborting"
    exit 1
  fi
  green "fixture ready"
}

# ── the transcript ───────────────────────────────────────────────────────────

main() {
  [ "${AC_DEMO_NO_BOOTSTRAP:-0}" = "1" ] || bootstrap
  preflight_image_age
  fixture

  echo; bold "═══ 1. Catalog gate: the grant is what enables the read ═══"
  run_deny_with_control alice "SELECT id FROM $T" \
    "alice has no grant on the table yet"

  run ok carol "GRANT SELECT ON $T TO ROLE \"analyst\"" \
    "carol grants SELECT to role analyst (alice is a member)"
  settle alice "SELECT id FROM $T" 0
  run ok alice "SELECT id, region FROM $T ORDER BY id" \
    "alice can now read all three rows" '\(3 rows\)' 

  echo; bold "═══ 2. Role vs user, and write separate from read ═══"
  run deny dave "SELECT id FROM $T" \
    "dave is in no role, so the role grant does not reach him"
  run deny alice "INSERT INTO $T VALUES (9,'EU',1.0,'999-99-9999','z@x')" \
    "SELECT does not imply INSERT"
  run ok carol "GRANT INSERT ON $T TO ROLE \"analyst\"" \
    "carol grants INSERT as a separate privilege"
  settle alice "INSERT INTO $T VALUES (9,'EU',1.0,'999-99-9999','z@x')" 0
  run ok alice "SELECT count(*) AS n FROM $T" \
    "the row landed, so the write privilege is real (expect 4)" '\| 4'
  run ok carol "DELETE FROM $T WHERE id = 9" "carol removes the probe row"

  echo; bold "═══ 3. Data gate: column masks ═══"
  dim "engineer (bob, carol) gets masks; analyst-only alice is the unmasked control."
  run ok carol "GRANT SELECT ON $T TO ROLE \"engineer\"" "grant engineer read access"
  hive_policy acdemo-mask-amount "$(cat <<JSON
{"service":"hive","name":"acdemo-mask-amount","policyType":1,"isEnabled":true,
 "resources":{"database":{"values":["$NS"]},"table":{"values":["orders"]},"column":{"values":["amount"]}},
 "dataMaskPolicyItems":[{"roles":["engineer"],"accesses":[{"type":"select","isAllowed":true}],
 "dataMaskInfo":{"dataMaskType":"MASK_NULL"}}]}
JSON
)"
  hive_policy acdemo-mask-ssn "$(cat <<JSON
{"service":"hive","name":"acdemo-mask-ssn","policyType":1,"isEnabled":true,
 "resources":{"database":{"values":["$NS"]},"table":{"values":["orders"]},"column":{"values":["ssn"]}},
 "dataMaskPolicyItems":[{"roles":["engineer"],"accesses":[{"type":"select","isAllowed":true}],
 "dataMaskInfo":{"dataMaskType":"MASK_SHOW_LAST_4"}}]}
JSON
)"
  hive_policy acdemo-mask-email "$(cat <<JSON
{"service":"hive","name":"acdemo-mask-email","policyType":1,"isEnabled":true,
 "resources":{"database":{"values":["$NS"]},"table":{"values":["orders"]},"column":{"values":["email"]}},
 "dataMaskPolicyItems":[{"roles":["engineer"],"accesses":[{"type":"select","isAllowed":true}],
 "dataMaskInfo":{"dataMaskType":"MASK_HASH"}}]}
JSON
)"
  run ok bob "SELECT id, amount, ssn, email FROM $T ORDER BY id" \
    "bob (engineer): amount NULL, ssn xxx-xx-NNNN, email an HMAC digest, still 3 rows" \
    'xxx-xx-1111.*[0-9a-f]{64}|[0-9a-f]{64}' '111-11-1111'
  # 3 rows AND no raw ssn: masking must transform, never drop.
  run ok bob "SELECT count(*) AS n FROM $T" "masking did not drop rows (expect 3)" '\| 3'
  run ok alice "SELECT id, amount, ssn, email FROM $T ORDER BY id" \
    "alice (analyst only): raw values, proving the mask is per-role" \
    '111-11-1111' 'xxx-xx-'

  echo; bold "═══ 4. Data gate: row filter ═══"
  hive_policy acdemo-rowfilter "$(cat <<JSON
{"service":"hive","name":"acdemo-rowfilter","policyType":2,"isEnabled":true,
 "resources":{"database":{"values":["$NS"]},"table":{"values":["orders"]}},
 "rowFilterPolicyItems":[{"roles":["engineer"],"accesses":[{"type":"select","isAllowed":true}],
 "rowFilterInfo":{"filterExpr":"region = 'EU'"}}]}
JSON
)"
  run ok bob "SELECT id, region FROM $T ORDER BY id" \
    "bob sees only the EU rows (1 and 3)" '\(2 rows\)' 'US'
  run ok alice "SELECT id, region FROM $T ORDER BY id" \
    "alice still sees all three" '\(3 rows\)' 

  echo; bold "═══ 5. Data gate: tag-based masking ═══"
  dim "The RULE lives in Ranger against the tag; the ASSOCIATION is Iceberg table metadata."
  run ok carol "ALTER TABLE $T SET TAGS (region = ('GEO'))" \
    "tag the region column with GEO, stored in the sqe.column-tags property"
  run ok carol "SHOW TAGS ON $T" "read the association back"
  ensure_tag_service || { red "tag service setup failed; skipping the tag demo"; FAIL=$((FAIL+1)); }
  # The mask type MUST be component-qualified. Ranger's tag servicedef defines
  # only `hive:MASK_NULL`, `trino:...` and friends, never the bare name.
  # MASK, not MASK_NULL. A tag carrying NO rule also nullifies the column
  # (fail-closed), so a NULL result cannot distinguish "the tag rule applied"
  # from "there was no rule and SQE denied". Full redact makes EU -> XX, which
  # only a working rule can produce. An earlier draft of this script used
  # MASK_NULL and passed while the policy POST was silently failing.
  tag_policy acdemo-tag-mask "$(cat <<JSON
{"service":"$TAG_SERVICE","name":"acdemo-tag-mask","policyType":1,"isEnabled":true,
 "resources":{"tag":{"values":["GEO"]}},
 "dataMaskPolicyItems":[{"roles":["engineer"],"accesses":[{"type":"hive:select","isAllowed":true}],
 "dataMaskInfo":{"dataMaskType":"hive:MASK"}}]}
JSON
)"
  run ok bob "SELECT id, region FROM $T ORDER BY id" \
    "bob: region redacted to XX by the TAG rule, with no column named in the policy" \
    'XX' 'EU'
  run ok alice "SELECT id, region FROM $T ORDER BY id" \
    "alice (not engineer): region raw, so the tag rule is role-scoped too" 'EU' 'XX'

  echo; bold "=== 5b. Tag fail-closed: a tag with no rule hides the column ==="
  run ok carol "ALTER TABLE $T SET TAGS (region = ('NO_RULE_FOR_THIS'))" \
    "retag region with a tag that has no policy anywhere"
  run ok bob "SELECT id, region FROM $T ORDER BY id" \
    "bob: region comes back NULL. Unknown protection denies rather than leaking" \
    'region' 'EU|XX'
  run ok carol "ALTER TABLE $T UNSET TAGS (region)" "remove the tag again"

  echo; bold "═══ 6. Introspection ═══"
  run ok carol "SHOW GRANTS ON $T" "who holds what on this table" 'table-data-read.*analyst|analyst' 
  run ok carol "CHECK ACCESS SELECT ON $T FOR USER \"alice\"" "does alice have SELECT on this table" 'alice|true|ALLOW' 

  echo; bold "═══ 7. The view gap (expected to fail) ═══"
  dim "The polaris service-def has no view resource level and no privilege maps to a"
  dim "view access type. The statement parses, then loses the object identity."
  run ok    carol "CREATE OR REPLACE VIEW $V AS SELECT id, region FROM $T WHERE region = 'EU'" \
    "carol can create a view (admin holds the view-* access types)"
  run error carol "GRANT SELECT ON VIEW $V TO ROLE \"analyst\"" \
    "GRANT ON VIEW fails with 'requires a catalog' despite the qualified name" \
    'requires a catalog' 

  echo
  bold "─────────────────────────────────────────────"
  if [ "$FAIL" -eq 0 ]; then green "all $PASS steps behaved as documented"; else red "$FAIL of $((PASS+FAIL)) steps did NOT match the documented behaviour"; fi
  bold "─────────────────────────────────────────────"

  cleanup_policies
  sqe carol "DROP VIEW IF EXISTS $V" >/dev/null 2>&1
  dim "stack left running; tear down with: (cd $STACK_DIR && docker compose down)"

  [ "$FAIL" -eq 0 ] || exit 1
}

main "$@"
