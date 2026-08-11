#!/usr/bin/env bash
# Readable, live SQE <-> Spark access-control comparison.
#
# Every probe runs against the SAME Iceberg table through both engines, with the
# same user identity at both authorization tiers:
#
#   Polaris object gate: Keycloak bearer token for the user
#   Ranger data gate:    the same username / role in the shared `query` service
#
# The script compares full normalized result sets after each policy change. It
# expects byte equality for portable policies and calls out two documented
# divergences explicitly:
#
#   named-mask rendering    MASK_SHOW_LAST_4 and MASK_HASH render differently
#                           (SQE follows the Hive servicedef transformer and
#                           hashes with sha256; Kyuubi uses its own character
#                           classes and md5)
#   filter/mask ordering    a row filter reading a tag-masked column sees raw
#                           values in SQE and masked values in Kyuubi, because
#                           Kyuubi injects the masking Project below its
#                           RowFilterMarker
#
# Mask precedence used to be a third: SQE resolved a contested column to the
# resource mask, Kyuubi to the tag mask. `policy.mask-precedence` now defaults to
# `tag` and the two agree. Setting it to `resource` re-opens that divergence, and
# section 5a starts failing, which is the intended signal rather than a bug.
# Every security mutation is printed before execution. GRANT/REVOKE, row
# filters, column masks, and tags are all authored through SQE SQL; Ranger is
# the shared backing store consumed by both engines.
#
# Covered (the complete access-control-demo.sh story):
#   1. catalog denial, role GRANT, user outside the role, and REVOKE
#   2. SELECT versus INSERT, including successful writes seen by both engines
#   3. MASK_NULL, MASK_SHOW_LAST_4, and MASK_HASH resource masks
#   4. row filtering and its unfiltered-role control
#   5. tag masking, mask composition, resource/tag precedence, and a row filter
#      that reads a tag-masked column
#   6. inert tags and invalid CUSTOM-policy SQL
#   7. SHOW GRANTS, CHECK ACCESS, and view grants
#
# Spark has no SQL surface for authoring Ranger policies or introspecting the
# Polaris Ranger policy model. Those management statements are deliberately run
# and asserted through SQE, then their enforcement is probed through both engines.
#
# Usage:
#   scripts/access-control-parity-demo.sh
#   AC_PARITY_NO_BOOTSTRAP=1 scripts/access-control-parity-demo.sh
#
# Each Spark probe starts spark-sql, so this takes minutes rather than seconds.
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STACK_DIR="$ROOT_DIR/quickstart/polaris-ranger-keycloak"
cd "$STACK_DIR" || { echo "missing $STACK_DIR" >&2; exit 1; }

[ -f .env ] && set -a && . ./.env && set +a
POLICY_BUDGET="${AC_PARITY_POLICY_BUDGET:-120}"

CAT=sales_wh
NS=acparity
TABLE=orders
T="$CAT.$NS.$TABLE"
POLICY_PREFIX=acparity-demo-
TAG_NAME=ACPARITY_PII
NO_RULE_TAG=ACPARITY_NO_RULE
NULL_TAG=ACPARITY_NULL

PASS=0
FAIL=0
STEP=0

bold()  { printf '\033[1m%s\033[0m\n' "$*"; }
green() { printf '\033[32m%s\033[0m\n' "$*"; }
red()   { printf '\033[31m%s\033[0m\n' "$*"; }
dim()   { printf '\033[2m%s\033[0m\n' "$*"; }

TMP_DIR="$(mktemp -d)"
cleanup_tmp() {
  find "$TMP_DIR" -type f -delete 2>/dev/null || true
  rmdir "$TMP_DIR" 2>/dev/null || true
}
trap cleanup_tmp EXIT

# ── stack ────────────────────────────────────────────────────────────────────

wait_oneshot() { # service ...
  local service cid state code
  for service in "$@"; do
    for _ in $(seq 1 60); do
      cid="$(docker compose ps -aq "$service" 2>/dev/null | head -1)"
      if [ -n "$cid" ]; then
        state="$(docker inspect -f '{{.State.Status}}' "$cid" 2>/dev/null || true)"
        code="$(docker inspect -f '{{.State.ExitCode}}' "$cid" 2>/dev/null || true)"
        if [ "$state" = exited ]; then
          [ "$code" = 0 ] && break
          red "$service exited $code"
          docker compose logs --tail 40 "$service" || true
          exit 1
        fi
      fi
      sleep 5
    done
  done
}

bootstrap() {
  bold "Bringing up SQE + Spark access-control stack"
  dim "Ranger first boot can take 2-4 minutes; each Spark comparison starts a JVM."

  docker compose up -d keycloak rustfs ranger-db ranger-admin >/dev/null
  docker compose up -d --wait keycloak rustfs ranger-db ranger-admin
  docker compose up -d keycloak-config bucket-init ranger-setup >/dev/null
  wait_oneshot keycloak-config bucket-init ranger-setup
  docker compose up -d polaris >/dev/null
  docker compose up -d --wait polaris
  docker compose up -d polaris-setup >/dev/null
  wait_oneshot polaris-setup
  docker compose up -d sqe spark >/dev/null
  docker compose up -d --wait sqe spark
}

if [ "${AC_PARITY_NO_BOOTSTRAP:-0}" != 1 ]; then
  bootstrap
fi

# A replaced host-side bind source leaves an already-running container without
# the file. Recreate only Spark, without rerunning data-seed or other dependencies.
if ! docker compose exec -T spark test -f /opt/spark/conf/spark-defaults.conf \
  || ! docker compose exec -T spark test -f /opt/spark/conf/ranger-spark-security.xml; then
  dim "Spark configuration mount is stale; recreating only the Spark service."
  docker compose up -d --force-recreate --no-deps spark
fi

docker compose exec -T spark test -f /opt/spark/conf/spark-defaults.conf \
  || { red "Spark config is unavailable after recreate"; exit 1; }
docker compose exec -T sqe /usr/local/bin/wget -q -O /dev/null http://127.0.0.1:9091/healthz \
  || { red "SQE is not healthy"; exit 1; }

# ── identities and engine runners ────────────────────────────────────────────

token_for() { # user
  docker compose exec -T spark sh -c '
    curl -fsS -X POST \
      -d grant_type=password \
      -d client_id=sqe-client \
      -d client_secret=sqe-secret-change-me \
      -d username="$1" \
      -d password="$1"123 \
      http://keycloak:8080/realms/iceberg-ranger/protocol/openid-connect/token
  ' sh "$1" | python3 -c 'import json,sys; print(json.load(sys.stdin)["access_token"])'
}

dim "Checking that Spark can mint a Polaris catalog identity for each user."
for demo_user in alice bob carol dave; do
  token_for "$demo_user" >/dev/null \
    || { red "could not obtain a $demo_user token"; exit 1; }
done

# Keycloak issues 300-second access tokens and a full run takes tens of minutes,
# so a token minted once at startup is expired for most of the transcript. An
# expired token makes Polaris answer 401 Not Authorized, which reads like a
# policy denial and would let a real allow pass as a documented deny. Mint one
# per Spark probe instead: a curl costs nothing beside the JVM start that
# follows it.

sqe_exec() { # user sql
  docker compose exec -T -e "SQE_PASSWORD=${1}123" sqe \
    sqe-cli --port 50051 --user "$1" -e "$2" 2>&1
}

sqe_tsv() { # user sql
  docker compose exec -T -e "SQE_PASSWORD=${1}123" sqe \
    sqe-cli --port 50051 --user "$1" --format tsv -e "$2" \
    2>"$TMP_DIR/sqe.err"
}

fresh_spark_policy_cache() {
  # The container runs no persistent Spark JVM. Clearing this explicit dev-cache
  # directory makes every short-lived spark-sql download the current policy.
  docker compose exec -T -u root spark sh -c '
    mkdir -p /tmp/ranger-policy-cache
    find /tmp/ranger-policy-cache -mindepth 1 -delete
    chmod 777 /tmp/ranger-policy-cache
  ' >/dev/null
}

spark_tsv() { # user sql
  local token
  token="$(token_for "$1")" || return 1
  [ -n "$token" ] || return 1
  fresh_spark_policy_cache || return 1
  docker compose exec -T -e "HADOOP_USER_NAME=$1" spark \
    /opt/spark/bin/spark-sql -S \
    --conf "spark.sql.catalog.sales_wh.token=$token" \
    --conf spark.sql.catalog.sales_wh.token-refresh-enabled=false \
    -e "$2" 2>"$TMP_DIR/spark.err"
}

normalize_tsv() {
  python3 -c 'import sys
for raw in sys.stdin:
    cells=raw.rstrip("\r\n").split("\t")
    print(" | ".join("<NULL>" if c in ("", "NULL") else c for c in cells))'
}

sqe_rows() { # user sql
  local raw
  raw="$(sqe_tsv "$1" "$2")" || return 1
  printf '%s\n' "$raw" | sed '1d' | normalize_tsv
}

spark_rows() { # user sql
  spark_tsv "$1" "$2" | normalize_tsv
}

is_denial() {
  grep -qiE 'not authorized|forbidden|unauthorized|403|denied|permission|not found|does not exist'
}

is_permission_denial() {
  grep -qiE 'not authorized|forbidden|403|access denied|permission denied'
}

useful_error() {
  grep -iE 'not authorized|forbiddenexception|accesscontrolexception|permission denied|table .*not found' \
    | head -2 | sed 's/^[[:space:]]*/       /'
}

# ── readable assertions ──────────────────────────────────────────────────────

show_pair() { # sqe spark
  echo "     SQE:"
  printf '%s\n' "$1" | sed 's/^/       /'
  echo "     Spark:"
  printf '%s\n' "$2" | sed 's/^/       /'
}

compare_equal() { # user sql description exact_expected
  local user="$1" sql="$2" desc="$3" expected="$4"
  local sqe_out="" spark_out="" deadline
  STEP=$((STEP+1))
  echo
  bold "[$STEP] $desc"
  dim "     user: $user   expectation: identical rows"
  echo "     SQL: $sql"
  deadline=$(( $(date +%s) + POLICY_BUDGET ))
  while :; do
    sqe_out="$(sqe_rows "$user" "$sql" 2>/dev/null || true)"
    spark_out="$(spark_rows "$user" "$sql" 2>/dev/null || true)"
    if [ "$sqe_out" = "$spark_out" ] && [ "$sqe_out" = "$expected" ]; then
      break
    fi
    [ "$(date +%s)" -ge "$deadline" ] && break
    dim "     policies not settled in both engines yet; retrying ..."
    sleep 5
  done
  show_pair "$sqe_out" "$spark_out"
  if [ "$sqe_out" = "$spark_out" ] && [ "$sqe_out" = "$expected" ]; then
    green "     PASS — byte-identical"
    PASS=$((PASS+1))
  else
    red "     FAIL — expected:"
    printf '%s\n' "$expected" | sed 's/^/       /'
    diff -u <(printf '%s\n' "$sqe_out") <(printf '%s\n' "$spark_out") || true
    FAIL=$((FAIL+1))
  fi
}

compare_expected() { # user sql description expected_sqe expected_spark explanation
  local user="$1" sql="$2" desc="$3" expected_sqe="$4" expected_spark="$5" note="$6"
  local sqe_out="" spark_out="" deadline
  STEP=$((STEP+1))
  echo
  bold "[$STEP] $desc"
  dim "     user: $user   expectation: $note"
  echo "     SQL: $sql"
  deadline=$(( $(date +%s) + POLICY_BUDGET ))
  while :; do
    sqe_out="$(sqe_rows "$user" "$sql" 2>/dev/null || true)"
    spark_out="$(spark_rows "$user" "$sql" 2>/dev/null || true)"
    if [ "$sqe_out" = "$expected_sqe" ] && [ "$spark_out" = "$expected_spark" ]; then
      break
    fi
    [ "$(date +%s)" -ge "$deadline" ] && break
    dim "     waiting for both engines to load the policies ..."
    sleep 5
  done
  show_pair "$sqe_out" "$spark_out"
  if [ "$sqe_out" = "$expected_sqe" ] && [ "$spark_out" = "$expected_spark" ]; then
    green "     PASS — $note"
    PASS=$((PASS+1))
  else
    red "     FAIL — expected SQE:"
    printf '%s\n' "$expected_sqe" | sed 's/^/       /'
    red "     expected Spark:"
    printf '%s\n' "$expected_spark" | sed 's/^/       /'
    FAIL=$((FAIL+1))
  fi
}

compare_denied() { # user sql description
  local user="$1" sql="$2" desc="$3" sqe_raw spark_raw sqe_denied=0 spark_denied=0
  local spark_polaris=0 deadline
  STEP=$((STEP+1))
  echo
  bold "[$STEP] $desc"
  dim "     user: $user   expectation: both engines deny"
  echo "     SQL: $sql"
  # A rerun starts with grants from the previous run. REVOKE writes immediately,
  # but Polaris's embedded Ranger authorizer polls, so establish the denied SQE
  # baseline before paying for a Spark JVM and comparing the two engines.
  deadline=$(( $(date +%s) + POLICY_BUDGET ))
  while :; do
    sqe_raw="$(sqe_exec "$user" "$sql")"
    if printf '%s' "$sqe_raw" | is_denial; then
      sqe_denied=1
      break
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      break
    fi
    dim "     waiting for the revoke to reach Polaris ..."
    sleep 5
  done
  spark_tsv "$user" "$sql" >"$TMP_DIR/spark.out" 2>"$TMP_DIR/spark.err" || true
  spark_raw="$(cat "$TMP_DIR/spark.out" "$TMP_DIR/spark.err")"
  printf '%s' "$spark_raw" | is_denial && spark_denied=1
  printf '%s' "$spark_raw" \
    | grep -qE "ForbiddenException.*Principal '$user'.*not authorized for op 'LOAD_TABLE'" \
    && spark_polaris=1
  echo "     SQE denial:"
  printf '%s\n' "$sqe_raw" | useful_error
  echo "     Spark denial:"
  printf '%s\n' "$spark_raw" | useful_error
  if [ "$sqe_denied" = 1 ] && [ "$spark_denied" = 1 ] && [ "$spark_polaris" = 1 ]; then
    green "     PASS — both deny; Spark reached Polaris as $user"
    PASS=$((PASS+1))
  else
    red "     FAIL — expected an SQE denial and Polaris LOAD_TABLE denial for Spark"
    red "            sqe=$sqe_denied spark=$spark_denied spark_polaris=$spark_polaris"
    FAIL=$((FAIL+1))
  fi
}

compare_write_denied() { # user sql description
  local user="$1" sql="$2" desc="$3" sqe_raw spark_raw sqe_denied=0 spark_denied=0
  STEP=$((STEP+1))
  echo
  bold "[$STEP] $desc"
  dim "     user: $user   expectation: SELECT alone does not authorize a commit"
  echo "     SQL: $sql"
  sqe_raw="$(sqe_exec "$user" "$sql")"
  spark_tsv "$user" "$sql" >"$TMP_DIR/spark.out" 2>"$TMP_DIR/spark.err" || true
  spark_raw="$(cat "$TMP_DIR/spark.out" "$TMP_DIR/spark.err")"
  printf '%s' "$sqe_raw" | is_permission_denial && sqe_denied=1
  printf '%s' "$spark_raw" | grep -qiE "ADD_TABLE_SNAPSHOT|not authorized|forbidden|permission denied" \
    && spark_denied=1
  echo "     SQE denial:"
  printf '%s\n' "$sqe_raw" | useful_error
  echo "     Spark denial:"
  printf '%s\n' "$spark_raw" | useful_error
  if [ "$sqe_denied" = 1 ] && [ "$spark_denied" = 1 ]; then
    green "     PASS — both engines refused the snapshot commit"
    PASS=$((PASS+1))
  else
    red "     FAIL — expected a write denial in both engines"
    FAIL=$((FAIL+1))
  fi
}

action_as() { # user sql description
  local user="$1" sql="$2" desc="$3" out deadline
  echo
  bold "$desc"
  echo "     SQL (SQE/$user): $sql"
  deadline=$(( $(date +%s) + POLICY_BUDGET ))
  while :; do
    out="$(sqe_exec "$user" "$sql")"
    ! printf '%s' "$out" | grep -qi 'error:' && break
    [ "$(date +%s)" -ge "$deadline" ] && break
    dim "     waiting for the grant to reach Polaris ..."
    sleep 5
  done
  printf '%s\n' "$out" | sed 's/^/       /'
  if printf '%s' "$out" | grep -qi 'error:'; then
    red "     action failed"
    exit 1
  fi
}

action() { # sql description [must_match]
  local out want="${3:-}"
  echo
  bold "$2"
  echo "     SQL (SQE/carol): $1"
  out="$(sqe_exec carol "$1")"
  printf '%s\n' "$out" | sed 's/^/       /'
  if printf '%s' "$out" | grep -qi 'error:'; then
    red "     action failed"
    exit 1
  fi
  if [ -n "$want" ] && ! printf '%s' "$out" | grep -Eq "$want"; then
    red "     action failed: output did not match /$want/"
    exit 1
  fi
}

spark_action_as() { # user sql description
  local user="$1" sql="$2" desc="$3" out rc=0 deadline
  echo
  bold "$desc"
  echo "     SQL (Spark/$user): $sql"
  deadline=$(( $(date +%s) + POLICY_BUDGET ))
  while :; do
    rc=0
    out="$(spark_tsv "$user" "$sql" 2>&1)" || rc=$?
    [ "$rc" -eq 0 ] && break
    [ "$(date +%s)" -ge "$deadline" ] && break
    dim "     waiting for the grant to reach Polaris ..."
    sleep 5
  done
  printf '%s\n' "$out" | sed 's/^/       /'
  if [ "$rc" -ne 0 ]; then
    sed 's/^/       /' "$TMP_DIR/spark.err"
    red "     action failed"
    exit 1
  fi
}

sqe_assert() { # expect(ok|error) sql description match [avoid]
  local expect="$1" sql="$2" desc="$3" want="$4" avoid="${5:-}" out verdict=PASS
  STEP=$((STEP+1))
  echo
  bold "[$STEP] $desc"
  dim "     management plane: SQE SQL (Spark has no equivalent statement)"
  echo "     SQL (SQE/carol): $sql"
  out="$(sqe_exec carol "$sql")"
  printf '%s\n' "$out" | sed 's/^/       /'
  case "$expect" in
    ok) printf '%s' "$out" | grep -qi 'error:' && verdict=FAIL ;;
    error) printf '%s' "$out" | grep -qi 'error:' || verdict=FAIL ;;
    *) verdict=FAIL ;;
  esac
  [ -n "$want" ] && ! printf '%s' "$out" | grep -Eq "$want" && verdict=FAIL
  [ -n "$avoid" ] && printf '%s' "$out" | grep -Eq "$avoid" && verdict=FAIL
  if [ "$verdict" = PASS ]; then green "     PASS"; PASS=$((PASS+1));
  else red "     FAIL"; FAIL=$((FAIL+1)); fi
}

best_effort_action() { # sql description
  local out
  echo
  bold "$2"
  echo "     SQL (SQE/carol): $1"
  out="$(sqe_exec carol "$1")"
  printf '%s\n' "$out" | sed 's/^/       /'
  if printf '%s' "$out" | grep -qi 'error:'; then
    dim "     ignored: prior fixture or grant may not exist"
  fi
}

# ── fixture and transcript ───────────────────────────────────────────────────

# geo-tag is the pre-rename name of pii-tag; it stays in the list so a stack
# left behind by an older revision of this script is still cleaned up.
POLICIES="amount-null ssn-last4 email-hash eu-rows geo-tag pii-tag ssn-null-tag broken"
for policy in $POLICIES; do
  best_effort_action "DROP POLICY IF EXISTS \"${POLICY_PREFIX}${policy}\"" \
    "Remove any policy left by an interrupted run"
done

# SET/UNSET TAGS is idempotent enough for an interrupted fixture. A missing table
# is expected on the first run and is intentionally ignored here.
best_effort_action "ALTER TABLE $T UNSET TAGS (phone, region, ssn)" \
  "Reset projected tag associations left by an interrupted run"
sqe_exec carol "DROP VIEW IF EXISTS $CAT.$NS.orders_eu" >/dev/null 2>&1 || true
sqe_exec carol "DROP TABLE IF EXISTS $T" >/dev/null 2>&1 || true
sqe_exec carol "CREATE SCHEMA IF NOT EXISTS $CAT.$NS" >/dev/null 2>&1 || true
# `phone` exists so the tag mask lands on a column no other policy touches. A
# tag mask over `region` would collide with the row filter that reads it, and a
# tag mask over amount/ssn/email would collide with their resource masks. Both
# collisions are real divergences, and both get their own section below.
action "CREATE TABLE $T (id BIGINT, region VARCHAR, amount DOUBLE, ssn VARCHAR, email VARCHAR, phone VARCHAR)" \
  "Fixture: one Iceberg table shared by SQE and Spark"
action "INSERT INTO $T VALUES \
(1,'EU',10.0,'111-11-1111','a@x','555-0001'), \
(2,'US',20.0,'222-22-2222','b@x','555-0002'), \
(3,'EU',30.0,'333-33-3333','c@x','555-0003')" \
  "Seed three rows through SQE"

for role in engineer analyst; do
  action "REVOKE SELECT ON $T FROM ROLE \"$role\"" \
    "Security baseline: revoke $role SELECT"
  action "REVOKE INSERT ON $T FROM ROLE \"$role\"" \
    "Security baseline: revoke $role INSERT"
done

Q="SELECT id, region, amount, ssn, email FROM $T ORDER BY id"
RAW=$'1 | EU | 10.0 | 111-11-1111 | a@x\n2 | US | 20.0 | 222-22-2222 | b@x\n3 | EU | 30.0 | 333-33-3333 | c@x'

echo; bold "═══ 1. Catalog gate: GRANT is what enables both engines ═══"
compare_equal carol "$Q" "Admin control proves the shared table and fixture exist" "$RAW"
compare_denied alice "$Q" "Before GRANT, neither engine may load the shared table"
action "GRANT SELECT ON $T TO ROLE \"analyst\"" \
  "Grant SELECT to analyst; Alice is a member"
compare_equal alice "$Q" "The role grant exposes the same three raw rows" "$RAW"

echo; bold "═══ 2. Role membership and write authority ═══"
compare_denied dave "$Q" "Dave is in no role, so the analyst grant does not reach him"
INSERT9="INSERT INTO $T VALUES (9,'EU',1.0,'999-99-9999','z@x','555-0009')"
compare_write_denied alice "$INSERT9" "SELECT does not imply INSERT"
action "GRANT INSERT ON $T TO ROLE \"analyst\"" \
  "Grant INSERT separately to the analyst role"
action_as alice "$INSERT9" "Alice inserts through SQE"
compare_equal alice "SELECT count(*) AS n FROM $T" \
  "Both engines see Alice's SQE commit" "4"
action "DELETE FROM $T WHERE id = 9" \
  "Carol removes the SQE probe row (cross-user snapshot invalidation)" \
  '\| 1[[:space:]]+\|'
compare_equal alice "SELECT count(*) AS n FROM $T" \
  "Both engines see Carol's delete" "3"
spark_action_as alice "INSERT INTO $T VALUES (10,'US',2.0,'000-00-0010','spark@x','555-0010')" \
  "Alice inserts through Spark with the same INSERT grant"
compare_equal alice "SELECT count(*) AS n FROM $T" \
  "Both engines see Alice's Spark commit" "4"
action "DELETE FROM $T WHERE id = 10" "Carol removes the Spark probe row" \
  '\| 1[[:space:]]+\|'
compare_equal alice "SELECT count(*) AS n FROM $T" \
  "Both engines return to the three-row fixture" "3"

echo; bold "═══ 3. Resource column masks ═══"
action "GRANT SELECT ON $T TO ROLE \"engineer\"" "Grant engineer read access"
action "CREATE OR REPLACE POLICY \"${POLICY_PREFIX}amount-null\" ON TABLE $T \
COLUMN MASK MASK_NULL TO ROLE engineer ON COLUMN amount" \
  "Create MASK_NULL on amount through SQE SQL"
action "CREATE OR REPLACE POLICY \"${POLICY_PREFIX}ssn-last4\" ON TABLE $T \
COLUMN MASK MASK_SHOW_LAST_4 TO ROLE engineer ON COLUMN ssn" \
  "Create MASK_SHOW_LAST_4 on ssn through SQE SQL"
action "CREATE OR REPLACE POLICY \"${POLICY_PREFIX}email-hash\" ON TABLE $T \
COLUMN MASK MASK_HASH TO ROLE engineer ON COLUMN email" \
  "Create MASK_HASH on email through SQE SQL"

# Named MASK_SHOW_LAST_4 deliberately is not byte-portable: SQE honors the Hive
# servicedef transformer, while Kyuubi uses its own character-class replacements.
SQE_NAMED=$'1 | EU | <NULL> | xxx-xx-1111\n2 | US | <NULL> | xxx-xx-2222\n3 | EU | <NULL> | xxx-xx-3333'
SPARK_NAMED=$'1 | EU | <NULL> | nnnUnnU1111\n2 | US | <NULL> | nnnUnnU2222\n3 | EU | <NULL> | nnnUnnU3333'
compare_expected bob "SELECT id, region, amount, ssn FROM $T ORDER BY id" \
  "NULL and last-four masks protect the same cells" "$SQE_NAMED" "$SPARK_NAMED" \
  "same protected cells; documented named-mask rendering difference"
# MASK_HASH is the second rendering difference: SQE hashes with sha256, Kyuubi
# with md5. Comparing digest lengths documents it without pinning digests.
compare_expected bob "SELECT id, length(email) AS email_len FROM $T ORDER BY id" \
  "MASK_HASH hides the address in both engines with different digests" \
  $'1 | 64\n2 | 64\n3 | 64' $'1 | 32\n2 | 32\n3 | 32' \
  "no raw address in either engine; SQE emits sha256, Kyuubi emits md5"
# Every masked-cell predicate below has to hold for both digest algorithms, so
# it asserts "not the raw value, and hash-shaped" rather than a fixed length.
compare_equal bob "SELECT count(*) AS rows_seen, \
sum(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) AS amount_null, \
sum(CASE WHEN substr(ssn,8,4) IN ('1111','2222','3333') AND ssn NOT IN ('111-11-1111','222-22-2222','333-33-3333') THEN 1 ELSE 0 END) AS ssn_masked, \
sum(CASE WHEN length(email) >= 32 AND email NOT IN ('a@x','b@x','c@x') THEN 1 ELSE 0 END) AS email_hashed FROM $T" \
  "All three resource masks apply without dropping rows" "3 | 3 | 3 | 3"
compare_equal alice "$Q" \
  "Alice is outside engineer and remains the raw-value control" "$RAW"

echo; bold "═══ 4. Row filtering ═══"
action "CREATE OR REPLACE POLICY \"${POLICY_PREFIX}eu-rows\" ON TABLE $T \
ROW FILTER TO ROLE engineer USING (region = 'EU')" \
  "Create the EU row filter through SQE SQL"
compare_equal bob "SELECT id, region FROM $T ORDER BY id" \
  "Bob sees only EU rows through both engines" $'1 | EU\n3 | EU'
compare_equal alice "SELECT id, region FROM $T ORDER BY id" \
  "Alice remains unfiltered in both engines" $'1 | EU\n2 | US\n3 | EU'

echo; bold "═══ 5. Tag masking and policy composition ═══"
action "ALTER TABLE $T SET TAGS (phone = ('$TAG_NAME'))" \
  "Tag phone in Iceberg and project the association to Ranger for Spark"
sqe_assert ok "SHOW TAGS ON $T" "Read the tag association back" "$TAG_NAME"
action "CREATE OR REPLACE POLICY \"${POLICY_PREFIX}pii-tag\" ON TAG $TAG_NAME \
COLUMN MASK CUSTOM TO ROLE engineer USING ('XX')" \
  "Create a portable tag mask through SQE SQL"
compare_equal bob "SELECT id, phone FROM $T ORDER BY id" \
  "The tag rule masks Bob's filtered rows identically" $'1 | XX\n3 | XX'
compare_equal alice "SELECT id, phone FROM $T ORDER BY id" \
  "The role-scoped tag rule leaves Alice raw" \
  $'1 | 555-0001\n2 | 555-0002\n3 | 555-0003'
compare_equal bob "SELECT count(*) AS rows_seen, \
sum(CASE WHEN phone = 'XX' THEN 1 ELSE 0 END) AS tag_masked, \
sum(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) AS amount_nullified, \
sum(CASE WHEN substr(ssn,8,4) IN ('1111','3333') AND ssn NOT IN ('111-11-1111','333-33-3333') THEN 1 ELSE 0 END) AS ssn_masked, \
sum(CASE WHEN length(email) >= 32 AND email NOT IN ('a@x','b@x','c@x') THEN 1 ELSE 0 END) AS email_hashed FROM $T" \
  "Row filter, three resource masks, and a tag mask compose in one plan" \
  "2 | 2 | 2 | 2 | 2"

echo; bold "═══ 5a. Resource and tag precedence ═══"
# This section used to assert a divergence. SQE resolved a contested column to
# the resource mask (most-specific-rule-wins) while Kyuubi resolved it to the
# tag mask (the standard Ranger plugin order). `policy.mask-precedence` now
# defaults to `tag`, so one policy set renders one value in both engines. Set it
# to `resource` to get the old behaviour back, and this step diverges again.
action "ALTER TABLE $T SET TAGS (ssn = ('$NULL_TAG'))" \
  "Apply a second tag to the already resource-masked ssn column"
action "CREATE OR REPLACE POLICY \"${POLICY_PREFIX}ssn-null-tag\" ON TAG $NULL_TAG \
COLUMN MASK MASK_NULL TO ROLE engineer" \
  "Create the competing tag MASK_NULL through SQE SQL"
compare_equal bob "SELECT id, ssn FROM $T ORDER BY id" \
  "A contested column resolves to the tag mask in both engines" \
  $'1 | <NULL>\n3 | <NULL>'
compare_equal alice "SELECT count(*) AS rows_seen, \
sum(CASE WHEN ssn IN ('111-11-1111','222-22-2222','333-33-3333') THEN 1 ELSE 0 END) AS raw_ssn, \
sum(CASE WHEN ssn IS NULL THEN 1 ELSE 0 END) AS null_ssn FROM $T" \
  "Both masks remain role-scoped for Alice" "3 | 3 | 0"
action "ALTER TABLE $T UNSET TAGS (ssn)" "Remove the precedence-test tag"

echo; bold "═══ 5b. Row filter reading a tag-masked column ═══"
# Tagging the column the row filter reads puts the two rules on a collision
# course, and the engines resolve the ordering differently. SQE evaluates the
# filter against stored values and masks the surviving rows. Kyuubi injects its
# masking Project *below* RowFilterMarker, so `region = 'EU'` is compared with
# the masked literal 'XX' and matches nothing. The count makes the divergence a
# value rather than an empty result set, which an error would also produce.
action "ALTER TABLE $T SET TAGS (region = ('$TAG_NAME'))" \
  "Also tag the column the EU row filter reads"
compare_expected bob "SELECT count(*) AS n FROM $T" \
  "Filter-then-mask versus mask-then-filter changes the row count" \
  "2" "0" \
  "SQE filters raw values then masks; Kyuubi masks below the row filter, so Bob sees no rows"
action "ALTER TABLE $T UNSET TAGS (region)" \
  "Remove the overlapping region tag"
compare_equal bob "SELECT count(*) AS n FROM $T" \
  "Both engines agree again once the collision is removed" "2"

echo; bold "═══ 5c. Inert tags and SQL validation ═══"
action "ALTER TABLE $T UNSET TAGS (phone)" "Remove the governed PII tag"
action "ALTER TABLE $T SET TAGS (region = ('$NO_RULE_TAG'))" \
  "Attach a tag for which no policy exists"
sqe_assert ok "SHOW TAGS ON $T" "Confirm only the no-rule region tag remains" \
  "$NO_RULE_TAG" "$TAG_NAME"
compare_equal bob "SELECT id, region FROM $T ORDER BY id" \
  "A tag without a policy is inert in both engines" $'1 | EU\n3 | EU'
sqe_assert error "CREATE OR REPLACE POLICY \"${POLICY_PREFIX}broken\" ON TAG $NO_RULE_TAG \
COLUMN MASK CUSTOM TO ROLE engineer" \
  "Reject CUSTOM without its required USING expression" \
  'CUSTOM COLUMN MASK requires USING'
compare_equal bob "SELECT id, region FROM $T ORDER BY id" \
  "The rejected policy changed neither engine" $'1 | EU\n3 | EU'
compare_equal alice "SELECT id, region FROM $T ORDER BY id" \
  "Alice remains unaffected" $'1 | EU\n2 | US\n3 | EU'
action "ALTER TABLE $T UNSET TAGS (region)" "Remove the inert tag"

echo; bold "═══ 6. SQE policy introspection ═══"
sqe_assert ok "SHOW GRANTS ON $T" "List the Ranger grants SQE authored" \
  'table-data-read.*ROLE.*analyst'
sqe_assert ok "CHECK ACCESS SELECT ON $T FOR USER \"alice\"" \
  "Explain Alice's role-derived access" 'true.*Allowed via ROLE' 'false'
sqe_assert ok "CHECK ACCESS SELECT ON $T FOR USER \"dave\"" \
  "Explain Dave's missing access" 'false.*No matching grant' 'true'

echo; bold "═══ 7. Views ═══"
action "CREATE OR REPLACE VIEW $CAT.$NS.orders_eu AS \
SELECT id, region FROM $T WHERE region = 'EU'" \
  "Create a view through SQE"
action "GRANT SELECT ON VIEW $CAT.$NS.orders_eu TO ROLE \"analyst\"" \
  "Grant the view name to analyst"
sqe_assert ok "SHOW GRANTS ON $CAT.$NS.orders_eu" \
  "Verify that GRANT ON VIEW wrote view access types" \
  'view-properties-read' 'table-data-read'
compare_equal alice "SELECT id, region FROM $CAT.$NS.orders_eu ORDER BY id" \
  "Both engines expand the view and still authorize its base table" $'1 | EU\n3 | EU'

echo; bold "═══ 8. REVOKE closes the catalog gate again ═══"
# Closing the gate takes three revokes, and the third is the interesting one.
# Bob holds BOTH demo roles in Keycloak, so revoking `engineer` leaves the
# section-1 analyst grant carrying him. Revoking analyst SELECT is still not
# enough: grant-profile.json expands `table-data-write` to include
# `table-data-read`, because a writer that cannot read its own table is useless.
# So the INSERT granted back in section 2 keeps conferring read, and
# `REVOKE SELECT` reports success while the row still comes back. Read access
# ends when the last privilege implying it is gone.
action "REVOKE SELECT ON $T FROM ROLE \"engineer\"" \
  "Revoke engineer SELECT"
action "REVOKE SELECT ON $T FROM ROLE \"analyst\"" \
  "Revoke Bob's second path to the table"
action "REVOKE INSERT ON $T FROM ROLE \"analyst\"" \
  "Revoke the INSERT whose table-data-write still implies table-data-read"
# Ask the engine whether the gate is shut before asking a query to prove it.
# When this step first failed it did so as 120 seconds of "waiting for the
# revoke to reach Polaris", because a surviving grant and a slow poll look
# identical from the outside. CHECK ACCESS distinguishes them in one call, and
# names the grant still carrying the user.
sqe_assert ok "CHECK ACCESS SELECT ON $T FOR USER \"bob\"" \
  "Confirm no grant path is left before asserting the denial" 'false' 'true'
compare_denied bob "$Q" "After REVOKE, both engines deny Bob again"

action "ALTER TABLE $T UNSET TAGS (phone, region, ssn)" \
  "Security teardown: remove projected tag associations"
for policy in $POLICIES; do
  action "DROP POLICY IF EXISTS \"${POLICY_PREFIX}${policy}\"" \
    "Security teardown: remove SQL-managed policy"
done
action "REVOKE INSERT ON $T FROM ROLE \"analyst\"" \
  "Security teardown: revoke analyst INSERT"
action "REVOKE SELECT ON $T FROM ROLE \"analyst\"" \
  "Security teardown: revoke analyst SELECT"

echo
bold "─────────────────────────────────────────────"
if [ "$FAIL" -eq 0 ]; then
  green "all $PASS cross-engine comparisons behaved as documented"
else
  red "$FAIL of $((PASS+FAIL)) cross-engine comparisons failed"
fi
bold "─────────────────────────────────────────────"
dim "shared table left in place: $T"
dim "stack left running; tear down with: (cd $STACK_DIR && docker compose down)"

[ "$FAIL" -eq 0 ]
