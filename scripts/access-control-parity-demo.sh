#!/usr/bin/env bash
# Readable, live SQE <-> Spark access-control comparison, told as an EU retail
# bank governance walkthrough.
#
# The fixture is a customer register and a payment ledger:
#
#   customers  12 rows  national_id, dob, iban, nationality, residency_region,
#                       branch, consent_marketing, pep_flag, risk_score, phone
#   payments   24 rows  booked_at, amount_eur, counterparty_iban,
#                       counterparty_country, channel, aml_alert, mcc
#
# Every probe runs against the SAME Iceberg tables through both engines, with the
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
# Covered:
#   1. catalog denial, role GRANT, user outside the role, and REVOKE
#   2. SELECT versus INSERT, including a denied INSERT in both engines
#      (compare_write_denied) and successful writes seen by both engines
#      (#426 cell 1)
#   3. five resource masks: MASK_NULL, MASK_SHOW_LAST_4, MASK_HASH,
#      MASK_DATE_SHOW_YEAR, and MASK on a name; then ADD COLUMN on that
#      masked table through SQE and through Spark (#426 cell 2)
#   4. GDPR data-residency row filtering and its unfiltered-role control
#   5. tag masking, mask composition, resource/tag precedence, and a row filter
#      that reads a tag-masked column
#   6. inert tags and invalid CUSTOM-policy SQL
#   7. SHOW GRANTS, CHECK ACCESS, and view grants
#   8. data minimisation for the fraud desk: one tag rule spanning two tables
#   9. audit right of access with a retention window, plus a join that carries
#      both a row filter and column masks
#  10. REVOKE closing the catalog gate again
#
# Spark has no SQL surface for authoring Ranger policies or introspecting the
# Polaris Ranger policy model. Those management statements are deliberately run
# and asserted through SQE, then their enforcement is probed through both engines.
#
# WHY THE ASSERTIONS ARE MOSTLY AGGREGATES: a `count(*)` plus `sum(CASE WHEN ...)`
# states the security claim ("no raw identifier survived") without pinning how an
# engine renders a digest, a float, or a date. Two engines that mask correctly but
# print differently still agree on the aggregate, and an engine that does not
# implement a mask at all fails it loudly. Pinned row dumps are reserved for the
# narrow cases where the rendering IS the subject.
#
# CALIBRATION STATUS: 43 of 43 comparisons were green against a live
# quickstart stack on 2026-08-12. Issue #426 adds two compare_equal probes
# (45 total). Those two are uncalibrated: if Spark ADD COLUMN diverges,
# keep the probe and document the rendering or error the same way as the
# named-mask cells. The Spark ADD uses a non-fatal helper so an uncalibrated
# step cannot abort sections 4 through 7.
# Two inferences that could have become a third documented divergence did not:
# Kyuubi truncates MASK_DATE_SHOW_YEAR to 1 January exactly as SQE does, and it
# applies a row filter and column masks to a JOINED relation the same way. Both
# are asserted rather than assumed, so a regression in either shows up as a
# failure here rather than as a comment nobody reads.
#
# HAZARDS worth knowing before editing:
#   - Kyuubi Spark 3.5 raises MISSING_ATTRIBUTES (#6889) when a row filter reads a
#     column the query does not project. Keep the filtered column in the SELECT.
#   - The Ranger `database` resource is the namespace's last component, so both
#     engines resolve `acparity`. Nothing here may depend on the catalog name.
#   - MASK_NONE is not a usable break-glass exemption: masks from other matching
#     policies are unioned in, so an exemption would need Ranger evaluation-order
#     priorities that SQE does not implement.
#   - Column restriction is not authorable through this surface; SQE populates
#     restricted_columns only fail-closed, on an unsupported mask type.
#
# Usage:
#   scripts/access-control-parity-demo.sh
#   AC_PARITY_NO_BOOTSTRAP=1 scripts/access-control-parity-demo.sh
#   AC_PARITY_SECTIONS="3,8,9" scripts/access-control-parity-demo.sh
#
# AC_PARITY_SECTIONS gates the cross-engine COMPARISONS only. Every GRANT, policy,
# and tag still runs, so a later section never misses a prerequisite; only the
# Spark JVM starts are skipped. Step numbers stay stable, so a selected run's
# numbering matches the full transcript. Valid ids: 1 2 3 4 5 5a 5b 5c 6 7 8 9 10.
#
# Each Spark probe starts spark-sql, so this takes minutes rather than seconds.
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STACK_DIR="$ROOT_DIR/quickstart/polaris-ranger-keycloak"
cd "$STACK_DIR" || { echo "missing $STACK_DIR" >&2; exit 1; }

[ -f .env ] && set -a && . ./.env && set +a
POLICY_BUDGET="${AC_PARITY_POLICY_BUDGET:-120}"
SECTIONS="${AC_PARITY_SECTIONS:-all}"

CAT=sales_wh
NS=acparity
C="$CAT.$NS.customers"
P="$CAT.$NS.payments"
VIEW="$CAT.$NS.customers_eu"
POLICY_PREFIX=acparity-demo-

# Tag vocabulary. The ACPARITY_ prefix is what makes teardown safe on a Ranger
# instance shared with other suites, so keep it on every new tag.
TAG_NAME=ACPARITY_PII          # governed PII, masked to a token for engineers
ACCOUNT_TAG=ACPARITY_ACCOUNT   # account identifiers, spans customers + payments
SPI_TAG=ACPARITY_SPI           # GDPR article 9 special category (nationality)
IDENTITY_TAG=ACPARITY_IDENTITY # direct identifiers: the name and the national id
NO_RULE_TAG=ACPARITY_NO_RULE   # attached but ungoverned: proves a tag is inert
NULL_TAG=ACPARITY_NULL         # the resource-vs-tag precedence probe

PASS=0
FAIL=0
STEP=0
CUR_SECTION=1

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

# erin and frank are the two bank personas. A missing token here, rather than a
# confusing denial ten minutes in, is the point of checking all six up front: a
# role that exists in Keycloak but not in Ranger (or without the baseline
# traverse grant) fails as an authorization error indistinguishable from the
# denials this script asserts deliberately.
dim "Checking that Spark can mint a Polaris catalog identity for each user."
for demo_user in alice bob carol dave erin frank; do
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

# ── side-by-side perspectives (display, not assertion) ───────────────────────
#
# The comparisons above prove the two engines agree. They do not SHOW what a
# policy did: a `CREATE POLICY` prints "(0 rows)", and an aggregate answers
# "0 leaks" rather than displaying the masked value a data owner would recognise.
#
# These panels run one query as several users and print each result. Through SQE
# only, deliberately: a Spark probe costs a JVM start (~7 s) while an SQE query
# costs ~200 ms, and parity is already asserted elsewhere. Showing four
# perspectives is therefore cheaper than one extra comparison.
#
# Nothing here asserts. A panel cannot fail the run, so it cannot create a false
# green either.

sqe_table() { # user sql -- the CLI's own table rendering, minus the connect banner
  # The CLI writes the table and its "(N rows)" line to different streams, so
  # under 2>&1 the count lands above the table as often as below it. Row COUNT is
  # the point of several panels (audit sees four where the analyst sees six), so
  # it is kept and re-emitted last instead of being dropped.
  local out rows
  out="$(sqe_exec "$1" "$2" | grep -vE '^sqe-cli [0-9.]+ connected to')"
  rows="$(printf '%s\n' "$out" | grep -oE '^\([0-9]+ rows?\)$' | head -1)"
  printf '%s\n' "$out" | grep -vE '^\([0-9]+ rows?\)$'
  [ -n "$rows" ] && printf '%s\n' "$rows"
}

perspectives() { # description sql user|label ...
  local desc="$1" sql="$2" spec user label
  shift 2
  echo
  bold "     .. $desc"
  dim "     SQL: $sql"
  for spec in "$@"; do
    user="${spec%%|*}"
    label="${spec#*|}"
    printf '\033[1m       %s\033[0m \033[2m(%s)\033[0m\n' "$label" "$user"
    sqe_table "$user" "$sql" | sed 's/^/         /'
  done
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

# ── section selection ────────────────────────────────────────────────────────

# Gates the Spark-paired comparisons, never the `action` statements: skipping a
# GRANT would break every later section, and a selective run has to leave the
# same policy state behind as a full one.
want_compare() {
  [ "$SECTIONS" = all ] && return 0
  case ",$SECTIONS," in
    *",$CUR_SECTION,"*) return 0 ;;
  esac
  return 1
}

section() { # id title
  CUR_SECTION="$1"
  echo
  bold "═══ $1. $2 ═══"
  want_compare \
    || dim "     comparisons skipped (AC_PARITY_SECTIONS=$SECTIONS); policy actions still run"
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
  want_compare || return 0
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
  want_compare || return 0
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
  want_compare || return 0
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
  # Both engines are SUPPOSED to fail here. The text is evidence, not a problem,
  # and SQE's "not found" is deliberate: a denied table is invisible rather than
  # forbidden, so a prober cannot map what exists.
  echo "     SQE denial (expected):"
  printf '%s\n' "$sqe_raw" | useful_error
  echo "     Spark denial (expected):"
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
  want_compare || return 0
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
  echo "     SQE denial (expected):"
  printf '%s\n' "$sqe_raw" | useful_error
  echo "     Spark denial (expected):"
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
  [ "$expect" = error ] && dim "     expectation: SQE REJECTS this statement; the error below is the evidence"
  echo "     SQL (SQE/carol): $sql"
  out="$(sqe_exec carol "$sql")"
  # An intended rejection prints an engine error, which is indistinguishable from
  # a real failure when someone is scanning the transcript. Label it at the line.
  if [ "$expect" = error ]; then
    printf '%s\n' "$out" | sed 's/^/       [expected] /'
  else
    printf '%s\n' "$out" | sed 's/^/       /'
  fi
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

# Same contract as spark_action_as, but a failure records and continues.
# Use this for uncalibrated Spark probes. spark_action_as aborts the demo.
spark_best_effort_action_as() { # user sql description
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
    [ -f "$TMP_DIR/spark.err" ] && sed 's/^/       /' "$TMP_DIR/spark.err"
    dim "     ignored: uncalibrated Spark action diverged; later sections still run"
  fi
}

# ── fixture and transcript ───────────────────────────────────────────────────

# The first six names are pre-bank-fixture policy names (amount/ssn/email/geo).
# They stay in the list so a stack left behind by an older revision of this
# script is still cleaned up.
POLICIES="amount-null ssn-last4 email-hash eu-rows geo-tag pii-tag ssn-null-tag broken \
risk-null nid-last4 iban-hash dob-year name-mask nid-null-tag \
account-tag-fraud spi-tag-fraud identity-tag-fraud retention-rows"
for policy in $POLICIES; do
  best_effort_action "DROP POLICY IF EXISTS \"${POLICY_PREFIX}${policy}\"" \
    "Remove any policy left by an interrupted run"
done

# SET/UNSET TAGS is idempotent enough for an interrupted fixture. A missing table
# is expected on the first run and is intentionally ignored here.
best_effort_action "ALTER TABLE $C DROP COLUMN acparity_nick" \
  "Drop the #426 ADD COLUMN probe if an interrupted run left it"
best_effort_action "ALTER TABLE $C UNSET TAGS (phone, residency_region, national_id, nationality, full_name)" \
  "Reset projected customer tag associations left by an interrupted run"
best_effort_action "ALTER TABLE $P UNSET TAGS (counterparty_iban)" \
  "Reset projected payment tag associations left by an interrupted run"
sqe_exec carol "DROP VIEW IF EXISTS $VIEW" >/dev/null 2>&1 || true
sqe_exec carol "DROP TABLE IF EXISTS $C" >/dev/null 2>&1 || true
sqe_exec carol "DROP TABLE IF EXISTS $P" >/dev/null 2>&1 || true
sqe_exec carol "CREATE SCHEMA IF NOT EXISTS $CAT.$NS" >/dev/null 2>&1 || true

# Column choices that carry weight later:
#   residency_region  the row filter reads it, so the GDPR data-residency story
#                     and the section 5b filter/mask collision share one column
#   risk_score        INT, so MASK_NULL can be asserted without float rendering
#   national_id       nine digits, no separators: MASK_SHOW_LAST_4 keeps the last
#                     four and replaces the rest, and "contains no x and no n"
#                     detects a leak under EITHER engine's replacement char
#   iban              uppercase letters and digits, so `iban = upper(iban)`
#                     distinguishes a raw account number from a lowercase hex
#                     digest, whichever digest algorithm produced it
#   dob               no customer is born on 1 January, which is what lets
#                     MASK_DATE_SHOW_YEAR be asserted as "now reports 1 January"
#   phone             the uncontested tag-mask target: no resource mask and no
#                     row filter touches it, so section 5 measures the tag alone
action "CREATE TABLE $C (cust_id BIGINT, full_name VARCHAR, national_id VARCHAR, \
dob DATE, iban VARCHAR, nationality VARCHAR, residency_region VARCHAR, \
branch VARCHAR, consent_marketing BOOLEAN, pep_flag BOOLEAN, risk_score INT, \
phone VARCHAR)" \
  "Fixture: the customer register, shared by SQE and Spark"

action "CREATE TABLE $P (pay_id BIGINT, cust_id BIGINT, booked_at DATE, \
amount_eur DOUBLE, counterparty_iban VARCHAR, counterparty_country VARCHAR, \
channel VARCHAR, aml_alert BOOLEAN, mcc INT)" \
  "Fixture: the payment ledger, shared by SQE and Spark"

# 12 customers: cust_id 1-7 are EU-resident, 8-12 are not. Two are PEPs (6, 10).
action "INSERT INTO $C VALUES \
(1,'Sanne de Vries','184729103',DATE '1978-03-14','NL91ABNA0417164300','NL','EU','AMS-01',true,false,12,'+31-20-555-0101'), \
(2,'Marek Kowalski','295830214',DATE '1985-07-02','PL27114020040000300201355387','PL','EU','WAW-02',false,false,34,'+48-22-555-0102'), \
(3,'Elena Rossi','306941325',DATE '1969-11-23','IT60X0542811101000000123456','IT','EU','MIL-01',true,false,8,'+39-02-555-0103'), \
(4,'Johan Andersson','417052436',DATE '1991-01-30','SE4550000000058398257466','SE','EU','STO-01',false,false,21,'+46-8-555-0104'), \
(5,'Fatima El Amrani','528163547',DATE '1982-06-17','FR7630006000011234567890189','MA','EU','PAR-03',true,false,47,'+33-1-555-0105'), \
(6,'Dieter Krause','639274658',DATE '1957-09-05','DE89370400440532013000','DE','EU','BER-01',false,true,76,'+49-30-555-0106'), \
(7,'Ana Silva','740385769',DATE '1996-04-11','PT50000201231234567890154','PT','EU','LIS-02',true,false,15,'+351-21-555-0107'), \
(8,'Michael Brennan','851496870',DATE '1974-12-08','US64SVBKUS6S3300958879','US','NON_EU','NYC-01',false,false,29,'+1-212-555-0108'), \
(9,'Wei Zhang','962507981',DATE '1988-02-26','SG12DBSS1234567890','SG','NON_EU','SIN-01',true,false,52,'+65-6555-0109'), \
(10,'Olga Petrova','073618092',DATE '1980-08-19','CH9300762011623852957','RU','NON_EU','ZRH-02',false,true,91,'+41-44-555-0110'), \
(11,'Rajesh Nair','184729203',DATE '1993-05-04','AE070331234567890123456','IN','NON_EU','DXB-01',true,false,38,'+971-4-555-0111'), \
(12,'Grace Mensah','295830314',DATE '1965-10-21','GB29NWBK60161331926819','GH','NON_EU','LON-04',false,false,63,'+233-30-555-0112')" \
  "Seed twelve customers through SQE"

# 24 payments. pay_id 1-6 are booked before 2019 and fall outside the retention
# window section 9 asserts; 7-24 are inside it. Four carry an AML alert (3, 5,
# 11, 14). Fifteen belong to an EU-resident customer, which is the row count the
# section 9 join produces once the residency filter applies.
action "INSERT INTO $P VALUES \
(1,1,DATE '2016-05-12',250.0,'DE89370400440532013000','DE','SEPA',false,5411), \
(2,3,DATE '2017-02-28',1200.0,'IT60X0542811101000000123456','IT','SEPA',false,5812), \
(3,6,DATE '2017-11-03',8400.0,'CH9300762011623852957','CH','SWIFT',true,6012), \
(4,8,DATE '2018-01-19',340.0,'US64SVBKUS6S3300958879','US','SWIFT',false,4111), \
(5,10,DATE '2018-06-30',19500.0,'CH9300762011623852957','CH','SWIFT',true,6012), \
(6,12,DATE '2018-09-14',76.0,'GB29NWBK60161331926819','GB','CARD',false,5814), \
(7,1,DATE '2019-03-02',45.0,'NL91ABNA0417164300','NL','IDEAL',false,5411), \
(8,2,DATE '2019-07-21',990.0,'PL27114020040000300201355387','PL','SEPA',false,5732), \
(9,4,DATE '2020-01-08',150.0,'SE4550000000058398257466','SE','CARD',false,5812), \
(10,5,DATE '2020-04-17',2750.0,'FR7630006000011234567890189','FR','SEPA',false,6300), \
(11,6,DATE '2020-09-29',12000.0,'AE070331234567890123456','AE','SWIFT',true,6012), \
(12,7,DATE '2021-02-11',65.0,'PT50000201231234567890154','PT','IDEAL',false,5411), \
(13,9,DATE '2021-06-05',3300.0,'SG12DBSS1234567890','SG','SWIFT',false,4722), \
(14,10,DATE '2021-10-23',27400.0,'CH9300762011623852957','CH','SWIFT',true,6012), \
(15,11,DATE '2022-01-30',480.0,'AE070331234567890123456','AE','CARD',false,5541), \
(16,12,DATE '2022-05-19',1150.0,'GB29NWBK60161331926819','GB','SEPA',false,5912), \
(17,2,DATE '2022-11-07',210.0,'PL27114020040000300201355387','PL','IDEAL',false,5814), \
(18,3,DATE '2023-03-15',5600.0,'IT60X0542811101000000123456','IT','SEPA',false,6300), \
(19,5,DATE '2023-08-24',89.0,'FR7630006000011234567890189','FR','CARD',false,5411), \
(20,8,DATE '2024-02-06',720.0,'US64SVBKUS6S3300958879','US','SWIFT',false,4111), \
(21,1,DATE '2024-07-18',1340.0,'NL91ABNA0417164300','NL','SEPA',false,5732), \
(22,4,DATE '2025-01-27',60.0,'SE4550000000058398257466','SE','IDEAL',false,5812), \
(23,7,DATE '2025-06-09',9800.0,'PT50000201231234567890154','PT','SWIFT',false,6012), \
(24,9,DATE '2026-01-14',430.0,'SG12DBSS1234567890','SG','CARD',false,4722)" \
  "Seed twenty-four payments through SQE"

# The Ranger grant API validates that a grantee role already EXISTS, so a stack
# bootstrapped before fraud_analyst/auditor were added fails on the first grant
# naming them. That failure arrives as a generic "action failed" twenty minutes
# into the run, which reads like a policy bug. Diagnose it in the first seconds
# instead. The Keycloak half of the same problem is already caught by the token
# loop above.
preflight_role() { # role
  local out
  out="$(sqe_exec carol "REVOKE SELECT ON $C FROM ROLE \"$1\"")"
  printf '%s' "$out" | grep -qi 'error:' || return 0
  red "Ranger role '$1' is not usable as a grantee:"
  printf '%s\n' "$out" | sed 's/^/       /'
  red "This stack predates the fraud_analyst/auditor roles. Re-seed Ranger with:"
  dim "  (cd $STACK_DIR && docker compose up -d --force-recreate ranger-setup)"
  exit 1
}
for role in fraud_analyst auditor; do
  preflight_role "$role"
done

# The token loop above proves Keycloak knows a user; it does NOT prove Polaris
# does. Polaris federation resolves an EXISTING principal entity by
# preferred_username and never creates one, so a user added to the realm but not
# to polaris/bootstrap-data.sh mints a token and then fails every read with 401
# "Failed to resolve principal". Under a role-scoped policy that reads as a
# denial, which is why it is checked here rather than discovered in section 8.
preflight_principal() { # user
  local out
  out="$(sqe_exec "$1" "SHOW SCHEMAS IN $CAT")"
  printf '%s' "$out" | grep -qiE 'failed to resolve principal' || return 0
  red "Polaris has no principal entity for '$1':"
  printf '%s\n' "$out" | sed 's/^/       /'
  red "This stack predates the erin/frank principals. Re-seed Polaris with:"
  dim "  (cd $STACK_DIR && docker compose up -d --force-recreate polaris-setup)"
  exit 1
}
for demo_user in erin frank; do
  preflight_principal "$demo_user"
done

for role in engineer analyst fraud_analyst auditor; do
  for tbl in "$C" "$P"; do
    action "REVOKE SELECT ON $tbl FROM ROLE \"$role\"" \
      "Security baseline: revoke $role SELECT on ${tbl##*.}"
    action "REVOKE INSERT ON $tbl FROM ROLE \"$role\"" \
      "Security baseline: revoke $role INSERT on ${tbl##*.}"
  done
done

# ── reusable assertion fragments ─────────────────────────────────────────────
#
# Each states a security claim in a way that is blind to how an engine renders
# the mask, so the same literal is correct for SQE and for Kyuubi.

# A raw national_id is nine digits. Every mask either nulls it or replaces the
# leading digits with x (SQE, Hive servicedef) or n (Kyuubi).
NID_LEAK="national_id IS NOT NULL AND national_id NOT LIKE '%x%' AND national_id NOT LIKE '%n%'"
# The seeded IBANs are 18 to 28 characters. A digest is 32 (md5) or 64 (sha256).
# Length separates raw from hashed without depending on hex case, on which digest
# algorithm ran, or on whether the hash is keyed.
IBAN_LEAK="length(iban) <= 28"
# Every seeded name contains a lowercase vowel. A masked name contains only X, x,
# n, and separators, under either engine's replacement characters.
NAME_LEAK="(full_name LIKE '%a%' OR full_name LIKE '%e%' OR full_name LIKE '%i%' \
OR full_name LIKE '%o%' OR full_name LIKE '%u%')"
# The date mask carries two separate claims, and they are asserted separately so
# a failure names itself.
#
# DOB_LEAK: the stored birth date is gone. True under ANY masking behaviour, so a
# failure here means the mask did not apply at all.
DOB_LEAK="dob IN (DATE '1957-09-05', DATE '1965-10-21', DATE '1969-11-23', \
DATE '1974-12-08', DATE '1978-03-14', DATE '1980-08-19', DATE '1982-06-17', \
DATE '1985-07-02', DATE '1988-02-26', DATE '1991-01-30', DATE '1993-05-04', \
DATE '1996-04-11')"
# DOB_YEAR_ONLY: the year survives, as 1 January. SQE truncates Date32 to
# 1 January of the same year (sqe-policy/tests/rewriter_integration.rs:625), and
# Kyuubi was MEASURED to do the same: both engines returned dob_year_only = 12,
# so this is portable and not a third divergence. Should either engine change,
# dob_year_only moves while dob_leaks stays 0, which names the failure as year
# handling rather than as a mask that did not apply.
#
# No seeded dob falls on 1 January, so DOB_YEAR_ONLY counts masked rows and
# nothing else. All twelve birth years are listed because section 3 runs before
# the residency filter exists.
DOB_YEAR_ONLY="dob IN (DATE '1957-01-01', DATE '1965-01-01', DATE '1969-01-01', \
DATE '1974-01-01', DATE '1978-01-01', DATE '1980-01-01', DATE '1982-01-01', \
DATE '1985-01-01', DATE '1988-01-01', DATE '1991-01-01', DATE '1993-01-01', \
DATE '1996-01-01')"

Q_CUST="SELECT cust_id, residency_region, risk_score, national_id, iban FROM $C ORDER BY cust_id"
RAW_CUST=$'1 | EU | 12 | 184729103 | NL91ABNA0417164300
2 | EU | 34 | 295830214 | PL27114020040000300201355387
3 | EU | 8 | 306941325 | IT60X0542811101000000123456
4 | EU | 21 | 417052436 | SE4550000000058398257466
5 | EU | 47 | 528163547 | FR7630006000011234567890189
6 | EU | 76 | 639274658 | DE89370400440532013000
7 | EU | 15 | 740385769 | PT50000201231234567890154
8 | NON_EU | 29 | 851496870 | US64SVBKUS6S3300958879
9 | NON_EU | 52 | 962507981 | SG12DBSS1234567890
10 | NON_EU | 91 | 073618092 | CH9300762011623852957
11 | NON_EU | 38 | 184729203 | AE070331234567890123456
12 | NON_EU | 63 | 295830314 | GB29NWBK60161331926819'

EU_ONLY=$'1 | EU\n2 | EU\n3 | EU\n4 | EU\n5 | EU\n6 | EU\n7 | EU'
ALL_REGIONS=$'1 | EU\n2 | EU\n3 | EU\n4 | EU\n5 | EU\n6 | EU\n7 | EU
8 | NON_EU\n9 | NON_EU\n10 | NON_EU\n11 | NON_EU\n12 | NON_EU'
Q_REGION="SELECT cust_id, residency_region FROM $C ORDER BY cust_id"

section 1 "Catalog gate: GRANT is what enables both engines"
compare_equal carol "$Q_CUST" \
  "Admin control proves the shared register and fixture exist" "$RAW_CUST"
compare_denied alice "$Q_CUST" "Before GRANT, neither engine may load the register"
action "GRANT SELECT ON $C TO ROLE \"analyst\"" \
  "Grant SELECT on the register to analyst; Alice is a member"
compare_equal alice "$Q_CUST" \
  "The role grant exposes the same twelve raw customer rows" "$RAW_CUST"

section 2 "Role membership and write authority"
compare_denied dave "$Q_CUST" "Dave is in no role, so the analyst grant does not reach him"
INSERT_PROBE="INSERT INTO $C VALUES (9001,'Probe Row','999999999',DATE '2000-01-02',\
'NL00PROBE0000000000','NL','EU','AMS-01',false,false,0,'+31-20-555-9001')"
# #426 cell 1: the same denied INSERT in both engines, same fixture state.
# Spark refuses at ADD_TABLE_SNAPSHOT (files may already be staged). SQE must
# refuse too, not only the Spark side.
compare_write_denied alice "$INSERT_PROBE" "SELECT does not imply INSERT"
action "GRANT INSERT ON $C TO ROLE \"analyst\"" \
  "Grant INSERT separately to the analyst role"
action_as alice "$INSERT_PROBE" "Alice onboards a customer through SQE"
compare_equal alice "SELECT count(*) AS n FROM $C" \
  "Both engines see Alice's SQE commit" "13"
action "DELETE FROM $C WHERE cust_id = 9001" \
  "Carol removes the SQE probe customer (cross-user snapshot invalidation)" \
  '\| 1[[:space:]]+\|'
compare_equal alice "SELECT count(*) AS n FROM $C" \
  "Both engines see Carol's delete" "12"
spark_action_as alice "INSERT INTO $C VALUES (9002,'Spark Probe','888888888',\
DATE '2001-02-03','NL00SPARK0000000000','NL','EU','AMS-01',false,false,0,'+31-20-555-9002')" \
  "Alice onboards a customer through Spark with the same INSERT grant"
compare_equal alice "SELECT count(*) AS n FROM $C" \
  "Both engines see Alice's Spark commit" "13"
action "DELETE FROM $C WHERE cust_id = 9002" "Carol removes the Spark probe customer" \
  '\| 1[[:space:]]+\|'
compare_equal alice "SELECT count(*) AS n FROM $C" \
  "Both engines return to the twelve-customer fixture" "12"

section 3 "Resource column masks: five protected columns on one table"
action "GRANT SELECT ON $C TO ROLE \"engineer\"" "Grant engineer read access to the register"
action "CREATE OR REPLACE POLICY \"${POLICY_PREFIX}risk-null\" ON TABLE $C \
COLUMN MASK MASK_NULL TO ROLE engineer ON COLUMN risk_score" \
  "Hide the internal risk score with MASK_NULL through SQE SQL"
action "CREATE OR REPLACE POLICY \"${POLICY_PREFIX}nid-last4\" ON TABLE $C \
COLUMN MASK MASK_SHOW_LAST_4 TO ROLE engineer ON COLUMN national_id" \
  "Reduce the national identifier to its last four digits through SQE SQL"
action "CREATE OR REPLACE POLICY \"${POLICY_PREFIX}iban-hash\" ON TABLE $C \
COLUMN MASK MASK_HASH TO ROLE engineer ON COLUMN iban" \
  "Pseudonymise the IBAN with MASK_HASH through SQE SQL"
action "CREATE OR REPLACE POLICY \"${POLICY_PREFIX}dob-year\" ON TABLE $C \
COLUMN MASK MASK_DATE_SHOW_YEAR TO ROLE engineer ON COLUMN dob" \
  "Reduce the date of birth to its year through SQE SQL"
action "CREATE OR REPLACE POLICY \"${POLICY_PREFIX}name-mask\" ON TABLE $C \
COLUMN MASK MASK TO ROLE engineer ON COLUMN full_name" \
  "Replace the customer name character-by-character through SQE SQL"


# Named MASK_SHOW_LAST_4 deliberately is not byte-portable: SQE honors the Hive
# servicedef transformer, while Kyuubi uses its own character-class replacements
# (digit -> n). Two rows are enough to document the rendering; the aggregate
# below carries the security claim for all twelve.
#
# Both sides are confirmed live. SQE's is also derivable from source:
# ranger_store.rs maps MASK_SHOW_LAST_4 to PartialMask{show_last: 4, digit: 'x'},
# so nine digits render as xxxxx9103. Kyuubi's nnnnn9103 follows its own classes
# (digit -> n), which is the same rule that produced nnnUnnU1111 for the
# pre-bank separated identifier.
SQE_NAMED=$'1 | xxxxx9103\n2 | xxxxx0214'
SPARK_NAMED=$'1 | nnnnn9103\n2 | nnnnn0214'
compare_expected bob "SELECT cust_id, national_id FROM $C WHERE cust_id IN (1,2) ORDER BY cust_id" \
  "The last four digits survive in both engines" "$SQE_NAMED" "$SPARK_NAMED" \
  "same protected digits; documented named-mask rendering difference"
# MASK_HASH is the second rendering difference: SQE hashes with sha256, Kyuubi
# with md5. Comparing digest lengths documents it without pinning digests.
compare_expected bob "SELECT length(iban) AS iban_digest_len FROM $C WHERE cust_id = 1" \
  "MASK_HASH hides the account number in both engines with different digests" \
  "64" "32" \
  "no raw IBAN in either engine; SQE emits sha256, Kyuubi emits md5"
# One query, five masks, and no dropped rows. Every predicate is rendering-blind,
# so the same expectation is correct for both engines.
compare_equal bob "SELECT count(*) AS rows_seen, \
sum(CASE WHEN risk_score IS NULL THEN 1 ELSE 0 END) AS score_nulled, \
sum(CASE WHEN $NID_LEAK THEN 1 ELSE 0 END) AS id_leaks, \
sum(CASE WHEN $IBAN_LEAK THEN 1 ELSE 0 END) AS iban_leaks, \
sum(CASE WHEN $NAME_LEAK THEN 1 ELSE 0 END) AS name_leaks, \
sum(CASE WHEN $DOB_LEAK THEN 1 ELSE 0 END) AS dob_leaks, \
sum(CASE WHEN $DOB_YEAR_ONLY THEN 1 ELSE 0 END) AS dob_year_only FROM $C" \
  "All five resource masks apply without dropping rows" "12 | 12 | 0 | 0 | 0 | 0 | 12"

# What those five statements actually did, on real rows. The aggregate above
# proves "no identifier leaked"; this shows a reviewer the values themselves.
# Placed after that assertion on purpose: by here the policies are known to be
# live in both engines, so the panel cannot display a pre-settle state.
perspectives "All five masks on the same three customers" \
  "SELECT cust_id, full_name, national_id, dob, iban, risk_score FROM $C WHERE cust_id <= 3 ORDER BY cust_id" \
  "alice|no mask policy names her (the control)" \
  "bob|engineer: name, national id, IBAN, date of birth, risk score"
# The same query for a role no mask policy names. Every column flips.
compare_equal alice "SELECT count(*) AS rows_seen, \
sum(CASE WHEN risk_score IS NULL THEN 1 ELSE 0 END) AS score_nulled, \
sum(CASE WHEN $NID_LEAK THEN 1 ELSE 0 END) AS id_leaks, \
sum(CASE WHEN $IBAN_LEAK THEN 1 ELSE 0 END) AS iban_leaks, \
sum(CASE WHEN $NAME_LEAK THEN 1 ELSE 0 END) AS name_leaks, \
sum(CASE WHEN $DOB_LEAK THEN 1 ELSE 0 END) AS dob_leaks, \
sum(CASE WHEN $DOB_YEAR_ONLY THEN 1 ELSE 0 END) AS dob_year_only FROM $C" \
  "Alice is outside engineer and remains the raw-value control" \
  "12 | 0 | 12 | 12 | 12 | 12 | 0"

# #426 cell 2: ADD COLUMN on a table that already has column masks.
# SQE used to become unqueryable (scan schema vs mask projection). That is
# fixed and guarded in spark_mask_parity_e2e. Spark was never run. Add the
# column through each engine in turn and assert the masked SELECT still
# returns twelve rows, twelve NULLs in the new column, and no national-id leak.
action "ALTER TABLE $C ADD COLUMN acparity_nick VARCHAR" \
  "Add a nullable column to the already-masked register through SQE"
compare_equal bob "SELECT count(*) AS rows_seen, \
sum(CASE WHEN acparity_nick IS NULL THEN 1 ELSE 0 END) AS nick_nulls, \
sum(CASE WHEN $NID_LEAK THEN 1 ELSE 0 END) AS id_leaks FROM $C" \
  "Masked SELECT stays queryable after SQE ADD COLUMN" "12 | 12 | 0"
action "ALTER TABLE $C DROP COLUMN acparity_nick" \
  "Remove the SQE-added column before the Spark-authored ADD"
spark_best_effort_action_as carol "ALTER TABLE $C ADD COLUMN acparity_nick STRING" \
  "Add the same column through Spark on the masked table"
compare_equal bob "SELECT count(*) AS rows_seen, \
sum(CASE WHEN acparity_nick IS NULL THEN 1 ELSE 0 END) AS nick_nulls, \
sum(CASE WHEN $NID_LEAK THEN 1 ELSE 0 END) AS id_leaks FROM $C" \
  "Masked SELECT stays queryable after Spark ADD COLUMN" "12 | 12 | 0"
best_effort_action "ALTER TABLE $C DROP COLUMN acparity_nick" \
  "Drop the Spark-added column so later sections see the original schema"

section 4 "Row filtering: GDPR data residency"
action "CREATE OR REPLACE POLICY \"${POLICY_PREFIX}eu-rows\" ON TABLE $C \
ROW FILTER TO ROLE engineer USING (residency_region = 'EU')" \
  "Restrict engineer to EU-resident customers through SQE SQL"
compare_equal bob "$Q_REGION" \
  "Bob sees only EU-resident customers through both engines" "$EU_ONLY"
compare_equal alice "$Q_REGION" \
  "Alice remains unfiltered in both engines" "$ALL_REGIONS"

section 5 "Tag masking and policy composition"
action "ALTER TABLE $C SET TAGS (phone = ('$TAG_NAME'))" \
  "Tag phone in Iceberg and project the association to Ranger for Spark"
sqe_assert ok "SHOW TAGS ON $C" "Read the tag association back" "$TAG_NAME"
action "CREATE OR REPLACE POLICY \"${POLICY_PREFIX}pii-tag\" ON TAG $TAG_NAME \
COLUMN MASK CUSTOM TO ROLE engineer USING ('XX')" \
  "Create a portable tag mask through SQE SQL"
compare_equal bob "SELECT cust_id, phone FROM $C ORDER BY cust_id" \
  "The tag rule masks Bob's filtered rows identically" \
  $'1 | XX\n2 | XX\n3 | XX\n4 | XX\n5 | XX\n6 | XX\n7 | XX'
compare_equal alice "SELECT cust_id, phone FROM $C WHERE cust_id <= 3 ORDER BY cust_id" \
  "The role-scoped tag rule leaves Alice raw" \
  $'1 | +31-20-555-0101\n2 | +48-22-555-0102\n3 | +39-02-555-0103'
compare_equal bob "SELECT count(*) AS rows_seen, \
sum(CASE WHEN phone = 'XX' THEN 1 ELSE 0 END) AS tag_masked, \
sum(CASE WHEN risk_score IS NULL THEN 1 ELSE 0 END) AS score_nulled, \
sum(CASE WHEN $NID_LEAK THEN 1 ELSE 0 END) AS id_leaks, \
sum(CASE WHEN $IBAN_LEAK THEN 1 ELSE 0 END) AS iban_leaks, \
sum(CASE WHEN $NAME_LEAK THEN 1 ELSE 0 END) AS name_leaks FROM $C" \
  "A row filter, five resource masks, and a tag mask compose in one plan" \
  "7 | 7 | 7 | 0 | 0 | 0"

section 5a "Resource and tag precedence"
# This section used to assert a divergence. SQE resolved a contested column to
# the resource mask (most-specific-rule-wins) while Kyuubi resolved it to the
# tag mask (the standard Ranger plugin order). `policy.mask-precedence` now
# defaults to `tag`, so one policy set renders one value in both engines. Set it
# to `resource` to get the old behaviour back, and this step diverges again.
action "ALTER TABLE $C SET TAGS (national_id = ('$NULL_TAG'))" \
  "Apply a second tag to the already resource-masked national_id column"
action "CREATE OR REPLACE POLICY \"${POLICY_PREFIX}nid-null-tag\" ON TAG $NULL_TAG \
COLUMN MASK MASK_NULL TO ROLE engineer" \
  "Create the competing tag MASK_NULL through SQE SQL"
compare_equal bob "SELECT cust_id, national_id FROM $C ORDER BY cust_id" \
  "A contested column resolves to the tag mask in both engines" \
  $'1 | <NULL>\n2 | <NULL>\n3 | <NULL>\n4 | <NULL>\n5 | <NULL>\n6 | <NULL>\n7 | <NULL>'
compare_equal alice "SELECT count(*) AS rows_seen, \
sum(CASE WHEN $NID_LEAK THEN 1 ELSE 0 END) AS raw_ids, \
sum(CASE WHEN national_id IS NULL THEN 1 ELSE 0 END) AS null_ids FROM $C" \
  "Both masks remain role-scoped for Alice" "12 | 12 | 0"
action "ALTER TABLE $C UNSET TAGS (national_id)" "Remove the precedence-test tag"

section 5b "Row filter reading a tag-masked column"
# Tagging the column the row filter reads puts the two rules on a collision
# course, and the engines resolve the ordering differently. SQE evaluates the
# filter against stored values and masks the surviving rows. Kyuubi injects its
# masking Project *below* RowFilterMarker, so `residency_region = 'EU'` is
# compared with the masked literal 'XX' and matches nothing. The count makes the
# divergence a value rather than an empty result set, which an error would also
# produce.
action "ALTER TABLE $C SET TAGS (residency_region = ('$TAG_NAME'))" \
  "Also tag the column the residency row filter reads"
compare_expected bob "SELECT count(*) AS n FROM $C" \
  "Filter-then-mask versus mask-then-filter changes the row count" \
  "7" "0" \
  "SQE filters raw values then masks; Kyuubi masks below the row filter, so Bob sees no rows"
action "ALTER TABLE $C UNSET TAGS (residency_region)" \
  "Remove the overlapping residency tag"
compare_equal bob "SELECT count(*) AS n FROM $C" \
  "Both engines agree again once the collision is removed" "7"

section 5c "Inert tags and SQL validation"
action "ALTER TABLE $C UNSET TAGS (phone)" "Remove the governed PII tag"
action "ALTER TABLE $C SET TAGS (residency_region = ('$NO_RULE_TAG'))" \
  "Attach a tag for which no policy exists"
sqe_assert ok "SHOW TAGS ON $C" "Confirm only the no-rule residency tag remains" \
  "$NO_RULE_TAG" "$TAG_NAME"
compare_equal bob "$Q_REGION" \
  "A tag without a policy is inert in both engines" "$EU_ONLY"
sqe_assert error "CREATE OR REPLACE POLICY \"${POLICY_PREFIX}broken\" ON TAG $NO_RULE_TAG \
COLUMN MASK CUSTOM TO ROLE engineer" \
  "Reject CUSTOM without its required USING expression" \
  'CUSTOM COLUMN MASK requires USING'
compare_equal bob "$Q_REGION" \
  "The rejected policy changed neither engine" "$EU_ONLY"
compare_equal alice "$Q_REGION" "Alice remains unaffected" "$ALL_REGIONS"
action "ALTER TABLE $C UNSET TAGS (residency_region)" "Remove the inert tag"

section 6 "SQE policy introspection"
sqe_assert ok "SHOW GRANTS ON $C" "List the Ranger grants SQE authored" \
  'table-data-read.*ROLE.*analyst'
sqe_assert ok "CHECK ACCESS SELECT ON $C FOR USER \"alice\"" \
  "Explain Alice's role-derived access" 'true.*Allowed via ROLE' 'false'
sqe_assert ok "CHECK ACCESS SELECT ON $C FOR USER \"dave\"" \
  "Explain Dave's missing access" 'false.*No matching grant' 'true'

section 7 "Views"
action "CREATE OR REPLACE VIEW $VIEW AS \
SELECT cust_id, residency_region FROM $C WHERE residency_region = 'EU'" \
  "Create an EU-resident view through SQE"
action "GRANT SELECT ON VIEW $VIEW TO ROLE \"analyst\"" \
  "Grant the view name to analyst"
sqe_assert ok "SHOW GRANTS ON $VIEW" \
  "Verify that GRANT ON VIEW wrote view access types" \
  'view-properties-read' 'table-data-read'
compare_equal alice "SELECT cust_id, residency_region FROM $VIEW ORDER BY cust_id" \
  "Both engines expand the view and still authorize its base table" "$EU_ONLY"

section 8 "Data minimisation for the fraud desk"
# The fraud desk is the shape analyst/engineer cannot express: it must see EVERY
# jurisdiction (no row filter) while seeing NO customer identity. Two tags carry
# that, and one of them spans both tables, so the rule is authored once and
# lands wherever the tag is attached.
#
# WHY A SEPARATE TAG rather than a second policy on ACPARITY_PII: Ranger refuses
# a policy whose resource signature already belongs to another policy
# ("Another policy already exists for matching resource"), so one tag cannot
# carry two different masks for two different roles.
action "GRANT SELECT ON $C TO ROLE \"fraud_analyst\"" \
  "Grant the fraud desk read access to the register"
action "GRANT SELECT ON $P TO ROLE \"fraud_analyst\"" \
  "Grant the fraud desk read access to the ledger"
action "GRANT SELECT ON $P TO ROLE \"analyst\"" \
  "Grant analyst read access to the ledger as the raw control"
action "ALTER TABLE $C SET TAGS (phone = ('$ACCOUNT_TAG'), nationality = ('$SPI_TAG'), \
national_id = ('$IDENTITY_TAG'), full_name = ('$IDENTITY_TAG'))" \
  "Tag the register: contact detail as an account identifier, nationality as special category, name and national id as direct identifiers"
action "ALTER TABLE $P SET TAGS (counterparty_iban = ('$ACCOUNT_TAG'))" \
  "Tag the counterparty account in the ledger with the SAME tag"
sqe_assert ok "SHOW TAGS ON $P" "Read the ledger tag association back" "$ACCOUNT_TAG"
action "CREATE OR REPLACE POLICY \"${POLICY_PREFIX}account-tag-fraud\" ON TAG $ACCOUNT_TAG \
COLUMN MASK CUSTOM TO ROLE fraud_analyst USING ('REDACTED')" \
  "One tag rule for account identifiers, authored once for both tables"
action "CREATE OR REPLACE POLICY \"${POLICY_PREFIX}spi-tag-fraud\" ON TAG $SPI_TAG \
COLUMN MASK MASK_NULL TO ROLE fraud_analyst" \
  "Null special-category data for the fraud desk"
# A separate tag rather than reusing SPI: Ranger refuses a second policy whose
# resource signature already belongs to one, and a name is a direct identifier
# rather than article 9 special-category data. Same mask, different reason, and
# the reason is what an auditor asks about.
action "CREATE OR REPLACE POLICY \"${POLICY_PREFIX}identity-tag-fraud\" ON TAG $IDENTITY_TAG \
COLUMN MASK MASK_NULL TO ROLE fraud_analyst" \
  "Null direct identifiers for the fraud desk"
# score_visible proves role scoping in the other direction: the engineer's
# MASK_NULL on risk_score must NOT reach Erin, who needs the score to work.
compare_equal erin "SELECT count(*) AS rows_seen, \
sum(CASE WHEN phone = 'REDACTED' THEN 1 ELSE 0 END) AS account_masked, \
sum(CASE WHEN nationality IS NULL THEN 1 ELSE 0 END) AS nationality_nulled, \
sum(CASE WHEN national_id IS NULL THEN 1 ELSE 0 END) AS id_nulled, \
sum(CASE WHEN full_name IS NULL THEN 1 ELSE 0 END) AS name_nulled, \
sum(CASE WHEN risk_score IS NULL THEN 1 ELSE 0 END) AS score_hidden FROM $C" \
  "The fraud desk keeps every jurisdiction and loses every identifier" \
  "12 | 12 | 12 | 12 | 12 | 0"
# The claim under test is that a tag policy is table-independent. Confirmed live
# at 24 | 24 | 4 in both engines. Were tag projection to reach only the register,
# account_masked would come back 0 here while the register probe above still
# passed, which is why the two are separate steps.
compare_equal erin "SELECT count(*) AS rows_seen, \
sum(CASE WHEN counterparty_iban = 'REDACTED' THEN 1 ELSE 0 END) AS account_masked, \
sum(CASE WHEN aml_alert THEN 1 ELSE 0 END) AS alerts_visible FROM $P" \
  "The same tag rule reaches the second table, and the AML signal survives" \
  "24 | 24 | 4"
compare_equal alice "SELECT count(*) AS rows_seen, \
sum(CASE WHEN counterparty_iban = 'REDACTED' THEN 1 ELSE 0 END) AS account_masked \
FROM $P" \
  "The tag rule is role-scoped: Alice reads the ledger raw" "24 | 0"

section 9 "Audit right of access with a retention window"
# The auditor is the mirror image of the fraud desk: no mask at all on the
# register, and a single restriction on the ledger. It also proves a filter is
# scoped to the table its policy names, which a one-table fixture cannot show.
action "GRANT SELECT ON $C TO ROLE \"auditor\"" \
  "Grant audit read access to the register"
action "GRANT SELECT ON $P TO ROLE \"auditor\"" \
  "Grant audit read access to the ledger"
action "GRANT SELECT ON $P TO ROLE \"engineer\"" \
  "Grant engineer read access to the ledger for the join probe"
action "CREATE OR REPLACE POLICY \"${POLICY_PREFIX}retention-rows\" ON TABLE $P \
ROW FILTER TO ROLE auditor USING (booked_at >= DATE '2019-01-01')" \
  "Limit audit to the retention window through SQE SQL"
compare_equal frank "SELECT count(*) AS rows_seen, \
sum(CASE WHEN $NID_LEAK THEN 1 ELSE 0 END) AS raw_ids, \
sum(CASE WHEN phone = 'REDACTED' THEN 1 ELSE 0 END) AS fraud_mask_reached, \
sum(CASE WHEN risk_score IS NULL THEN 1 ELSE 0 END) AS engineer_mask_reached FROM $C" \
  "Audit reads the register unmasked; no other role's mask reaches it" \
  "12 | 12 | 0 | 0"
# booked_at is projected inside the CASE deliberately: Kyuubi Spark 3.5 raises
# MISSING_ATTRIBUTES (#6889) when a row filter reads a column the query does not
# project.
compare_equal frank "SELECT count(*) AS rows_seen, \
sum(CASE WHEN booked_at < DATE '2019-01-01' THEN 1 ELSE 0 END) AS before_window FROM $P" \
  "The retention window drops the six pre-2019 payments in both engines" "18 | 0"
compare_equal alice "SELECT count(*) AS rows_seen, \
sum(CASE WHEN booked_at < DATE '2019-01-01' THEN 1 ELSE 0 END) AS before_window FROM $P" \
  "The retention filter is role-scoped: Alice still sees the full ledger" "24 | 6"
# The join is what an analyst actually writes. Bob's residency filter and his
# five customer-side masks both have to survive it, and the payment side has to
# stay whole. 15 is the number of seeded payments belonging to an EU-resident
# customer (cust_id 1-7).
# The open question was never the count but whether Kyuubi applies a row filter
# and a column mask to a JOINED relation the way SQE does. Confirmed live at
# 15 | 15 | 15 | 0 in both engines. If either masked or filtered only one side,
# joined_rows or score_masked would move.
compare_equal bob "SELECT count(*) AS joined_rows, \
sum(CASE WHEN c.residency_region = 'EU' THEN 1 ELSE 0 END) AS eu_rows, \
sum(CASE WHEN c.risk_score IS NULL THEN 1 ELSE 0 END) AS score_masked, \
sum(CASE WHEN c.national_id IS NOT NULL AND c.national_id NOT LIKE '%x%' \
AND c.national_id NOT LIKE '%n%' THEN 1 ELSE 0 END) AS id_leaks \
FROM $C c JOIN $P p ON p.cust_id = c.cust_id" \
  "Row filter and column masks both survive a join in both engines" \
  "15 | 15 | 15 | 0"

# The whole demo in one frame: one join, four personas, four different answers.
# Nothing about the SQL changes between these runs. Only who asked.
#
#   carol  sqe_admin AND engineer AND analyst in Keycloak, so the engineer
#          policy applies to her too. Being an admin at the OBJECT gate is not
#          an exemption from the DATA gate, and her row proves it: she reads the
#          same four masked EU rows Bob does. Measured, not assumed; the first
#          version of this panel called her "every row, every column" and the
#          live run contradicted it.
#   alice  analyst: unfiltered, unmasked
#   bob    engineer: EU customers only, five columns masked
#   erin   fraud desk: every jurisdiction, identity and counterparty removed
#   frank  audit: unmasked register, payments inside the retention window only
perspectives "The same join, five ways" \
  "SELECT c.cust_id, c.full_name, c.national_id, c.residency_region, c.risk_score, \
p.booked_at, p.counterparty_iban, p.counterparty_country \
FROM $C c JOIN $P p ON p.cust_id = c.cust_id WHERE p.amount_eur > 5000 ORDER BY c.cust_id, p.pay_id" \
  "carol|admin, but also in engineer: masked and filtered like Bob" \
  "alice|analyst: unfiltered and unmasked" \
  "bob|engineer: EU residents only, five masked columns" \
  "erin|fraud desk: all jurisdictions, no identity" \
  "frank|audit: retention window on the ledger only"

section 10 "REVOKE closes the catalog gate again"
# Closing the gate takes three revokes, and the third is the interesting one.
# Bob holds BOTH demo roles in Keycloak, so revoking `engineer` leaves the
# section-1 analyst grant carrying him. Revoking analyst SELECT is still not
# enough: grant-profile.json expands `table-data-write` to include
# `table-data-read`, because a writer that cannot read its own table is useless.
# So the INSERT granted back in section 2 keeps conferring read, and
# `REVOKE SELECT` reports success while the row still comes back. Read access
# ends when the last privilege implying it is gone.
action "REVOKE SELECT ON $C FROM ROLE \"engineer\"" \
  "Revoke engineer SELECT on the register"
action "REVOKE SELECT ON $C FROM ROLE \"analyst\"" \
  "Revoke Bob's second path to the register"
action "REVOKE INSERT ON $C FROM ROLE \"analyst\"" \
  "Revoke the INSERT whose table-data-write still implies table-data-read"
# Ask the engine whether the gate is shut before asking a query to prove it.
# When this step first failed it did so as 120 seconds of "waiting for the
# revoke to reach Polaris", because a surviving grant and a slow poll look
# identical from the outside. CHECK ACCESS distinguishes them in one call, and
# names the grant still carrying the user.
sqe_assert ok "CHECK ACCESS SELECT ON $C FOR USER \"bob\"" \
  "Confirm no grant path is left before asserting the denial" 'false' 'true'
compare_denied bob "$Q_CUST" "After REVOKE, both engines deny Bob again"

best_effort_action "ALTER TABLE $C DROP COLUMN acparity_nick" \
  "Security teardown: drop the #426 ADD COLUMN probe if still present"
action "ALTER TABLE $C UNSET TAGS (phone, residency_region, national_id, nationality, full_name)" \
  "Security teardown: remove projected register tag associations"
action "ALTER TABLE $P UNSET TAGS (counterparty_iban)" \
  "Security teardown: remove projected ledger tag associations"
for policy in $POLICIES; do
  action "DROP POLICY IF EXISTS \"${POLICY_PREFIX}${policy}\"" \
    "Security teardown: remove SQL-managed policy"
done
for role in engineer analyst fraud_analyst auditor; do
  for tbl in "$C" "$P"; do
    action "REVOKE SELECT ON $tbl FROM ROLE \"$role\"" \
      "Security teardown: revoke $role SELECT on ${tbl##*.}"
    action "REVOKE INSERT ON $tbl FROM ROLE \"$role\"" \
      "Security teardown: revoke $role INSERT on ${tbl##*.}"
  done
done

echo
bold "─────────────────────────────────────────────"
if [ "$FAIL" -eq 0 ]; then
  green "all $PASS cross-engine comparisons behaved as documented"
else
  red "$FAIL of $((PASS+FAIL)) cross-engine comparisons failed"
fi
bold "─────────────────────────────────────────────"
dim "shared tables left in place: $C and $P"
dim "stack left running; tear down with: (cd $STACK_DIR && docker compose down)"

[ "$FAIL" -eq 0 ]
