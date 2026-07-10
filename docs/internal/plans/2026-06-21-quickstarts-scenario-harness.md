# Quickstarts Scenario Harness + Docs Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Implement on branch `docs/quickstart-scenario-harness` in worktree `/Users/jjverhoeks/sqe-c-wt` (NOT the primary checkout). Steps use checkbox (`- [ ]`) tracking.

**Goal:** Make every quickstart both an asserted test scenario and a good documented starting point, unified under a two-tier harness with one entry point, wired into CI.

**Architecture:** Tier 1 = existing cargo engine tests (`integration-test.sh`). Tier 2 = quickstart `run.sh --check` scenarios using shared bash assert helpers (promoted from `distributed-test.sh`), driven by one entry point `scripts/test.sh`. Each quickstart's README is brought to a gold-standard what/how/why/configs structure. `OUTPUT.md` is the shared evidence bridging docs and tests.

**Tech Stack:** bash, docker compose, sqe-cli (Flight + Trino HTTP), cargo, GitLab CI (docker-in-docker), mdBook.

**Spec:** `docs/internal/specs/2026-06-21-quickstarts-scenario-harness-design.md`

**Worktree note:** All work in `/Users/jjverhoeks/sqe-c-wt`. Confirm `git branch --show-current` = `docs/quickstart-scenario-harness` at the start of every task.

---

## Phase 1: Shared assert helpers + reference --check + entry point

### Task 1: Add assert helpers to `quickstart/_shared/lib.sh`

**Files:**
- Modify: `quickstart/_shared/lib.sh` (append, after the `cap()` function)

- [ ] **Step 1: Append the assert helpers + summary**

Add to the end of `quickstart/_shared/lib.sh`:
```bash
# --- Assertions (for run.sh --check mode) -----------------------------------
# Promoted from scripts/distributed-test.sh so quickstarts + distributed share
# one assertion vocabulary. Counters are global; call check_summary at the end.
CHECK_PASS=0
CHECK_FAIL=0

# assert_contains <label> <output> <expected-substring>  (case-insensitive)
assert_contains() {
  local label=$1 output=$2 expected=$3
  if printf '%s' "$output" | grep -qi -- "$expected"; then
    ok "$label"; CHECK_PASS=$((CHECK_PASS + 1))
  else
    warn "$label (expected to contain '$expected')"
    printf '       got: %s\n' "$(printf '%s' "$output" | head -3)"
    CHECK_FAIL=$((CHECK_FAIL + 1))
  fi
}

# assert_not_empty <label> <output>  (non-empty and not "0 rows")
assert_not_empty() {
  local label=$1 output=$2
  if [ -n "$output" ] && ! printf '%s' "$output" | grep -q "0 rows"; then
    ok "$label"; CHECK_PASS=$((CHECK_PASS + 1))
  else
    warn "$label (empty result)"; CHECK_FAIL=$((CHECK_FAIL + 1))
  fi
}

# assert_not_contains <label> <output> <forbidden-substring>
assert_not_contains() {
  local label=$1 output=$2 forbidden=$3
  if printf '%s' "$output" | grep -qi -- "$forbidden"; then
    warn "$label (should NOT contain '$forbidden')"
    printf '       got: %s\n' "$(printf '%s' "$output" | head -3)"
    CHECK_FAIL=$((CHECK_FAIL + 1))
  else
    ok "$label"; CHECK_PASS=$((CHECK_PASS + 1))
  fi
}

# check_summary — print totals and exit non-zero if any assertion failed.
check_summary() {
  echo
  step "checks: ${CHECK_PASS} passed, ${CHECK_FAIL} failed"
  [ "$CHECK_FAIL" -eq 0 ] || die "${CHECK_FAIL} assertion(s) failed"
  ok "all checks passed"
}
```

- [ ] **Step 2: Syntax-check**

Run: `bash -n quickstart/_shared/lib.sh && echo OK`
Expected: `OK`.

- [ ] **Step 3: Commit**

```bash
git add quickstart/_shared/lib.sh
git commit -m "test(quickstart): add shared assert helpers to _shared/lib.sh"
```

### Task 2: Add `scripts/test.sh` unified entry point

**Files:**
- Create: `scripts/test.sh`

- [ ] **Step 1: Write the entry point**

Create `scripts/test.sh`:
```bash
#!/usr/bin/env bash
# Unified SQE test entry point. Two tiers:
#   engine    Tier 1 - cargo integration tests (delegates to integration-test.sh)
#   scenario  Tier 2 - quickstart run.sh --check scenarios
#   ci        Tier 1 + the self-contained Tier-2 scenarios (what CI runs)
#
#   scripts/test.sh engine
#   scripts/test.sh scenario <name>|all
#   scripts/test.sh ci
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

# Self-contained scenarios (no cloud creds). AWS scenarios are gated separately.
SELF_CONTAINED=(polaris-keycloak-client-id polaris-keycloak-user-token \
  polaris-ranger-keycloak nessie unity-oss embedded-files \
  embedded-sqlite-catalog attach-catalogs quack observability benchmark)

run_scenario() {
  local name=$1 dir="$ROOT_DIR/quickstart/$1"
  [ -d "$dir" ] || { echo "no such scenario: $name" >&2; return 2; }
  echo "━━━ scenario: $name ━━━"
  ( cd "$dir" && ./run.sh --check )
}

cmd=${1:-ci}
case "$cmd" in
  engine)
    "$ROOT_DIR/scripts/integration-test.sh" "${@:2}" ;;
  scenario)
    target=${2:-all}
    if [ "$target" = all ]; then
      rc=0
      for s in "${SELF_CONTAINED[@]}"; do run_scenario "$s" || rc=1; done
      exit $rc
    else
      run_scenario "$target"
    fi ;;
  ci)
    "$ROOT_DIR/scripts/integration-test.sh"
    rc=0
    for s in "${SELF_CONTAINED[@]}"; do run_scenario "$s" || rc=1; done
    exit $rc ;;
  *) echo "usage: test.sh {engine|scenario <name>|all|ci}" >&2; exit 2 ;;
esac
```

- [ ] **Step 2: Make executable + syntax-check**

Run: `chmod +x scripts/test.sh && bash -n scripts/test.sh && echo OK`
Expected: `OK`.

- [ ] **Step 3: Commit**

```bash
git add scripts/test.sh
git commit -m "test: add unified scripts/test.sh entry point (engine/scenario/ci)"
```

### Task 3: Add `--check` to two reference quickstarts (worked examples)

**Files:**
- Modify: `quickstart/embedded-files/run.sh`, `quickstart/polaris-keycloak-client-id/run.sh`

These are the pattern other quickstarts copy. Read each `run.sh` + its `queries.sql` + `OUTPUT.md` first to derive exact expected values.

- [ ] **Step 1: embedded-files `--check`**

In `quickstart/embedded-files/run.sh`: it already parses args and runs queries via the CLI in `--embedded` mode. Add a `--check` flag to the arg loop (alongside any existing flags) that sets `CHECK=1`, and after the queries run, add:
```bash
if [ "${CHECK:-0}" = 1 ]; then
  step "checking invariants"
  # read_parquet over the sample file returns rows (derive expected count from queries.sql / OUTPUT.md)
  out=$(cap_query "SELECT count(*) AS n FROM read_parquet('${SAMPLE_PARQUET:-/data/sales.parquet}')")
  assert_not_empty "read_parquet returns rows" "$out"
  assert_not_contains "no query error" "$out" "error"
  check_summary
fi
```
Adapt `cap_query` to however this quickstart invokes the CLI (reuse its existing query-running function/inline `sqe-cli ... -e`). The exact table/file/count: read `quickstart/embedded-files/queries.sql` and assert on a real query from it.

- [ ] **Step 2: polaris-keycloak-client-id `--check`**

This scenario runs demo queries as two users (admin + restricted). Its `run.sh` already has `--with-tests`; add `--check`. After the captured queries, assert the security invariants that define the scenario:
```bash
if [ "${CHECK:-0}" = 1 ]; then
  step "checking invariants"
  admin=$(run_user_query "$ADMIN_USER" "SELECT count(*) FROM <demo_table>")
  assert_not_empty "admin can read the table" "$admin"
  restricted=$(run_user_query "$RESTRICTED_USER" "SELECT <masked_col> FROM <demo_table> LIMIT 1")
  assert_contains "restricted user sees masked/empty value" "$restricted" "<expected mask marker>"
  check_summary
fi
```
Read this quickstart's `README.md` "Output" + `OUTPUT.md` + `queries.sql` to fill `<demo_table>`, `<masked_col>`, the user vars, and the exact mask marker (e.g. `***` or NULL). Reuse the script's existing per-user CLI invocation.

- [ ] **Step 3: Run both `--check` against a live stack**

Run:
```bash
( cd quickstart/embedded-files && ./run.sh --check )
( cd quickstart/polaris-keycloak-client-id && ./run.sh --down && ./run.sh --check )
```
Expected: each ends with `all checks passed`. (embedded-files needs no server stack; polaris needs docker.)

- [ ] **Step 4: Prove the asserts bite**

Temporarily change one expected value to something wrong, re-run `--check`, confirm it prints a failed assertion and exits non-zero, then revert.

- [ ] **Step 5: Commit**

```bash
git add quickstart/embedded-files/run.sh quickstart/polaris-keycloak-client-id/run.sh
git commit -m "test(quickstart): --check assertions for embedded-files + polaris-keycloak-client-id (reference)"
```

---

## Phase 2: Roll --check across all self-contained scenarios + fold distributed-test

### Task 4: Add `--check` to the remaining self-contained quickstarts

**Files:** Modify `run.sh` in: `polaris-keycloak-user-token`, `polaris-ranger-keycloak`, `nessie`, `unity-oss`, `embedded-sqlite-catalog`, `attach-catalogs`, `quack`, `observability`, `benchmark`.

For EACH: read its `run.sh` + `queries.sql` + `OUTPUT.md`, then add the `--check` flag + an invariants block ending in `check_summary`, asserting the invariant intents below. Derive exact expected strings from that scenario's real output.

| Quickstart | `--check` invariant intent |
|---|---|
| `polaris-keycloak-user-token` | a query as the pre-minted-token user returns rows; bad/no token is rejected |
| `polaris-ranger-keycloak` | masked column shows the Ranger mask for the restricted user; admin sees raw; (this is the Spark-parity stack) |
| `nessie` | a table created/seeded in the Nessie catalog is listed + readable |
| `unity-oss` | the seeded `unity.default.marksheet_uniform` (or current seed) lists + selects rows |
| `embedded-sqlite-catalog` | a table persisted to the SQLite catalog survives a CLI restart (reload + select rows) |
| `attach-catalogs` | a cross-catalog JOIN across two attached catalogs returns rows |
| `quack` | a DuckDB Quack round-trip returns the expected value both directions |
| `observability` | Prometheus metrics endpoint exposes `sqe_query_count_total` (or current metric) after a query |
| `benchmark` | a small-scale load+run reports pass count == total (no failed queries) |

- [ ] **Step 1 (repeat per quickstart): add `--check` + invariants, run `./run.sh --down && ./run.sh --check`, confirm `all checks passed`.** For each, also do the break-it/revert sanity check once.

- [ ] **Step 2: Run the whole set via the entry point**

Run: `scripts/test.sh scenario all`
Expected: every self-contained scenario ends `all checks passed`; overall exit 0.

- [ ] **Step 3: Commit**

```bash
git add quickstart/*/run.sh
git commit -m "test(quickstart): --check assertions across all self-contained scenarios"
```

### Task 5: Fold `distributed-test.sh` into the scenario model

**Files:**
- Create: `quickstart/distributed/` (README.md, run.sh, references `docker-compose.distributed.yml` + `bootstrap-distributed.sh` at repo root) OR adapt `distributed-test.sh` to source `_shared/lib.sh` and run under the entry point. Decide per the spec open item; default: a `quickstart/distributed/` scenario reusing the root distributed compose.
- Modify/Delete: `scripts/distributed-test.sh` (retire once its assertions move)
- Modify: `scripts/test.sh` (add `distributed` to a cloud/heavy group if not in SELF_CONTAINED, or include it)

- [ ] **Step 1: Port the distributed assertions** (connectivity, `system.runtime.nodes` lists the worker, `system.runtime.queries` shows FINISHED, CTAS round-trip, result-cache hit, Trino HTTP) into a `run.sh --check` using the shared helpers. Reuse `run_sql`/`run_sql_trino` (move them into the scenario or `_shared/lib.sh` as `cli_query`/`trino_query`).
- [ ] **Step 2: Run it**: bring up `docker-compose.distributed.yml` + bootstrap, then `./run.sh --check` (or `scripts/test.sh scenario distributed`). Expected: `all checks passed`.
- [ ] **Step 3: Retire `scripts/distributed-test.sh`** (`git rm`) once parity is confirmed; update any references (README/testing docs/CI).
- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "test(quickstart): fold distributed-test into a distributed scenario; retire distributed-test.sh"
```

---

## Phase 3: CI wiring

### Task 6: Add the `scenario-test` CI job (+ gated AWS job)

**Files:**
- Modify: `.gitlab-ci.yml`

Mirror the existing `integration-test` job (docker-in-docker, Alpine toolchain). Read that job first for the exact `image`, `services`, `before_script` toolchain install, and `rules`.

- [ ] **Step 1: Add `scenario-test`** in the `test` stage, same docker-in-docker setup as `integration-test`, with `script: ./scripts/test.sh scenario all` (self-contained set). Set `rules` to run on changes to `quickstart/**`, `crates/**`, `scripts/test.sh`, or the compose files, plus scheduled pipelines. Allow a reasonable timeout (each scenario builds/pulls images).
- [ ] **Step 2: Add `scenario-test-aws`** (manual + scheduled, `when: manual`) that sets `RUN_AWS_SCENARIOS=1` and runs the AWS scenarios, gated on AWS secret CI variables; `allow_failure: false` only when creds present. Document that it is opt-in.
- [ ] **Step 3: Lint** `.gitlab-ci.yml` (`glab ci lint` if available, else YAML parse). Expected: valid.
- [ ] **Step 4: Commit**

```bash
git add .gitlab-ci.yml
git commit -m "ci: add scenario-test job (self-contained quickstarts) + gated AWS scenario job"
```

---

## Phase 4: Documentation

### Task 7: Upgrade every quickstart README to the gold standard

**Files:** Modify `README.md` in each quickstart below the bar: `attach-catalogs`, `polaris-ranger-keycloak`, `observability`, `aws-s3-tables`, `embedded-sqlite-catalog`, and any other under ~120 lines. Reference: `quickstart/polaris-keycloak-client-id/README.md`.

- [ ] **Step 1 (per README):** ensure these sections exist and are substantive: frontmatter (slug/title/description); **What you get** (incl. the use-case/why); **Prerequisites**; **Run it** (`./run.sh`, `--down`, `--check`); **How it works** (the flow that matters); **Configuration explained** (annotate EVERY config file the scenario uses: `sqe.toml`/CLI flags, `.env(.example)`, `docker-compose.yml`, relevant `_shared` assets); **Output**; **How it is tested** (the `--check` invariants); **Gotchas**. Style: no emdash/endash/unicode-arrows; no "comprehensive/leverage/utilize"; short sentences.
- [ ] **Step 2: Document the standard** in `quickstart/README.md` (make the existing layout sketch an explicit "every README has these sections" checklist).
- [ ] **Step 3: Verify the docs sync still resolves**: the getsqe `docs-website` sync reads `quickstart/*/README.md` (lead para) + `OUTPUT.md`; confirm each upgraded README still has an H1-then-prose lead before the first `##`.
- [ ] **Step 4: Commit**

```bash
git add quickstart/*/README.md quickstart/README.md
git commit -m "docs(quickstart): bring all READMEs to the gold-standard what/how/why/configs structure"
```

### Task 8: Rewrite `development/testing.md` as the canonical testing guide

**Files:**
- Modify: `docs/site/book/src/development/testing.md`

- [ ] **Step 1:** Rewrite to document: the two tiers (Tier 1 engine/cargo, Tier 2 scenarios); how to run each via `scripts/test.sh` (engine/scenario/ci); the scenario catalog (each scenario: what it covers, self-contained vs cloud-gated); how `OUTPUT.md` (evidence) + `--check` (asserts) relate; and the CI jobs. Keep the existing accurate test-inventory content; replace stale references to `distributed-test.sh`.
- [ ] **Step 2: Build the book**: `cd docs/site/book && mdbook build -d /tmp/c-verify 2>&1 | grep -iE 'error|incomplete|broken' || echo clean`. Expected: clean.
- [ ] **Step 3: Commit**

```bash
git add docs/site/book/src/development/testing.md
git commit -m "docs: rewrite testing guide for the two-tier scenario harness"
```

---

## Phase 5: Verify + finish

### Task 9: Full verification

- [ ] **Step 1:** `scripts/test.sh scenario all` -> all self-contained scenarios `all checks passed`, exit 0.
- [ ] **Step 2:** `scripts/test.sh engine` -> Tier 1 cargo tests pass (needs the test stack; same prerequisites as `integration-test.sh`).
- [ ] **Step 3:** Break-it check on one scenario (disable a mask / point to a missing table) -> its `--check` FAILS and `scripts/test.sh` exits non-zero. Revert.
- [ ] **Step 4:** `make rustbook` builds; `make leak-scan` clean; quickstart READMEs render.
- [ ] **Step 5:** Push `docs/quickstart-scenario-harness`, open the MR (target main). The getsqe docs sync is unaffected (READMEs/OUTPUT shape preserved).

---

## Self-review (coverage check)

- Two-tier harness + one entry point -> Tasks 1,2 (+ engine delegates to integration-test.sh). ✓
- Quickstarts become asserted -> Tasks 3,4 (+ shared helpers Task 1). ✓
- Fold distributed-test -> Task 5. ✓
- CI wiring + AWS gating -> Task 6. ✓
- Every quickstart documented to the standard -> Task 7. ✓
- Canonical testing guide -> Task 8. ✓
- Dual purpose (one artifact, README + --check + OUTPUT) -> threaded through Tasks 3,4,7,8. ✓
- Verify incl. asserts-bite -> Tasks 3.4, 9.3. ✓
- Open items (distributed-as-dir vs entry; CI cadence; per-scenario exact invariants) -> resolved in Tasks 4,5,6 during execution. ✓
