# Benchmark Attach Golden Catalog Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Skip the per-run Iceberg load in benchmarks by publishing golden Iceberg tables once and attaching them read-only, cloning them only for the two write suites.

**Architecture:** Three reuse tiers — parquet source (exists), golden Iceberg tables published once into a persistent Polaris and attached read-only via coordinator-wide `ATTACH ... TYPE iceberg_rest`, and a bench-internal shallow clone (copy one `metadata.json` + `register_table`) for the write suites. The 6 read-only suites run their existing queries against the `golden` catalog with zero load; `sqe-bench test` already qualifies tables as `<catalog>.<namespace>.<table>`.

**Tech Stack:** Bash scripts, Rust (`sqe-bench` crate, clap CLI, Arrow Flight SQL client), Apache Polaris (Iceberg REST), vendored iceberg-rust, RustFS/StorageGrid S3.

## Global Constraints

- Branch + MR workflow; never push to main. This work is on branch `feat/bench-attach-golden-catalog`.
- Committed baseline benchmark numbers must come from `PROFILE=release`; use `PROFILE=dev-release` only while iterating.
- The golden catalog backend is `iceberg_rest` (Polaris). Do NOT use `sqlite` (verified S3 credential/endpoint gap in `build_sqlite`) or `hadoop`/`jdbc` (`error_not_yet` stubs).
- Golden Iceberg metadata embeds absolute `s3://` paths; the golden catalog is location-pinned. The clone must keep copied metadata pointing at the original data-file paths; only table `location` + write paths move local.
- ATTACH is coordinator-wide and persists across sessions (`RuntimeCatalogRegistry`, a global `RwLock<HashMap>`). Attach once per stack; do not re-attach per query.
- Read-only suites: `tpch`, `ssb`, `tpcds`, `tpcbb`, `clickbench`, `bank`. Write suites: `tpcc`, `tpce`.
- `bench_namespace(benchmark, scale)` yields `<benchmark>_sf<scale>` (e.g. `tpcds_sf10`); `tpcbb` reuses the `tpcds` namespace.
- Docs prose: no emdash/endash/unicode-arrows (CLAUDE.md writing rules).

---

## Task 1: Phase 0 feasibility spike (manual gate)

This task is a manual de-risking gate, not code. It must pass before Tasks 2+ are built. It proves that an attached `iceberg_rest` catalog's FileIO reaches a custom S3 endpoint for BOTH metadata reads and data-file reads. RustFS (`http://localhost:19000`) is the same custom-endpoint case as StorageGrid.

**Files:** none (throwaway shell session).

**Interfaces:**
- Consumes: running local stack (Polaris `:18181`, RustFS `:19000`), `scripts/benchmark-load.sh`.
- Produces: a yes/no answer recorded in the plan. Gates all later tasks.

- [x] **Step 1: Ensure the local data stack is up**

Run: `docker ps --format '{{.Names}}' | grep -E 'polaris|rustfs'`
Expected: `sqlengine-polaris-1` and `sqlengine-rustfs-1` present. If not, bring up the test stack per `scripts/integration-test.sh`.

- [x] **Step 2: Load a tiny suite into the local Polaris (the future "golden")**

Run: `BENCH_SCALE=0.01 BENCH_KEEP_RUNNING=1 ./scripts/benchmark-load.sh tpch`
Expected: TPC-H SF0.01 loads and SQE stays running on flight port 60051. Note the catalog/namespace it used (default namespace `tpch_sf0.01`).

Actual namespace was `tpch_sf0_01` (underscore, not a dot) — `bench_namespace`/`format_scale` (`crates/sqe-bench/src/main.rs`) already avoid the dot; the dot only appears in a cosmetic log line in `benchmark-load.sh`'s summary output. Warehouse is `test_warehouse`, not `quickstart_catalog`.

- [x] **Step 3: Attach that same Polaris as a second catalog `golden` and count rows**

Use the existing `sqe-cli -e` single-statement runner (`crates/sqe-cli`). ATTACH is coordinator-wide and persists, so a separate invocation for the SELECT sees the attached catalog:

```bash
cargo run -p sqe-cli -- --host localhost --port 60051 \
  -e "ATTACH 'http://polaris:8181/api/catalog' AS golden (TYPE iceberg_rest, WAREHOUSE 'quickstart_catalog', TOKEN '<bearer-from-the-running-stack>')"
cargo run -p sqe-cli -- --host localhost --port 60051 \
  -e "SELECT count(*) FROM golden.tpch_sf0_01.lineitem"
```

Substitute the real Polaris URL, warehouse name, and bearer token the running stack uses (see `scripts/benchmark-load.sh` env and the stack bootstrap). Confirm the exact `sqe-cli` connection flags in `crates/sqe-cli/src/main.rs`.

Expected: a non-zero count matching the loaded row count. This proves (a) the attached catalog read `metadata.json` + manifests over the custom endpoint and (b) DataFusion's `register_s3_store_if_needed` read the data files.

- [x] **Step 4: Record the outcome**

If PASS: append a line to this plan under Task 1 (`Phase 0: PASS on <date>`) and proceed to Task 2.
If FAIL: STOP. Capture the exact error. Revisit the backend decision in the spec (fix `build_sqlite` S3 threading, or co-locate a golden Polaris with the writable warehouse) before building Phase 1. Do not proceed.

**Phase 0: PASS on 2026-07-15.** `SELECT count(*) FROM golden.tpch_sf0_01.lineitem` returned `60000`, matching the local (non-attached) copy of the same table. Since `count(*)` alone can be answered from Iceberg manifest-list metadata without ever opening a Parquet file, this was followed up with `SELECT sum(l_quantity), min(l_shipdate) FROM golden.tpch_sf0_01.lineitem` (forces DataFusion to read actual column bytes) — result matched the local-catalog copy exactly (`sum = 1527864.00`, `min = 1992-01-03`), confirming genuine data-file reads over the custom S3 endpoint, not just a metadata-level answer. Full run log, exact commands, and two blockers hit along the way (both resolved without code changes) are in `.superpowers/sdd/task-1-report.md`. Summary of the two blockers:

1. `ATTACH` requires an admin role (`service_admin`/`catalog_admin`) that the test stack's default `client_credentials` auth never grants (Polaris's own JWT has no `realm_access.roles` claim, so sessions get `roles: []`). Worked around for the spike with a `[[auth.providers]] type = "bearer_passthrough"` entry in a throwaway copy of the coordinator config (not committed to `tests/sqe-test.toml`), which assigns a fixed role list while still forwarding the caller's real bearer token as the session's catalog credential. Task 2 (this plan's new ATTACH-config task) decides how `benchmark-attach-golden.sh` obtains an admin-capable credential against whatever coordinator it targets.
2. **Real gap, not just environmental:** `crates/sqe-catalog/src/mount.rs::build_iceberg_rest` (the `ATTACH`-path catalog builder) sets only `uri`/`warehouse`/`token`/`prefix` — it never sets `s3.endpoint`/`s3.region`/`s3.access-key-id`/`s3.secret-access-key`/`s3.path-style-access` the way the default catalog builder (`crates/sqe-catalog/src/rest_catalog.rs:868-894`) does, and `ATTACH`'s SQL grammar has no options to carry them even if it did. Without them, FileIO fails with `region is missing` when it tries to read the manifest list from a non-AWS endpoint (RustFS/StorageGRID). Worked around for the spike by exporting `AWS_REGION`/`AWS_ENDPOINT_URL_S3`/`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` in the coordinator process's environment (S3 FileIO falls back to the AWS SDK default chain). **Resolved in Task 2 (chose the proper fix): add `S3_ENDPOINT`/`S3_REGION`/`S3_ACCESS_KEY`/`S3_SECRET_KEY` ATTACH options to `attach.rs` + `mount.rs` so the golden catalog's S3 config travels with the `ATTACH` statement instead of depending on ambient env vars.**

- [x] **Step 5: Commit the recorded outcome**

```bash
git add docs/superpowers/plans/2026-07-15-benchmark-attach-golden-catalog.md
git commit -m "chore(bench): record Phase 0 attach spike outcome"
```

---

## Task 2: Add S3 options to ATTACH + bench attach coordinator config

The Phase 0 spike (Task 1) proved attach works but ONLY via ambient `AWS_*` env vars: `build_iceberg_rest` in `mount.rs` never sets the `s3.*` FileIO props, so reading the manifest list from a non-AWS endpoint (RustFS/StorageGrid) fails with `region is missing`. Fix it properly so the golden catalog's S3 config travels in the `ATTACH` statement. Also commit a bench coordinator config that grants an admin role so `ATTACH` passes its admin gate (the spike used a throwaway `bearer_passthrough` provider).

The ATTACH parser already collects arbitrary `KEY = 'value'` options into a `BTreeMap<String, OptionValue>` (verified in `attach.rs::parse_option_list`), so NO grammar change is needed. The work is: read five new options in `build_iceberg_rest`, plus the committed config.

**Files:**
- Modify: `crates/sqe-catalog/src/mount.rs` (add pure helper `s3_props_from_options`; call it in `build_iceberg_rest` after the existing `uri`/`warehouse`/`token`/`prefix` inserts).
- Test: `crates/sqe-catalog/src/mount.rs` (unit test module for `s3_props_from_options`).
- Create: `tests/benchmark-attach/coordinator-attach.toml` (bench coordinator config with a `bearer_passthrough` auth provider granting an admin role).
- Reference: `crates/sqe-catalog/src/rest_catalog.rs:868-894` (exact `s3.*` prop keys), `crates/sqe-sql/src/attach.rs` (generic `parse_option_list`, `OptionValue::as_str`), `.superpowers/sdd/task-1-report.md` (the proven `bearer_passthrough` config snippet + the admin role names `service_admin`/`catalog_admin`), `crates/sqe-auth/src/factory.rs:241` + `crates/sqe-auth/src/bearer_passthrough.rs` (provider config fields `user`, `roles`; find the exact serde tag from `AuthProviderConfig`).

**Interfaces:**
- Consumes: nothing new (parser already collects options).
- Produces: `ATTACH '<url>' AS golden (TYPE iceberg_rest, WAREHOUSE '<wh>', TOKEN '<tok>', S3_ENDPOINT '<url>', S3_REGION '<r>', S3_ACCESS_KEY '<k>', S3_SECRET_KEY '<s>', S3_PATH_STYLE 'true')` sets `s3.endpoint` / `s3.region` / `s3.access-key-id` / `s3.secret-access-key` / `s3.path-style-access` on the catalog props. Helper signature: `fn s3_props_from_options(options: &BTreeMap<String, OptionValue>) -> Vec<(String, String)>`. Tasks 3/4 consume these option names.

- [ ] **Step 1: Write the failing test**

Add to `crates/sqe-catalog/src/mount.rs` (bottom of file):

```rust
#[cfg(test)]
mod s3_option_tests {
    use super::*;
    use std::collections::BTreeMap;
    use sqe_sql::attach::OptionValue;

    fn opt(s: &str) -> OptionValue { OptionValue::String(s.to_string()) }

    #[test]
    fn s3_options_map_to_props() {
        let mut o = BTreeMap::new();
        o.insert("S3_ENDPOINT".to_string(), opt("http://localhost:19000"));
        o.insert("S3_REGION".to_string(), opt("us-east-1"));
        o.insert("S3_ACCESS_KEY".to_string(), opt("ak"));
        o.insert("S3_SECRET_KEY".to_string(), opt("sk"));
        o.insert("S3_PATH_STYLE".to_string(), opt("true"));
        let props: std::collections::HashMap<_, _> =
            s3_props_from_options(&o).into_iter().collect();
        assert_eq!(props.get("s3.endpoint").map(String::as_str), Some("http://localhost:19000"));
        assert_eq!(props.get("s3.region").map(String::as_str), Some("us-east-1"));
        assert_eq!(props.get("s3.access-key-id").map(String::as_str), Some("ak"));
        assert_eq!(props.get("s3.secret-access-key").map(String::as_str), Some("sk"));
        assert_eq!(props.get("s3.path-style-access").map(String::as_str), Some("true"));
    }

    #[test]
    fn absent_s3_options_yield_no_props() {
        let o: BTreeMap<String, OptionValue> = BTreeMap::new();
        assert!(s3_props_from_options(&o).is_empty());
    }
}
```

Confirm the exact import path for `OptionValue` (it may be re-exported as `sqe_sql::attach::OptionValue` or `sqe_sql::OptionValue`); `mount.rs` already references `OptionValue`, so mirror its existing `use`.

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p sqe-catalog s3_option_tests`
Expected: FAIL — `s3_props_from_options` is not defined.

- [ ] **Step 3: Implement the helper and wire it into `build_iceberg_rest`**

Add the helper (NOT behind the `rest` feature, so tests run without it):

```rust
/// Map ATTACH `S3_*` options to the `s3.*` catalog FileIO props the REST
/// catalog builder consumes (mirrors `rest_catalog.rs:868-894`). Only
/// present options produce props; absent ones are skipped so ambient config
/// still applies as a fallback.
fn s3_props_from_options(options: &BTreeMap<String, OptionValue>) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for (opt_key, prop_key) in [
        ("S3_ENDPOINT", "s3.endpoint"),
        ("S3_REGION", "s3.region"),
        ("S3_ACCESS_KEY", "s3.access-key-id"),
        ("S3_SECRET_KEY", "s3.secret-access-key"),
        ("S3_PATH_STYLE", "s3.path-style-access"),
    ] {
        if let Some(v) = options.get(opt_key).and_then(OptionValue::as_str) {
            out.push((prop_key.to_string(), v.to_string()));
        }
    }
    out
}
```

Then, in `build_iceberg_rest` (the `#[cfg(feature = "rest")]` one), after the existing `props.insert` calls for `uri`/`warehouse` and before `RestCatalogBuilder::default().load(...)`, add:

```rust
    for (k, v) in s3_props_from_options(options) {
        props.insert(k, v);
    }
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p sqe-catalog s3_option_tests`
Expected: PASS (both tests).

- [ ] **Step 5: Clippy + feature build**

Run: `cargo clippy -p sqe-catalog --features rest -- -D warnings`
Expected: no warnings. Confirms `build_iceberg_rest` still compiles with the new insert.

- [ ] **Step 6: Create the bench attach coordinator config**

Create `tests/benchmark-attach/coordinator-attach.toml`: start from `tests/sqe-test.toml` (or `tests/parity/coordinator-parity.toml`) and add a `bearer_passthrough` auth provider that assigns an admin role and forwards the caller's bearer as the catalog credential. Use the exact provider serde tag + fields from `crates/sqe-auth/src/factory.rs` and the working snippet recorded in `.superpowers/sdd/task-1-report.md`. The roles must include whatever the ATTACH admin gate checks (`service_admin`/`catalog_admin` per the spike). Add a header comment explaining this config exists so `ATTACH` passes its admin gate in the bench rig, and that it forwards the real bearer for catalog ACLs.

- [ ] **Step 7: Commit**

```bash
git add crates/sqe-catalog/src/mount.rs tests/benchmark-attach/coordinator-attach.toml
git commit -m "feat(attach): carry S3 endpoint/region/creds in ATTACH options + bench attach config"
```

---

## Task 3: `benchmark-publish-iceberg.sh` — publish golden tables once

Load each read-only suite's parquet into a persistent golden Polaris, once, idempotently. Mirrors `benchmark-publish-data.sh` (skip-if-present, `BENCH_FORCE=1` override) but the artifact is Iceberg tables via `sqe-bench load`, not raw parquet.

**Files:**
- Create: `scripts/benchmark-publish-iceberg.sh`
- Reference: `scripts/benchmark-publish-data.sh` (env + skip-if-present pattern), `scripts/benchmark-load.sh` (load invocation + S3/auth env), `crates/sqe-bench/src/cli.rs` (the `Load` subcommand args).

**Interfaces:**
- Consumes: `sqe-bench load` (existing), an existing golden Polaris URL.
- Produces: golden Iceberg tables at namespace `<bench>_sf<scale>` in the golden Polaris. Env contract:
  `BENCH_GOLDEN_POLARIS_URL`, `BENCH_GOLDEN_WAREHOUSE`, `BENCH_GOLDEN_TOKEN` (bearer for the REST skip-check + load auth), `BENCH_SCALE`, `BENCH_DATA_SOURCE` (s3:// parquet from Tier 1), `BENCH_S3_ENDPOINT`, `BENCH_S3_PROFILE`, and auth (`SQE_TOKEN_ENDPOINT`/`SQE_CLIENT_ID`/`SQE_CLIENT_SECRET` or `ICEBERG_BEARER_TOKEN`), `BENCH_FORCE`.

- [ ] **Step 1: Write the failing test (dry-run guard)**

Create the script skeleton so that running it with no required env fails loudly. Add at the top of `scripts/benchmark-publish-iceberg.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail

# Publish golden Iceberg tables once into a persistent Polaris. Load-many
# afterwards attaches them read-only (see benchmark-attach-golden.sh) instead
# of re-loading. Read-only suites only; write suites (tpcc/tpce) are cloned
# at run time, not published here.

ALL_READ_SUITES=(tpch ssb tpcds tpcbb clickbench bank)

BENCH_SCALE="${BENCH_SCALE:-0.1}"
BENCH_GOLDEN_POLARIS_URL="${BENCH_GOLDEN_POLARIS_URL:-}"
BENCH_GOLDEN_WAREHOUSE="${BENCH_GOLDEN_WAREHOUSE:-}"
BENCH_FORCE="${BENCH_FORCE:-0}"

if [ -z "$BENCH_GOLDEN_POLARIS_URL" ] || [ -z "$BENCH_GOLDEN_WAREHOUSE" ]; then
  echo "ERROR: BENCH_GOLDEN_POLARIS_URL and BENCH_GOLDEN_WAREHOUSE must be set." >&2
  exit 1
fi
```

- [ ] **Step 2: Run it to verify it fails without env**

Run: `bash scripts/benchmark-publish-iceberg.sh`
Expected: exits non-zero with the `BENCH_GOLDEN_POLARIS_URL ... must be set` error.

- [ ] **Step 3: Implement the publish loop**

Append the suite loop. Each suite: check whether the golden namespace already has tables (skip unless `BENCH_FORCE=1`), else invoke `sqe-bench load` pointed at the golden Polaris, reading parquet from the Tier-1 source. Note `tpcbb` shares the `tpcds` namespace, so publishing `tpcds` covers it; publish `tpcbb` only if `tpcds` was not requested.

```bash
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT_DIR"

PROFILE="${PROFILE:-release}"
cargo build -p sqe-bench --profile "$PROFILE" 2>&1
BENCH_BIN="$ROOT_DIR/target/$( [ "$PROFILE" = release ] && echo release || echo "$PROFILE" )/sqe-bench"

SUITES=("$@"); [ $# -eq 0 ] && SUITES=("${ALL_READ_SUITES[@]}")

for BENCH in "${SUITES[@]}"; do
  # tpcbb reuses tpcds tables; publishing tpcds covers it.
  if [ "$BENCH" = "tpcbb" ]; then
    echo "SKIP tpcbb: reuses the tpcds namespace; publish tpcds instead."
    continue
  fi
  NS="${BENCH}_sf${BENCH_SCALE}"
  echo "== publishing $BENCH -> $BENCH_GOLDEN_POLARIS_URL ns=$NS =="
  # Skip-if-present: query the golden Polaris REST catalog directly for the
  # namespace's table list. Nothing is ATTACHed at publish time, so this must
  # not go through an attached `golden` catalog. Uses the Iceberg REST
  # listTables endpoint with the golden bearer token.
  if [ "$BENCH_FORCE" != "1" ]; then
    LIST_URL="${BENCH_GOLDEN_POLARIS_URL%/}/v1/${BENCH_GOLDEN_WAREHOUSE}/namespaces/${NS}/tables"
    COUNT=$(curl -sf -H "Authorization: Bearer ${BENCH_GOLDEN_TOKEN:-}" "$LIST_URL" \
              2>/dev/null | python3 -c 'import sys,json;print(len(json.load(sys.stdin).get("identifiers",[])))' 2>/dev/null || echo 0)
    if [ "${COUNT:-0}" -gt 0 ]; then
      echo "SKIP $BENCH: golden namespace $NS already has $COUNT tables."
      continue
    fi
  fi
  "$BENCH_BIN" load "$BENCH" \
    --scale "$BENCH_SCALE" \
    --data "${BENCH_DATA_SOURCE:-/tmp/sqe-bench-data}" \
    --catalog-uri "$BENCH_GOLDEN_POLARIS_URL" \
    --warehouse "$BENCH_GOLDEN_WAREHOUSE" \
    --namespace "$NS" \
    ${BENCH_FORCE:+--recreate}
done
echo "golden publish complete."
```

Adjust flag names to the exact `Load` subcommand args in `crates/sqe-bench/src/cli.rs` (`--catalog`, `--namespace`, `--s3-endpoint`, auth flags). Keep the S3/auth env passthrough identical to `benchmark-load.sh`.

- [ ] **Step 4: Verify against the local stack at SF0.01**

Run (golden = the local Polaris for this smoke):
`BENCH_SCALE=0.01 BENCH_GOLDEN_POLARIS_URL=http://localhost:18181/api/catalog BENCH_GOLDEN_WAREHOUSE=quickstart_catalog BENCH_DATA_SOURCE=/tmp/sqe-bench-data bash scripts/benchmark-publish-iceberg.sh tpch`
Expected: TPC-H golden tables created; a second run prints `SKIP tpch: golden namespace tpch_sf0.01 already populated.`

- [ ] **Step 5: Commit**

```bash
git add scripts/benchmark-publish-iceberg.sh
git commit -m "feat(bench): publish golden Iceberg tables once (benchmark-publish-iceberg.sh)"
```

---

## Task 4: `benchmark-attach-golden.sh` (using existing `sqe-cli -e`)

Issue the one-shot coordinator-wide `ATTACH` from a script. No new binary code: the existing `sqe-cli -e "<sql>"` (crates/sqe-cli/src/main.rs:312) runs a single statement over Flight, and ATTACH is coordinator-wide and persists, so one invocation is enough. Task 4 is just the wrapper script.

**Files:**
- Create: `scripts/benchmark-attach-golden.sh`
- Reference: `crates/sqe-cli/src/main.rs` (exact connection flags for `--host`/`--port`/auth and `-e`).

**Interfaces:**
- Consumes: `sqe-cli -e "<sql>"` (exists).
- Produces: `benchmark-attach-golden.sh` builds and runs `ATTACH '<url>' AS golden (TYPE iceberg_rest, WAREHOUSE '<wh>', TOKEN '<tok>')` against the coordinator, exiting 0 on success. After it runs, every session sees the `golden` catalog.

- [ ] **Step 1: Write the failing test (env guard)**

Create `scripts/benchmark-attach-golden.sh` with only the env guard first:

```bash
#!/usr/bin/env bash
set -euo pipefail
# Issue the one-shot coordinator-wide ATTACH for the golden catalog.
# ATTACH is global and persists across sessions, so run this once after the
# coordinator is up and before any `sqe-bench test --catalog golden`.
: "${BENCH_GOLDEN_POLARIS_URL:?set BENCH_GOLDEN_POLARIS_URL}"
: "${BENCH_GOLDEN_WAREHOUSE:?set BENCH_GOLDEN_WAREHOUSE}"
: "${BENCH_GOLDEN_TOKEN:?set BENCH_GOLDEN_TOKEN}"
```

- [ ] **Step 2: Run it to verify it fails without env**

Run: `bash scripts/benchmark-attach-golden.sh`
Expected: exits non-zero complaining that `BENCH_GOLDEN_POLARIS_URL` is unset.

- [ ] **Step 3: Implement the ATTACH invocation**

Append:

```bash
HOST="${BENCH_HOST:-localhost}"
PORT="${BENCH_PORT_FLIGHT:-60051}"
# S3 config travels IN the ATTACH statement (Task 2 added these options), so
# the golden catalog's FileIO reaches a custom endpoint without relying on
# ambient AWS_* env vars on the coordinator.
: "${BENCH_S3_ENDPOINT:?set BENCH_S3_ENDPOINT}"
: "${BENCH_GOLDEN_S3_ACCESS_KEY:?set BENCH_GOLDEN_S3_ACCESS_KEY}"
: "${BENCH_GOLDEN_S3_SECRET_KEY:?set BENCH_GOLDEN_S3_SECRET_KEY}"
S3_REGION="${BENCH_S3_REGION:-us-east-1}"
S3_PATH_STYLE="${BENCH_S3_PATH_STYLE:-true}"
SQL="ATTACH '${BENCH_GOLDEN_POLARIS_URL}' AS golden (TYPE iceberg_rest, WAREHOUSE '${BENCH_GOLDEN_WAREHOUSE}', TOKEN '${BENCH_GOLDEN_TOKEN}', S3_ENDPOINT '${BENCH_S3_ENDPOINT}', S3_REGION '${S3_REGION}', S3_ACCESS_KEY '${BENCH_GOLDEN_S3_ACCESS_KEY}', S3_SECRET_KEY '${BENCH_GOLDEN_S3_SECRET_KEY}', S3_PATH_STYLE '${S3_PATH_STYLE}')"

cargo run -q -p sqe-cli -- --host "$HOST" --port "$PORT" -e "$SQL"
echo "attached golden: ${BENCH_GOLDEN_POLARIS_URL}"
```

Adjust `--host`/`--port` (and any auth flags) to the exact `sqe-cli` arg names in `crates/sqe-cli/src/main.rs`. If a prebuilt binary is preferred, call `target/release/sqe-cli` instead of `cargo run`. The coordinator this targets must run with the admin-capable config from Task 2 (`tests/benchmark-attach/coordinator-attach.toml`) so ATTACH passes its admin gate.

- [ ] **Step 4: Verify against the running stack**

Run (coordinator up, golden published to local Polaris):
`BENCH_GOLDEN_POLARIS_URL=http://localhost:18181/api/catalog BENCH_GOLDEN_WAREHOUSE=quickstart_catalog BENCH_GOLDEN_TOKEN=<tok> bash scripts/benchmark-attach-golden.sh`
Then confirm: `cargo run -q -p sqe-cli -- --host localhost --port 60051 -e "SHOW CATALOGS"` lists `golden`.
Expected: `golden` appears; attach exit code 0.

- [ ] **Step 5: Commit**

```bash
git add scripts/benchmark-attach-golden.sh
git commit -m "feat(bench): benchmark-attach-golden.sh (one-shot ATTACH via sqe-cli)"
```

---

## Task 5: `benchmark-test.sh` attach mode + end-to-end parity smoke

Wire an attach source into the run path: when `BENCH_DATA_SOURCE=attach`, skip generate+load for the read-only suites, attach golden once, and run `sqe-bench test --catalog golden`. Prove row-count parity against a freshly-loaded run.

**Files:**
- Modify: `scripts/benchmark-test.sh` (add attach branch), possibly `scripts/benchmark-load.sh` (recognize `BENCH_DATA_SOURCE=attach` and no-op the load for read suites).
- Reference: `crates/sqe-bench/src/test.rs` (`--catalog` qualification already exists), `benchmarks/expected/` (row-count baselines).

**Interfaces:**
- Consumes: Task 3 (golden tables exist), Task 4 (`benchmark-attach-golden.sh`), `sqe-bench test --catalog golden --namespace <ns>`.
- Produces: `BENCH_DATA_SOURCE=attach ./scripts/benchmark-test.sh <read-suite>` runs queries with zero load.

- [ ] **Step 1: Write the failing parity check**

Create `scripts/ci/attach-parity-smoke.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail
# Load tpch SF0.01 the normal way, capture q1 row count; then run the same
# suite via attach mode and assert identical counts.
BENCH_SCALE=0.01
LOADED=$(target/release/sqe-bench test tpch --scale $BENCH_SCALE --host localhost \
  --port 60051 --namespace tpch_sf0.01 --query q1 --json 2>/dev/null \
  | python3 -c 'import sys,json;print(json.load(sys.stdin)[0]["rows"])')
ATTACHED=$(target/release/sqe-bench test tpch --scale $BENCH_SCALE --host localhost \
  --port 60051 --catalog golden --namespace tpch_sf0.01 --query q1 --json 2>/dev/null \
  | python3 -c 'import sys,json;print(json.load(sys.stdin)[0]["rows"])')
[ "$LOADED" = "$ATTACHED" ] || { echo "PARITY FAIL: loaded=$LOADED attached=$ATTACHED"; exit 1; }
echo "PARITY OK: $LOADED rows both paths"
```

(If `sqe-bench test` has no `--json`, capture the printed row count from its normal output instead; confirm the exact flag in `cli.rs`.)

- [ ] **Step 2: Run it to verify it fails**

Run: `bash scripts/ci/attach-parity-smoke.sh`
Expected: FAIL — `golden` is not attached yet (query errors / catalog not found).

- [ ] **Step 3: Add the attach branch to benchmark-test.sh**

In `scripts/benchmark-test.sh`, before the per-suite test loop, add:

```bash
if [ "${BENCH_DATA_SOURCE:-generate}" = "attach" ]; then
  echo "attach mode: skipping generate+load for read-only suites"
  bash "$SCRIPT_DIR/benchmark-attach-golden.sh"
  BENCH_TEST_CATALOG="golden"
else
  BENCH_TEST_CATALOG=""
fi
```

and thread `${BENCH_TEST_CATALOG:+--catalog $BENCH_TEST_CATALOG}` into every `sqe-bench test` invocation in this script.

- [ ] **Step 4: Run the parity smoke to verify it passes**

Run (with golden published to local Polaris from Task 3, coordinator up, attach issued):
`bash scripts/ci/attach-parity-smoke.sh`
Expected: `PARITY OK: <n> rows both paths`.

- [ ] **Step 5: Full read-suite smoke at SF0.01**

Run: `BENCH_SCALE=0.01 BENCH_DATA_SOURCE=attach ./scripts/benchmark-test.sh tpch ssb`
Expected: all queries pass with no load step in the log; wall-clock excludes any load.

- [ ] **Step 6: Commit**

```bash
git add scripts/benchmark-test.sh scripts/ci/attach-parity-smoke.sh
git commit -m "feat(bench): attach mode in benchmark-test.sh + parity smoke"
```

---

## Task 6: bench-internal shallow clone (Phase 2, gated on Tasks 1-5)

Add a clone step that makes a writable local copy of a golden table by copying its `metadata.json` into the local warehouse (rewriting `location` + write paths to local) and calling the existing `register_table`. Data files + manifests stay shared on the golden bucket.

**Files:**
- Create: `crates/sqe-bench/src/clone.rs` (module), test `crates/sqe-bench/tests/clone.rs`.
- Modify: `crates/sqe-bench/src/main.rs` (add a `Clone` subcommand), `crates/sqe-bench/src/cli.rs`.
- Reference: `crates/sqe-catalog/src/rest_catalog.rs:1838` (`register_table`), `crates/sqe-coordinator/src/maintenance.rs:161` (`system.register_table` CALL), the Iceberg `metadata.json` schema (`location`, `properties["write.data.path"]`, `properties["write.metadata.path"]`).

**Interfaces:**
- Consumes: golden catalog (Task 3), `register_table(ident, metadata_location)`.
- Produces: `sqe-bench clone --from golden.<ns>.<table> --to <localcat>.<ns>.<table>` creates a writable local table sharing golden data files. Function signature: `async fn shallow_clone(from: TableIdent, to: TableIdent, local_warehouse: &str) -> anyhow::Result<()>`.

- [ ] **Step 1: Write the failing test**

Create `crates/sqe-bench/tests/clone.rs`:

```rust
// Unit test: rewrite_metadata_location retargets location + write paths
// to the local warehouse while leaving data-file manifest paths untouched.
use sqe_bench::clone::rewrite_metadata_for_local;

#[test]
fn rewrite_points_location_local_keeps_data_paths() {
    let golden = r#"{"location":"s3://golden/tpch/lineitem",
        "properties":{"write.data.path":"s3://golden/tpch/lineitem/data"}}"#;
    let out = rewrite_metadata_for_local(golden, "s3://local/tpch/lineitem").unwrap();
    assert!(out.contains("\"location\":\"s3://local/tpch/lineitem\""));
    assert!(out.contains("s3://local/tpch/lineitem/data") ||
            !out.contains("write.data.path")); // write path moved local or dropped
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p sqe-bench --test clone`
Expected: FAIL — `sqe_bench::clone` does not exist.

- [ ] **Step 3: Implement `rewrite_metadata_for_local`**

Create `crates/sqe-bench/src/clone.rs`:

```rust
//! Bench-internal shallow clone of a golden Iceberg table. Copies one
//! metadata.json into the local warehouse with the table location and
//! write paths retargeted local; data files and manifests keep their
//! original (immutable, golden-bucket) absolute paths and are shared.
use serde_json::Value;

/// Retarget `location` and `write.{data,metadata}.path` to `new_location`,
/// leaving all manifest/data-file paths intact.
pub fn rewrite_metadata_for_local(metadata_json: &str, new_location: &str)
    -> anyhow::Result<String> {
    let mut v: Value = serde_json::from_str(metadata_json)?;
    v["location"] = Value::String(new_location.to_string());
    if let Some(props) = v.get_mut("properties").and_then(|p| p.as_object_mut()) {
        props.insert("write.data.path".into(),
            Value::String(format!("{new_location}/data")));
        props.insert("write.metadata.path".into(),
            Value::String(format!("{new_location}/metadata")));
    }
    Ok(serde_json::to_string(&v)?)
}
```

Export the module from `lib.rs` (`pub mod clone;`).

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p sqe-bench --test clone`
Expected: PASS.

- [ ] **Step 5: Implement the clone orchestration + `Clone` subcommand**

Add `async fn shallow_clone(...)` in `clone.rs` that: loads the golden table's current metadata location, reads the JSON via the object store, calls `rewrite_metadata_for_local`, writes the new `metadata.json` to the local warehouse, then issues `CALL <localcat>.system.register_table('<ns.table>', '<new-metadata-location>')` via the Flight client `execute_update`. Add the `Clone { from, to, local_warehouse, host, port }` clap variant and a main.rs dispatch arm calling `shallow_clone`.

- [ ] **Step 6: Commit**

```bash
git add crates/sqe-bench/src/clone.rs crates/sqe-bench/src/lib.rs crates/sqe-bench/src/cli.rs crates/sqe-bench/src/main.rs crates/sqe-bench/tests/clone.rs
git commit -m "feat(bench): shallow-clone step for write-suite golden tables"
```

---

## Task 7: wire tpcc/tpce clone-then-write + golden-immutability check (Phase 2)

Use the clone step so the write suites run against a writable local copy while the golden tables stay untouched.

**Files:**
- Modify: `scripts/benchmark-test.sh` (write-suite branch), `scripts/ci/attach-parity-smoke.sh` (extend, or new `scripts/ci/clone-immutability-smoke.sh`).
- Reference: Task 6 `sqe-bench clone`, `crates/sqe-catalog/src/iceberg_metadata_tvf.rs` (snapshot id read).

**Interfaces:**
- Consumes: Task 6 (`sqe-bench clone`), golden tpcc/tpce tables (publish them via Task 3 with the write suites added to its suite list, or a `--include-write` flag).
- Produces: `BENCH_DATA_SOURCE=attach ./scripts/benchmark-test.sh tpcc` clones then runs write DML; golden snapshot id unchanged.

- [ ] **Step 1: Write the failing immutability check**

Create `scripts/ci/clone-immutability-smoke.sh`: capture the golden tpcc `orders` snapshot id (via the iceberg metadata TVF), clone it, run one tpcc write query against the clone, then re-read the golden snapshot id and assert it is unchanged and the clone's snapshot id advanced.

- [ ] **Step 2: Run it to verify it fails**

Run: `bash scripts/ci/clone-immutability-smoke.sh`
Expected: FAIL — write suites are not yet wired to clone (query hits read-only golden or missing local table).

- [ ] **Step 3: Add the write-suite clone branch**

In `scripts/benchmark-test.sh`, when in attach mode and the suite is `tpcc` or `tpce`: for each table, `sqe-bench clone --from golden.<ns>.<t> --to <localcat>.<ns>.<t> --local-warehouse <wh>`, then run the suite against `<localcat>` (not `golden`).

- [ ] **Step 4: Run the immutability check to verify it passes**

Run: `bash scripts/ci/clone-immutability-smoke.sh`
Expected: `IMMUTABLE OK: golden snapshot <id> unchanged; clone advanced`.

- [ ] **Step 5: Commit**

```bash
git add scripts/benchmark-test.sh scripts/ci/clone-immutability-smoke.sh
git commit -m "feat(bench): clone-then-write for tpcc/tpce with golden-immutability check"
```

---

## Task 8: docs + project-state updates

**Files:**
- Modify: `README.md` (roadmap), `nextsteps.md` (status), the design spec status line, and add a short usage section to `benchmarks/` docs or the scripts' header comments.

- [ ] **Step 1: Update README roadmap + nextsteps**

Mark the attach-golden benchmark speedup as delivered (Phase 1) / in-progress (Phase 2). Follow the "After Completing Work" checklist in CLAUDE.md.

- [ ] **Step 2: Document the workflow**

Add a short "Fast benchmark runs via attached golden tables" section documenting: publish once (`benchmark-publish-iceberg.sh`), then run with `BENCH_DATA_SOURCE=attach`. No emdash/endash/unicode-arrows.

- [ ] **Step 3: Commit**

```bash
git add README.md nextsteps.md docs/superpowers/specs/2026-07-15-benchmark-attach-golden-catalog-design.md
git commit -m "docs(bench): document attached-golden fast benchmark workflow"
```

---

## Self-Review

- **Spec coverage:** Tier 1 (exists, no task). ATTACH S3 config + admin cred -> Task 2. Tier 2 golden publish -> Task 3; attach -> Tasks 4-5; Tier 3 clone -> Tasks 6-7. Phase 0 spike -> Task 1. Error handling (loud attach failure, missing-golden guidance, clone conflict) -> covered in Task 3 skip-logic, Task 5 attach step, Task 7 branch; the "no silent fallback to load" constraint is enforced by attach mode skipping load unconditionally. Testing section -> Tasks 1, 5, 7. Location-pinning constraint -> Task 6 (`rewrite_metadata_for_local` keeps data paths).
- **Placeholder scan:** each code step has concrete content; flag/signature exactness is deferred to the referenced source files by design (the CLI arg names are read from `cli.rs`), which is a lookup, not a placeholder.
- **Type consistency:** `rewrite_metadata_for_local(&str, &str) -> Result<String>` used consistently in Task 6; `s3_props_from_options(&BTreeMap<String, OptionValue>) -> Vec<(String, String)>` (Task 2) uses prop keys matching rest_catalog.rs:868-894; `register_table(ident, metadata_location)` matches rest_catalog.rs:1838; catalog qualifier `golden` consistent across Tasks 4-7.
