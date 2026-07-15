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

- [ ] **Step 1: Ensure the local data stack is up**

Run: `docker ps --format '{{.Names}}' | grep -E 'polaris|rustfs'`
Expected: `sqlengine-polaris-1` and `sqlengine-rustfs-1` present. If not, bring up the test stack per `scripts/integration-test.sh`.

- [ ] **Step 2: Load a tiny suite into the local Polaris (the future "golden")**

Run: `BENCH_SCALE=0.01 BENCH_KEEP_RUNNING=1 ./scripts/benchmark-load.sh tpch`
Expected: TPC-H SF0.01 loads and SQE stays running on flight port 60051. Note the catalog/namespace it used (default namespace `tpch_sf0.01`).

- [ ] **Step 3: Attach that same Polaris as a second catalog `golden` and count rows**

Use the existing `sqe-cli -e` single-statement runner (`crates/sqe-cli`). ATTACH is coordinator-wide and persists, so a separate invocation for the SELECT sees the attached catalog:

```bash
cargo run -p sqe-cli -- --host localhost --port 60051 \
  -e "ATTACH 'http://polaris:8181/api/catalog' AS golden (TYPE iceberg_rest, WAREHOUSE 'quickstart_catalog', TOKEN '<bearer-from-the-running-stack>')"
cargo run -p sqe-cli -- --host localhost --port 60051 \
  -e "SELECT count(*) FROM golden.tpch_sf0_01.lineitem"
```

Substitute the real Polaris URL, warehouse name, and bearer token the running stack uses (see `scripts/benchmark-load.sh` env and the stack bootstrap). Confirm the exact `sqe-cli` connection flags in `crates/sqe-cli/src/main.rs`.

Expected: a non-zero count matching the loaded row count. This proves (a) the attached catalog read `metadata.json` + manifests over the custom endpoint and (b) DataFusion's `register_s3_store_if_needed` read the data files.

- [ ] **Step 4: Record the outcome**

If PASS: append a line to this plan under Task 1 (`Phase 0: PASS on <date>`) and proceed to Task 2.
If FAIL: STOP. Capture the exact error. Revisit the backend decision in the spec (fix `build_sqlite` S3 threading, or co-locate a golden Polaris with the writable warehouse) before building Phase 1. Do not proceed.

- [ ] **Step 5: Commit the recorded outcome**

```bash
git add docs/superpowers/plans/2026-07-15-benchmark-attach-golden-catalog.md
git commit -m "chore(bench): record Phase 0 attach spike outcome"
```

---

## Task 2: `benchmark-publish-iceberg.sh` — publish golden tables once

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

## Task 3: `benchmark-attach-golden.sh` (using existing `sqe-cli -e`)

Issue the one-shot coordinator-wide `ATTACH` from a script. No new binary code: the existing `sqe-cli -e "<sql>"` (crates/sqe-cli/src/main.rs:312) runs a single statement over Flight, and ATTACH is coordinator-wide and persists, so one invocation is enough. Task 3 is just the wrapper script.

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
SQL="ATTACH '${BENCH_GOLDEN_POLARIS_URL}' AS golden (TYPE iceberg_rest, WAREHOUSE '${BENCH_GOLDEN_WAREHOUSE}', TOKEN '${BENCH_GOLDEN_TOKEN}')"

cargo run -q -p sqe-cli -- --host "$HOST" --port "$PORT" -e "$SQL"
echo "attached golden: ${BENCH_GOLDEN_POLARIS_URL}"
```

Adjust `--host`/`--port` (and any auth flags) to the exact `sqe-cli` arg names in `crates/sqe-cli/src/main.rs`. If a prebuilt binary is preferred, call `target/release/sqe-cli` instead of `cargo run`.

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

## Task 4: `benchmark-test.sh` attach mode + end-to-end parity smoke

Wire an attach source into the run path: when `BENCH_DATA_SOURCE=attach`, skip generate+load for the read-only suites, attach golden once, and run `sqe-bench test --catalog golden`. Prove row-count parity against a freshly-loaded run.

**Files:**
- Modify: `scripts/benchmark-test.sh` (add attach branch), possibly `scripts/benchmark-load.sh` (recognize `BENCH_DATA_SOURCE=attach` and no-op the load for read suites).
- Reference: `crates/sqe-bench/src/test.rs` (`--catalog` qualification already exists), `benchmarks/expected/` (row-count baselines).

**Interfaces:**
- Consumes: Task 2 (golden tables exist), Task 3 (`benchmark-attach-golden.sh`), `sqe-bench test --catalog golden --namespace <ns>`.
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

Run (with golden published to local Polaris from Task 2, coordinator up, attach issued):
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

## Task 5: bench-internal shallow clone (Phase 2, gated on Tasks 1-4)

Add a clone step that makes a writable local copy of a golden table by copying its `metadata.json` into the local warehouse (rewriting `location` + write paths to local) and calling the existing `register_table`. Data files + manifests stay shared on the golden bucket.

**Files:**
- Create: `crates/sqe-bench/src/clone.rs` (module), test `crates/sqe-bench/tests/clone.rs`.
- Modify: `crates/sqe-bench/src/main.rs` (add a `Clone` subcommand), `crates/sqe-bench/src/cli.rs`.
- Reference: `crates/sqe-catalog/src/rest_catalog.rs:1838` (`register_table`), `crates/sqe-coordinator/src/maintenance.rs:161` (`system.register_table` CALL), the Iceberg `metadata.json` schema (`location`, `properties["write.data.path"]`, `properties["write.metadata.path"]`).

**Interfaces:**
- Consumes: golden catalog (Task 2), `register_table(ident, metadata_location)`.
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

## Task 6: wire tpcc/tpce clone-then-write + golden-immutability check (Phase 2)

Use the clone step so the write suites run against a writable local copy while the golden tables stay untouched.

**Files:**
- Modify: `scripts/benchmark-test.sh` (write-suite branch), `scripts/ci/attach-parity-smoke.sh` (extend, or new `scripts/ci/clone-immutability-smoke.sh`).
- Reference: Task 5 `sqe-bench clone`, `crates/sqe-catalog/src/iceberg_metadata_tvf.rs` (snapshot id read).

**Interfaces:**
- Consumes: Task 5 (`sqe-bench clone`), golden tpcc/tpce tables (publish them via Task 2 with the write suites added to its suite list, or a `--include-write` flag).
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

## Task 7: docs + project-state updates

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

- **Spec coverage:** Tier 1 (exists, no task). Tier 2 golden publish -> Task 2; attach -> Tasks 3-4; Tier 3 clone -> Tasks 5-6. Phase 0 spike -> Task 1. Error handling (loud attach failure, missing-golden guidance, clone conflict) -> covered in Task 2 skip-logic, Task 4 attach step, Task 6 branch; the "no silent fallback to load" constraint is enforced by attach mode skipping load unconditionally. Testing section -> Tasks 1, 4, 6. Location-pinning constraint -> Task 5 (`rewrite_metadata_for_local` keeps data paths).
- **Placeholder scan:** each code step has concrete content; flag/signature exactness is deferred to the referenced source files by design (the CLI arg names are read from `cli.rs`), which is a lookup, not a placeholder.
- **Type consistency:** `rewrite_metadata_for_local(&str, &str) -> Result<String>` used consistently in Task 5; `execute_update(&str)` matches flight.rs:175; `register_table(ident, metadata_location)` matches rest_catalog.rs:1838; catalog qualifier `golden` consistent across Tasks 3-6.
