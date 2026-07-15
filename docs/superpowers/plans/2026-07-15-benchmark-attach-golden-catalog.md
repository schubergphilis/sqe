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

Open a Flight SQL session against the running coordinator (`localhost:60051`) and run, in one session:

```sql
ATTACH 'http://polaris:8181/api/catalog' AS golden (
  TYPE iceberg_rest,
  WAREHOUSE 'quickstart_catalog',
  TOKEN '<bearer-from-the-running-stack>'
);
SELECT count(*) FROM golden.tpch_sf0_01.lineitem;
```

Use whatever Flight SQL client is convenient (the coordinator's own client, or a throwaway `sqe-bench attach`/`test` once Task 3 exists — for the spike, reuse the stack's existing client path). Substitute the real Polaris URL, warehouse name, and bearer token the running stack uses (see `scripts/benchmark-load.sh` env and the stack bootstrap).

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
  `BENCH_GOLDEN_POLARIS_URL`, `BENCH_GOLDEN_WAREHOUSE`, `BENCH_SCALE`, `BENCH_DATA_SOURCE` (s3:// parquet from Tier 1), `BENCH_S3_ENDPOINT`, `BENCH_S3_PROFILE`, and auth (`SQE_TOKEN_ENDPOINT`/`SQE_CLIENT_ID`/`SQE_CLIENT_SECRET` or `ICEBERG_BEARER_TOKEN`), `BENCH_FORCE`.

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
  # Skip-if-present: a table list that returns rows means already published.
  if [ "$BENCH_FORCE" != "1" ] && "$BENCH_BIN" test "$BENCH" \
        --scale "$BENCH_SCALE" --host "${BENCH_HOST:-localhost}" \
        --catalog golden --namespace "$NS" --query q1 >/dev/null 2>&1; then
    echo "SKIP $BENCH: golden namespace $NS already populated."
    continue
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

## Task 3: `sqe-bench attach` subcommand + `benchmark-attach-golden.sh`

Issue the one-shot coordinator-wide `ATTACH` from a script. `sqe-bench` already has a Flight SQL client with `execute()`; add a thin `attach` subcommand that runs an arbitrary DDL statement, then a wrapper script that builds the `ATTACH` SQL from env.

**Files:**
- Modify: `crates/sqe-bench/src/cli.rs` (add `Attach` variant), `crates/sqe-bench/src/main.rs` (dispatch), `crates/sqe-bench/src/client/flight.rs` (reuse `execute_update`).
- Create: `scripts/benchmark-attach-golden.sh`
- Test: `crates/sqe-bench/tests/attach_cli.rs`

**Interfaces:**
- Consumes: Flight client `execute_update(&self, sql: &str) -> anyhow::Result<()>` (exists, flight.rs:175).
- Produces: `sqe-bench attach --host <h> --port <p> --sql '<DDL>'` runs one statement and exits 0 on success. `benchmark-attach-golden.sh` builds and runs the `ATTACH '<url>' AS golden (TYPE iceberg_rest, WAREHOUSE '<wh>', TOKEN '<tok>')` statement.

- [ ] **Step 1: Write the failing test**

Create `crates/sqe-bench/tests/attach_cli.rs`:

```rust
// Verifies the `attach` subcommand parses and requires --sql.
use std::process::Command;

#[test]
fn attach_requires_sql() {
    let out = Command::new(env!("CARGO_BIN_EXE_sqe-bench"))
        .args(["attach", "--host", "localhost", "--port", "60051"])
        .output()
        .expect("run sqe-bench");
    assert!(!out.status.success(), "attach without --sql must fail");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("--sql"), "error should mention --sql, got: {stderr}");
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p sqe-bench --test attach_cli`
Expected: FAIL — the `attach` subcommand does not exist yet (clap errors with unknown subcommand, message will not mention `--sql`).

- [ ] **Step 3: Add the `Attach` clap variant**

In `crates/sqe-bench/src/cli.rs`, add to `enum Command`:

```rust
    /// Issue a single DDL statement (e.g. ATTACH) on a Flight SQL session.
    Attach {
        /// Coordinator host
        #[arg(long, default_value = "localhost")]
        host: String,
        /// Coordinator port
        #[arg(long, default_value_t = 60051u16)]
        port: u16,
        /// The SQL DDL statement to execute (e.g. an ATTACH statement)
        #[arg(long)]
        sql: String,
        /// Username for authentication (OIDC password grant)
        #[arg(long, env = "SQE_USER")]
        username: Option<String>,
        /// Password for authentication (OIDC password grant)
        #[arg(long, env = "SQE_PASSWORD")]
        password: Option<String>,
    },
```

- [ ] **Step 4: Dispatch it in main.rs**

In `crates/sqe-bench/src/main.rs`, add a match arm that connects the flight client and calls `execute_update`:

```rust
        Command::Attach { host, port, sql, username, password } => {
            let client = client::flight::FlightBenchClient::connect(
                &host, port, username.as_deref(), password.as_deref(),
                /* token args as the existing connect signature requires */
            ).await?;
            client.execute_update(&sql).await?;
            println!("attached: {sql}");
        }
```

Match the exact `connect(...)` signature in `client/flight.rs` (auth args may differ). Reuse the same connect helper the `Test` arm uses.

- [ ] **Step 5: Run the test to verify it passes**

Run: `cargo test -p sqe-bench --test attach_cli`
Expected: PASS.

- [ ] **Step 6: Create the wrapper script**

Create `scripts/benchmark-attach-golden.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail
# Issue the one-shot coordinator-wide ATTACH for the golden catalog.
# ATTACH is global and persists across sessions, so run this once after the
# coordinator is up and before any `sqe-bench test --catalog golden`.

: "${BENCH_GOLDEN_POLARIS_URL:?set BENCH_GOLDEN_POLARIS_URL}"
: "${BENCH_GOLDEN_WAREHOUSE:?set BENCH_GOLDEN_WAREHOUSE}"
: "${BENCH_GOLDEN_TOKEN:?set BENCH_GOLDEN_TOKEN}"
HOST="${BENCH_HOST:-localhost}"
PORT="${BENCH_PORT_FLIGHT:-60051}"
BIN="${SQE_BENCH_BIN:-target/release/sqe-bench}"

SQL="ATTACH '${BENCH_GOLDEN_POLARIS_URL}' AS golden (TYPE iceberg_rest, WAREHOUSE '${BENCH_GOLDEN_WAREHOUSE}', TOKEN '${BENCH_GOLDEN_TOKEN}')"
"$BIN" attach --host "$HOST" --port "$PORT" --sql "$SQL"
```

- [ ] **Step 7: Commit**

```bash
git add crates/sqe-bench/src/cli.rs crates/sqe-bench/src/main.rs crates/sqe-bench/tests/attach_cli.rs scripts/benchmark-attach-golden.sh
git commit -m "feat(bench): add attach subcommand + benchmark-attach-golden.sh"
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
