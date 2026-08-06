# Spark/Ranger Access-Control Parity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Spark, against the same Polaris catalog and Ranger instance as SQE, is subject
to the same object-level access control, proven by a test suite that mirrors the SQE
access-control suite.

**Architecture:** Polaris already enforces the `polaris` Ranger service against the OIDC
identity, so Spark needs a per-user Keycloak JWT rather than the `root` service account.
The shared frontend service is renamed `hive` to `query` and carries one deliberate
blanket `policyType-0` allow so Kyuubi defers object decisions to Polaris and holds only
masks and row filters. Tests live beside the SQE suite and reuse its fixture.

**Tech Stack:** Rust integration tests in `sqe-coordinator`, Apache Ranger 2.8 REST,
Polaris 1.7, Spark 3.5.9 with Kyuubi Authz 1.11.1 shaded, Iceberg 1.8.1.

**Spec:** `docs/superpowers/specs/2026-08-06-spark-ranger-access-control-parity-design.md`

## Global Constraints

- Never push to `main`. Branch is `feat/spark-ranger-access-control`, land via `glab` MR.
- Never run `cargo fmt --all`: `main` is not rustfmt-clean and it rewrites ~100 unrelated files.
- Never `git add -u` or `git add .`: `benchmarks/results/bounded-memory-phase0-baseline.json` is a test byproduct and must stay out of commits. Add paths explicitly.
- zsh does not word-split unquoted `$VAR`. Never build up flag strings in a variable; inline flags or use an array.
- Prose in `docs/site/` and all commit messages: no emdash, no endash, no Unicode arrows, none of the forbidden AI-tell words, no sentence starting with "This" referring to the previous sentence.
- Coordinator test suites need `RUST_MIN_STACK=33554432`.
- The servicedef type stays `hive`. Only the service INSTANCE is renamed. Kyuubi is hardwired to the `database`/`table`/`column` resource shape.
- `default_ranger_policy_service_name()` in `crates/sqe-core/src/config.rs` keeps returning `"hive"`. Configs name `query` explicitly.
- Every Spark object-level assertion checks WHICH tier denied, never merely that the query failed.

---

## File Structure

| File | Responsibility |
|---|---|
| `crates/sqe-coordinator/tests/it/common/spark_runner.rs` | new. Runs `spark-sql` in the quickstart container, parses rows, classifies the denial tier. Pure classifier is unit-tested without Docker. |
| `crates/sqe-coordinator/tests/it/spark_access_control_e2e.rs` | new. The object-level suite. |
| `crates/sqe-coordinator/tests/it/common/mod.rs` | add `pub mod spark_runner;` |
| `crates/sqe-coordinator/tests/it/main.rs` | add `mod spark_access_control_e2e;` |
| `crates/sqe-coordinator/tests/it/common/ranger_fixture.rs` | seed the defer policy into the test-owned frontend service |
| `scripts/spark-access-control-test.sh` | new. Brings up the stack subset plus `spark`, sets the gate, runs the suite. |
| `Makefile` | new target `test-access-control-spark` |
| quickstart + docs | the `hive` to `query` rename, enumerated in Task 1 |

---

### Task 1: Rename the shared frontend service to `query` and seed the defer policy

**Files:**
- Modify: `quickstart/polaris-ranger-keycloak/ranger/bootstrap-ranger.sh`
- Modify: `quickstart/polaris-ranger-keycloak/spark/ranger-spark-security.xml`
- Modify: `quickstart/polaris-ranger-keycloak/sqe.toml`
- Modify: `quickstart/polaris-ranger-keycloak/test.sh`, `parity-test.sh`, `OVERVIEW.md`
- Modify: `quickstart/polaris-ranger-service-principal/ranger/bootstrap-ranger.sh`
- Modify: `scripts/access-control-demo.sh`
- Modify: `docs/site/book/src/design-notes/ranger-access-control.md`, `docs/site/book/src/features/access-control-tutorial.md`, `docs/site/book/src/features/fine-grained-access-control.md`

**Interfaces:**
- Produces: a Ranger service instance named `query`, type `hive`, `tagService: tag`, carrying a policy named `defer-object-level-to-polaris`.
- Consumes: nothing.

- [ ] **Step 1: Find every reference so none is missed**

```bash
grep -rn 'service.name.*hive\|service-name = "hive"\|"service": *"hive"\|"hive"' \
  quickstart/ scripts/ docs/site/book/src/design-notes/ranger-access-control.md \
  docs/site/book/src/features/ | grep -v sqe_ac_hive
```

Expected: hits in the 10 files listed above. `sqe_ac_hive` is the test-owned service and must NOT be renamed.

- [ ] **Step 2: Rename the service instance in the bootstrap**

In `quickstart/polaris-ranger-keycloak/ranger/bootstrap-ranger.sh`, the service creation currently posts `{"name":"hive","type":"hive",...}`. Change the name only, and attach the tag service:

```bash
    -d '{"name":"query","type":"hive","configs":{"username":"admin","password":"none","jdbc.driverClassName":"org.apache.hive.jdbc.HiveDriver","jdbc.url":"none"},"tagService":"tag","isEnabled":true}'
```

Then change every seeded policy's `"service":"hive"` to `"service":"query"`.

- [ ] **Step 3: Seed the defer policy**

Append to the same bootstrap, after the other policy posts. The comment is required: read
out of context the policy says "everyone may select everything".

```bash
# Object-level authorization belongs to POLARIS, not to this service. Polaris runs
# its own Ranger plugin against the `polaris` service and returns 403 at LOAD_TABLE
# for an unauthorized read. Kyuubi, however, checks its own privilege FIRST and
# short-circuits before Polaris is ever consulted: without a matching policyType-0
# item it fails with
#   AccessControlException: Permission denied: user [bob] does not have [select]
#   privilege on [acdemo/orders/id]
# even when Polaris would have allowed the read. SQE ignores policyType-0 entirely,
# so leaving this out makes the two engines disagree on every object-level grant.
#
# One blanket allow makes Kyuubi defer. Masks and row filters are UNAFFECTED: they
# are policy types 1 and 2, evaluated separately.
#
# DO NOT DELETE to "tighten security". It does not grant data access; Polaris still
# decides. Deleting it breaks Spark entirely. DO NOT copy it into a service whose
# engine does not also authorize through Polaris: there it WOULD be a wide-open hole.
#
# Every access type Kyuubi may check must be listed. It checks `update` for INSERT
# and `create` for DDL, and a missing one short-circuits exactly as above.
cat > /tmp/query-defer.json <<'EOF'
{
  "service": "query",
  "name": "defer-object-level-to-polaris",
  "description": "Kyuubi checks its own privilege before Polaris is consulted. This makes it defer; Polaris decides object level. Masks and row filters are unaffected.",
  "policyType": 0,
  "isEnabled": true,
  "resources": {
    "database": {"values": ["*"]},
    "table":    {"values": ["*"]},
    "column":   {"values": ["*"]}
  },
  "policyItems": [{
    "groups": ["public"],
    "accesses": [
      {"type": "select", "isAllowed": true},
      {"type": "update", "isAllowed": true},
      {"type": "create", "isAllowed": true},
      {"type": "drop", "isAllowed": true},
      {"type": "alter", "isAllowed": true},
      {"type": "index", "isAllowed": true},
      {"type": "lock", "isAllowed": true},
      {"type": "read", "isAllowed": true},
      {"type": "write", "isAllowed": true}
    ]
  }]
}
EOF
post_hive_policy /tmp/query-defer.json
```

- [ ] **Step 4: Point both engines at the renamed service**

`quickstart/polaris-ranger-keycloak/spark/ranger-spark-security.xml`:

```xml
  <property>
    <name>ranger.plugin.spark.service.name</name>
    <value>query</value>
  </property>
```

`quickstart/polaris-ranger-keycloak/sqe.toml`:

```toml
[policy.ranger]
service-name = "query"
```

Update the surrounding comments in both files: they currently say the name must match
the `hive` service created by the bootstrap.

- [ ] **Step 5: Update the remaining references**

Mechanical, in `test.sh`, `parity-test.sh` (5 references), `OVERVIEW.md`,
`polaris-ranger-service-principal/ranger/bootstrap-ranger.sh`,
`scripts/access-control-demo.sh`, and the 3 doc files. In the docs, add the defer policy
to the architecture description with the same reasoning as Step 3, and add the standing
constraint: any engine reading `query` must also authorize through Polaris.

- [ ] **Step 6: Verify against the live stack**

```bash
cd quickstart/polaris-ranger-keycloak
docker compose up -d --force-recreate ranger-setup
sleep 20
set -a; . ./.env; set +a
curl -s -u "admin:${RANGER_ADMIN_PASSWORD:-rangerR0cks!}" \
  "http://localhost:${RANGER_PORT:-26080}/service/public/v2/api/service" \
  | python3 -c "import sys,json; print([(s['name'],s['type'],s.get('tagService')) for s in json.load(sys.stdin)])"
```

Expected: `('query', 'hive', 'tag')` present, and no service named `hive` other than the
test-owned `sqe_ac_hive`.

```bash
curl -s -u "admin:${RANGER_ADMIN_PASSWORD:-rangerR0cks!}" \
  "http://localhost:${RANGER_PORT:-26080}/service/plugins/policies/service/name/query" \
  | python3 -c "import sys,json; d=json.load(sys.stdin); p=d if isinstance(d,list) else d.get('policies',d); print([x['name'] for x in p])"
```

Expected: includes `defer-object-level-to-polaris`.

- [ ] **Step 7: Prove the rename did not break fine-grained enforcement**

```bash
docker compose up -d --wait sqe spark
./parity-test.sh
```

Expected: PASS. Both engines still apply the ssn mask byte-identically, now through
`query`. A failure here means the rename missed a reference.

- [ ] **Step 8: Commit**

```bash
git add quickstart/ scripts/access-control-demo.sh docs/site/book/src/design-notes/ranger-access-control.md docs/site/book/src/features/access-control-tutorial.md docs/site/book/src/features/fine-grained-access-control.md
git commit -m "refactor(ranger): rename the shared frontend service hive to query

The name claimed a Hive metastore was involved. It is the frontend query plane
both SQE and Spark read. Servicedef type stays hive because Kyuubi is hardwired
to the database/table/column resource shape; only the instance is renamed.

Seeds defer-object-level-to-polaris. Kyuubi checks its own privilege BEFORE
Polaris is consulted and short-circuits without a matching policyType-0 item,
while SQE ignores policyType-0 entirely, so identical grants made the two
engines disagree on every object-level grant. One blanket allow makes Kyuubi
defer and leaves object level to Polaris. Masks and row filters are policy
types 1 and 2 and are unaffected."
```

---

### Task 2: `spark_sql` runner with tier classification

**Files:**
- Create: `crates/sqe-coordinator/tests/it/common/spark_runner.rs`
- Modify: `crates/sqe-coordinator/tests/it/common/mod.rs` (add `pub mod spark_runner;`)

**Interfaces:**
- Produces: `spark_sql(session, hadoop_user, sql) -> SparkOutcome`, `SparkOutcome::rows()`, `SparkOutcome::tier()`, `DenialTier`, and a pure `classify(&str) -> DenialTier`.
- Consumes: `sqe_core::Session::access_token()`, which returns `&SecretString` (needs `expose_secret()`).

- [ ] **Step 1: Write the failing unit tests for the classifier**

The classifier is the part that must not be wrong, and it needs no Docker. Add to
`spark_runner.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_polaris_denial_is_attributed_to_polaris() {
        let err = "org.apache.iceberg.exceptions.ForbiddenException: Forbidden: \
                   Principal 'bob' is not authorized for op 'LOAD_TABLE'";
        match classify(err) {
            DenialTier::Polaris { op, .. } => assert_eq!(op, "LOAD_TABLE"),
            other => panic!("expected Polaris, got {other:?}"),
        }
    }

    #[test]
    fn a_kyuubi_denial_is_attributed_to_kyuubi() {
        let err = "org.apache.kyuubi.plugin.spark.authz.AccessControlException: \
                   Permission denied: user [bob] does not have [select] privilege \
                   on [acdemo/orders/id]";
        match classify(err) {
            DenialTier::Kyuubi { privilege, .. } => assert_eq!(privilege, "select"),
            other => panic!("expected Kyuubi, got {other:?}"),
        }
    }

    // The whole point of the defer policy is that Kyuubi stops denying. If a
    // Kyuubi denial were ever classified as a Polaris one, the guard test would
    // pass while the defer policy was missing.
    #[test]
    fn the_two_denials_are_never_confused() {
        let kyuubi = "AccessControlException: Permission denied: user [bob] does \
                      not have [select] privilege on [ac/orders/id]";
        assert!(matches!(classify(kyuubi), DenialTier::Kyuubi { .. }));
        let polaris = "ForbiddenException: Forbidden: Principal 'bob' is not \
                       authorized for op 'ADD_TABLE_SNAPSHOT'";
        assert!(matches!(classify(polaris), DenialTier::Polaris { .. }));
    }

    #[test]
    fn the_kyuubi_row_filter_bug_is_not_a_denial() {
        let err = "org.apache.spark.sql.AnalysisException: MISSING_ATTRIBUTES ...";
        assert!(matches!(classify(err), DenialTier::KyuubiRowFilterBug));
    }

    #[test]
    fn a_clean_run_has_no_denial() {
        assert!(matches!(classify("Time taken: 2.7 seconds, Fetched 3 row(s)"), DenialTier::None));
    }
}
```

- [ ] **Step 2: Run them to verify they fail**

```bash
cd /Users/jjverhoeks/git/schuberg/vpf-data-ai/chameleon/Applications/sqlengine
RUST_MIN_STACK=33554432 cargo test -p sqe-coordinator --test it spark_runner 2>&1 | tail -20
```

Expected: compile error, `classify` not found.

- [ ] **Step 3: Implement the runner**

```rust
//! Runs `spark-sql` inside the quickstart's `spark` container and attributes any
//! authorization failure to the tier that produced it.
//!
//! Two tiers can refuse the same query and the difference is the whole point of
//! the Spark suite:
//!
//! - Polaris, enforcing the `polaris` Ranger service against the bearer token:
//!   `ForbiddenException: ... not authorized for op 'LOAD_TABLE'`
//! - Kyuubi, enforcing the frontend service against `HADOOP_USER_NAME`:
//!   `AccessControlException: Permission denied: user [...] does not have [...]`
//!
//! A test that only asserts "the query failed" passes when the WRONG tier
//! refused, which is exactly the defect the `defer-object-level-to-polaris`
//! policy exists to prevent.

use secrecy::ExposeSecret;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DenialTier {
    None,
    /// Polaris refused. `op` is the Polaris operation name.
    Polaris { principal: String, op: String },
    /// Kyuubi refused before Polaris was consulted.
    Kyuubi { user: String, privilege: String },
    /// Kyuubi cannot apply a row filter over an unprojected column (Kyuubi #6889).
    /// Not a denial: a bug, and it must never be counted as enforcement.
    KyuubiRowFilterBug,
    /// Failed for some other reason; carries the first useful line.
    Other(String),
}

/// Attribute a spark-sql failure to a tier. Pure, so it is unit-tested without Docker.
pub fn classify(output: &str) -> DenialTier {
    if output.contains("MISSING_ATTRIBUTES") {
        return DenialTier::KyuubiRowFilterBug;
    }
    if let Some(rest) = output.split("AccessControlException").nth(1) {
        // `Permission denied: user [bob] does not have [select] privilege on [db/t/c]`
        let user = between(rest, "user [", "]").unwrap_or_default();
        let privilege = between(rest, "have [", "]").unwrap_or_default();
        return DenialTier::Kyuubi { user, privilege };
    }
    if let Some(rest) = output.split("ForbiddenException").nth(1) {
        let principal = between(rest, "Principal '", "'").unwrap_or_default();
        let op = between(rest, "for op '", "'").unwrap_or_default();
        return DenialTier::Polaris { principal, op };
    }
    for marker in ["Exception", "ERROR SparkSQLDriver"] {
        if let Some(line) = output.lines().find(|l| l.contains(marker)) {
            return DenialTier::Other(line.trim().to_string());
        }
    }
    DenialTier::None
}

fn between(s: &str, open: &str, close: &str) -> Option<String> {
    let start = s.find(open)? + open.len();
    let end = s[start..].find(close)? + start;
    Some(s[start..end].to_string())
}

pub struct SparkOutcome {
    pub rows: Vec<Vec<String>>,
    pub tier: DenialTier,
    pub raw: String,
}

impl SparkOutcome {
    pub fn expect_ok(&self, what: &str) -> &Vec<Vec<String>> {
        assert_eq!(self.tier, DenialTier::None, "{what}: {}", self.raw);
        &self.rows
    }
    /// Assert POLARIS refused, naming the op. A Kyuubi denial fails here on purpose.
    pub fn expect_polaris_denial(&self, op: &str, what: &str) {
        match &self.tier {
            DenialTier::Polaris { op: got, .. } => assert_eq!(
                got, op,
                "{what}: Polaris refused a different op\n{}", self.raw
            ),
            other => panic!(
                "{what}: expected a POLARIS denial on {op}, got {other:?}.\n\
                 A Kyuubi denial here means defer-object-level-to-polaris is \
                 missing from the frontend service.\n{}",
                self.raw
            ),
        }
    }
}

/// The catalog name the suite registers inside Spark.
pub const SPARK_CATALOG: &str = "ac";

/// Run `sql` as `session`'s user. The bearer token gives Polaris the identity;
/// `HADOOP_USER_NAME` gives Kyuubi its (asserted) one. They are passed separately
/// on purpose: one test deliberately mismatches them.
pub async fn spark_sql(
    session: &sqe_core::Session,
    hadoop_user: &str,
    sql: &str,
) -> SparkOutcome {
    let token = session.access_token().expose_secret().to_string();
    let c = SPARK_CATALOG;
    // Each --conf is TWO argv entries. Building one string would pass
    // "--conf spark.sql.extensions=..." as a single argument and spark-sql
    // reports `Unrecognized option`.
    let mut args: Vec<String> = vec![
        "compose".into(), "-f".into(), compose_file(), "exec".into(), "-T".into(),
        "-e".into(), format!("HADOOP_USER_NAME={hadoop_user}"),
        "spark".into(), "/opt/spark/bin/spark-sql".into(),
    ];
    for kv in [
        "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.apache.kyuubi.plugin.spark.authz.ranger.RangerSparkExtension".to_string(),
        format!("spark.sql.catalog.{c}=org.apache.iceberg.spark.SparkCatalog"),
        format!("spark.sql.catalog.{c}.catalog-impl=org.apache.iceberg.rest.RESTCatalog"),
        format!("spark.sql.catalog.{c}.uri=http://polaris:8181/api/catalog"),
        format!("spark.sql.catalog.{c}.warehouse=sales_wh"),
        format!("spark.sql.catalog.{c}.token={token}"),
        // Load-bearing. With refresh ON, Iceberg exchanges this external JWT
        // against Polaris's token endpoint and the identity reverts to root.
        format!("spark.sql.catalog.{c}.token-refresh-enabled=false"),
        format!("spark.sql.catalog.{c}.header.Polaris-Realm=iceberg-ranger"),
        format!("spark.sql.catalog.{c}.io-impl=org.apache.iceberg.aws.s3.S3FileIO"),
        format!("spark.sql.catalog.{c}.s3.endpoint=http://rustfs:9000"),
        format!("spark.sql.catalog.{c}.s3.path-style-access=true"),
        format!("spark.sql.catalog.{c}.s3.access-key-id=s3admin"),
        format!("spark.sql.catalog.{c}.s3.secret-access-key=s3adminpw"),
    ] {
        args.push("--conf".into());
        args.push(kv);
    }
    args.push("-e".into());
    args.push(sql.to_string());

    let out = tokio::process::Command::new("docker")
        .args(&args)
        .output()
        .await
        .expect("spawn docker compose exec spark");
    let raw = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    let tier = classify(&raw);
    SparkOutcome { rows: parse_rows(&raw), tier, raw }
}

/// spark-sql prints tab-separated rows, mixed into log lines. Keep only lines
/// that are plausibly data: no log level, no timestamp, at least one tab or a
/// bare scalar, and not the "Time taken" trailer.
fn parse_rows(raw: &str) -> Vec<Vec<String>> {
    raw.lines()
        .filter(|l| {
            !l.is_empty()
                && !l.starts_with("2")   // log timestamps
                && !l.contains("WARN")
                && !l.contains("ERROR")
                && !l.contains("INFO")
                && !l.starts_with("Time taken")
                && !l.contains("Exception")
                && !l.contains("\tat ")
        })
        .map(|l| l.split('\t').map(|c| c.trim().to_string()).collect())
        .collect()
}

fn compose_file() -> String {
    let manifest = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".into());
    std::path::Path::new(&manifest)
        .parent().and_then(|p| p.parent()).unwrap_or(std::path::Path::new("."))
        .join("quickstart/polaris-ranger-keycloak/docker-compose.yml")
        .to_string_lossy().to_string()
}
```

Add `pub mod spark_runner;` to `crates/sqe-coordinator/tests/it/common/mod.rs`.

- [ ] **Step 4: Run the unit tests to verify they pass**

```bash
RUST_MIN_STACK=33554432 cargo test -p sqe-coordinator --test it spark_runner 2>&1 | tail -12
```

Expected: 5 passed.

- [ ] **Step 5: Mutation-check the classifier**

Swap the `AccessControlException` and `ForbiddenException` branches so Kyuubi denials
classify as Polaris. Re-run. `the_two_denials_are_never_confused` MUST fail. Revert.
A classifier that cannot tell the tiers apart makes every later assertion vacuous.

- [ ] **Step 6: Commit**

```bash
git add crates/sqe-coordinator/tests/it/common/spark_runner.rs crates/sqe-coordinator/tests/it/common/mod.rs
git commit -m "test(spark): spark-sql runner that attributes denials to a tier

Polaris and Kyuubi can both refuse the same query, and which one did is the
whole point of the Spark suite: a Kyuubi denial where Polaris was expected means
the defer policy is missing. The classifier is pure and unit-tested without
Docker, including a case asserting the two denials are never confused."
```

---

### Task 3: Defer policy in the test-owned frontend service

**Files:**
- Modify: `crates/sqe-coordinator/tests/it/common/ranger_fixture.rs`

**Interfaces:**
- Consumes: `RangerAdmin::bootstrap()`, `HIVE_SERVICE` (`sqe_ac_hive`), `PREFIX`.
- Produces: the defer policy present in `sqe_ac_hive` after `bootstrap()`.

The suite owns `sqe_ac_hive`, not the quickstart's `query`, so Task 1's bootstrap does
not cover it. Without the defer policy, every Spark object-level test fails with a
Kyuubi denial.

- [ ] **Step 1: Add the seeding call inside `bootstrap()`**

After the block that creates `HIVE_SERVICE` and links `TAG_SERVICE`, add a
`policyType-0` blanket allow named `{PREFIX}defer-object-level-to-polaris`, with the same
9 access types and the same explanatory comment as Task 1 Step 3, posted idempotently the
way the fixture posts its other policies.

- [ ] **Step 2: Verify it lands**

```bash
scripts/access-control-test.sh ranger_wiring_smoke 2>&1 | tail -5
set -a; . quickstart/polaris-ranger-keycloak/.env; set +a
curl -s -u "admin:${RANGER_ADMIN_PASSWORD:-rangerR0cks!}" \
  "http://localhost:${RANGER_PORT:-26080}/service/plugins/policies/service/name/sqe_ac_hive" \
  | python3 -c "import sys,json; d=json.load(sys.stdin); p=d if isinstance(d,list) else d.get('policies',d); print([(x['name'],x['policyType']) for x in p])"
```

Expected: a `policyType` 0 entry whose name ends `defer-object-level-to-polaris`.

- [ ] **Step 3: Verify the SQE suite is unaffected**

```bash
RUST_MIN_STACK=33554432 scripts/access-control-test.sh 2>&1 | tail -6
```

Expected: 31 passed, 0 failed. SQE ignores `policyType-0`, so adding one must change
nothing. A failure means SQE does read them after all, which would invalidate the whole
defer approach and must stop the plan.

- [ ] **Step 4: Commit**

```bash
git add crates/sqe-coordinator/tests/it/common/ranger_fixture.rs
git commit -m "test(ranger): seed the defer policy in the test-owned frontend service

The suite owns sqe_ac_hive rather than the quickstart's query service, so the
bootstrap rename does not cover it. Without a policyType-0 blanket allow, Kyuubi
refuses every Spark read before Polaris is consulted. Verifies the SQE suite
still passes 31/31, which is what proves SQE really does ignore policyType-0."
```

---

### Task 4: Object-level read cases

**Files:**
- Create: `crates/sqe-coordinator/tests/it/spark_access_control_e2e.rs`
- Modify: `crates/sqe-coordinator/tests/it/main.rs`

**Interfaces:**
- Consumes: `common::spark_runner::{spark_sql, DenialTier, SPARK_CATALOG}`, and the SQE suite's fixture. Reuse `ac_setup`-equivalent bootstrapping by extracting what is needed; do not duplicate the table seeding.
- Produces: the read half of the suite.

Grants are issued through SQE (`ctx.handler.execute(&ctx.carol, "GRANT ...")`), then
asserted through Spark. One grant path, two engines, which is the parity being tested.

- [ ] **Step 1: Write the first test, the guard, and watch it fail**

```rust
//! Object-level access control for Spark against the same Polaris catalog and
//! Ranger services as the SQE suite.
//!
//! Grants are written through SQE's GRANT statement and asserted through Spark.
//! Object level is decided by POLARIS: Kyuubi defers via
//! `defer-object-level-to-polaris` in the frontend service. Every denial
//! assertion therefore names the tier, because a Kyuubi denial here would mean
//! the defer policy went missing and the test would otherwise pass for the
//! wrong reason.
//!
//! Run with: scripts/spark-access-control-test.sh

use crate::common::spark_runner::{spark_sql, DenialTier, SPARK_CATALOG};

macro_rules! spark_gate {
    () => {
        if !crate::common::ac_enabled() {
            eprintln!("skipping spark_access_control_e2e: set SQE_AC_E2E=1 \
                       (use scripts/spark-access-control-test.sh)");
            return;
        }
    };
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs quickstart/polaris-ranger-keycloak plus spark; run scripts/spark-access-control-test.sh"]
async fn spark_denied_before_any_grant() {
    spark_gate!();
    let _guard = crate::common::serial().lock().await;
    let ctx = crate::access_control_e2e::ac_setup_for_spark().await;

    let out = spark_sql(
        &ctx.bob, "bob",
        &format!("SELECT count(*) FROM {SPARK_CATALOG}.ac.orders"),
    ).await;

    // Polaris must be the one refusing. A Kyuubi denial means the defer policy
    // is missing, and the read never reached the tier under test.
    out.expect_polaris_denial("LOAD_TABLE", "no grant yet");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs quickstart/polaris-ranger-keycloak plus spark; run scripts/spark-access-control-test.sh"]
async fn object_denial_survives_the_frontend_defer_policy() {
    spark_gate!();
    let _guard = crate::common::serial().lock().await;
    let ctx = crate::access_control_e2e::ac_setup_for_spark().await;

    // The defer policy grants `select` on database=* table=* to group public.
    // If it leaked object-level authority, this read would SUCCEED with no
    // Polaris grant in place. It must still be refused, by Polaris.
    let out = spark_sql(
        &ctx.bob, "bob",
        &format!("SELECT * FROM {SPARK_CATALOG}.ac.orders"),
    ).await;
    out.expect_polaris_denial("LOAD_TABLE", "defer policy must not grant data access");
}
```

`ac_setup` and `AcCtx` are currently private to `access_control_e2e`. Expose a
`pub(crate) async fn ac_setup_for_spark()` there that reuses the same body, rather than
copying the fixture. Add `mod spark_access_control_e2e;` to `main.rs`.

- [ ] **Step 2: Run and confirm both fail for the right reason**

```bash
RUST_MIN_STACK=33554432 scripts/spark-access-control-test.sh spark_denied_before_any_grant 2>&1 | tail -20
```

Expected before Task 3's defer policy exists: a panic naming a Kyuubi denial. That is the
failure mode Task 3 removes, and seeing it here proves the assertion is not vacuous.
With Task 3 in place: PASS.

- [ ] **Step 3: Add the remaining read cases**

Same shape. For each, issue the grant through SQE as `carol`, wait with
`crate::common::eventually_within` (Polaris polls Ranger; 120s budget as the SQE suite
uses), then assert through Spark.

| Test | Grant issued via SQE | Spark assertion |
|---|---|---|
| `spark_grant_select_to_role_enables_exact_rows` | `GRANT SELECT ON sales_wh.ac.orders TO ROLE "engineer"` | `expect_ok`, exactly 3 rows |
| `spark_role_grant_and_user_grant_both_apply` | role grant to `analyst`, user grant to `dave` | both read; `dave` holds no role |
| `spark_revoke_disables_access` | grant, read, then `REVOKE SELECT ...` | `expect_polaris_denial("LOAD_TABLE", ...)` after the revoke settles |
| `spark_ranger_deny_overrides_allow` | grant plus a deny item (reuse `add_deny_item_to_audit_policy`) | denied while the deny item is present |
| `spark_all_tables_in_schema_grant_covers_the_namespace` | `GRANT SELECT ON ALL TABLES IN SCHEMA sales_wh.ac TO ROLE "engineer"` | a table not named individually reads |
| `spark_namespace_listing_requires_namespace_list` | no `namespace-list` grant | `SHOW NAMESPACES IN ac` refused |
| `spark_view_read_requires_view_access_types` | grant `view-*` but not `table-*` on a view | view reads, base table does not |

- [ ] **Step 4: Run the read half**

```bash
RUST_MIN_STACK=33554432 scripts/spark-access-control-test.sh 2>&1 | tail -15
```

Expected: all read cases pass. Note the wall time; each `spark-sql` is a fresh JVM.

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-coordinator/tests/it/spark_access_control_e2e.rs crates/sqe-coordinator/tests/it/main.rs crates/sqe-coordinator/tests/it/access_control_e2e.rs
git commit -m "test(spark): object-level read access control for Spark

Grants are written through SQE's GRANT statement and asserted through Spark, so
one grant path is checked against two engines. Every denial assertion names the
tier: a Kyuubi denial where Polaris was expected means the defer policy went
missing, which is the failure this suite exists to catch. Includes the guard
proving the blanket defer policy grants no data access of its own."
```

---

### Task 5: Object-level write cases

**Files:**
- Modify: `crates/sqe-coordinator/tests/it/spark_access_control_e2e.rs`

**Interfaces:**
- Consumes: Task 4's fixture accessor and runner.

- [ ] **Step 1: Write the write-separation test**

The measured denial op is `ADD_TABLE_SNAPSHOT`, not `LOAD_TABLE`: Polaris refuses at the
snapshot commit. Asserting `LOAD_TABLE` here would fail.

```rust
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs quickstart/polaris-ranger-keycloak plus spark; run scripts/spark-access-control-test.sh"]
async fn spark_write_privileges_are_separate_from_read() {
    spark_gate!();
    let _guard = crate::common::serial().lock().await;
    let ctx = crate::access_control_e2e::ac_setup_for_spark().await;

    // Read only.
    exec_ok(&ctx, &ctx.carol,
        "GRANT SELECT ON sales_wh.ac.orders TO ROLE \"engineer\"").await;
    crate::common::eventually_within(
        std::time::Duration::from_secs(120),
        "bob to read after the grant",
        || async {
            let o = spark_sql(&ctx.bob, "bob",
                &format!("SELECT count(*) FROM {SPARK_CATALOG}.ac.orders")).await;
            if o.tier == DenialTier::None { Ok(()) } else { Err(format!("{:?}", o.tier)) }
        },
    ).await;

    // The write must still be refused, and refused by POLARIS at commit time.
    let before = row_count(&ctx).await;
    let out = spark_sql(&ctx.bob, "bob",
        &format!("INSERT INTO {SPARK_CATALOG}.ac.orders VALUES \
                  (99,'EU',1.0,'999-99-9999','z@x',DATE '2024-01-01')")).await;
    out.expect_polaris_denial("ADD_TABLE_SNAPSHOT", "read grant must not confer write");
    // Authorization is only real if nothing landed.
    assert_eq!(before, row_count(&ctx).await, "a refused INSERT changed the table");

    // Granting the write admits it.
    exec_ok(&ctx, &ctx.carol,
        "GRANT INSERT ON sales_wh.ac.orders TO ROLE \"engineer\"").await;
    crate::common::eventually_within(
        std::time::Duration::from_secs(120),
        "bob to write after the INSERT grant",
        || async {
            let o = spark_sql(&ctx.bob, "bob",
                &format!("INSERT INTO {SPARK_CATALOG}.ac.orders VALUES \
                          (99,'EU',1.0,'999-99-9999','z@x',DATE '2024-01-01')")).await;
            if o.tier == DenialTier::None { Ok(()) } else { Err(format!("{:?}", o.tier)) }
        },
    ).await;
    assert_eq!(before + 1, row_count(&ctx).await, "the granted INSERT did not land");
}
```

`row_count` reads through SQE as `carol` so the count is never itself subject to the
grant under test.

- [ ] **Step 2: Add the identity-split test**

```rust
/// Documents a real property, not a bug to fix: the object tier verifies a JWT
/// signature, the fine-grained tier trusts an asserted HADOOP_USER_NAME. A
/// mismatched pair gets one user's object rights with another's masks. Closing
/// it means running Spark behind a Kyuubi server with real authentication.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs quickstart/polaris-ranger-keycloak plus spark; run scripts/spark-access-control-test.sh"]
async fn mismatched_identity_reveals_the_two_tier_trust_split() { /* alice token, HADOOP_USER_NAME=bob */ }
```

Grant `SELECT` to `analyst` (alice) only, then run with alice's token and
`HADOOP_USER_NAME=bob`. The read succeeds on alice's object rights. Assert the succeeding
read and record in the test body that mask selection followed `bob`.

- [ ] **Step 3: Run the whole suite**

```bash
RUST_MIN_STACK=33554432 scripts/spark-access-control-test.sh 2>&1 | tail -15
```

Expected: all cases pass, 0 failed.

- [ ] **Step 4: Commit**

```bash
git add crates/sqe-coordinator/tests/it/spark_access_control_e2e.rs
git commit -m "test(spark): write-path object authorization and the identity split

Polaris refuses an unauthorized INSERT at ADD_TABLE_SNAPSHOT rather than before
the data is staged, so the test asserts that op and also that the row count did
not move: authorization is only real if nothing landed. Adds a test documenting
that the object tier verifies a JWT while the fine-grained tier trusts an
asserted HADOOP_USER_NAME, so a mismatched pair yields one user's object rights
with another's masks."
```

---

### Task 6: Script and make target

**Files:**
- Create: `scripts/spark-access-control-test.sh`
- Modify: `Makefile`

- [ ] **Step 1: Write the script**

Copy `scripts/access-control-test.sh` and change: add `spark` to the `--wait` service
list, keep the existing two-phase bring-up and `wait_oneshot` logic verbatim, and run
`cargo test -p sqe-coordinator --test it spark_access_control_e2e -- --ignored`. Resolve
ports from the stack's own `.env`, as the original does: a developer with
`RANGER_PORT=46080` must not silently hit a different Ranger.

- [ ] **Step 2: Add the target**

```makefile
# ── Spark access-control parity (Polaris + Ranger + Keycloak + Spark) ─────
# Separate from test-access-control on purpose: this one brings up `spark`
# (a JVM per query) and takes several minutes longer.
test-access-control-spark:
	@scripts/spark-access-control-test.sh
```

Add it to the `help` block alongside `test-access-control`.

- [ ] **Step 3: Verify from a cold stack**

```bash
cd quickstart/polaris-ranger-keycloak && docker compose down -v && cd -
make test-access-control-spark 2>&1 | tail -20
```

Expected: stack comes up, suite passes. A cold run is the only way to catch a missing
dependency in the bring-up list.

- [ ] **Step 4: Commit**

```bash
git add scripts/spark-access-control-test.sh Makefile
git commit -m "test(spark): make test-access-control-spark

Kept separate from test-access-control, which deliberately excludes spark and
data-seed from its dependency chain and stays fast. Verified from a torn-down
stack, the only way a missing bring-up dependency shows up."
```

---

### Task 7: Documentation and project state

**Files:**
- Modify: `docs/site/book/src/features/access-control-matrix.md`
- Modify: `docs/site/book/src/design-notes/ranger-access-control.md`
- Modify: `README.md`, `nextsteps.md`

- [ ] **Step 1: Record the five divergences in the matrix**

Identity assurance differs by tier; Polaris denial messages name principal and op;
Kyuubi row filters need the filter column projected; named mask types render differently
per engine; a refused write is refused at commit and can leave staged files. The tag gap
stays listed as open until the projector lands, with its fail-open direction stated.

- [ ] **Step 2: Add the two-tier section to the design note**

The identity routes, the defer policy and why it is not a hole, and the standing
constraint that any engine reading the frontend service must also authorize through
Polaris. While there, fix the stale section that still describes the deleted
`map_sql_to_ranger_access` and `build_resource_map`.

- [ ] **Step 3: Style gate**

```bash
grep -rn '—\|–\|→\|←\|▶' docs/site/book/src/features/access-control-matrix.md docs/site/book/src/design-notes/ranger-access-control.md
```

Expected: no output.

- [ ] **Step 4: Update roadmap and next steps, then commit**

```bash
git add docs/site/book/src/features/access-control-matrix.md docs/site/book/src/design-notes/ranger-access-control.md README.md nextsteps.md
git commit -m "docs(access-control): Spark parity, the defer policy, and five divergences"
```

---

### Task 8: Full verification and MR

- [ ] **Step 1: Strict clippy**

```bash
cargo clippy --all-targets --all-features -- -D warnings 2>&1 | tail -5; echo "EXIT=$?"
```

Read the exit code from `$?` directly. zsh uses `pipestatus`, not bash's `PIPESTATUS`,
so a `${PIPESTATUS[0]}` check prints nothing and tells you nothing.

- [ ] **Step 2: Workspace suite**

```bash
RUST_MIN_STACK=33554432 cargo test --workspace --exclude sqe-cli 2>&1 | tail -8; echo "EXIT=$?"
```

- [ ] **Step 3: Both e2e suites**

```bash
RUST_MIN_STACK=33554432 scripts/access-control-test.sh 2>&1 | tail -4        # expect 31 passed
RUST_MIN_STACK=33554432 scripts/spark-access-control-test.sh 2>&1 | tail -4  # expect all passed
```

- [ ] **Step 4: Open the MR**

```bash
git push -u origin feat/spark-ranger-access-control
glab mr create --fill --yes
```

Never `gh pr`: the canonical remote is GitLab, and `github` is a mirror that
`git remote -v` lists first.

---

## Follow-on plans, deliberately not in this one

- **Phase 2a, mask and row-filter parity.** Cheap once this harness exists, but it is a
  distinct deliverable: one policy in `query`, two engines, byte-identical output
  asserted directly rather than per engine.
- **Phase 2b, the tag projector.** Engine code, not config: `ALTER TABLE ... SET TAG`
  projects the association into Ranger's tag store so Kyuubi sees it. Until it lands, a
  tag-masked column is protected in SQE and returned raw by Spark.
- **Migrating the quickstart demo to per-user Spark identity.** `spark-defaults.conf`
  still connects as `root`. The suite injects its own catalog, so the demo is untouched
  by this plan. Changing it means reworking `test.sh` and `parity-test.sh`, which
  currently depend on root access.
