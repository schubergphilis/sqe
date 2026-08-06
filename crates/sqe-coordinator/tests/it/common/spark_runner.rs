//! Runs `spark-sql` inside the quickstart's `spark` container and attributes any
//! authorization failure to the tier that produced it.
//!
//! TWO tiers can refuse the same query, and which one did is the whole point of
//! the Spark suite:
//!
//! - **Polaris**, enforcing the `polaris` Ranger service against the bearer
//!   token: `ForbiddenException: ... not authorized for op 'LOAD_TABLE'`.
//! - **Kyuubi**, enforcing the frontend service against `HADOOP_USER_NAME`:
//!   `AccessControlException: Permission denied: user [...] does not have [...]`.
//!
//! Object level belongs to Polaris. Kyuubi defers via the blanket `policyType-0`
//! item granted to group `public` on the frontend service. A test that asserts
//! only "the query failed" therefore passes when the WRONG tier refused, which is
//! exactly the state the defer item exists to prevent. Every assertion here names
//! the tier.
//!
//! Identity reaches the two tiers by different routes, and they are passed
//! separately on purpose: the object tier verifies a JWT signature, the
//! fine-grained tier trusts an asserted OS username, and one test deliberately
//! mismatches them to document the split.

/// Which tier refused, and what it said.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub enum DenialTier {
    /// No failure detected.
    None,
    /// Polaris refused. `op` is the Polaris operation name.
    Polaris { principal: String, op: String },
    /// Kyuubi refused before Polaris was consulted.
    Kyuubi { user: String, privilege: String },
    /// Kyuubi cannot apply a row filter over a column the query does not project
    /// (Kyuubi #6889). A bug, not enforcement, and never counted as a denial.
    KyuubiRowFilterBug,
    /// Failed for some other reason; carries the first useful line.
    Other(String),
}

/// Attribute a `spark-sql` failure to a tier.
///
/// Pure on purpose: the classifier is the part that must not be wrong, and this
/// way it is unit-tested without Docker or a live stack.
pub fn classify(output: &str) -> DenialTier {
    // Checked first: the row-filter bug surfaces as an AnalysisException and
    // would otherwise fall through to `Other`, reading like a real failure.
    if output.contains("MISSING_ATTRIBUTES") {
        return DenialTier::KyuubiRowFilterBug;
    }
    if let Some(rest) = output.split("AccessControlException").nth(1) {
        // Permission denied: user [bob] does not have [select] privilege on [db/t/c]
        return DenialTier::Kyuubi {
            user: between(rest, "user [", "]").unwrap_or_default(),
            privilege: between(rest, "have [", "]").unwrap_or_default(),
        };
    }
    if let Some(rest) = output.split("ForbiddenException").nth(1) {
        // Forbidden: Principal 'bob' is not authorized for op 'LOAD_TABLE'
        return DenialTier::Polaris {
            principal: between(rest, "Principal '", "'").unwrap_or_default(),
            op: between(rest, "for op '", "'").unwrap_or_default(),
        };
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

/// What one `spark-sql` invocation produced.
pub struct SparkOutcome {
    pub rows: Vec<Vec<String>>,
    pub tier: DenialTier,
    /// Full stdout+stderr, so a panic message can show what actually happened.
    pub raw: String,
}

#[allow(dead_code)]
impl SparkOutcome {
    /// Assert the query succeeded, and hand back the rows.
    pub fn expect_ok(&self, what: &str) -> &Vec<Vec<String>> {
        assert_eq!(
            self.tier,
            DenialTier::None,
            "{what}: expected success, got {:?}\n{}",
            self.tier,
            self.raw
        );
        &self.rows
    }

    /// Assert POLARIS refused, naming the operation.
    ///
    /// A Kyuubi denial fails here deliberately. Accepting it would let the suite
    /// pass with the defer item missing, having never reached the tier under test.
    pub fn expect_polaris_denial(&self, op: &str, what: &str) {
        match &self.tier {
            DenialTier::Polaris { op: got, .. } if got == op => {}
            DenialTier::Polaris { op: got, principal } => panic!(
                "{what}: Polaris refused '{got}' for '{principal}', expected '{op}'\n{}",
                self.raw
            ),
            DenialTier::Kyuubi { user, privilege } => panic!(
                "{what}: expected a POLARIS denial on '{op}', but KYUUBI refused \
                 [{privilege}] for [{user}] before Polaris was consulted.\n\
                 The blanket policyType-0 item for group 'public' is missing from \
                 the frontend service, so this never tested object level at all.\n{}",
                self.raw
            ),
            other => panic!(
                "{what}: expected a POLARIS denial on '{op}', got {other:?}\n{}",
                self.raw
            ),
        }
    }

    /// Single scalar cell, for `SELECT count(*)`.
    pub fn scalar(&self, what: &str) -> String {
        let rows = self.expect_ok(what);
        assert_eq!(rows.len(), 1, "{what}: expected 1 row, got {rows:?}");
        assert_eq!(rows[0].len(), 1, "{what}: expected 1 column, got {rows:?}");
        rows[0][0].clone()
    }
}

/// The catalog name the suite registers inside Spark. Deliberately not
/// `sales_wh`, which `spark-defaults.conf` already binds to the `root` service
/// account: a per-user catalog must not inherit those credentials.
pub const SPARK_CATALOG: &str = "ac";

/// Run `sql` in the quickstart's Spark container as `session`'s user.
///
/// `session`'s bearer token gives POLARIS the identity. `hadoop_user` gives
/// KYUUBI its (merely asserted) one. Callers normally pass the same person.
pub async fn spark_sql(
    session: &sqe_core::Session,
    hadoop_user: &str,
    sql: &str,
) -> SparkOutcome {
    let token = session.access_token().expose().to_string();
    let sql = sql.to_string();
    let hadoop_user = hadoop_user.to_string();
    // tokio is built here without the `process` feature, so shell out on a
    // blocking thread rather than widening a workspace dependency for a test
    // helper.
    tokio::task::spawn_blocking(move || run_blocking(&token, &hadoop_user, &sql))
        .await
        .expect("spark-sql blocking task panicked")
}

fn run_blocking(token: &str, hadoop_user: &str, sql: &str) -> SparkOutcome {
    let c = SPARK_CATALOG;
    let mut cmd = std::process::Command::new("docker");
    cmd.args([
        "compose",
        "-f",
        &compose_file(),
        "exec",
        "-T",
        "-e",
        &format!("HADOOP_USER_NAME={hadoop_user}"),
        "spark",
        "/opt/spark/bin/spark-sql",
    ]);
    // Each --conf is TWO argv entries. One combined string arrives as a single
    // argument and spark-sql answers `Unrecognized option: --conf ...`.
    for kv in [
        "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.apache.kyuubi.plugin.spark.authz.ranger.RangerSparkExtension".to_string(),
        format!("spark.sql.catalog.{c}=org.apache.iceberg.spark.SparkCatalog"),
        format!("spark.sql.catalog.{c}.catalog-impl=org.apache.iceberg.rest.RESTCatalog"),
        format!("spark.sql.catalog.{c}.uri=http://polaris:8181/api/catalog"),
        format!("spark.sql.catalog.{c}.warehouse=sales_wh"),
        format!("spark.sql.catalog.{c}.token={token}"),
        // Load-bearing. With refresh ON, Iceberg exchanges this external
        // Keycloak JWT against Polaris's own token endpoint and the identity
        // silently reverts to the service account, so every denial test passes
        // for the wrong reason.
        format!("spark.sql.catalog.{c}.token-refresh-enabled=false"),
        format!("spark.sql.catalog.{c}.header.Polaris-Realm=iceberg-ranger"),
        format!("spark.sql.catalog.{c}.io-impl=org.apache.iceberg.aws.s3.S3FileIO"),
        format!("spark.sql.catalog.{c}.s3.endpoint=http://rustfs:9000"),
        format!("spark.sql.catalog.{c}.s3.path-style-access=true"),
        format!("spark.sql.catalog.{c}.s3.access-key-id=s3admin"),
        format!("spark.sql.catalog.{c}.s3.secret-access-key=s3adminpw"),
    ] {
        cmd.arg("--conf").arg(kv);
    }
    cmd.arg("-e").arg(sql);

    let out = cmd.output().expect("spawn docker compose exec spark");
    let raw = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    let tier = classify(&raw);
    SparkOutcome {
        rows: parse_rows(&raw),
        tier,
        raw,
    }
}

/// `spark-sql` prints tab-separated result rows interleaved with log lines. Keep
/// only lines that are plausibly data.
fn parse_rows(raw: &str) -> Vec<Vec<String>> {
    raw.lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .filter(|l| {
            // Log lines start with a two-digit year in this image (`26/08/06 ...`).
            !l.starts_with("26/")
                && !l.contains("WARN")
                && !l.contains("ERROR")
                && !l.contains("INFO")
                && !l.starts_with("Time taken")
                && !l.contains("Exception")
                && !l.starts_with("at ")
                && !l.starts_with("SLF4J")
                && !l.starts_with("Setting default log level")
                && !l.starts_with("To adjust logging level")
        })
        .map(|l| l.split('\t').map(|c| c.trim().to_string()).collect())
        .collect()
}

fn compose_file() -> String {
    let manifest = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".into());
    std::path::Path::new(&manifest)
        .parent()
        .and_then(|p| p.parent())
        .unwrap_or(std::path::Path::new("."))
        .join("quickstart/polaris-ranger-keycloak/docker-compose.yml")
        .to_string_lossy()
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_polaris_denial_is_attributed_to_polaris() {
        let err = "org.apache.iceberg.exceptions.ForbiddenException: Forbidden: \
                   Principal 'bob' is not authorized for op 'LOAD_TABLE'";
        match classify(err) {
            DenialTier::Polaris { principal, op } => {
                assert_eq!(principal, "bob");
                assert_eq!(op, "LOAD_TABLE");
            }
            other => panic!("expected Polaris, got {other:?}"),
        }
    }

    #[test]
    fn a_kyuubi_denial_is_attributed_to_kyuubi() {
        let err = "org.apache.kyuubi.plugin.spark.authz.AccessControlException: \
                   Permission denied: user [bob] does not have [select] privilege \
                   on [acdemo/orders/id]";
        match classify(err) {
            DenialTier::Kyuubi { user, privilege } => {
                assert_eq!(user, "bob");
                assert_eq!(privilege, "select");
            }
            other => panic!("expected Kyuubi, got {other:?}"),
        }
    }

    /// The defer item exists so Kyuubi stops denying. If a Kyuubi denial were
    /// ever classified as a Polaris one, the whole suite would pass with the
    /// item missing, having never exercised object level.
    #[test]
    fn the_two_denials_are_never_confused() {
        let kyuubi = "AccessControlException: Permission denied: user [bob] does \
                      not have [select] privilege on [ac/orders/id]";
        assert!(matches!(classify(kyuubi), DenialTier::Kyuubi { .. }));
        let polaris = "ForbiddenException: Forbidden: Principal 'bob' is not \
                       authorized for op 'ADD_TABLE_SNAPSHOT'";
        assert!(matches!(classify(polaris), DenialTier::Polaris { .. }));
    }

    /// A write refusal lands on the snapshot commit, not on the load. A test
    /// asserting LOAD_TABLE for a denied INSERT would be wrong.
    #[test]
    fn a_write_denial_names_the_snapshot_commit() {
        let err = "org.apache.iceberg.exceptions.ForbiddenException: Forbidden: \
                   Principal 'bob' is not authorized for op 'ADD_TABLE_SNAPSHOT'";
        match classify(err) {
            DenialTier::Polaris { op, .. } => assert_eq!(op, "ADD_TABLE_SNAPSHOT"),
            other => panic!("expected Polaris, got {other:?}"),
        }
    }

    #[test]
    fn the_kyuubi_row_filter_bug_is_not_a_denial() {
        let err = "org.apache.spark.sql.AnalysisException: [MISSING_ATTRIBUTES] \
                   Resolved attribute(s) region#12 missing";
        assert_eq!(classify(err), DenialTier::KyuubiRowFilterBug);
    }

    #[test]
    fn a_clean_run_has_no_denial() {
        assert_eq!(
            classify("1\txxx-xx-1111\nTime taken: 2.7 seconds, Fetched 1 row(s)"),
            DenialTier::None
        );
    }

    #[test]
    fn rows_are_parsed_out_of_the_log_noise() {
        let out = "26/08/06 09:15:17 WARN ObjectStore: Failed to get database\n\
                   SLF4J: See http://www.slf4j.org/codes.html for details\n\
                   1\txxx-xx-1111\n\
                   2\txxx-xx-2222\n\
                   Time taken: 4.187 seconds, Fetched 2 row(s)";
        assert_eq!(
            parse_rows(out),
            vec![
                vec!["1".to_string(), "xxx-xx-1111".to_string()],
                vec!["2".to_string(), "xxx-xx-2222".to_string()],
            ]
        );
    }
}
