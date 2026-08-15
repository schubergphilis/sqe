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
    /// The catalog carries NO identity, so the request never became an
    /// authorization question. Polaris rejects it before it can answer with a
    /// structured error, and Iceberg reports an unparseable error body rather than
    /// a `ForbiddenException`.
    ///
    /// Distinct from `Polaris` on purpose. A 403 on `LOAD_TABLE` means "we know who
    /// you are and you may not"; this means "we do not know who you are". Both
    /// refuse, and a test that accepted either could not tell a revoked grant from
    /// a catalog that was never given a token.
    Unauthenticated,
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
    // Checked before the ForbiddenException arm: an unauthenticated catalog produces
    // no parseable error body, so there is no `ForbiddenException` to find and this
    // would otherwise fall through to `Other` and read like an unrelated crash.
    if output.contains("No content to map due to end-of-input")
        || output.contains("Unable to parse error response")
    {
        return DenialTier::Unauthenticated;
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
        if let Some(line) = output
            .lines()
            .find(|l| l.contains(marker) && !is_benign_noise(l))
        {
            return DenialTier::Other(line.trim().to_string());
        }
    }
    DenialTier::None
}

/// Lines that carry an exception name but are not a query failure.
///
/// `PartialGroupNameException` appears on EVERY run: the container has no OS user
/// named `bob`, so Hadoop's Unix group lookup fails. It is irrelevant, because
/// Kyuubi resolves role membership from the Ranger user store rather than from
/// Unix groups, and the query proceeds normally.
fn is_benign_noise(line: &str) -> bool {
    line.contains("PartialGroupNameException")
        || line.contains("unable to return groups for user")
        || line.contains("failed to create script engine")
        || line.contains("failed to initialize condition")
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

    /// Assert the catalog had NO usable identity, so the request never reached an
    /// authorization decision.
    pub fn expect_unauthenticated(&self, what: &str) {
        assert_eq!(
            self.tier,
            DenialTier::Unauthenticated,
            "{what}: expected an unauthenticated catalog, got {:?}\n{}",
            self.tier,
            self.raw
        );
    }

    /// Single scalar cell, for `SELECT count(*)`.
    pub fn scalar(&self, what: &str) -> String {
        let rows = self.expect_ok(what);
        assert_eq!(rows.len(), 1, "{what}: expected 1 row, got {rows:?}");
        assert_eq!(rows[0].len(), 1, "{what}: expected 1 column, got {rows:?}");
        rows[0][0].clone()
    }
}

/// The Ranger service KYUUBI reads, which is NOT the service SQE reads.
///
/// SQE's tests point at the test-owned `sqe_ac_hive`. Kyuubi reads whatever
/// `ranger.plugin.spark.service.name` names in the Spark container's
/// `ranger-spark-security.xml`, which the quickstart sets to `query`. The
/// distinction is easy to miss and quietly fatal: a precondition asserted against
/// the test-owned service says nothing about what Kyuubi will do.
/// `kyuubi_service_in_container` re-reads the container so this constant cannot
/// drift away from the deployed config unnoticed.
pub const KYUUBI_SERVICE: &str = "query";

/// The service name the Spark container's Ranger plugin config actually names.
///
/// Returns `None` when the file or the property is unreadable, which is itself
/// worth failing on: without that config Kyuubi enforces nothing.
pub async fn kyuubi_service_in_container() -> Option<String> {
    let file = compose_file();
    let out = tokio::task::spawn_blocking(move || {
        std::process::Command::new("docker")
            .args([
                "compose", "-f", &file, "exec", "-T", "spark", "sh", "-c",
                "sed -n '/ranger.plugin.spark.service.name/,/<\\/property>/p'                  /opt/spark/conf/ranger-spark-security.xml",
            ])
            .output()
    })
    .await
    .ok()?
    .ok()?;
    let text = String::from_utf8_lossy(&out.stdout).to_string();
    between(&text, "<value>", "</value>")
}

/// The catalog name the suite registers inside Spark. Deliberately not
/// `sales_wh`, which `spark-defaults.conf` already binds to the `root` service
/// account: a per-user catalog must not inherit those credentials.
pub const SPARK_CATALOG: &str = "acwh";

/// Run `sql` in the quickstart's Spark container as `session`'s user.
///
/// `session`'s bearer token gives POLARIS the identity. `hadoop_user` gives
/// KYUUBI its (merely asserted) one. Callers normally pass the same person.
pub async fn spark_sql(session: &sqe_core::Session, hadoop_user: &str, sql: &str) -> SparkOutcome {
    spark_sql_inner(session, hadoop_user, sql, None).await
}

/// Same, but make Kyuubi read `frontend_service` instead of the one the container
/// is configured with.
///
/// Cross-engine parity needs BOTH engines reading the SAME service: SQE's tests
/// point at the test-owned `sqe_ac_hive`, while the container names the
/// quickstart's `query`. Writing mask policies into `query` instead would change
/// the bundle the demo's `parity-test.sh` cross-compares against, which is exactly
/// what the test-owned services exist to avoid.
pub async fn spark_sql_on_service(
    session: &sqe_core::Session,
    hadoop_user: &str,
    frontend_service: &str,
    sql: &str,
) -> SparkOutcome {
    spark_sql_inner(session, hadoop_user, sql, Some(frontend_service)).await
}

async fn spark_sql_inner(
    session: &sqe_core::Session,
    hadoop_user: &str,
    sql: &str,
    frontend_service: Option<&str>,
) -> SparkOutcome {
    let token = session.access_token().expose().to_string();
    let sql = sql.to_string();
    let hadoop_user = hadoop_user.to_string();
    let frontend = frontend_service.map(str::to_string);
    // tokio is built here without the `process` feature, so shell out on a
    // blocking thread rather than widening a workspace dependency for a test
    // helper.
    tokio::task::spawn_blocking(move || {
        run_blocking(&token, &hadoop_user, &sql, frontend.as_deref())
    })
    .await
    .expect("spark-sql blocking task panicked")
}

/// Ranger plugin config naming `service`, written into the container along with a
/// FRESH policy-cache directory.
///
/// The cache is wiped every call on purpose. The plugin persists the downloaded
/// bundle and refreshes on a 10s poll, so a short-lived `spark-sql` JVM can
/// otherwise enforce a bundle from before the policy under test existed, and a
/// parity assertion taken too soon proves nothing.
fn write_plugin_conf(service: &str) -> (String, String) {
    let conf_dir = format!("/tmp/rgconf-{service}");
    let cache_dir = format!("/tmp/rgcache-{service}");
    let xml = format!(
        r#"<?xml version="1.0"?>
<configuration>
  <property><name>ranger.plugin.spark.policy.rest.url</name><value>http://ranger-admin:6080</value></property>
  <property><name>ranger.plugin.spark.service.name</name><value>{service}</value></property>
  <property><name>ranger.plugin.spark.policy.cache.dir</name><value>{cache_dir}</value></property>
  <property><name>ranger.plugin.spark.policy.pollIntervalMs</name><value>5000</value></property>
  <property><name>ranger.plugin.spark.plugin.mode</name><value>ACTIVE</value></property>
  <property><name>ranger.plugin.spark.enable.implicit.userstore.enricher</name><value>true</value></property>
  <property><name>ranger.plugin.spark.policy.rest.client.username</name><value>admin</value></property>
  <property><name>ranger.plugin.spark.policy.rest.client.password</name><value>rangerR0cks!</value></property>
</configuration>
"#
    );
    // Content goes through argv, not the shell, so the XML needs no quoting.
    let script = r#"mkdir -p "$1" && printf '%s' "$2" > "$1"/ranger-spark-security.xml                     && rm -rf "$3" && mkdir -p "$3" && chmod -R 777 "$1" "$3""#;
    let out = std::process::Command::new("docker")
        .args([
            "compose",
            "-f",
            &compose_file(),
            "exec",
            "-T",
            "-u",
            "root",
            "spark",
            "sh",
            "-c",
            script,
            "sh",
            &conf_dir,
            &xml,
            &cache_dir,
        ])
        .output()
        .expect("write the ranger plugin conf into the spark container");
    assert!(
        out.status.success(),
        "writing {conf_dir} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    (conf_dir, cache_dir)
}

fn run_blocking(
    token: &str,
    hadoop_user: &str,
    sql: &str,
    frontend_service: Option<&str>,
) -> SparkOutcome {
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
    if let Some(service) = frontend_service {
        let (conf_dir, _cache) = write_plugin_conf(service);
        // --driver-class-path puts the written conf AHEAD of /opt/spark/conf, which
        // is how the plugin resolves ranger-spark-security.xml as a classpath
        // resource. The conf.dir property alone is not enough.
        cmd.arg("--driver-class-path").arg(&conf_dir);
        cmd.arg("--conf")
            .arg(format!("spark.kyuubi.authz.ranger.conf.dir={conf_dir}"));
    }
    cmd.arg("-e").arg(sql);

    let out = cmd.output().expect("spawn docker compose exec spark");
    let raw = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    // The exit code is the authority on whether the statement ran. Text
    // classification only explains a FAILURE; used as the success signal it
    // misreads routine log noise as a query error.
    let tier = if out.status.success() {
        DenialTier::None
    } else {
        classify(&raw)
    };
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
                && !l.contains("PartialGroupNameException")
                && !l.starts_with("id: ")
                && !l.starts_with("Spark ")
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

    /// spark-sql logs this on every run and it is not a failure. Before the
    /// exit-code gate, a SUCCESSFUL read was reported as a failure because the
    /// line contains the word "Exception".
    #[test]
    fn the_unix_group_lookup_warning_is_not_a_failure() {
        let noisy = "26/08/06 09:33:55 WARN ShellBasedUnixGroupsMapping: unable to \
                     return groups for user carol\n\
                     PartialGroupNameException The user name 'carol' is not found.\n\
                     EU\nUS";
        assert_eq!(classify(noisy), DenialTier::None);
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

#[cfg(test)]
mod unauthenticated_tests {
    use super::*;

    /// A catalog with no credential and no token cannot authenticate, so Polaris
    /// never answers with a structured error and Iceberg reports an unparseable
    /// body. Measured on the migrated quickstart.
    #[test]
    fn an_unauthenticated_catalog_is_its_own_tier() {
        let err = "26/08/08 09:33 WARN ErrorHandlers: Unable to parse error response\n\
                   java.io.UncheckedIOException: org.apache.iceberg.shaded.com.\
                   fasterxml.jackson.databind.exc.MismatchedInputException: No content \
                   to map due to end-of-input";
        assert_eq!(classify(err), DenialTier::Unauthenticated);
    }

    /// It must NOT be confused with a real authorization denial. A 403 on
    /// LOAD_TABLE means the caller was identified and refused; the tier above means
    /// the caller was never identified. A test accepting either could not tell a
    /// revoked grant from a catalog nobody gave a token.
    #[test]
    fn it_is_not_confused_with_a_polaris_denial() {
        let forbidden = "org.apache.iceberg.exceptions.ForbiddenException: Forbidden: \
                         Principal 'bob' is not authorized for op 'LOAD_TABLE'";
        assert!(matches!(classify(forbidden), DenialTier::Polaris { .. }));
    }
}
