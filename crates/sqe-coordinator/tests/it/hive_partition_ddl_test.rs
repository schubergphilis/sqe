//! Cycle-2 dispatch: `ALTER TABLE ... ADD/DROP PARTITION`, `MSCK REPAIR
//! TABLE`, and `SHOW PARTITIONS` against a Hive root, through the real
//! `QueryHandler::execute` path (classifier -> dispatch -> handler ->
//! manifest store), not a handler called directly.

use std::path::PathBuf;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::RecordBatch;
use sqe_catalog::hive::provider::HiveMetadataCache;
use sqe_coordinator::{query_tracker::QueryTracker, QueryHandler, RuntimeCatalogRegistry};
use sqe_core::{SecretStore, Session, SqeConfig};
use sqe_metrics::audit::AuditLogger;
use sqe_policy::plan_rewriter::HiveCatalogs;
use sqe_policy::policy_store::InMemoryPolicyStore;
use sqe_policy::{PassthroughEnforcer, PolicyStore, ResolvedPolicy};
use tempfile::TempDir;

fn minimal_config(catalog_url: &str) -> SqeConfig {
    let toml_text = format!(
        "[coordinator]\n\n[auth]\n\n[catalog]\ncatalog_url = \"{catalog_url}\"\n"
    );
    let mut config: SqeConfig = toml::from_str(&toml_text).expect("minimal config parses");
    config.storage.tvf.allow_local_paths = true;
    config.storage.tvf.object_store_admin_roles = vec!["service_admin".to_string()];
    config
}

fn session_with_roles(roles: Vec<String>) -> Session {
    Session::new(
        format!("tester-{}", uuid::Uuid::new_v4()),
        sqe_core::SecretString::new(uuid::Uuid::new_v4().to_string()),
        None,
        chrono::Utc::now() + chrono::Duration::hours(1),
        roles,
    )
}

fn dummy_session() -> Session {
    session_with_roles(vec!["service_admin".to_string()])
}

async fn mock_polaris() -> wiremock::MockServer {
    let server = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/v1/config"))
        .respond_with(
            wiremock::ResponseTemplate::new(200).set_body_string(r#"{"overrides":{},"defaults":{}}"#),
        )
        .mount(&server)
        .await;
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/v1/namespaces"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_string(r#"{"namespaces":[]}"#))
        .mount(&server)
        .await;
    server
}

struct Harness {
    handler: QueryHandler,
    session: Session,
    audit: Arc<AuditLogger>,
    log_path: PathBuf,
    _dirs: Vec<TempDir>,
    _mock: wiremock::MockServer,
}

impl Harness {
    async fn execute_for_test(&self, sql: &str) -> sqe_core::Result<Vec<RecordBatch>> {
        self.handler.execute(&self.session, sql, None).await
    }

    fn audit_events_for_test(&self) -> Vec<String> {
        self.audit.flush();
        let content = std::fs::read_to_string(&self.log_path).unwrap_or_default();
        content
            .lines()
            .filter(|l| !l.is_empty())
            .map(str::to_string)
            .collect()
    }

    async fn count_sql(&self, sql: &str) -> i64 {
        let batches = self.execute_for_test(sql).await.expect(sql);
        batches
            .first()
            .expect("count(*) returns a batch")
            .column(0)
            .as_primitive::<Int64Type>()
            .value(0)
    }

    async fn show_partitions(&self) -> Vec<String> {
        let batches = self
            .execute_for_test("SHOW PARTITIONS lake.sales.orders")
            .await
            .expect("SHOW PARTITIONS");
        batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_string::<i32>()
                    .iter()
                    .filter_map(|s| s.map(str::to_string))
                    .collect::<Vec<_>>()
            })
            .collect()
    }
}

/// A harness with a Hive root `lake` attached and a partitioned CSV table
/// `lake.sales.orders` created over a two-partition fixture.
async fn harness_with_orders() -> Harness {
    let root_dir = tempfile::tempdir().expect("tempdir");
    let audit_dir = tempfile::tempdir().expect("tempdir");
    let log_path = audit_dir.path().join("audit.jsonl");
    let audit = Arc::new(AuditLogger::new(log_path.to_str().unwrap()).expect("audit logger opens"));
    let mock = mock_polaris().await;

    let config = minimal_config(&mock.uri());
    let tracker = Arc::new(QueryTracker::new(&config.query_history));
    let handler = QueryHandler::new(
        Arc::new(PassthroughEnforcer),
        None,
        config,
        None,
        None,
        None,
        Some(audit.clone()),
        tracker,
        None,
        None,
        None,
        RuntimeCatalogRegistry::new(),
        SecretStore::new(),
    )
    .expect("QueryHandler::new")
    .with_hive_registries(
        Arc::new(HiveCatalogs::new()),
        Arc::new(HiveMetadataCache::new()),
    );

    let session = dummy_session();
    let sql = format!(
        "ATTACH 'file://{}/wh' AS lake (TYPE hive_external)",
        root_dir.path().display()
    );
    handler
        .execute(&session, &sql, None)
        .await
        .expect("attach hive root");

    // Two-partition CSV fixture under the root's own prefix (unlike the
    // cycle-1 file's data-outside-root layout: index maintenance lists the
    // table location through the root's store).
    let data_dir = root_dir.path().join("wh/data");
    for (dt, rows) in [
        ("2026-01-01", "1,10.5\n2,20.0\n"),
        ("2026-01-02", "3,30.5\n"),
    ] {
        let partition = data_dir.join(format!("dt={dt}"));
        std::fs::create_dir_all(&partition).expect("mkdir partition");
        std::fs::write(partition.join("a.csv"), format!("id,amount\n{rows}"))
            .expect("write csv");
    }
    let ddl = format!(
        "CREATE EXTERNAL TABLE lake.sales.orders (id BIGINT, amount DOUBLE) \
         PARTITIONED BY (dt string) \
         ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE \
         LOCATION 'file://{}' \
         TBLPROPERTIES ('skip.header.line.count' = '1')",
        data_dir.display()
    );
    handler
        .execute(&session, &ddl, None)
        .await
        .expect("create orders");

    Harness {
        handler,
        session,
        audit,
        log_path,
        _dirs: vec![root_dir, audit_dir],
        _mock: mock,
    }
}

#[test]
fn msck_repair_then_show_partitions_reports_every_discovered_partition() {
    block_on_prod_stack(async {
    let h = harness_with_orders().await;
    h.execute_for_test("MSCK REPAIR TABLE lake.sales.orders")
        .await
        .expect("repair");
    let specs = h.show_partitions().await;
    assert_eq!(specs, vec!["dt=2026-01-01", "dt=2026-01-02"], "{specs:?}");
    });
}
#[test]
fn add_partition_on_an_existing_directory_is_queryable_via_show_partitions() {
    block_on_prod_stack(async {
    let h = harness_with_orders().await;
    let loc = format!(
        "file://{}/wh/data/dt=2026-01-02/",
        h._dirs[0].path().display()
    );
    h.execute_for_test(&format!(
        "ALTER TABLE lake.sales.orders ADD PARTITION (dt='2026-01-02') LOCATION '{loc}'"
    ))
    .await
    .expect("add existing partition");
    let specs = h.show_partitions().await;
    assert!(specs.contains(&"dt=2026-01-02".to_string()), "{specs:?}");
    });
}
#[test]
fn duplicate_add_errors_and_if_not_exists_noops() {
    block_on_prod_stack(async {
    let h = harness_with_orders().await;
    let loc = format!(
        "file://{}/wh/data/dt=2026-01-01/",
        h._dirs[0].path().display()
    );
    let sql = format!(
        "ALTER TABLE lake.sales.orders ADD PARTITION (dt='2026-01-01') LOCATION '{loc}'"
    );
    h.execute_for_test(&sql).await.expect("first add");
    let err = h
        .execute_for_test(&sql)
        .await
        .expect_err("duplicate without IF NOT EXISTS must error");
    assert!(err.to_string().contains("already exists"), "{err}");

    let ine = sql.replace("ADD PARTITION", "ADD IF NOT EXISTS PARTITION");
    h.execute_for_test(&ine).await.expect("IF NOT EXISTS no-op");
    assert_eq!(h.show_partitions().await.len(), 1);
    });
}

/// Production workers have an 8 MiB stack (`WORKER_STACK_BYTES` in
/// `sqe-coordinator` main). `#[tokio::test]` uses the 2 MiB default, which
/// is not enough for `QueryHandler::execute`'s future in debug.
fn block_on_prod_stack<F>(fut: F) -> F::Output
where
    F: std::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(1)
        .thread_stack_size(8 * 1024 * 1024)
        .build()
        .expect("runtime")
        .block_on(async { tokio::spawn(fut).await.expect("join") })
}
#[test]
fn drop_partition_removes_the_entry_and_leaves_the_data() {
    block_on_prod_stack(async {
    let h = harness_with_orders().await;
    h.execute_for_test("MSCK REPAIR TABLE lake.sales.orders")
        .await
        .expect("repair");
    h.execute_for_test("ALTER TABLE lake.sales.orders DROP PARTITION (dt='2026-01-02')")
        .await
        .expect("drop");
    assert_eq!(h.show_partitions().await, vec!["dt=2026-01-01"]);

    // Data untouched on disk.
    assert!(
        h._dirs[0]
            .path()
            .join("wh/data/dt=2026-01-02/a.csv")
            .exists(),
        "DROP PARTITION must never delete data files"
    );
    // ...but the dropped partition no longer reads.
    assert_eq!(
        h.count_sql("SELECT count(*) FROM lake.sales.orders").await,
        2
    );
    assert_eq!(
        h.count_sql("SELECT count(*) FROM lake.sales.orders WHERE dt = '2026-01-02'")
            .await,
        0
    );

    // Absent without IF EXISTS errors; with it, no-op.
    let err = h
        .execute_for_test("ALTER TABLE lake.sales.orders DROP PARTITION (dt='2026-01-02')")
        .await
        .expect_err("must error without IF EXISTS");
    assert!(err.to_string().contains("does not exist"), "{err}");
    h.execute_for_test(
        "ALTER TABLE lake.sales.orders DROP IF EXISTS PARTITION (dt='2026-01-02')",
    )
    .await
    .expect("IF EXISTS no-op");
    });
}

#[test]
fn drop_partition_purge_is_rejected_naming_cycle_4() {
    block_on_prod_stack(async {
    let h = harness_with_orders().await;
    let err = h
        .execute_for_test(
            "ALTER TABLE lake.sales.orders DROP PARTITION (dt='2026-01-02') PURGE",
        )
        .await
        .expect_err("PURGE must be refused");
    let msg = err.to_string();
    assert!(msg.contains("PURGE"), "{msg}");
    assert!(msg.contains("cycle 4"), "{msg}");
    });
}
#[test]
fn non_hive_names_keep_todays_routing() {
    block_on_prod_stack(async {
    let h = harness_with_orders().await;
    for sql in [
        "ALTER TABLE iceberg.hr.t ADD PARTITION (dt='x')",
        "MSCK REPAIR TABLE iceberg.hr.t",
        "SHOW PARTITIONS iceberg.hr.t",
    ] {
        let err = h
            .execute_for_test(sql)
            .await
            .expect_err("non-Hive names must not reach the Hive path");
        let msg = err.to_string();
        assert!(
            msg.contains("hive_external"),
            "{sql}: should name the feature: {msg}"
        );
    }
    });
}
#[test]
fn index_maintenance_emits_audit_events() {
    block_on_prod_stack(async {
    let h = harness_with_orders().await;
    h.execute_for_test("MSCK REPAIR TABLE lake.sales.orders")
        .await
        .expect("repair");
    h.execute_for_test("ALTER TABLE lake.sales.orders DROP PARTITION (dt='2026-01-02')")
        .await
        .expect("drop");
    let events = h.audit_events_for_test();
    // Resource identity is split across fields (catalog/namespace/name), so
    // match on the statement label plus the table name field, not a dotted
    // FQN no single line literally contains.
    for (statement, name) in [
        ("MSCK REPAIR TABLE", "\"name\":\"orders\""),
        ("ALTER TABLE DROP PARTITION", "\"name\":\"orders\""),
    ] {
        assert!(
            events
                .iter()
                .any(|e| e.contains(&format!("\"statement_type\":\"{statement}\""))
                    && e.contains(name)),
            "missing audit event for {statement}: {events:?}"
        );
    }
    });
}
#[test]
fn analyze_table_persists_row_counts_and_audits() {
    block_on_prod_stack(async {
    let h = harness_with_orders().await;
    h.execute_for_test("MSCK REPAIR TABLE lake.sales.orders")
        .await
        .expect("repair");
    h.execute_for_test("ANALYZE TABLE lake.sales.orders")
        .await
        .expect("analyze");
    let events = h.audit_events_for_test();
    assert!(
        events
            .iter()
            .any(|e| e.contains("\"statement_type\":\"ANALYZE TABLE\"")
                && e.contains("\"name\":\"orders\"")),
        "missing ANALYZE audit: {events:?}"
    );
    });
}
#[test]
fn create_with_unknown_projection_key_is_refused() {
    block_on_prod_stack(async {
    let h = harness_with_orders().await;
    let err = h
        .execute_for_test(
            "CREATE EXTERNAL TABLE lake.sales.proj (id BIGINT) \
             PARTITIONED BY (dt string) \
             STORED AS PARQUET LOCATION 'file:///tmp/x' \
             TBLPROPERTIES ('projection.enabled'='true', 'projection.dt.type'='enum', \
             'projection.dt.values'='a', 'projection.dt.bogus'='x')",
        )
        .await
        .expect_err("unknown projection key");
    assert!(
        err.to_string().contains("projection.dt.bogus"),
        "{err}"
    );
    });
}

/// `lake.sales.orders` exists, then a deny-all policy lands on it keyed the
/// way a `SELECT` resolves a Hive table (`{root}.{database}`, #715). The
/// table is created BEFORE the policy because CREATE is gated too (#717).
async fn harness_with_denied_orders() -> Harness {
    let store = Arc::new(InMemoryPolicyStore::new());
    let root_dir = tempfile::tempdir().expect("tempdir");
    let audit_dir = tempfile::tempdir().expect("tempdir");
    let log_path = audit_dir.path().join("audit.jsonl");
    let audit = Arc::new(AuditLogger::new(log_path.to_str().unwrap()).expect("audit logger opens"));
    let mock = mock_polaris().await;
    let config = minimal_config(&mock.uri());
    let tracker = Arc::new(QueryTracker::new(&config.query_history));
    let handler = QueryHandler::new(
        Arc::new(PassthroughEnforcer),
        Some(Arc::clone(&store) as Arc<dyn PolicyStore>),
        config,
        None,
        None,
        None,
        Some(audit.clone()),
        tracker,
        None,
        None,
        None,
        RuntimeCatalogRegistry::new(),
        SecretStore::new(),
    )
    .expect("QueryHandler::new")
    .with_hive_registries(
        Arc::new(HiveCatalogs::new()),
        Arc::new(HiveMetadataCache::new()),
    );
    let session = dummy_session();
    handler
        .execute(
            &session,
            &format!(
                "ATTACH 'file://{}/wh' AS lake (TYPE hive_external)",
                root_dir.path().display()
            ),
            None,
        )
        .await
        .expect("attach");
    let data_dir = root_dir.path().join("wh/data");
    std::fs::create_dir_all(data_dir.join("dt=2026-01-01")).unwrap();
    std::fs::write(
        data_dir.join("dt=2026-01-01/a.csv"),
        "id,amount\n1,10.5\n",
    )
    .unwrap();
    handler
        .execute(
            &session,
            &format!(
                "CREATE EXTERNAL TABLE lake.sales.orders (id BIGINT, amount DOUBLE) \
                 PARTITIONED BY (dt string) \
                 ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE \
                 LOCATION 'file://{}' TBLPROPERTIES ('skip.header.line.count' = '1')",
                data_dir.display()
            ),
            None,
        )
        .await
        .expect("create");
    store
        .add_table_policy(
            "lake.sales",
            "orders",
            ResolvedPolicy {
                row_filters: vec![datafusion::logical_expr::lit(false)],
                ..Default::default()
            },
        )
        .await;
    Harness {
        handler,
        session,
        audit,
        log_path,
        _dirs: vec![root_dir, audit_dir],
        _mock: mock,
    }
}

#[test]
fn denied_user_cannot_create_or_drop_and_show_tables_omits_the_table() {
    block_on_prod_stack(async {
        let h = harness_with_denied_orders().await;
        for sql in [
            "CREATE EXTERNAL TABLE lake.sales.orders (id BIGINT) STORED AS PARQUET \
             LOCATION 'file:///nowhere'",
            "DROP TABLE lake.sales.orders",
        ] {
            let err = h.execute_for_test(sql).await.expect_err(sql);
            assert!(err.to_string().contains("denied"), "{sql}: {err}");
        }
        let listed = h
            .execute_for_test("SHOW TABLES FROM lake.sales")
            .await
            .expect("SHOW TABLES");
        let names: Vec<String> = listed
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_string::<i32>()
                    .iter()
                    .filter_map(|s| s.map(str::to_string))
                    .collect::<Vec<_>>()
            })
            .collect();
        assert!(names.is_empty(), "denied table listed: {names:?}");
    });
}

#[test]
fn denied_user_cannot_add_drop_repair_or_analyze() {
    block_on_prod_stack(async {
        let h = harness_with_denied_orders().await;
        for sql in [
            "ALTER TABLE lake.sales.orders ADD PARTITION (dt='2026-01-01')",
            "ALTER TABLE lake.sales.orders DROP PARTITION (dt='2026-01-01')",
            "MSCK REPAIR TABLE lake.sales.orders",
            "ANALYZE TABLE lake.sales.orders",
        ] {
            let err = h.execute_for_test(sql).await.expect_err(sql);
            let msg = err.to_string();
            assert!(
                msg.contains("denied") || msg.contains("table not found"),
                "{sql}: {msg}"
            );
        }
        let show = h
            .execute_for_test("SHOW PARTITIONS lake.sales.orders")
            .await
            .expect_err("SHOW PARTITIONS");
        assert!(
            show.to_string().contains("table not found"),
            "{show}"
        );
    });
}

#[test]
fn msck_then_select_then_out_of_band_then_repair() {
    block_on_prod_stack(async {
        let h = harness_with_orders().await;
        h.execute_for_test("MSCK REPAIR TABLE lake.sales.orders")
            .await
            .expect("repair");
        assert_eq!(
            h.count_sql("SELECT count(*) FROM lake.sales.orders").await,
            3
        );
        let extra = h._dirs[0].path().join("wh/data/dt=2026-01-03");
        std::fs::create_dir_all(&extra).unwrap();
        std::fs::write(extra.join("a.csv"), "id,amount\n9,9.0\n").unwrap();
        assert_eq!(
            h.count_sql("SELECT count(*) FROM lake.sales.orders").await,
            3,
            "out-of-band partition is omitted until repair"
        );
        h.execute_for_test("MSCK REPAIR TABLE lake.sales.orders")
            .await
            .expect("repair again");
        assert_eq!(
            h.count_sql("SELECT count(*) FROM lake.sales.orders").await,
            4
        );
    });
}

#[test]
fn add_partition_for_a_new_directory_is_queryable() {
    block_on_prod_stack(async {
        let h = harness_with_orders().await;
        let extra = h._dirs[0].path().join("wh/data/dt=2026-01-03");
        std::fs::create_dir_all(&extra).unwrap();
        std::fs::write(extra.join("a.csv"), "id,amount\n9,9.0\n").unwrap();
        h.execute_for_test(
            "ALTER TABLE lake.sales.orders ADD PARTITION (dt='2026-01-03')",
        )
        .await
        .expect("add");
        assert_eq!(
            h.count_sql("SELECT count(*) FROM lake.sales.orders WHERE dt = '2026-01-03'")
                .await,
            1
        );
    });
}

#[test]
fn show_partitions_format_is_k_eq_v() {
    block_on_prod_stack(async {
        let h = harness_with_orders().await;
        h.execute_for_test("MSCK REPAIR TABLE lake.sales.orders")
            .await
            .expect("repair");
        let specs = h.show_partitions().await;
        for s in &specs {
            assert!(s.starts_with("dt="), "Athena-style spec, got {s}");
            assert!(!s.contains('/'), "single-key spec has no slash: {s}");
        }
    });
}

#[test]
fn add_on_projected_table_is_refused_through_sql() {
    block_on_prod_stack(async {
        let h = harness_with_orders().await;
        let loc = format!("file://{}/wh/proj", h._dirs[0].path().display());
        std::fs::create_dir_all(h._dirs[0].path().join("wh/proj")).unwrap();
        h.execute_for_test(&format!(
            "CREATE EXTERNAL TABLE lake.sales.proj (id BIGINT) \
             PARTITIONED BY (dt string) STORED AS PARQUET \
             LOCATION '{loc}' \
             TBLPROPERTIES ('projection.enabled'='true', \
             'projection.dt.type'='enum', 'projection.dt.values'='a')"
        ))
        .await
        .expect("create projected table");
        let add = h
            .execute_for_test("ALTER TABLE lake.sales.proj ADD PARTITION (dt='a')")
            .await
            .expect_err("ADD on projected table");
        assert!(add.to_string().contains("projection"), "{add}");
        let drop = h
            .execute_for_test("ALTER TABLE lake.sales.proj DROP PARTITION (dt='a')")
            .await
            .expect_err("DROP on projected table");
        assert!(drop.to_string().contains("projection"), "{drop}");
        h.execute_for_test("MSCK REPAIR TABLE lake.sales.proj")
            .await
            .expect("MSCK on a projected table is a no-op");
    });
}

#[test]
fn msck_skips_junk_directories_through_sql() {
    block_on_prod_stack(async {
        let h = harness_with_orders().await;
        let junk = h._dirs[0].path().join("wh/data/not-a-partition");
        std::fs::create_dir_all(&junk).unwrap();
        std::fs::write(junk.join("a.csv"), "id,amount\n9,9.0\n").unwrap();
        std::fs::write(h._dirs[0].path().join("wh/data/orphan.csv"), "id,amount\n8,8.0\n")
            .unwrap();
        h.execute_for_test("MSCK REPAIR TABLE lake.sales.orders")
            .await
            .expect("junk is skipped");
        let specs = h.show_partitions().await;
        assert_eq!(specs, vec!["dt=2026-01-01", "dt=2026-01-02"], "{specs:?}");
    });
}

#[test]
fn show_partitions_without_an_index_lists_from_storage() {
    block_on_prod_stack(async {
        let h = harness_with_orders().await;
        let specs = h.show_partitions().await;
        assert_eq!(specs, vec!["dt=2026-01-01", "dt=2026-01-02"], "{specs:?}");
    });
}

#[test]
fn add_two_partitions_in_one_statement() {
    block_on_prod_stack(async {
        let h = harness_with_orders().await;
        h.execute_for_test(
            "ALTER TABLE lake.sales.orders ADD \
             PARTITION (dt='2026-01-01'), \
             PARTITION (dt='2026-01-02')",
        )
        .await
        .expect("multi add");
        let specs = h.show_partitions().await;
        assert_eq!(specs, vec!["dt=2026-01-01", "dt=2026-01-02"], "{specs:?}");
        assert_eq!(
            h.count_sql("SELECT count(*) FROM lake.sales.orders").await,
            3
        );
    });
}

#[test]
fn out_of_prefix_partition_is_queryable() {
    block_on_prod_stack(async {
        let h = harness_with_orders().await;
        let elsewhere = h._dirs[0].path().join("wh/elsewhere/dt=2026-04-01");
        std::fs::create_dir_all(&elsewhere).expect("mkdir");
        std::fs::write(elsewhere.join("b.csv"), "id,amount\n9,90.0\n").expect("write");
        let loc = format!("file://{}/", elsewhere.display());
        h.execute_for_test(&format!(
            "ALTER TABLE lake.sales.orders ADD PARTITION (dt='2026-04-01') LOCATION '{loc}'"
        ))
        .await
        .expect("add");
        assert_eq!(
            h.count_sql("SELECT count(*) FROM lake.sales.orders WHERE dt = '2026-04-01'")
                .await,
            1
        );
        h.execute_for_test("ALTER TABLE lake.sales.orders DROP PARTITION (dt='2026-04-01')")
            .await
            .expect("drop");
        assert!(
            elsewhere.join("b.csv").exists(),
            "DROP must leave the out-of-prefix files in place"
        );
        assert_eq!(
            h.count_sql("SELECT count(*) FROM lake.sales.orders WHERE dt = '2026-04-01'")
                .await,
            0
        );
    });
}

#[test]
fn if_not_exists_does_not_replace_the_original_location() {
    block_on_prod_stack(async {
        let h = harness_with_orders().await;
        let orig = format!(
            "file://{}/wh/data/dt=2026-01-01/",
            h._dirs[0].path().display()
        );
        h.execute_for_test(&format!(
            "ALTER TABLE lake.sales.orders ADD PARTITION (dt='2026-01-01') LOCATION '{orig}'"
        ))
        .await
        .expect("first");
        let elsewhere = h._dirs[0].path().join("wh/elsewhere/dt=2026-01-01");
        std::fs::create_dir_all(&elsewhere).unwrap();
        std::fs::write(elsewhere.join("b.csv"), "id,amount\n99,99.0\n").unwrap();
        let other = format!("file://{}/", elsewhere.display());
        h.execute_for_test(&format!(
            "ALTER TABLE lake.sales.orders ADD IF NOT EXISTS PARTITION (dt='2026-01-01') \
             LOCATION '{other}'"
        ))
        .await
        .expect("no-op");
        assert_eq!(
            h.count_sql("SELECT count(*) FROM lake.sales.orders WHERE dt = '2026-01-01'")
                .await,
            2,
            "the original two-row partition must still be the one scanned"
        );
    });
}

#[test]
fn default_partition_added_by_sql_reads_as_null() {
    block_on_prod_stack(async {
        let h = harness_with_orders().await;
        let sentinel = h
            ._dirs[0]
            .path()
            .join("wh/data/dt=__HIVE_DEFAULT_PARTITION__");
        std::fs::create_dir_all(&sentinel).unwrap();
        std::fs::write(sentinel.join("a.csv"), "id,amount\n8,8.0\n").unwrap();
        h.execute_for_test(
            "ALTER TABLE lake.sales.orders ADD PARTITION (dt='__HIVE_DEFAULT_PARTITION__')",
        )
        .await
        .expect("add sentinel");
        assert_eq!(
            h.count_sql("SELECT count(*) FROM lake.sales.orders WHERE dt IS NULL")
                .await,
            1
        );
    });
}

#[test]
fn show_partitions_on_a_missing_table_is_not_found() {
    block_on_prod_stack(async {
        let h = harness_with_orders().await;
        let err = h
            .execute_for_test("SHOW PARTITIONS lake.sales.nope")
            .await
            .expect_err("missing table");
        let msg = err.to_string().to_lowercase();
        assert!(
            msg.contains("not found") || msg.contains("nope"),
            "{err}"
        );
    });
}

#[test]
fn analyze_then_read_still_returns_the_same_rows() {
    block_on_prod_stack(async {
        let h = harness_with_orders().await;
        h.execute_for_test("MSCK REPAIR TABLE lake.sales.orders")
            .await
            .expect("repair");
        h.execute_for_test("ANALYZE TABLE lake.sales.orders")
            .await
            .expect("analyze");
        assert_eq!(
            h.count_sql("SELECT count(*) FROM lake.sales.orders").await,
            3
        );
    });
}
