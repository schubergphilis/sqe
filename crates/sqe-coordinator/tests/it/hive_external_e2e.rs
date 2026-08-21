//! End-to-end coverage for Hive external tables against REAL object
//! storage (RustFS, the S3-compatible store `docker-compose.test.yml`
//! brings up). Everything else this feature has, at the end of Task 10b,
//! is a unit test, an in-process `QueryHandler::execute()` call, or one
//! embedded-mode two-process check against the local filesystem
//! (`crates/sqe-cli/src/embedded.rs`, `crates/sqe-coordinator/tests/hive_ddl_test.rs`).
//! Nothing before this file has ever run a Hive table against a real
//! store, so this suite exists to catch what only a real store can break:
//! real SigV4 auth, a real listing of a real prefix, a real Parquet
//! footer, a real second bucket read from a second, independent session.
//!
//! Local only. There is no CI equivalent: issue #387 removed the dind
//! jobs, so `make test-integration` is the gate. Bring the stack up with
//! `make test-integration` (it preflights fixed ports and bootstraps for
//! you), or by hand: `docker compose -f docker-compose.test.yml up -d`
//! then `./scripts/bootstrap-test.sh`. Tear down with
//! `make test-integration-down`.
//!
//! ## Distribution: this scan is coordinator-local, verified by reading
//!
//! Task 10b's report left an open, load-bearing question: is a Hive
//! `ListingTable` scan eligible for distribution to a worker, or
//! coordinator-local by construction? Every existing Hive test runs with
//! `worker_registry: None`, so nothing had answered it before this task.
//!
//! Answer, read from the coordinator's own dispatch code rather than
//! inferred (line numbers below drift as the coordinator changes; the
//! function and helper names do not): `QueryHandler::try_distribute` in
//! `crates/sqe-coordinator/src/query_handler.rs` walks the physical plan
//! looking ONLY for `IcebergScanExec` nodes, via its private
//! `find_iceberg_scan` helper, which downcasts every node in the tree
//! against that one concrete type and returns `None` if none match. When
//! `find_iceberg_scan` returns `None`, `try_distribute` returns the plan
//! UNCHANGED (its own log line names the reason: "No IcebergScanExec
//! found in plan, executing locally") -- there is no fallback branch that
//! inspects a `ListingTable`/`DataSourceExec` scan for distributability by
//! any other means. A Hive external table's physical
//! scan is DataFusion's own `ListingTable` machinery (wrapped, per Task
//! 6's `HiveTableProvider`), never an `IcebergScanExec`, so every Hive
//! query in this cycle runs coordinator-local, unconditionally, regardless
//! of `worker_registry`. This is a structural fact about the current
//! dispatch code, not a configuration a deployment could flip.
//!
//! Consequence: a coordinator-plus-worker scenario for a Hive scan would
//! be testing a code path that provably cannot be reached in this cycle.
//! Adding one now would be theater, not proof, so this file does not add
//! one. The moment cycle 2 (or later) teaches `try_distribute` to consider
//! non-Iceberg scans, that change needs its own worker-fragment test as
//! part of the SAME change, because nothing here or in Task 10b's suite
//! would catch a Hive scan silently failing plan serialization or
//! worker-side object-store resolution.
//!
//! ## Allowlist and credentials
//!
//! Per Task 8's spec correction, Hive reads use the ENGINE's storage
//! credentials (`[storage]` in config), never the caller's -- there is no
//! mechanism to vend object-store credentials for a raw prefix. Per-user
//! authorization survives entirely through `[storage.tvf]
//! allowed_object_store_prefixes`, checked on every manifest load
//! (`HiveManifestStore::get`/`put`, `hive/store.rs`), not only at CREATE.
//! `hive_load_denied_when_location_fails_the_storage_allowlist` below is
//! the first test anywhere in this feature that exercises that gate
//! against a real store rather than a local temp directory.
//!
//! ## Bucket layout
//!
//! Reuses the `warehouse` bucket `scripts/bootstrap-test.sh` already
//! creates (no new bucket, no change to that script) under a prefix
//! private to this file (`hive-t11-*`), so it cannot collide with the
//! Iceberg catalog's own `s3://warehouse/` tree or with a re-run of this
//! suite: every test resets its own prefix before writing fixtures.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Float64Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use futures::TryStreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use parquet::arrow::ArrowWriter;

use sqe_catalog::hive::provider::HiveMetadataCache;
use sqe_coordinator::{query_tracker::QueryTracker, QueryHandler, RuntimeCatalogRegistry};
use sqe_core::{SecretStore, Session, SqeConfig};
use sqe_policy::plan_rewriter::HiveCatalogs;
use sqe_policy::PassthroughEnforcer;

const S3_ENDPOINT: &str = "http://localhost:19000";
const S3_ACCESS_KEY: &str = "s3admin";
const S3_SECRET_KEY: &str = "s3admin";
const S3_REGION: &str = "us-east-1";
const BUCKET: &str = "warehouse";

// ---------------------------------------------------------------------------
// S3 fixture helpers -- a plain `object_store` client independent of the
// coordinator, so uploading data and reading the manifest/data objects back
// proves something the coordinator's own read path didn't just assert about
// itself.
// ---------------------------------------------------------------------------

fn s3_store() -> Arc<dyn ObjectStore> {
    Arc::new(
        AmazonS3Builder::new()
            .with_bucket_name(BUCKET)
            .with_endpoint(S3_ENDPOINT)
            .with_region(S3_REGION)
            .with_access_key_id(S3_ACCESS_KEY)
            .with_secret_access_key(S3_SECRET_KEY)
            .with_virtual_hosted_style_request(false)
            .with_allow_http(true)
            .build()
            .expect("build test S3 client"),
    )
}

/// Delete every object under `prefix` (best-effort), so a re-run of this
/// file against a persisted RustFS volume starts from a clean slate rather
/// than colliding with a previous run's manifest or data.
async fn reset_prefix(store: &dyn ObjectStore, prefix: &str) {
    let path = ObjectPath::from(prefix);
    let mut listing = store.list(Some(&path));
    while let Some(meta) = listing.try_next().await.expect("list prefix") {
        let _ = store.delete(&meta.location).await;
    }
}

async fn put_object(store: &dyn ObjectStore, key: &str, bytes: Vec<u8>) {
    store
        .put(&ObjectPath::from(key), PutPayload::from(bytes))
        .await
        .unwrap_or_else(|e| panic!("put '{key}' failed: {e}"));
}

async fn object_exists(store: &dyn ObjectStore, key: &str) -> bool {
    store.head(&ObjectPath::from(key)).await.is_ok()
}

fn csv_partition_fixture() -> Vec<(&'static str, &'static str)> {
    vec![
        ("dt=2026-01-01/a.csv", "id,amount\n1,10.5\n2,20.0\n"),
        ("dt=2026-01-02/a.csv", "id,amount\n3,30.5\n"),
    ]
}

async fn upload_partitioned_csv(store: &dyn ObjectStore, prefix: &str) {
    for (rel, contents) in csv_partition_fixture() {
        put_object(
            store,
            &format!("{prefix}/{rel}"),
            contents.as_bytes().to_vec(),
        )
        .await;
    }
}

fn write_parquet_bytes(ids: &[i64], amounts: &[f64]) -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("amount", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(Float64Array::from(amounts.to_vec())),
        ],
    )
    .expect("build parquet fixture batch");

    let mut buf = Vec::new();
    let mut writer =
        ArrowWriter::try_new(&mut buf, Arc::clone(&schema), None).expect("open ArrowWriter");
    writer.write(&batch).expect("write parquet batch");
    writer.close().expect("close ArrowWriter");
    buf
}

fn ndjson_bytes(rows: &[(i64, f64)]) -> Vec<u8> {
    let mut out = String::new();
    for (id, amount) in rows {
        out.push_str(&format!("{{\"id\":{id},\"amount\":{amount}}}\n"));
    }
    out.into_bytes()
}

// ---------------------------------------------------------------------------
// Coordinator harness -- mirrors `hive_ddl_test.rs`'s `handler_with_root`
// (a real `QueryHandler` with `ATTACH ... TYPE hive_external` run through
// SQL dispatch, the shared `HiveCatalogs`/`HiveMetadataCache` wired the
// same way `main.rs`/`bin/sqe_server.rs` wire them). The one difference:
// the root and every table LOCATION are `s3://`, against real RustFS,
// instead of `file://` against a tempdir. Polaris itself is mocked, same
// as `hive_ddl_test.rs` -- a Hive table never reaches Polaris (Task 8's
// spec correction), the mock exists only so the default Iceberg catalog's
// `flattened_catalogs()` has something to answer `/v1/config` /
// `/v1/namespaces` with. Real RustFS on real S3-client code is the part
// this file exists to prove; a real Polaris would prove nothing extra
// about the Hive path and would add a second real-network dependency this
// suite does not need.
// ---------------------------------------------------------------------------

async fn mock_polaris() -> wiremock::MockServer {
    let server = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/v1/config"))
        .respond_with(
            wiremock::ResponseTemplate::new(200)
                .set_body_string(r#"{"overrides":{},"defaults":{}}"#),
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

/// Fresh, UUID-identified session, never touching the real `Authenticator`.
/// Two sessions built this way are provably distinct
/// `SESSION_CONTEXT_CACHE` keys (`username:token_hash:catalog:schema`),
/// which matters for the second-session assertion in
/// `hive_external_table_round_trips_on_object_storage` below: without
/// distinct identities, a shared cache entry could make session 2 "see"
/// the table for a reason that has nothing to do with the manifest living
/// in the bucket.
fn fresh_session() -> Session {
    Session::new(
        format!("hive-t11-{}", uuid::Uuid::new_v4()),
        sqe_core::SecretString::new(uuid::Uuid::new_v4().to_string()),
        None,
        chrono::Utc::now() + chrono::Duration::hours(1),
        vec!["service_admin".to_string()],
    )
}

/// Build a `QueryHandler` with the Hive registries wired and the given
/// object-store prefixes allowlisted, then ATTACH the shared test root
/// (`s3://warehouse/hive-t11-wh/`) under the name `lake`. Returns the
/// handler, a session, and the `MockServer` (kept alive: dropping it would
/// close the port a later `flattened_catalogs()` call might still reach).
async fn attached_handler(extra_prefixes: &[String]) -> (Session, QueryHandler, wiremock::MockServer) {
    let mock = mock_polaris().await;
    let toml_text = format!(
        "[coordinator]\n\n[auth]\n\n[catalog]\ncatalog_url = \"{}\"\nwarehouse = \"test_warehouse\"\n\n\
         [storage]\ns3_endpoint = \"{S3_ENDPOINT}\"\ns3_access_key = \"{S3_ACCESS_KEY}\"\n\
         s3_secret_key = \"{S3_SECRET_KEY}\"\ns3_region = \"{S3_REGION}\"\ns3_path_style = true\n\
         s3_allow_http = true\n",
        mock.uri()
    );
    let mut config: SqeConfig = toml::from_str(&toml_text).expect("test config parses");
    config.storage.tvf.allow_local_paths = false;
    config.storage.tvf.allowed_object_store_prefixes = extra_prefixes.to_vec();

    let tracker = Arc::new(QueryTracker::new(&config.query_history));
    let handler = QueryHandler::new(
        Arc::new(PassthroughEnforcer),
        None,
        config,
        None,
        None,
        None,
        None,
        tracker,
        None,
        None,
        None,
        RuntimeCatalogRegistry::new(),
        SecretStore::new(),
    )
    .expect("QueryHandler::new")
    .with_hive_registries(Arc::new(HiveCatalogs::new()), Arc::new(HiveMetadataCache::new()));

    let session = fresh_session();
    handler
        .execute(
            &session,
            "ATTACH 's3://warehouse/hive-t11-wh/' AS lake (TYPE hive_external)",
            None,
        )
        .await
        .expect("attach hive root on real RustFS");

    (session, handler, mock)
}

async fn count_of(handler: &QueryHandler, session: &Session, sql: &str) -> i64 {
    let batches = handler
        .execute(session, sql, None)
        .await
        .unwrap_or_else(|e| panic!("query '{sql}' failed: {e}"));
    let batch = batches.first().expect("query returned no batches");
    batch.column(0).as_primitive::<Int64Type>().value(0)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// The brief's own scenario: ATTACH, CREATE EXTERNAL TABLE over a
/// partitioned CSV tree, SHOW TABLES, a full read, a partition-pruned
/// read, a second independent session with no DDL replay, and DROP TABLE
/// leaving the data behind -- all against real RustFS.
///
/// Standing test audit, per test:
/// - `count(*) == 3` / `count(*) WHERE dt = ... == 1`: would fail against
///   a partition-pruning bug that dropped or duplicated a partition, or
///   against a scan that read the wrong prefix. Distinguishing (not just
///   "returns some rows").
/// - the second-session read: distinguishing BECAUSE the second handler
///   never runs `CREATE EXTERNAL TABLE` -- a broken implementation that
///   only registered the table in call-scoped, non-persisted state (never
///   reaching the bucket) would make this specific assertion fail while
///   the first session's own read kept passing.
/// - the post-DROP assertions read the OBJECTS directly (not through SQL),
///   so they are not sensitive to `SESSION_CONTEXT_CACHE`'s known
///   provider-reuse window (see `hive_ddl_test.rs`'s comment on why a
///   SQL-level post-DROP read was removed as non-discriminating there).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires the local Polaris + RustFS stack"]
async fn hive_external_table_round_trips_on_object_storage() {
    let store = s3_store();
    reset_prefix(store.as_ref(), "hive-t11-wh/sales/orders").await;
    reset_prefix(store.as_ref(), "hive-t11-raw/orders").await;
    upload_partitioned_csv(store.as_ref(), "hive-t11-raw/orders").await;

    let prefixes = vec![
        "s3://warehouse/hive-t11-wh/".to_string(),
        "s3://warehouse/hive-t11-raw/".to_string(),
    ];
    let (session, handler, _mock) = attached_handler(&prefixes).await;

    handler
        .execute(
            &session,
            "CREATE EXTERNAL TABLE lake.sales.orders (id BIGINT, amount DOUBLE) \
             PARTITIONED BY (dt string) \
             ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE \
             LOCATION 's3://warehouse/hive-t11-raw/orders/' \
             TBLPROPERTIES ('skip.header.line.count' = '1')",
            None,
        )
        .await
        .expect("create external table over real RustFS");

    let show_tables = handler
        .execute(&session, "SHOW TABLES FROM lake.sales", None)
        .await
        .expect("SHOW TABLES");
    let table_names: Vec<String> = show_tables
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_string::<i32>()
                .iter()
                .filter_map(|s| s.map(str::to_string))
                .collect::<Vec<_>>()
        })
        .collect();
    assert!(
        table_names.contains(&"orders".to_string()),
        "SHOW TABLES should list 'orders': {table_names:?}"
    );

    assert_eq!(
        count_of(&handler, &session, "SELECT count(*) FROM lake.sales.orders").await,
        3,
        "expected all 3 rows across both partitions"
    );
    assert_eq!(
        count_of(
            &handler,
            &session,
            "SELECT count(*) FROM lake.sales.orders WHERE dt = '2026-01-02'"
        )
        .await,
        1,
        "partition-pruned predicate should return only the dt=2026-01-02 row"
    );

    // A second, independent session (its own handler, own registry, own
    // Hive catalogs, own ATTACH) sees the table with NO DDL replay: the
    // definition lives in the bucket, not in the first session's process
    // state.
    let (session2, handler2, _mock2) = attached_handler(&prefixes).await;
    assert_eq!(
        count_of(
            &handler2,
            &session2,
            "SELECT count(*) FROM lake.sales.orders"
        )
        .await,
        3,
        "a fresh session that never ran CREATE EXTERNAL TABLE must still see the table"
    );

    handler
        .execute(&session, "DROP TABLE lake.sales.orders", None)
        .await
        .expect("drop");

    assert!(
        object_exists(store.as_ref(), "hive-t11-raw/orders/dt=2026-01-01/a.csv").await,
        "DROP TABLE must never delete the underlying data"
    );
    assert!(
        !object_exists(store.as_ref(), "hive-t11-wh/sales/orders/_sqe/table.json").await,
        "DROP TABLE must delete the manifest object"
    );
}

/// The brief's second scenario, extended to all three readable formats.
/// Parquet is the only one with a real footer and the only one where
/// projection and predicate pushdown apply, so it gets its own
/// column-projected, predicate-filtered read rather than a bare count.
///
/// Standing test audit: the parquet assertion checks the VALUE returned
/// for a specific `id`, not just a row count, so a projection bug that
/// returned the wrong column (or the right column for the wrong row)
/// fails it; a mutation that swapped `id`/`amount` in the fixture's own
/// column order would also fail it, since the assertion is keyed by `id`
/// found via `WHERE`, not by position.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires the local Polaris + RustFS stack"]
async fn parquet_and_json_tables_read_on_object_storage() {
    let store = s3_store();
    reset_prefix(store.as_ref(), "hive-t11-wh/sales/orders_parquet").await;
    reset_prefix(store.as_ref(), "hive-t11-wh/sales/orders_json").await;
    reset_prefix(store.as_ref(), "hive-t11-raw/orders_parquet").await;
    reset_prefix(store.as_ref(), "hive-t11-raw/orders_json").await;

    put_object(
        store.as_ref(),
        "hive-t11-raw/orders_parquet/data.parquet",
        write_parquet_bytes(&[1, 2, 3, 4], &[10.0, 20.0, 30.0, 40.0]),
    )
    .await;
    put_object(
        store.as_ref(),
        "hive-t11-raw/orders_json/data.json",
        ndjson_bytes(&[(1, 5.5), (2, 6.5)]),
    )
    .await;

    let prefixes = vec![
        "s3://warehouse/hive-t11-wh/".to_string(),
        "s3://warehouse/hive-t11-raw/".to_string(),
    ];
    let (session, handler, _mock) = attached_handler(&prefixes).await;

    handler
        .execute(
            &session,
            "CREATE EXTERNAL TABLE lake.sales.orders_parquet (id BIGINT, amount DOUBLE) \
             STORED AS PARQUET LOCATION 's3://warehouse/hive-t11-raw/orders_parquet/'",
            None,
        )
        .await
        .expect("create parquet external table over real RustFS");

    handler
        .execute(
            &session,
            "CREATE EXTERNAL TABLE lake.sales.orders_json (id BIGINT, amount DOUBLE) \
             STORED AS JSONFILE LOCATION 's3://warehouse/hive-t11-raw/orders_json/'",
            None,
        )
        .await
        .expect("create json external table over real RustFS");

    assert_eq!(
        count_of(
            &handler,
            &session,
            "SELECT count(*) FROM lake.sales.orders_parquet"
        )
        .await,
        4,
        "parquet footer must be read correctly"
    );

    // Projection + predicate pushdown over a real Parquet footer: only the
    // `amount` column for the row where `id = 3`.
    let batches = handler
        .execute(
            &session,
            "SELECT amount FROM lake.sales.orders_parquet WHERE id = 3",
            None,
        )
        .await
        .expect("projected, filtered parquet read");
    let batch = batches.first().expect("one batch");
    assert_eq!(batch.num_columns(), 1, "projection must narrow to one column");
    let amount = batch
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("amount column is Float64")
        .value(0);
    assert!(
        (amount - 30.0).abs() < 1e-9,
        "expected amount=30.0 for id=3, got {amount}"
    );

    assert_eq!(
        count_of(
            &handler,
            &session,
            "SELECT count(*) FROM lake.sales.orders_json"
        )
        .await,
        2,
        "line-delimited JSON must be read correctly"
    );
}

/// The feature's main security property, exercised against a real store
/// for the first time: a location that fails `[storage.tvf]
/// allowed_object_store_prefixes` is refused on LOAD, not only at CREATE.
///
/// Sequence: session A (root prefix AND data prefix allowed) creates the
/// table, proving the data prefix is genuinely reachable. Session B (root
/// prefix allowed so its own ATTACH succeeds, data prefix deliberately
/// NOT allowed) then tries to read the SAME already-created table and
/// must be refused. Because the manifest already exists and names a
/// location neither party can change, the only thing that can make
/// session B's read differ from session A's is `TvfPolicy::check_path`
/// re-evaluating the caller's OWN configured allowlist on every load
/// (`HiveManifestStore::get`, `hive/store.rs`) -- proving the gate runs
/// per load, not once at creation and then trusted forever.
///
/// Standing test audit: a plausibly-broken implementation that checked
/// the allowlist only inside `put` (CREATE) and never again inside `get`
/// would make session A's create succeed (as today) but would ALSO let
/// session B's read succeed, since nothing would re-validate the location
/// on load. This test fails exactly that mutation.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires the local Polaris + RustFS stack"]
async fn hive_load_denied_when_location_fails_the_storage_allowlist() {
    let store = s3_store();
    reset_prefix(store.as_ref(), "hive-t11-wh/sales/guarded").await;
    reset_prefix(store.as_ref(), "hive-t11-raw/guarded").await;
    upload_partitioned_csv(store.as_ref(), "hive-t11-raw/guarded").await;

    let root_only = vec!["s3://warehouse/hive-t11-wh/".to_string()];
    let root_and_data = vec![
        "s3://warehouse/hive-t11-wh/".to_string(),
        "s3://warehouse/hive-t11-raw/".to_string(),
    ];

    // Session A: full access, creates the table.
    let (session_a, handler_a, _mock_a) = attached_handler(&root_and_data).await;
    handler_a
        .execute(
            &session_a,
            "CREATE EXTERNAL TABLE lake.sales.guarded (id BIGINT, amount DOUBLE) \
             PARTITIONED BY (dt string) \
             ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE \
             LOCATION 's3://warehouse/hive-t11-raw/guarded/' \
             TBLPROPERTIES ('skip.header.line.count' = '1')",
            None,
        )
        .await
        .expect("session A can create where its own allowlist covers the data prefix");
    assert_eq!(
        count_of(&handler_a, &session_a, "SELECT count(*) FROM lake.sales.guarded").await,
        3,
        "session A's own read must succeed as a control"
    );

    // Session B: root allowed (so ATTACH itself works), data prefix NOT
    // allowed. The manifest B is about to read already exists, written by
    // session A above.
    let (session_b, handler_b, _mock_b) = attached_handler(&root_only).await;
    let err = handler_b
        .execute(&session_b, "SELECT count(*) FROM lake.sales.guarded", None)
        .await
        .expect_err("a load whose data location fails the allowlist must be refused");
    let msg = err.to_string();
    assert!(
        msg.contains("allowed_object_store_prefixes"),
        "error should name the allowlist gate that denied it: {msg}"
    );
}
