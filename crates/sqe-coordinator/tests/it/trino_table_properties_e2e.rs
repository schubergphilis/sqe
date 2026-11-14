//! #897: Trino `WITH (...)` table properties through `/v1/statement`, in
//! the statement shapes stock dbt-trino sends.

use std::sync::Arc;

use iceberg::spec::FormatVersion;

const NS: &str = "default";

/// Every request authenticates as the session `setup_handler` built.
struct FixedAuth(sqe_core::Session);

#[async_trait::async_trait]
impl sqe_trino_compat::server::TrinoAuthenticator for FixedAuth {
    async fn authenticate(
        &self,
        _username: &str,
        _password: &str,
        _peer_ip: &str,
    ) -> Result<sqe_core::Session, sqe_core::SqeError> {
        Ok(self.0.clone())
    }
}

/// The handler's `execute`, which takes DDL and DML as well as queries.
struct Execute(Arc<sqe_coordinator::QueryHandler>);

#[async_trait::async_trait]
impl sqe_trino_compat::server::TrinoQueryExecutor for Execute {
    async fn execute(
        &self,
        session: &sqe_core::Session,
        sql: &str,
    ) -> Result<sqe_trino_compat::server::TrinoResultStream, sqe_core::SqeError> {
        self.0
            .execute(session, sql, None)
            .await
            .map(sqe_trino_compat::server::TrinoResultStream::from_batches)
    }
}

/// Start the Trino endpoint on a free port; returns its base URL.
async fn start_server(
    session: &sqe_core::Session,
    handler: &Arc<sqe_coordinator::QueryHandler>,
) -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = listener.local_addr().expect("addr").port();
    drop(listener);
    let node = sqe_trino_compat::server::NodeContext {
        version: "test".to_string(),
        ready: Arc::new(std::sync::atomic::AtomicBool::new(true)),
        started_at: std::time::Instant::now(),
    };
    let _server = sqe_trino_compat::server::start_trino_server(
        Arc::new(FixedAuth(session.clone())),
        Arc::new(Execute(Arc::clone(handler))),
        port,
        node,
        None,
    );
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    format!("http://127.0.0.1:{port}")
}

/// Run `sql` through `/v1/statement` to its last page; the error message
/// when it failed.
async fn run(base_url: &str, sql: &str) -> Result<(), String> {
    let client = reqwest::Client::new();
    let mut body: serde_json::Value = client
        .post(format!("{base_url}/v1/statement"))
        .basic_auth("root", Some("s3cr3t"))
        .body(sql.to_string())
        .send()
        .await
        .expect("POST /v1/statement")
        .json()
        .await
        .expect("statement json");
    while let Some(next) = body.get("nextUri").and_then(|v| v.as_str()) {
        body = client
            .get(next)
            .basic_auth("root", Some("s3cr3t"))
            .send()
            .await
            .expect("GET nextUri")
            .json()
            .await
            .expect("page json");
    }
    match body.get("error") {
        Some(err) => Err(err["message"].as_str().unwrap_or_default().to_string()),
        None => Ok(()),
    }
}

async fn ok(base_url: &str, sql: &str) {
    run(base_url, sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

/// The table as Polaris holds it.
async fn load(session: &sqe_core::Session, name: &str) -> iceberg::table::Table {
    let config = sqe_core::SqeConfig::load(&crate::common::test_config_path()).expect("config");
    let catalog = sqe_catalog::rest_catalog::SessionCatalog::for_session(
        &config,
        None,
        session.access_token().expose(),
    )
    .await
    .expect("session catalog");
    let ident = iceberg::TableIdent::from_strs([NS, name]).expect("ident");
    catalog.load_table(&ident).await.expect("load table")
}

/// dbt-trino's table materialization: `create table ... comment '...'
/// WITH (format_version = N) as (select ...)`, both versions whatever the
/// server default is, then `ALTER TABLE ... SET PROPERTIES format_version`
/// upgrades and refuses a downgrade.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn trino_format_version_property_897() {
    let (session, handler) = crate::common::setup_handler().await;
    let handler = Arc::new(handler);
    let base_url = start_server(&session, &handler).await;
    let (v2, v3) = ("trino_props_v2_897", "trino_props_v3_897");
    for t in [v2, v3] {
        ok(&base_url, &format!("DROP TABLE IF EXISTS {NS}.{t}")).await;
    }

    for (t, version, want) in [(v2, "2", FormatVersion::V2), (v3, "'3'", FormatVersion::V3)] {
        let sql = format!(
            "create table {NS}.{t}\n      comment 'a mart'\n      WITH (\n          \
             format_version = {version}\n      )\n    as (\n      \
             select 1 as id, 'a' as name\n    );"
        );
        ok(&base_url, &sql).await;
        let table = load(&session, t).await;
        assert_eq!(table.metadata().format_version(), want, "{sql}");
        let props = table.metadata().properties();
        assert!(!props.contains_key("format_version"), "{t}: {props:?}");
        assert_eq!(
            props.get("comment").map(String::as_str),
            Some("a mart"),
            "{t}"
        );
    }

    ok(
        &base_url,
        &format!("ALTER TABLE {NS}.{v2} SET PROPERTIES format_version = 3"),
    )
    .await;
    assert_eq!(
        load(&session, v2).await.metadata().format_version(),
        FormatVersion::V3
    );
    let err = run(
        &base_url,
        &format!("ALTER TABLE {NS}.{v2} SET PROPERTIES format_version = 2"),
    )
    .await
    .expect_err("a downgrade is refused");
    assert!(err.contains("cannot lower the format version"), "{err}");
    // The upgraded table takes every kind of write.
    for sql in [
        format!("INSERT INTO {NS}.{v2} VALUES (2, 'b'), (3, 'c')"),
        format!("UPDATE {NS}.{v2} SET name = 'z' WHERE id = 2"),
        format!("DELETE FROM {NS}.{v2} WHERE id = 3"),
    ] {
        ok(&base_url, &sql).await;
    }

    // An enforced dbt contract: `create table (cols) comment ... WITH (...)`,
    // then `insert into`.
    let contract = "trino_props_contract_897";
    ok(&base_url, &format!("DROP TABLE IF EXISTS {NS}.{contract}")).await;
    let sql = format!(
        "create table {NS}.{contract} (\n  id integer,\n  name varchar\n)\n\
         comment 'a contract'\nWITH (\n  format_version = 3\n)"
    );
    ok(&base_url, &sql).await;
    ok(
        &base_url,
        &format!("insert into {NS}.{contract} (id, name) values (1, 'a')"),
    )
    .await;
    let table = load(&session, contract).await;
    assert_eq!(
        table.metadata().format_version(),
        FormatVersion::V3,
        "{sql}"
    );
    let props = table.metadata().properties();
    assert_eq!(props.get("comment").map(String::as_str), Some("a contract"));

    for t in [v2, v3, contract] {
        ok(&base_url, &format!("DROP TABLE IF EXISTS {NS}.{t}")).await;
    }
}

async fn exists(session: &sqe_core::Session, name: &str) -> bool {
    let config = sqe_core::SqeConfig::load(&crate::common::test_config_path()).expect("config");
    let catalog = sqe_catalog::rest_catalog::SessionCatalog::for_session(
        &config,
        None,
        session.access_token().expose(),
    )
    .await
    .expect("session catalog");
    let ident = iceberg::TableIdent::from_strs([NS, name]).expect("ident");
    catalog.table_exists(&ident).await.expect("table_exists")
}

/// The partition fields as `(source column, transform)`.
fn spec_of(table: &iceberg::table::Table) -> Vec<(String, String)> {
    let meta = table.metadata();
    let schema = meta.current_schema();
    meta.default_partition_spec()
        .fields()
        .iter()
        .map(|f| {
            let column = schema.name_by_field_id(f.source_id).unwrap_or("?");
            (column.to_string(), f.transform.to_string())
        })
        .collect()
}

/// The default sort order as `(column, direction, null order)`.
fn sort_of(table: &iceberg::table::Table) -> Vec<String> {
    let meta = table.metadata();
    let schema = meta.current_schema();
    meta.default_sort_order()
        .fields
        .iter()
        .map(|f| {
            let column = schema.name_by_field_id(f.source_id).unwrap_or("?");
            format!("{column} {} nulls {}", f.direction, f.null_order)
        })
        .collect()
}

/// The rest of the Trino vocabulary on CTAS, CREATE TABLE and CREATE OR
/// REPLACE: partitioning in Trino's argument order, sorted_by, location,
/// extra_properties and the mapped Parquet keys; an unknown key fails the
/// statement before a table exists.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn trino_table_vocabulary_897() {
    let (session, handler) = crate::common::setup_handler().await;
    let handler = Arc::new(handler);
    let base_url = start_server(&session, &handler).await;
    let (ctas, plain, unknown) = (
        "trino_vocab_ctas_897",
        "trino_vocab_plain_897",
        "trino_vocab_unknown_897",
    );
    for t in [ctas, plain, unknown] {
        ok(&base_url, &format!("DROP TABLE IF EXISTS {NS}.{t}")).await;
    }

    let sql = format!(
        "create table {NS}.{ctas}\n  WITH (\n    format = 'PARQUET',\n    \
         partitioning = ARRAY['region', 'bucket(id, 4)'],\n    \
         sorted_by = ARRAY['id DESC'],\n    gc_enabled = false,\n    \
         parquet_bloom_filter_columns = ARRAY['region'],\n    \
         parquet_writer_row_group_size = '64MB',\n    \
         extra_properties = MAP(ARRAY['owner.team'], ARRAY['marts'])\n  )\nas (\n  \
         select 1 as id, 'eu' as region union all select 2, 'us'\n);"
    );
    ok(&base_url, &sql).await;
    let table = load(&session, ctas).await;
    assert_eq!(
        spec_of(&table),
        [("region", "identity"), ("id", "bucket[4]")].map(|(c, t)| (c.to_string(), t.to_string())),
        "{sql}"
    );
    assert_eq!(sort_of(&table), ["id descending nulls last"], "{sql}");
    let props = table.metadata().properties();
    let get = |k: &str| props.get(k).map(String::as_str);
    assert_eq!(get("gc.enabled"), Some("false"));
    assert_eq!(
        get("write.parquet.bloom-filter-enabled.column.region"),
        Some("true")
    );
    assert_eq!(get("write.parquet.row-group-size-bytes"), Some("67108864"));
    assert_eq!(get("owner.team"), Some("marts"));
    for gone in [
        "format",
        "partitioning",
        "sorted_by",
        "extra_properties",
        "gc_enabled",
    ] {
        assert_eq!(get(gone), None, "{gone}: {props:?}");
    }
    ok(
        &base_url,
        &format!("INSERT INTO {NS}.{ctas} VALUES (3, 'eu')"),
    )
    .await;

    // CREATE OR REPLACE with a new sort order stages a new table and keeps
    // the partitioning it asks for; the table's own location is accepted
    // only in place, another one never.
    let replace = format!(
        "create or replace table {NS}.{ctas} WITH (partitioning = ARRAY['region', \
         'bucket(id, 4)'], sorted_by = ARRAY['region']) as select 1 as id, 'eu' as region"
    );
    ok(&base_url, &replace).await;
    let table = load(&session, ctas).await;
    assert_eq!(
        sort_of(&table),
        ["region ascending nulls first"],
        "{replace}"
    );
    let live_location = table.metadata().location().to_string();
    let same = format!(
        "create or replace table {NS}.{ctas} WITH (location = '{live_location}', \
         partitioning = ARRAY['region', 'bucket(id, 4)'], sorted_by = ARRAY['region']) \
         as select 2 as id, 'us' as region"
    );
    ok(&base_url, &same).await;
    let elsewhere = format!("{live_location}_elsewhere");
    let err = run(
        &base_url,
        &format!(
            "create or replace table {NS}.{ctas} WITH (location = '{elsewhere}', \
             partitioning = ARRAY['region', 'bucket(id, 4)'], sorted_by = ARRAY['region']) \
             as select 3 as id, 'eu' as region"
        ),
    )
    .await
    .expect_err("a moved location is refused");
    assert!(err.contains("cannot move the table"), "{err}");

    // CREATE TABLE: month() of a timestamp, NULLS spelled out, and a
    // location the statement chooses.
    let plain_location = format!(
        "{}/{plain}_custom",
        live_location.rsplit_once('/').expect("a parent").0
    );
    let sql = format!(
        "create table {NS}.{plain} (id integer, ts timestamp(6), name varchar)\n\
         WITH (partitioning = ARRAY['month(ts)'], \
         sorted_by = ARRAY['name ASC NULLS LAST'], location = '{plain_location}')"
    );
    ok(&base_url, &sql).await;
    let table = load(&session, plain).await;
    assert_eq!(spec_of(&table), [("ts".to_string(), "month".to_string())]);
    assert_eq!(sort_of(&table), ["name ascending nulls last"]);
    assert_eq!(table.metadata().location(), plain_location);
    ok(
        &base_url,
        &format!("INSERT INTO {NS}.{plain} VALUES (1, TIMESTAMP '2026-11-14 10:00:00', 'a')"),
    )
    .await;

    // Refused and unknown keys fail before any table exists.
    for (with, want) in [
        ("foo = 'bar'", "unknown table property `foo`"),
        ("compression_codec = 'ZSTD'", "iceberg.compression_codec"),
        ("format = 'ORC'", "Parquet only"),
        ("partitioning = ARRAY['nope']", "nope"),
        ("sorted_by = ARRAY['nope']", "nope"),
    ] {
        let sql = format!("create table {NS}.{unknown} WITH ({with}) as select 1 as id");
        let err = run(&base_url, &sql).await.expect_err(&sql);
        assert!(err.contains(want), "{sql}: {err}");
        assert!(!exists(&session, unknown).await, "{sql} left a table");
    }

    for t in [ctas, plain, unknown] {
        ok(&base_url, &format!("DROP TABLE IF EXISTS {NS}.{t}")).await;
    }
}

/// The partition field ids of the default spec.
/// The spec ids of the manifests the current snapshot added.
async fn new_manifest_specs(table: &iceberg::table::Table) -> Vec<i32> {
    let meta = table.metadata();
    let snapshot = meta.current_snapshot().expect("snapshot");
    let list = table
        .manifest_list_reader(snapshot)
        .load()
        .await
        .expect("manifest list");
    list.entries()
        .iter()
        .filter(|m| m.added_snapshot_id == snapshot.snapshot_id())
        .map(|m| m.partition_spec_id)
        .collect()
}

/// The single BIGINT a `SELECT count(*)` returns.
async fn count_rows(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    sql: &str,
) -> i64 {
    let batches = handler
        .execute(session, sql, None)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .expect("count is BIGINT")
        .value(0)
}

fn field_ids(table: &iceberg::table::Table) -> Vec<i32> {
    let spec = table.metadata().default_partition_spec();
    spec.fields().iter().map(|f| f.field_id).collect()
}

/// `ALTER TABLE ... SET PROPERTIES` with the Trino layout keys:
/// `partitioning` evolves the spec (a field the table had keeps its id),
/// `sorted_by` replaces the sort order, `extra_properties` sets properties.
/// `= DEFAULT`, `location` and refused keys fail and change nothing.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn trino_alter_properties_897() {
    let (session, handler) = crate::common::setup_handler().await;
    let handler = Arc::new(handler);
    let base_url = start_server(&session, &handler).await;
    let t = "trino_alter_props_897";
    ok(&base_url, &format!("DROP TABLE IF EXISTS {NS}.{t}")).await;
    ok(
        &base_url,
        &format!(
            "create table {NS}.{t} WITH (partitioning = ARRAY['region']) \
             as select 1 as id, 'eu' as region"
        ),
    )
    .await;
    let region_id = field_ids(&load(&session, t).await)[0];

    let alter = format!(
        "ALTER TABLE {NS}.{t} SET PROPERTIES partitioning = ARRAY['region', 'bucket(id, 4)'], \
         sorted_by = ARRAY['id DESC'], extra_properties = MAP(ARRAY['owner.team'], ARRAY['marts'])"
    );
    ok(&base_url, &alter).await;
    let table = load(&session, t).await;
    assert_eq!(
        spec_of(&table),
        [("region", "identity"), ("id", "bucket[4]")].map(|(c, t)| (c.to_string(), t.to_string())),
        "{alter}"
    );
    assert_eq!(field_ids(&table), [region_id, region_id + 1], "{alter}");
    assert_eq!(sort_of(&table), ["id descending nulls last"], "{alter}");
    let props = table.metadata().properties();
    assert_eq!(props.get("owner.team").map(String::as_str), Some("marts"));
    for gone in ["partitioning", "sorted_by", "extra_properties"] {
        assert!(!props.contains_key(gone), "{gone}: {props:?}");
    }
    let bucket4_spec = table.metadata().default_partition_spec_id();
    assert_ne!(bucket4_spec, 0, "{alter}");
    ok(&base_url, &format!("INSERT INTO {NS}.{t} VALUES (2, 'us')")).await;
    // The write after the ALTER uses the new spec.
    assert_eq!(
        new_manifest_specs(&load(&session, t).await).await,
        [bucket4_spec]
    );

    // Another bucket count on the same column is a new field, and a new name.
    let bucket8 = format!(
        "ALTER TABLE {NS}.{t} SET PROPERTIES partitioning = ARRAY['region', 'bucket(id, 8)']"
    );
    ok(&base_url, &bucket8).await;
    let table = load(&session, t).await;
    let names: Vec<_> = table
        .metadata()
        .default_partition_spec()
        .fields()
        .iter()
        .map(|f| (f.name.clone(), f.field_id))
        .collect();
    assert_eq!(
        names,
        [
            ("region".to_string(), region_id),
            ("id_bucket_8".to_string(), region_id + 2)
        ],
        "{bucket8}"
    );
    ok(&base_url, &format!("INSERT INTO {NS}.{t} VALUES (4, 'us')")).await;

    // Back to the first layout: the field keeps its id, the order empties,
    // and the table's first spec is the default again.
    let back = format!(
        "ALTER TABLE {NS}.{t} SET PROPERTIES partitioning = ARRAY['region'], sorted_by = ARRAY[]"
    );
    ok(&base_url, &back).await;
    let table = load(&session, t).await;
    assert_eq!(field_ids(&table), [region_id], "{back}");
    assert_eq!(table.metadata().default_partition_spec_id(), 0, "{back}");
    assert!(sort_of(&table).is_empty(), "{back}");
    ok(&base_url, &format!("INSERT INTO {NS}.{t} VALUES (3, 'eu')")).await;
    assert_eq!(new_manifest_specs(&load(&session, t).await).await, [0]);
    // Rows written under the three specs all read back.
    let count = |filter: &str| format!("SELECT count(*) FROM {NS}.{t} {filter}");
    assert_eq!(count_rows(&handler, &session, &count("")).await, 4);
    assert_eq!(
        count_rows(&handler, &session, &count("WHERE region = 'eu'")).await,
        2
    );
    // The same layout again is no commit at all.
    let commits = load(&session, t).await.metadata().metadata_log().len();
    ok(&base_url, &back).await;
    assert_eq!(
        load(&session, t).await.metadata().metadata_log().len(),
        commits,
        "an unchanged layout commits nothing"
    );

    for (set, want) in [
        ("sorted_by = DEFAULT", "DEFAULT is not supported"),
        (
            "location = 's3://elsewhere/t'",
            "cannot change the table property `location`",
        ),
        ("max_commit_retry = 8", "not supported"),
        ("partitioning = ARRAY['nope']", "nope"),
    ] {
        let sql = format!("ALTER TABLE {NS}.{t} SET PROPERTIES {set}");
        let err = run(&base_url, &sql).await.expect_err(&sql);
        assert!(err.contains(want), "{sql}: {err}");
    }
    assert_eq!(field_ids(&load(&session, t).await), [region_id]);

    ok(&base_url, &format!("DROP TABLE IF EXISTS {NS}.{t}")).await;
}
