//! #892: schema-changing `CREATE OR REPLACE` on a SQLite catalog: one
//! commit on the live table, its refusals and its races.

use super::*;

/// #892: a different column set is one commit on the same table: the
/// new schema becomes current with the data, the UUID and the earlier
/// snapshots stay, and no staging table ever exists.
#[tokio::test(flavor = "multi_thread")]
async fn create_or_replace_with_new_schema_keeps_the_table() {
    let rig = SqliteRig::new().await;
    let h = handler(&rig);
    run_ctas(
        &h,
        &rig,
        "CREATE TABLE default.cs AS SELECT * FROM (VALUES (1, 'a')) AS s(id, v)",
    )
    .await;
    let ident = rig.ident("cs");
    let before = rig.load(&ident).await;
    let first_snapshot = before.metadata().current_snapshot_id().expect("snapshot");

    run_ctas(
        &h,
        &rig,
        "CREATE OR REPLACE TABLE default.cs AS SELECT * FROM (VALUES (5, 'q', 2.5)) AS s(id, v, w)",
    )
    .await;

    let after = rig.load(&ident).await;
    let meta = after.metadata();
    assert_eq!(meta.uuid(), before.metadata().uuid(), "same table");
    assert!(
        meta.snapshot_by_id(first_snapshot).is_some(),
        "the earlier snapshot stays for time travel"
    );
    let current = meta.current_snapshot().expect("snapshot");
    assert_eq!(
        current.schema_id(),
        Some(meta.current_schema_id()),
        "the data snapshot names the new schema"
    );
    let fields = meta.current_schema().as_struct().fields();
    assert_eq!(fields.len(), 3);
    let old_fields = before.metadata().current_schema().as_struct().fields();
    assert_eq!(
        (fields[0].id, fields[1].id),
        (old_fields[0].id, old_fields[1].id),
        "kept columns keep their field ids"
    );
    assert_eq!(rig.scan_i64(&ident, "id").await, vec![5]);
    let tables = rig
        .catalog
        .list_tables(&NamespaceIdent::new("default".to_string()))
        .await
        .expect("list tables");
    assert_eq!(
        tables
            .iter()
            .map(|t| t.name().to_string())
            .collect::<Vec<_>>(),
        vec!["cs".to_string()],
        "no staging table left behind"
    );
}

/// #892: partitioning set by one replace and dropped by the next
/// (no `PARTITIONED BY` = unpartitioned), all on the same table; a
/// shape seen before reuses its schema id instead of adding a copy.
#[tokio::test(flavor = "multi_thread")]
async fn create_or_replace_changes_partitioning_and_reuses_schemas() {
    let rig = SqliteRig::new().await;
    let h = handler(&rig);
    let a = "CREATE OR REPLACE TABLE default.cp AS SELECT * FROM (VALUES (1, 'a')) AS s(id, v)";
    run_ctas(&h, &rig, a).await;
    let ident = rig.ident("cp");
    let first = rig.load(&ident).await;
    let schema_a = first.metadata().current_schema_id();

    // The SQL front door moves `PARTITIONED BY (v)` from Hive's
    // `hive_distribution` to `partition_by`, as on every wire.
    let partitioned = sqe_sql::SqlFrontDoor::new(sqe_sql::WireDialect::Embedded)
        .normalize(
            "CREATE OR REPLACE TABLE default.cp PARTITIONED BY (v) \
             AS SELECT * FROM (VALUES (2, 'b', 1.5)) AS s(id, v, w)",
        )
        .expect("normalize")
        .statement()
        .cloned()
        .expect("statement");
    h.handle_ctas_streaming(
        &rig.session(),
        &partitioned,
        &DFSessionContext::new(),
        &ctas_select_sql(&partitioned),
        &mut None,
        &mut None,
    )
    .await
    .expect("partitioned replace");
    let meta = rig.load(&ident).await.metadata().clone();
    assert_eq!(meta.uuid(), first.metadata().uuid());
    assert_eq!(meta.default_partition_spec().fields().len(), 1);
    assert_ne!(meta.current_schema_id(), schema_a);

    run_ctas(&h, &rig, a).await;
    let meta = rig.load(&ident).await.metadata().clone();
    assert_eq!(meta.uuid(), first.metadata().uuid());
    assert!(meta.default_partition_spec().is_unpartitioned());
    assert_eq!(
        meta.current_schema_id(),
        schema_a,
        "shape A again reuses its schema"
    );
    assert_eq!(rig.scan_i64(&ident, "id").await, vec![1]);
}

/// #892: a replace never lowers the format version; asking for a
/// lower one fails before anything is written.
#[tokio::test(flavor = "multi_thread")]
async fn create_or_replace_refuses_a_format_downgrade() {
    let rig = SqliteRig::new().await;
    let h = handler(&rig);
    run_ctas(
        &h,
        &rig,
        "CREATE TABLE default.fv WITH (format_version = 3) AS SELECT 1 AS id",
    )
    .await;
    let ident = rig.ident("fv");
    let before = rig.load(&ident).await.metadata().clone();

    let stmt = parse_one(
        "CREATE OR REPLACE TABLE default.fv WITH (format_version = 2) AS SELECT 2 AS id, 'x' AS v",
    );
    let err = h
        .handle_ctas_streaming(
            &rig.session(),
            &stmt,
            &DFSessionContext::new(),
            &ctas_select_sql(&stmt),
            &mut None,
            &mut None,
        )
        .await
        .expect_err("downgrade refused")
        .to_string();
    assert!(err.contains("from v3 to v2"), "{err}");
    let after = rig.load(&ident).await;
    assert_eq!(
        after.metadata().current_snapshot_id(),
        before.current_snapshot_id()
    );
    assert_eq!(after.metadata().format_version(), FormatVersion::V3);

    // Without the option the replace keeps v3 and changes the columns.
    run_ctas(
        &h,
        &rig,
        "CREATE OR REPLACE TABLE default.fv AS SELECT 2 AS id, 'x' AS v",
    )
    .await;
    let after = rig.load(&ident).await;
    assert_eq!(after.metadata().format_version(), FormatVersion::V3);
    assert_eq!(
        after.metadata().current_schema().as_struct().fields().len(),
        2
    );
}

const SHAPE_REPLACE: &str = "CREATE OR REPLACE TABLE default.{t} AS \
                             SELECT * FROM (VALUES (5, 'q', 2.5)) AS s(id, v, w)";

/// Parquet files under a local table's location.
fn parquet_files_under(table: &IcebergTable) -> usize {
    fn walk(dir: &std::path::Path) -> usize {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return 0;
        };
        entries
            .flatten()
            .map(|e| {
                let p = e.path();
                if p.is_dir() {
                    walk(&p)
                } else {
                    usize::from(p.extension().is_some_and(|x| x == "parquet"))
                }
            })
            .sum()
    }
    let location = table.metadata().location();
    walk(std::path::Path::new(
        location.strip_prefix("file://").unwrap_or(location),
    ))
}

/// #892: an append that lands while a schema-changing replace is
/// writing moves `main`, so the replace reruns against the new head:
/// the table holds only the replace's rows and the append's snapshot
/// stays in history.
#[tokio::test(flavor = "multi_thread")]
async fn a_shape_replace_reruns_over_a_concurrent_append() {
    let rig = SqliteRig::new().await;
    let ident = seeded_id_v(&rig, "sr_append").await;
    let before = snapshot_count(&rig.load(&ident).await);
    let (hook, fired) = late_append_hook(&rig, &ident);
    let h = handler(&rig).with_test_commit_race_hook(hook);
    run_ctas(&h, &rig, &SHAPE_REPLACE.replace("{t}", "sr_append")).await;

    assert!(fired.load(AtomicOrdering::SeqCst), "the race was exercised");
    let after = rig.load(&ident).await;
    assert_eq!(
        after.metadata().current_schema().as_struct().fields().len(),
        3
    );
    assert_eq!(rig.scan_i64(&ident, "id").await, vec![5]);
    assert_eq!(
        snapshot_count(&after),
        before + 2,
        "the append stays in history"
    );
}

/// #892: a schema change that commits while a schema-changing replace
/// is writing fails the replace, naming the change; the other
/// change stands and the replace's files are removed.
#[tokio::test(flavor = "multi_thread")]
async fn a_shape_replace_fails_over_a_concurrent_schema_change() {
    use iceberg::spec::{NestedField, PrimitiveType, Type, UnboundPartitionSpec};

    let rig = SqliteRig::new().await;
    let ident = seeded_id_v(&rig, "sr_alter").await;
    let files_before = parquet_files_under(&rig.load(&ident).await);
    assert!(files_before > 0, "the seed's files are on local disk");
    let fired = Arc::new(AtomicBool::new(false));
    let hook: CommitRaceHook = {
        let (catalog, ident, fired) = (rig.catalog.clone(), ident.clone(), fired.clone());
        Arc::new(move || {
            let (catalog, ident, fired) = (catalog.clone(), ident.clone(), fired.clone());
            Box::pin(async move {
                if fired.swap(true, AtomicOrdering::SeqCst) {
                    return;
                }
                // ALTER TABLE ... ADD COLUMN added, as another writer.
                let table = catalog.load_table(&ident).await.expect("load");
                let schema = table.metadata().current_schema();
                let mut fields = schema.as_struct().fields().to_vec();
                fields.push(Arc::new(NestedField::optional(
                    schema.highest_field_id() + 1,
                    "added",
                    Type::Primitive(PrimitiveType::Int),
                )));
                let schema = IcebergSchema::builder()
                    .with_fields(fields)
                    .build()
                    .expect("schema");
                let spec = UnboundPartitionSpec::builder().build();
                let alter =
                    ReplaceTableShapeAction::new(&table, schema, spec).expect("alter action");
                let tx = alter.apply(Transaction::new(&table)).expect("apply");
                tx.commit(catalog.as_ref()).await.expect("concurrent ALTER");
            })
        })
    };
    let h = handler(&rig).with_test_commit_race_hook(hook);
    let stmt = parse_one(&SHAPE_REPLACE.replace("{t}", "sr_alter"));
    let err = h
        .handle_ctas_streaming(
            &rig.session(),
            &stmt,
            &DFSessionContext::new(),
            &ctas_select_sql(&stmt),
            &mut None,
            &mut None,
        )
        .await
        .expect_err("the replace loses to the schema change")
        .to_string();

    assert!(fired.load(AtomicOrdering::SeqCst), "the race was exercised");
    assert!(
        err.contains("concurrent schema or partition change"),
        "{err}"
    );
    let after = rig.load(&ident).await;
    let names: Vec<String> = after
        .metadata()
        .current_schema()
        .as_struct()
        .fields()
        .iter()
        .map(|f| f.name.clone())
        .collect();
    assert_eq!(names, ["id", "v", "added"], "the ALTER stands");
    assert_eq!(rig.scan_i64(&ident, "id").await, vec![1, 2, 3]);
    assert_eq!(
        parquet_files_under(&after),
        files_before,
        "the replace's files are removed"
    );
}

/// #892: `SET SNAPSHOT_PROPERTIES` stamps the schema-changing
/// replace's snapshot, as it does the in-place one.
#[tokio::test(flavor = "multi_thread")]
async fn a_shape_replace_carries_snapshot_properties() {
    let rig = SqliteRig::new().await;
    let ident = seeded_id_v(&rig, "sr_props").await;
    let h = handler(&rig);
    with_props(
        RUN,
        run_ctas(&h, &rig, &SHAPE_REPLACE.replace("{t}", "sr_props")),
    )
    .await;
    assert_eq!(
        rig.load(&ident)
            .await
            .metadata()
            .current_schema()
            .as_struct()
            .fields()
            .len(),
        3
    );
    assert_stamped(&head_summary(&rig, &ident).await);
}
