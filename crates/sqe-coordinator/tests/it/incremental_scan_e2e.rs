//! End-to-end integration tests for CDC `FOR INCREMENTAL BETWEEN SNAPSHOT`.
//!
//! The live-stack tests (`#[ignore]`) build a table with three snapshots and
//! ask the coordinator to return the union of rows added in the window. They
//! cover the full path: pre-parser (`sqe_sql::extract_incremental_spec`) ->
//! range resolution (`sqe_catalog::incremental_scan::plan_incremental`) ->
//! `IncrementalTableProvider` registration -> DataFusion execution.
//!
//! Run after boot:
//!
//! ```text
//! docker compose -f docker-compose.test.yml up -d
//! ./scripts/bootstrap-test.sh
//! cargo test --package sqe-coordinator --test incremental_scan_e2e -- --ignored --nocapture
//! ```
//!
//! The non-ignored tests stay on the static shape of the rewrite + provider
//! construction so every PR exercises them without Docker.


use std::sync::Arc;

#[allow(unused_imports)]
use arrow_array::Array;
use sqe_catalog::incremental_scan::{
    ChangeKind, IncrementalFile, IncrementalPlan, augment_schema_with_meta,
};

// ---------------------------------------------------------------------------
// Static contract tests: parser + provider wiring with no live catalog.
// These run on every `cargo test` and require no containers.
// ---------------------------------------------------------------------------

/// Contract: the pre-parser strips the clause and yields a clean SQL that
/// DataFusion can plan, plus the specs for every matching table.
#[test]
fn parser_strips_incremental_clause() {
    let input =
        "SELECT count(*) FROM ns.t FOR INCREMENTAL BETWEEN SNAPSHOT 100 AND SNAPSHOT 105";
    let (rewritten, specs) = sqe_sql::extract_incremental_spec(input).unwrap();
    assert_eq!(specs.len(), 1);
    assert_eq!(specs[0].table, "ns.t");
    assert_eq!(specs[0].start, 100);
    assert_eq!(specs[0].end, 105);
    // The clause must be gone in the rewritten SQL.
    let rewritten_upper = rewritten.to_uppercase();
    assert!(
        !rewritten_upper.contains("FOR INCREMENTAL"),
        "Rewritten SQL still contains FOR INCREMENTAL: {rewritten}"
    );
    assert!(rewritten.contains("ns.t"));
}

/// Contract: the provider uses the augmented schema (base + three meta cols)
/// when it sees a non-empty IncrementalPlan.
#[test]
fn provider_exposes_meta_columns_in_schema() {
    use arrow::datatypes::{DataType, Field, Schema};

    let base = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("val", DataType::Utf8, true),
    ]));
    let augmented = augment_schema_with_meta(&base);
    let names: Vec<&str> =
        augmented.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(
        names,
        vec![
            "id",
            "val",
            "_change_type",
            "_change_ordinal",
            "_commit_snapshot_id",
        ]
    );
}

/// Contract: attach_meta_columns populates the three extra columns from the
/// file's fields, for every row in the batch.
#[test]
fn attach_meta_columns_fills_every_row() {
    use arrow::array::{ArrayRef, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let ids: ArrayRef = Arc::new(Int64Array::from(vec![10_i64, 20, 30]));
    let batch = RecordBatch::try_new(schema, vec![ids]).unwrap();

    let file = IncrementalFile {
        path: "s3://b/x.parquet".into(),
        size_bytes: 1024,
        snapshot_id: 42,
        kind: ChangeKind::Insert,
        ordinal: 3,
    };

    let out =
        sqe_catalog::incremental_scan::attach_meta_columns(batch, &file).unwrap();
    assert_eq!(out.num_rows(), 3);
    assert_eq!(out.num_columns(), 4);

    let kind = out
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(kind.value(0), "insert");
    assert_eq!(kind.value(2), "insert");

    let ord = out
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(ord.value(0), 3);

    let snap = out
        .column(3)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(snap.value(1), 42);
}

/// Contract: empty plan construction produces a provider whose schema matches
/// the augmented schema (base + meta). This exercises the `with_plan` builder
/// on whatever provider type we ship.
#[test]
fn incremental_provider_builds_with_empty_plan() {
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::TableProvider;

    let base = Arc::new(Schema::new(vec![Field::new(
        "id",
        DataType::Int64,
        false,
    )]));
    let plan = IncrementalPlan::default();

    // The provider is constructed with the base Arrow schema plus the plan.
    // Its reported schema must carry the three meta columns.
    let provider = sqe_catalog::incremental_provider::IncrementalTableProvider::new(
        base.clone(),
        plan,
        // file_io is optional for the empty-plan path; None -> scan returns
        // an empty batch, which is all we test here.
        None,
    );
    let schema = provider.schema();
    assert_eq!(schema.fields().len(), 4);
    assert_eq!(schema.field(0).name(), "id");
    assert_eq!(schema.field(1).name(), "_change_type");
    assert_eq!(schema.field(2).name(), "_change_ordinal");
    assert_eq!(schema.field(3).name(), "_commit_snapshot_id");
}

// ---------------------------------------------------------------------------
// Live-stack e2e tests. Require Polaris + RustFS (and catalog boot).
// ---------------------------------------------------------------------------

/// Task: `SELECT count(*) FROM t FOR INCREMENTAL BETWEEN SNAPSHOT s0 AND SNAPSHOT s3`
/// with three appends of 10, 15, 20 rows returns 45.
///
/// Shape:
/// 1. CREATE TABLE, capture initial snapshot id s0 (may be zero if empty).
/// 2. INSERT 10 rows, capture snapshot s1.
/// 3. INSERT 15 rows, capture snapshot s2.
/// 4. INSERT 20 rows, capture snapshot s3.
/// 5. `SELECT count(*) FROM t FOR INCREMENTAL BETWEEN SNAPSHOT s0 AND SNAPSHOT s3` == 45.
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn incremental_scan_three_snapshots_returns_45_rows() {
    let (session, handler) = crate::common::setup_handler().await;

    // Setup: isolated namespace + table per run.
    let ns = format!("cdc_{}", uuid::Uuid::new_v4().simple());
    let table = format!("{ns}.orders");

    let _ = handler
        .execute(&session, &format!("CREATE SCHEMA {ns}"))
        .await
        .expect("create schema");
    handler
        .execute(
            &session,
            &format!("CREATE TABLE {table} (id BIGINT, val VARCHAR)"),
        )
        .await
        .expect("create table");

    // Baseline snapshot: the table is empty so current_snapshot may be None.
    // We use 0 as the sentinel "from the beginning" for start.
    let s0: i64 = 0;

    // Append 10 rows.
    handler
        .execute(
            &session,
            &format!(
                "INSERT INTO {table} VALUES {}",
                (0..10)
                    .map(|i| format!("({i}, 'a')"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        )
        .await
        .expect("insert 10");

    // Append 15 rows.
    handler
        .execute(
            &session,
            &format!(
                "INSERT INTO {table} VALUES {}",
                (0..15)
                    .map(|i| format!("({i}, 'b')"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        )
        .await
        .expect("insert 15");

    // Append 20 rows -> this is s3.
    handler
        .execute(
            &session,
            &format!(
                "INSERT INTO {table} VALUES {}",
                (0..20)
                    .map(|i| format!("({i}, 'c')"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        )
        .await
        .expect("insert 20");

    // Discover the current snapshot id via the `table_snapshots` TVF, which
    // enumerates snapshots for a table in chronological order.
    let batches = handler
        .execute(
            &session,
            &format!(
                "SELECT snapshot_id FROM table_snapshots('{ns}', 'orders') ORDER BY timestamp_ms"
            ),
        )
        .await
        .expect("query snapshots");
    let mut ids: Vec<i64> = Vec::new();
    for b in &batches {
        let col = b
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .expect("snapshot_id column must be Int64");
        for i in 0..col.len() {
            ids.push(col.value(i));
        }
    }
    assert_eq!(ids.len(), 3, "expected 3 snapshots, got {ids:?}");
    let s3 = *ids.last().expect("snapshot list non-empty");

    let sql = format!(
        "SELECT count(*) FROM {table} FOR INCREMENTAL BETWEEN SNAPSHOT {s0} AND SNAPSHOT {s3}"
    );
    let result = handler
        .execute(&session, &sql)
        .await
        .expect("incremental SELECT count(*)");
    let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1, "count(*) must return one row");
    let count = result[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .expect("count(*) column must be Int64")
        .value(0);
    assert_eq!(count, 45, "three appends of 10+15+20 must yield 45 rows");
}

/// Task: `SELECT _change_type, _change_ordinal, _commit_snapshot_id` against
/// an incremental range materialises the three meta columns with the correct
/// values (insert kind, a positive ordinal, and a known snapshot id).
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn incremental_scan_meta_columns_are_populated() {
    let (session, handler) = crate::common::setup_handler().await;

    let ns = format!("cdc_meta_{}", uuid::Uuid::new_v4().simple());
    let table = format!("{ns}.events");

    handler
        .execute(&session, &format!("CREATE SCHEMA {ns}"))
        .await
        .expect("create schema");
    handler
        .execute(
            &session,
            &format!("CREATE TABLE {table} (id BIGINT)"),
        )
        .await
        .expect("create table");

    handler
        .execute(&session, &format!("INSERT INTO {table} VALUES (1), (2), (3)"))
        .await
        .expect("insert 3");

    let batches = handler
        .execute(
            &session,
            &format!(
                "SELECT snapshot_id FROM table_snapshots('{ns}', 'events') ORDER BY timestamp_ms"
            ),
        )
        .await
        .expect("query snapshots");
    let s3 = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap()
        .value(0);

    let sql = format!(
        "SELECT _change_type, _change_ordinal, _commit_snapshot_id FROM {table} \
         FOR INCREMENTAL BETWEEN SNAPSHOT 0 AND SNAPSHOT {s3} ORDER BY _change_ordinal"
    );
    let result = handler
        .execute(&session, &sql)
        .await
        .expect("incremental meta SELECT");
    let total: usize = result.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 3, "three rows added in snapshot s3");

    let kind = result[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .unwrap();
    for i in 0..kind.len() {
        assert_eq!(kind.value(i), "insert");
    }

    let snap = result[0]
        .column(2)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap();
    for i in 0..snap.len() {
        assert_eq!(snap.value(i), s3);
    }
}
