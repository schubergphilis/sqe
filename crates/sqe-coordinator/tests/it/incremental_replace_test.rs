//! `FOR INCREMENTAL BETWEEN SNAPSHOT` must not count compacted rows twice.
//!
//! A `replace` snapshot (compaction) re-adds rows an earlier snapshot already
//! produced. The planner used to emit its added files as new inserts, so a
//! range spanning an append and its compaction returned every row twice.
//!
//! This drives `plan_incremental` against a real SQLite-backed Iceberg
//! catalog with three committed snapshots: an append of 10 rows, a
//! `rewrite_files` compaction of that file, and an append of 5 rows. The data
//! files are fabricated (metadata only, never read), so the row count is the
//! sum of the planned files' `record_count`, which is what a `count(*)` over
//! the incremental provider reads.
//!
//! Run with `cargo test -p sqe-coordinator --features test-sqlite --test it
//! incremental_replace`.

#![cfg(feature = "test-sqlite")]

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use iceberg::spec::{
    DataContentType, DataFile, DataFileBuilder, DataFileFormat, NestedField, Operation,
    PrimitiveType, Schema, Struct, Type,
};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use sqe_catalog::incremental_scan::plan_incremental;
use sqe_core::SecretStore;
use sqe_sql::CatalogKind;

fn data_file(path: &str, rows: u64) -> DataFile {
    DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path(path.to_string())
        .file_format(DataFileFormat::Parquet)
        .file_size_in_bytes(1)
        .record_count(rows)
        .partition(Struct::from_iter(std::iter::empty()))
        .partition_spec_id(0)
        .build()
        .expect("build data file")
}

async fn commit(catalog: &dyn Catalog, tx: Transaction) -> Table {
    tx.commit(catalog).await.expect("commit")
}

fn current_snapshot(table: &Table) -> (i64, Operation) {
    let snap = table.metadata().current_snapshot().expect("snapshot");
    (snap.snapshot_id(), snap.summary().operation.clone())
}

/// Planned row count for `(start, end]`: the sum of each planned data file's
/// record count, looked up by path.
async fn planned_rows(table: &Table, start: i64, end: i64, rows: &HashMap<&str, u64>) -> u64 {
    let plan = plan_incremental(table, start, end)
        .await
        .expect("plan_incremental");
    assert!(plan.delete_files.is_empty(), "no deletes in this table");
    plan.data_files
        .iter()
        .map(|f| {
            *rows
                .get(f.path.as_str())
                .unwrap_or_else(|| panic!("unexpected file in plan: {}", f.path))
        })
        .sum()
}

#[tokio::test]
async fn incremental_range_skips_compaction_snapshot() {
    let dir = tempfile::tempdir().expect("tempdir");
    let location = dir.path().to_str().expect("tempdir path is UTF-8");
    let catalog: Arc<dyn Catalog> = sqe_catalog::mount::build_catalog(
        location,
        CatalogKind::Sqlite,
        &BTreeMap::new(),
        &SecretStore::new(),
    )
    .await
    .expect("sqlite catalog builds");

    let ns = NamespaceIdent::new("default".to_string());
    catalog
        .create_namespace(&ns, HashMap::new())
        .await
        .expect("create namespace");
    let schema = Schema::builder()
        .with_fields(vec![NestedField::required(
            1,
            "id",
            Type::Primitive(PrimitiveType::Long),
        )
        .into()])
        .build()
        .expect("build schema");
    let table = catalog
        .create_table(
            &ns,
            TableCreation::builder()
                .name("incr_replace".to_string())
                .schema(schema)
                .build(),
        )
        .await
        .expect("create table");

    let first = "mem://incr_replace/data/first.parquet";
    let compacted = "mem://incr_replace/data/compacted.parquet";
    let second = "mem://incr_replace/data/second.parquet";
    let rows: HashMap<&str, u64> = [(first, 10), (compacted, 10), (second, 5)].into();

    // Snapshot A: append 10 rows.
    let tx = Transaction::new(&table);
    let tx = tx
        .fast_append()
        .add_data_files(vec![data_file(first, 10)])
        .apply(tx)
        .expect("apply append");
    let table = commit(catalog.as_ref(), tx).await;
    let (snap_a, op) = current_snapshot(&table);
    assert_eq!(op, Operation::Append);

    // Snapshot B: compaction rewrites A's file into one new file.
    let tx = Transaction::new(&table);
    let tx = tx
        .rewrite_files()
        .delete_files(vec![data_file(first, 10)])
        .add_data_files(vec![data_file(compacted, 10)])
        .apply(tx)
        .expect("apply rewrite");
    let table = commit(catalog.as_ref(), tx).await;
    let (snap_b, op) = current_snapshot(&table);
    assert_eq!(
        op,
        Operation::Replace,
        "rewrite_files commits a replace snapshot"
    );

    // Snapshot C: append 5 rows.
    let tx = Transaction::new(&table);
    let tx = tx
        .fast_append()
        .add_data_files(vec![data_file(second, 5)])
        .apply(tx)
        .expect("apply append");
    let table = commit(catalog.as_ref(), tx).await;
    let (snap_c, op) = current_snapshot(&table);
    assert_eq!(op, Operation::Append);

    let table = catalog
        .load_table(&TableIdent::new(ns, "incr_replace".to_string()))
        .await
        .expect("reload table");

    // From the beginning: A's 10 rows + C's 5 rows, the compaction adds none.
    assert_eq!(planned_rows(&table, 0, snap_c, &rows).await, 15);
    // Starting after the append: only C's 5 rows.
    assert_eq!(planned_rows(&table, snap_a, snap_c, &rows).await, 5);
    // A range holding only the compaction is empty.
    assert_eq!(planned_rows(&table, snap_a, snap_b, &rows).await, 0);

    // The walk still reports the compaction as part of the range.
    let plan = plan_incremental(&table, 0, snap_c).await.expect("plan");
    assert_eq!(plan.snapshots_in_range, vec![snap_a, snap_b, snap_c]);
}
