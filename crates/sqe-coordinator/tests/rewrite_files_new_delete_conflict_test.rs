//! Guard test for the vendored `RewriteFilesAction::set_validate_from_snapshot_id`
//! conflict check (VENDOR PATCH fix/compaction-concurrent-delete-conflict,
//! `vendor/iceberg-rust/crates/iceberg/src/transaction/rewrite_files.rs`).
//!
//! ## The bug this guards against
//!
//! `Transaction::commit` (`vendor/iceberg-rust/crates/iceberg/src/transaction/mod.rs`
//! `do_commit`) reloads the table from the catalog on every attempt and
//! silently RE-APPLIES a `RewriteFilesAction` against whatever snapshot it
//! finds -- there was no built-in check that a *new* delete had landed for
//! one of the data files being rewritten in the meantime. The
//! `set_new_data_file_sequence_number` seq-pin only protects EQUALITY
//! deletes (sequence-number-based matching). It does NOT protect POSITION
//! deletes, which match by file *path*: if a concurrent MoR position delete
//! lands on a data file that a rewrite is replacing, mid-commit-window, the
//! rewrite's compacted output lands under a NEW path while the position
//! delete (which still points at the OLD path) becomes dangling and matches
//! nothing. The deleted rows silently resurrect.
//!
//! ## What this test proves
//!
//! 1. `conflicting_new_position_delete_on_rewritten_file_is_a_retryable_conflict`:
//!    plan a rewrite against snapshot S0, commit a concurrent position delete
//!    on the very file being rewritten (advancing to S1), then commit the
//!    rewrite with `set_validate_from_snapshot_id(Some(s0))` set -- it must
//!    return an `Err`, and that error must be treated as retryable both by
//!    the vendored `Error::retryable()` (which the vendored `do_commit` retry
//!    keys on) and by SQE's `classify_commit_error` message-sniffing (which
//!    keys on the string "conflict"/"retry"; see
//!    `crates/sqe-coordinator/src/maintenance.rs::classify_commit_error`).
//!
//! 2. `same_scenario_without_baseline_silently_resurrects_rows` (the
//!    "fail-before" control): the exact same race, with no
//!    `set_validate_from_snapshot_id` call (i.e. today's behavior for every
//!    existing caller) -- the rewrite commit SUCCEEDS despite the dangling
//!    position delete, proving the bug reproduces whenever the caller does
//!    not opt in to the new validation. This is what
//!    `conflicting_new_position_delete_on_rewritten_file_is_a_retryable_conflict`
//!    would have done before this patch (there was no setter to call).
//!
//! 3. `unrelated_new_delete_does_not_block_rewrite` (control): a concurrent
//!    position delete on a file the rewrite does NOT touch must not block
//!    the rewrite, even with the baseline set.
//!
//! 4. `no_new_delete_commits_cleanly_with_baseline_set` (control): with the
//!    baseline set but no concurrent delete at all, the rewrite commits
//!    normally -- baseline validation must not be a no-op-breaking regression.
//!
//! Run with `cargo test -p sqe-coordinator --features test-sqlite --test rewrite_files_new_delete_conflict_test`.

#![cfg(feature = "test-sqlite")]

use std::collections::BTreeMap;
use std::sync::Arc;

use iceberg::spec::{
    DataContentType, DataFileBuilder, DataFileFormat, NestedField, PrimitiveType, Schema, Struct,
    Type,
};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use sqe_core::SecretStore;
use sqe_sql::CatalogKind;
use tempfile::TempDir;

/// Build a fresh SQLite-backed `Arc<dyn Catalog>` rooted at `dir`.
async fn sqlite_catalog(dir: &TempDir) -> Arc<dyn Catalog> {
    let location = dir.path().to_str().expect("tempdir path is UTF-8");
    sqe_catalog::mount::build_catalog(location, CatalogKind::Sqlite, &BTreeMap::new(), &SecretStore::new())
        .await
        .expect("sqlite catalog builds")
}

fn minimal_schema() -> Schema {
    Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
        ])
        .build()
        .expect("build schema")
}

/// Fast, deterministic commit-retry properties so a forced conflict spins
/// through its (small) retry budget and surfaces in well under a second,
/// instead of the multi-minute default (`commit.retry.num-retries=4`,
/// `commit.retry.min-wait-ms=100`, exponential backoff).
fn fast_retry_properties() -> std::collections::HashMap<String, String> {
    std::collections::HashMap::from([
        ("commit.retry.num-retries".to_string(), "1".to_string()),
        ("commit.retry.min-wait-ms".to_string(), "1".to_string()),
        ("commit.retry.max-wait-ms".to_string(), "1".to_string()),
        ("commit.retry.total-timeout-ms".to_string(), "1000".to_string()),
    ])
}

/// A fabricated data file. No physical parquet bytes are read on any path
/// exercised here (no `set_check_file_existence`, no delete-filter manager),
/// so nothing needs to exist on disk for these metadata-only commits.
fn data_file(path: &str) -> iceberg::spec::DataFile {
    DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path(path.to_string())
        .file_format(DataFileFormat::Parquet)
        .file_size_in_bytes(100)
        .record_count(10)
        .partition(Struct::from_iter(std::iter::empty()))
        .partition_spec_id(0)
        .build()
        .expect("build data file")
}

/// A position delete file referencing `referenced` by path.
fn position_delete_file(path: &str, referenced: &str) -> iceberg::spec::DataFile {
    DataFileBuilder::default()
        .content(DataContentType::PositionDeletes)
        .file_path(path.to_string())
        .file_format(DataFileFormat::Parquet)
        .file_size_in_bytes(10)
        .record_count(1)
        .partition(Struct::from_iter(std::iter::empty()))
        .partition_spec_id(0)
        .referenced_data_file(Some(referenced.to_string()))
        .build()
        .expect("build position delete file")
}

async fn create_table(
    catalog: &Arc<dyn Catalog>,
    ns: &NamespaceIdent,
    name: &str,
) -> iceberg::table::Table {
    let creation = TableCreation::builder()
        .name(name.to_string())
        .schema(minimal_schema())
        .properties(fast_retry_properties())
        .build();
    catalog.create_table(ns, creation).await.expect("create table")
}

/// THE GUARD: a rewrite planned against S0 that removes `old.parquet`, racing
/// a concurrent position delete on `old.parquet` that lands as S1 before the
/// rewrite commits, must be rejected as a retryable conflict -- never
/// silently committed.
#[tokio::test]
async fn conflicting_new_position_delete_on_rewritten_file_is_a_retryable_conflict() {
    let dir = tempfile::tempdir().expect("tempdir");
    let catalog = sqlite_catalog(&dir).await;
    let ns = NamespaceIdent::new("default".to_string());
    catalog
        .create_namespace(&ns, std::collections::HashMap::new())
        .await
        .expect("create namespace");

    let table = create_table(&catalog, &ns, "conflict_test").await;
    let ident = TableIdent::new(ns.clone(), "conflict_test".to_string());

    let old_path = "mem://conflict_test/data/old.parquet";
    let new_path = "mem://conflict_test/data/new.parquet";
    let pos_delete_path = "mem://conflict_test/data/pos-del-1.parquet";

    // S0: seed the table with the data file the rewrite will later replace.
    let tx = Transaction::new(&table);
    let action = tx.fast_append().add_data_files(vec![data_file(old_path)]);
    let tx_applied = action.apply(tx).expect("apply fast_append");
    tx_applied.commit(catalog.as_ref()).await.expect("commit seed append");

    let table_at_s0 = catalog.load_table(&ident).await.expect("reload at s0");
    let baseline_snapshot_id = table_at_s0
        .metadata()
        .current_snapshot_id()
        .expect("s0 exists");

    // Concurrent writer: a MoR DELETE lands a position delete against
    // old.parquet, landing S1 -- this is the row-resurrection hazard.
    let tx2 = Transaction::new(&table_at_s0);
    let action2 = tx2
        .row_delta()
        .add_delete_files(vec![position_delete_file(pos_delete_path, old_path)]);
    let tx2_applied = action2.apply(tx2).expect("apply row_delta");
    tx2_applied
        .commit(catalog.as_ref())
        .await
        .expect("commit concurrent position delete");

    // The compaction job planned its rewrite against the STALE table_at_s0
    // (as a real distributed compaction plan would: it read S0, computed
    // compacted output, and only now attempts to commit). It captured S0 as
    // its validation baseline.
    let tx3 = Transaction::new(&table_at_s0);
    let action3 = tx3
        .rewrite_files()
        .add_data_files(vec![data_file(new_path)])
        .delete_files(vec![data_file(old_path)])
        .set_validate_from_snapshot_id(Some(baseline_snapshot_id));
    let tx3_applied = action3.apply(tx3).expect("apply rewrite_files");

    let result = tx3_applied.commit(catalog.as_ref()).await;

    let err = result.expect_err(
        "rewrite must be rejected: a new position delete landed on old.parquet after the \
         plan baseline, and committing the rewrite as-is would resurrect the deleted rows",
    );

    let msg = err.to_string();
    assert!(
        msg.to_lowercase().contains("conflict"),
        "error message must contain 'conflict' so SQE's classify_commit_error \
         (crates/sqe-coordinator/src/maintenance.rs) treats it as retryable, got: {msg}"
    );

    // The table must still be at S1 (the position delete's snapshot) -- the
    // stale compacted output must never have been committed.
    let reloaded = catalog.load_table(&ident).await.expect("reload after rejected commit");
    let final_snapshot = reloaded
        .metadata()
        .current_snapshot()
        .expect("snapshot exists");
    // old.parquet must still be live (not replaced by new.parquet) -- proof
    // that the compacted output was never committed over the dangling delete.
    let manifest_list = final_snapshot
        .load_manifest_list(reloaded.file_io(), reloaded.metadata())
        .await
        .expect("load manifest list");
    let mut live_data_paths = std::collections::HashSet::new();
    for manifest_file in manifest_list.entries() {
        let manifest = manifest_file.load_manifest(reloaded.file_io()).await.expect("load manifest");
        for entry in manifest.entries() {
            if entry.is_alive() && entry.content_type() == DataContentType::Data {
                live_data_paths.insert(entry.file_path().to_string());
            }
        }
    }
    assert!(
        live_data_paths.contains(old_path),
        "old.parquet must still be the live data file; got live paths: {live_data_paths:?}"
    );
    assert!(
        !live_data_paths.contains(new_path),
        "new.parquet (stale compacted output) must NOT have been committed; \
         got live paths: {live_data_paths:?}"
    );
}

/// FAIL-BEFORE control: the identical race, but without
/// `set_validate_from_snapshot_id` -- i.e. exactly what every caller does
/// today (this patch is opt-in / backward-compatible). The rewrite commits
/// successfully despite the dangling position delete, reproducing the
/// row-resurrection bug this patch guards against. This is the "before"
/// behavior the guard test above proves is fixed once the baseline is set.
#[tokio::test]
async fn same_scenario_without_baseline_silently_resurrects_rows() {
    let dir = tempfile::tempdir().expect("tempdir");
    let catalog = sqlite_catalog(&dir).await;
    let ns = NamespaceIdent::new("default".to_string());
    catalog
        .create_namespace(&ns, std::collections::HashMap::new())
        .await
        .expect("create namespace");

    let table = create_table(&catalog, &ns, "no_baseline_test").await;
    let ident = TableIdent::new(ns.clone(), "no_baseline_test".to_string());

    let old_path = "mem://no_baseline_test/data/old.parquet";
    let new_path = "mem://no_baseline_test/data/new.parquet";
    let pos_delete_path = "mem://no_baseline_test/data/pos-del-1.parquet";

    let tx = Transaction::new(&table);
    let action = tx.fast_append().add_data_files(vec![data_file(old_path)]);
    let tx_applied = action.apply(tx).expect("apply fast_append");
    tx_applied.commit(catalog.as_ref()).await.expect("commit seed append");

    let table_at_s0 = catalog.load_table(&ident).await.expect("reload at s0");

    let tx2 = Transaction::new(&table_at_s0);
    let action2 = tx2
        .row_delta()
        .add_delete_files(vec![position_delete_file(pos_delete_path, old_path)]);
    let tx2_applied = action2.apply(tx2).expect("apply row_delta");
    tx2_applied
        .commit(catalog.as_ref())
        .await
        .expect("commit concurrent position delete");

    // Same stale-base rewrite, but NO set_validate_from_snapshot_id call --
    // mirrors every caller prior to this patch.
    let tx3 = Transaction::new(&table_at_s0);
    let action3 = tx3
        .rewrite_files()
        .add_data_files(vec![data_file(new_path)])
        .delete_files(vec![data_file(old_path)]);
    let tx3_applied = action3.apply(tx3).expect("apply rewrite_files");

    tx3_applied
        .commit(catalog.as_ref())
        .await
        .expect("commit succeeds silently without baseline validation (the bug)");

    let reloaded = catalog.load_table(&ident).await.expect("reload after commit");
    let final_snapshot = reloaded
        .metadata()
        .current_snapshot()
        .expect("snapshot exists");
    let manifest_list = final_snapshot
        .load_manifest_list(reloaded.file_io(), reloaded.metadata())
        .await
        .expect("load manifest list");
    let mut live_data_paths = std::collections::HashSet::new();
    for manifest_file in manifest_list.entries() {
        let manifest = manifest_file.load_manifest(reloaded.file_io()).await.expect("load manifest");
        for entry in manifest.entries() {
            if entry.is_alive() && entry.content_type() == DataContentType::Data {
                live_data_paths.insert(entry.file_path().to_string());
            }
        }
    }
    assert!(
        live_data_paths.contains(new_path),
        "bug reproduction: without the baseline, the rewrite silently replaces \
         old.parquet with new.parquet even though a position delete for \
         old.parquet landed concurrently -- got live paths: {live_data_paths:?}"
    );
    assert!(
        !live_data_paths.contains(old_path),
        "old.parquet should have been removed by the rewrite -- got live paths: {live_data_paths:?}"
    );
    // The dangling position delete now references a path (`old_path`) that
    // no longer has a live data file entry at all -- it matches nothing.
    // Any rows it was meant to delete have resurrected in `new.parquet`.
}

/// Control: a concurrent delete on a file the rewrite does NOT touch must
/// not block the rewrite, even with the baseline set.
#[tokio::test]
async fn unrelated_new_delete_does_not_block_rewrite() {
    let dir = tempfile::tempdir().expect("tempdir");
    let catalog = sqlite_catalog(&dir).await;
    let ns = NamespaceIdent::new("default".to_string());
    catalog
        .create_namespace(&ns, std::collections::HashMap::new())
        .await
        .expect("create namespace");

    let table = create_table(&catalog, &ns, "unrelated_delete_test").await;
    let ident = TableIdent::new(ns.clone(), "unrelated_delete_test".to_string());

    let old_path = "mem://unrelated_delete_test/data/old.parquet";
    let other_path = "mem://unrelated_delete_test/data/other.parquet";
    let new_path = "mem://unrelated_delete_test/data/new.parquet";
    let pos_delete_path = "mem://unrelated_delete_test/data/pos-del-1.parquet";

    // S0: two data files, only one of which the rewrite will touch.
    let tx = Transaction::new(&table);
    let action = tx
        .fast_append()
        .add_data_files(vec![data_file(old_path), data_file(other_path)]);
    let tx_applied = action.apply(tx).expect("apply fast_append");
    tx_applied.commit(catalog.as_ref()).await.expect("commit seed append");

    let table_at_s0 = catalog.load_table(&ident).await.expect("reload at s0");
    let baseline_snapshot_id = table_at_s0
        .metadata()
        .current_snapshot_id()
        .expect("s0 exists");

    // Concurrent delete targets `other.parquet`, which the rewrite below
    // never removes.
    let tx2 = Transaction::new(&table_at_s0);
    let action2 = tx2
        .row_delta()
        .add_delete_files(vec![position_delete_file(pos_delete_path, other_path)]);
    let tx2_applied = action2.apply(tx2).expect("apply row_delta");
    tx2_applied
        .commit(catalog.as_ref())
        .await
        .expect("commit concurrent unrelated position delete");

    let tx3 = Transaction::new(&table_at_s0);
    let action3 = tx3
        .rewrite_files()
        .add_data_files(vec![data_file(new_path)])
        .delete_files(vec![data_file(old_path)])
        .set_validate_from_snapshot_id(Some(baseline_snapshot_id));
    let tx3_applied = action3.apply(tx3).expect("apply rewrite_files");

    tx3_applied
        .commit(catalog.as_ref())
        .await
        .expect("rewrite must succeed: the concurrent delete does not touch a rewritten file");
}

/// Control: with the baseline set but no concurrent commit at all, the
/// rewrite must commit normally -- the validation is a no-op on the common
/// (uncontended) path.
#[tokio::test]
async fn no_new_delete_commits_cleanly_with_baseline_set() {
    let dir = tempfile::tempdir().expect("tempdir");
    let catalog = sqlite_catalog(&dir).await;
    let ns = NamespaceIdent::new("default".to_string());
    catalog
        .create_namespace(&ns, std::collections::HashMap::new())
        .await
        .expect("create namespace");

    let table = create_table(&catalog, &ns, "uncontended_test").await;
    let ident = TableIdent::new(ns.clone(), "uncontended_test".to_string());

    let old_path = "mem://uncontended_test/data/old.parquet";
    let new_path = "mem://uncontended_test/data/new.parquet";

    let tx = Transaction::new(&table);
    let action = tx.fast_append().add_data_files(vec![data_file(old_path)]);
    let tx_applied = action.apply(tx).expect("apply fast_append");
    tx_applied.commit(catalog.as_ref()).await.expect("commit seed append");

    let table_at_s0 = catalog.load_table(&ident).await.expect("reload at s0");
    let baseline_snapshot_id = table_at_s0
        .metadata()
        .current_snapshot_id()
        .expect("s0 exists");

    let tx3 = Transaction::new(&table_at_s0);
    let action3 = tx3
        .rewrite_files()
        .add_data_files(vec![data_file(new_path)])
        .delete_files(vec![data_file(old_path)])
        .set_validate_from_snapshot_id(Some(baseline_snapshot_id));
    let tx3_applied = action3.apply(tx3).expect("apply rewrite_files");

    tx3_applied
        .commit(catalog.as_ref())
        .await
        .expect("rewrite must succeed on the uncontended (no concurrent commit) path");
}
