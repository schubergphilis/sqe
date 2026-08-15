//! Phase 4d Task 1 spike: which Iceberg commit action gives mutual exclusion
//! on an `sqe_system`-shaped state table, for the later maintenance lease
//! (Task 2)?
//!
//! # Conclusion (empirical, see the two tests below)
//!
//! - **`fast_append` gives NO exclusion.** Two claimants racing a plain
//!   `Transaction::fast_append()` against the same base snapshot BOTH land.
//!   `Transaction::commit()` proactively reloads the table and rebuilds the
//!   pending action against the freshest metadata on every attempt (see
//!   `vendor/iceberg-rust/crates/iceberg/src/transaction/mod.rs`,
//!   `Transaction::do_commit`), and appends are commutative with any base,
//!   so a "stale" claimant's append always succeeds once rebuilt. This
//!   confirms the brief's premise: an append-only claim row cannot be used
//!   as a lock.
//!
//! - **`Transaction::rewrite_files().delete_files([sentinel]).add_data_files([claim])
//!   .set_check_file_existence(true)` gives HARD exclusion.** Both claimants
//!   race to delete the SAME pre-existing "sentinel" data file and replace it
//!   with their own claim file. The winner's delete target is present in the
//!   live manifest, so it succeeds. The loser's `Transaction::commit()` also
//!   reloads to the freshest table (now missing the sentinel, because the
//!   winner already removed it) and rebuilds its `RewriteFilesAction` against
//!   that reload -- `check_file_existence` then runs
//!   `SnapshotProducer::validate_data_file_changes` against the CURRENT
//!   manifest and finds the delete target gone, so it hard-fails. This is
//!   deterministic (not a timing-dependent race): once the sentinel is gone,
//!   every subsequent attempt by the loser rediscovers the same fact, so
//!   there's no retry count that heals it.
//!
//! # The error is NOT retryable -- this is the load-bearing surprise
//!
//! The `check_file_existence` conflict surfaces as
//! `ErrorKind::DataInvalid` with `retryable() == false`
//! (`SnapshotProducer::validate_data_file_changes`,
//! `vendor/iceberg-rust/crates/iceberg/src/transaction/snapshot.rs`, builds
//! the error via a plain `Error::new(...)` with no `.with_retryable(true)`).
//! Contrast with the `RefSnapshotIdMatch` / `UuidMatch` requirement checks
//! and the SQL catalog's optimistic-concurrency `UPDATE ... WHERE
//! metadata_location = ?` (`vendor/iceberg-rust/crates/catalog/sql/src/catalog.rs`,
//! `update_table`), which both raise `ErrorKind::CatalogCommitConflicts`
//! with `retryable() == true` -- those are what `Transaction::commit()`'s
//! internal backoff (and this crate's `write_handler::commit_with_retry`)
//! auto-heal past. A `check_file_existence` conflict does NOT look like a
//! "conflict" to either classifier:
//!
//! - `iceberg::Error::retryable()` is `false`.
//! - `sqe_coordinator::write_handler`'s `is_conflict_message` heuristic
//!   (`commitconflict` / `commit conflict` / `stale snapshot` / `rowdelta
//!   conflict` / `retryable`) does not match the message text either
//!   ("Cannot delete files that are not in the current snapshot, files:
//!   ...").
//!
//! **Task 2 must treat `ErrorKind::DataInvalid` from a claim-commit
//! specially**: it means "lost the race for this claim", not "transient
//! conflict, blindly retry". If the claim commit is routed through
//! `commit_with_retry` unmodified, a losing claimant gets a single-shot hard
//! failure (which is actually the right lease behavior -- "someone else has
//! it, back off and re-check next tick" -- but it must be classified and
//! handled, not treated as an unexpected error).
//!
//! # Recommendation for Task 2
//!
//! Use `Transaction::rewrite_files()` with `set_check_file_existence(true)`,
//! deleting a single well-known "lease sentinel" data file and replacing it
//! with a new one that encodes the new holder, as the claim commit. Classify
//! a losing claim by `e.kind() == ErrorKind::DataInvalid` (in addition to the
//! existing retryable/conflict-message checks for genuine transient
//! conflicts), and treat it as "lease held by someone else" rather than
//! retrying the same commit.
//!
//! An untested alternative worth a follow-up spike: a claim modeled as
//! `Transaction::create_tag(...)` (or any action whose `TableRequirement` is
//! `RefSnapshotIdMatch { snapshot_id: None, .. }`, i.e. "this named ref must
//! not already exist") would instead surface as `CatalogCommitConflicts`
//! with `retryable() == true`. That matches the brief's original assumption
//! of a *retryable* conflict, but retryable there is actively wrong for a
//! lease: `commit_with_retry` would spin the loser against a ref that never
//! becomes available until the holder releases it. Not verified in this
//! spike; noted only as a contrast, not a recommendation.
//!
//! Run with `cargo test -p sqe-coordinator --features test-sqlite --test lease_cas_spike_test`.

#![cfg(feature = "test-sqlite")]

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use arrow_array::{Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use futures::TryStreamExt;
use iceberg::spec::Schema as IcebergSchema;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, ErrorKind, NamespaceIdent, TableCreation, TableIdent};
use sqe_coordinator::writer::{new_upload_tracker, parse_parquet_compression, write_data_files};
use sqe_core::SecretStore;
use sqe_sql::CatalogKind;
use tempfile::TempDir;

/// Build a fresh SQLite-backed `Arc<dyn Catalog>` rooted at `dir`. Mirrors
/// `maintenance_log_test.rs`'s harness.
async fn sqlite_catalog(dir: &TempDir) -> Arc<dyn Catalog> {
    let location = dir.path().to_str().expect("tempdir path is UTF-8");
    sqe_catalog::mount::build_catalog(
        location,
        CatalogKind::Sqlite,
        &BTreeMap::new(),
        &SecretStore::new(),
    )
    .await
    .expect("sqlite catalog builds")
}

/// Minimal state-table-shaped schema: one STRING column naming the claimant.
/// The real `sqe_system.maintenance_log` schema is much wider
/// (`maintenance_log.rs`), but nothing about this spike depends on that
/// shape -- only on whether a commit action conflicts.
fn claim_arrow_schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![Field::new(
        "claim_owner",
        DataType::Utf8,
        false,
    )]))
}

async fn create_claim_table(catalog: &Arc<dyn Catalog>, table_name: &str) -> TableIdent {
    let ns = NamespaceIdent::new("sqe_system".to_string());
    if !catalog
        .namespace_exists(&ns)
        .await
        .expect("namespace_exists check")
    {
        catalog
            .create_namespace(&ns, HashMap::new())
            .await
            .expect("create sqe_system namespace");
    }

    let arrow_schema = claim_arrow_schema();
    let iceberg_schema: IcebergSchema =
        iceberg::arrow::arrow_schema_to_schema_auto_assign_ids(&arrow_schema)
            .expect("arrow schema converts to iceberg schema");

    let creation = TableCreation::builder()
        .name(table_name.to_string())
        .schema(iceberg_schema)
        .build();

    catalog
        .create_table(&ns, creation)
        .await
        .expect("create claim table");

    TableIdent::new(ns, table_name.to_string())
}

fn claim_row(owner: &str) -> RecordBatch {
    RecordBatch::try_new(
        claim_arrow_schema(),
        vec![Arc::new(StringArray::from(vec![owner]))],
    )
    .expect("build one-row claim batch")
}

/// Write one claim row as a Parquet data file and return its `DataFile`
/// descriptor (not yet committed to any snapshot).
async fn write_claim_file(table: &iceberg::table::Table, owner: &str) -> iceberg::spec::DataFile {
    let batch = claim_row(owner);
    let tracker = new_upload_tracker();
    let compression = parse_parquet_compression("zstd");
    let mut files = write_data_files(table, vec![batch], "lease-cas-spike", compression, tracker)
        .await
        .expect("write claim data file");
    assert_eq!(
        files.len(),
        1,
        "one-row batch must produce exactly one data file"
    );
    files.remove(0)
}

async fn scan_owners(catalog: &Arc<dyn Catalog>, ident: &TableIdent) -> Vec<String> {
    let table = catalog
        .load_table(ident)
        .await
        .expect("reload table for scan");
    let batches: Vec<_> = table
        .scan()
        .build()
        .expect("build scan")
        .to_arrow()
        .await
        .expect("scan to arrow")
        .try_collect()
        .await
        .expect("collect batches");

    let mut owners = Vec::new();
    for batch in &batches {
        if let Some(col) = batch.column_by_name("claim_owner") {
            let arr = col
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("claim_owner is Utf8");
            for i in 0..arr.len() {
                owners.push(arr.value(i).to_string());
            }
        }
    }
    owners
}

/// Variant A: plain `fast_append` from two claimants racing against the same
/// base snapshot. EXPECTED (per the brief): both succeed -- fast-append is
/// commutative, so it cannot be used as a mutual-exclusion primitive.
#[tokio::test]
async fn fast_append_claim_gives_no_exclusion_both_racers_land() {
    let dir = tempfile::tempdir().expect("tempdir");
    let catalog = sqlite_catalog(&dir).await;
    let ident = create_claim_table(&catalog, "lease_spike_append").await;

    // Two independent `Table` handles loaded at the SAME base snapshot
    // (the table has no snapshot yet at this point -- both start from
    // "no current snapshot").
    let table_a = catalog
        .load_table(&ident)
        .await
        .expect("load table for racer A");
    let table_b = catalog
        .load_table(&ident)
        .await
        .expect("load table for racer B");

    let file_a = write_claim_file(&table_a, "coordinator-a").await;
    let file_b = write_claim_file(&table_b, "coordinator-b").await;

    // Racer A commits first from its (currently non-stale) base.
    let tx_a = Transaction::new(&table_a);
    let action_a = tx_a.fast_append().add_data_files(vec![file_a]);
    let tx_a = action_a.apply(tx_a).expect("apply fast_append for A");
    let result_a = tx_a.commit(catalog.as_ref()).await;
    assert!(
        result_a.is_ok(),
        "racer A's fast_append must succeed, got: {result_a:?}"
    );

    // Racer B commits from its now-STALE base (built before A landed).
    // `Transaction::commit` reloads the table and rebuilds the fast_append
    // action against the fresh base internally -- appends are commutative
    // with any base, so this must ALSO succeed, demonstrating fast_append
    // gives no exclusion.
    let tx_b = Transaction::new(&table_b);
    let action_b = tx_b.fast_append().add_data_files(vec![file_b]);
    let tx_b = action_b.apply(tx_b).expect("apply fast_append for B");
    let result_b = tx_b.commit(catalog.as_ref()).await;
    assert!(
        result_b.is_ok(),
        "racer B's fast_append must ALSO succeed against its stale base (commutative, auto-rebased), got: {result_b:?}"
    );

    let owners = scan_owners(&catalog, &ident).await;
    assert_eq!(
        owners.len(),
        2,
        "both racers' claim rows must have landed (no exclusion): {owners:?}"
    );
    assert!(owners.contains(&"coordinator-a".to_string()));
    assert!(owners.contains(&"coordinator-b".to_string()));
}

/// Variant B: `rewrite_files` with `check_file_existence(true)`, racing two
/// claimants to delete-and-replace the SAME pre-existing "sentinel" data
/// file. EXPECTED: exactly one succeeds; the loser gets a hard, classified
/// conflict this crate's retry machinery does NOT currently recognize as
/// retryable (see the module doc's "load-bearing surprise" section).
#[tokio::test]
async fn rewrite_files_check_existence_claim_gives_hard_exclusion() {
    let dir = tempfile::tempdir().expect("tempdir");
    let catalog = sqlite_catalog(&dir).await;
    let ident = create_claim_table(&catalog, "lease_spike_rewrite").await;

    // Seed a "lease sentinel" row/file with a single fast_append. This is
    // the file both racers will race to delete-and-replace.
    let seed_table = catalog
        .load_table(&ident)
        .await
        .expect("load table to seed sentinel");
    let sentinel_file = write_claim_file(&seed_table, "unclaimed").await;
    let seed_tx = Transaction::new(&seed_table);
    let seed_action = seed_tx
        .fast_append()
        .add_data_files(vec![sentinel_file.clone()]);
    let seed_tx = seed_action.apply(seed_tx).expect("apply seed fast_append");
    seed_tx
        .commit(catalog.as_ref())
        .await
        .expect("seed commit succeeds");

    // Two independent `Table` handles loaded at the SAME base snapshot
    // (post-seed), simulating two coordinators racing to claim the lease.
    let table_c = catalog
        .load_table(&ident)
        .await
        .expect("load table for racer C");
    let table_d = catalog
        .load_table(&ident)
        .await
        .expect("load table for racer D");

    let claim_c = write_claim_file(&table_c, "coordinator-c").await;
    let claim_d = write_claim_file(&table_d, "coordinator-d").await;

    // Racer C claims first: delete the sentinel, add its own claim file.
    let tx_c = Transaction::new(&table_c);
    let action_c = tx_c
        .rewrite_files()
        .delete_files(vec![sentinel_file.clone()])
        .add_data_files(vec![claim_c])
        .set_check_file_existence(true);
    let tx_c = action_c.apply(tx_c).expect("apply rewrite_files for C");
    let result_c = tx_c.commit(catalog.as_ref()).await;
    assert!(
        result_c.is_ok(),
        "racer C must win the claim, got: {result_c:?}"
    );

    // Racer D races the SAME sentinel from its now-stale base. Its
    // `Transaction::commit` reloads to the fresh (post-C) table and rebuilds
    // the rewrite against it -- `check_file_existence` then finds the
    // sentinel already gone and hard-fails.
    let tx_d = Transaction::new(&table_d);
    let action_d = tx_d
        .rewrite_files()
        .delete_files(vec![sentinel_file])
        .add_data_files(vec![claim_d])
        .set_check_file_existence(true);
    let tx_d = action_d.apply(tx_d).expect("apply rewrite_files for D");
    let result_d = tx_d.commit(catalog.as_ref()).await;

    let err = result_d
        .expect_err("racer D must lose the claim and get a hard conflict, not silently succeed");

    // Classify the same way `write_handler::commit_with_retry` /
    // `is_conflict_message` would. This is the headline finding: it is
    // NOT retryable, and does NOT match the existing conflict-message
    // heuristic (`commitconflict` / `commit conflict` / `stale snapshot` /
    // `rowdelta conflict` / `retryable`).
    assert_eq!(
        err.kind(),
        ErrorKind::DataInvalid,
        "a losing check_file_existence claim must surface as DataInvalid, got kind: {:?}",
        err.kind()
    );
    assert!(
        !err.retryable(),
        "a losing check_file_existence claim must NOT be marked retryable by iceberg-rust \
         (Task 2 must special-case ErrorKind::DataInvalid, not rely on retryable()/is_conflict_message), \
         got retryable() = {}",
        err.retryable()
    );
    let msg_lower = err.to_string().to_lowercase();
    let looks_like_existing_conflict_heuristic = msg_lower.contains("commitconflict")
        || msg_lower.contains("commit conflict")
        || msg_lower.contains("stale snapshot")
        || msg_lower.contains("rowdelta conflict")
        || msg_lower.contains("retryable");
    assert!(
        !looks_like_existing_conflict_heuristic,
        "this crate's is_conflict_message() heuristic must NOT already match this error message \
         (documenting that Task 2 needs a NEW classification branch, not the existing one); \
         message was: {err}"
    );

    // Exactly one claim row landed: the sentinel was replaced by C's claim,
    // D's claim never made it in.
    let owners = scan_owners(&catalog, &ident).await;
    assert_eq!(
        owners,
        vec!["coordinator-c".to_string()],
        "exactly one claimant must win: {owners:?}"
    );
}
