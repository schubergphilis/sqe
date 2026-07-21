//! Integration tests for `sqe_coordinator::maintenance_lease` (Phase 4d,
//! Task 2): the catalog-native HA lease over `sqe_system.maintenance_log`.
//!
//! Uses a real SQLite-backed Iceberg catalog over a tempdir warehouse, same
//! harness as `maintenance_log_test.rs` / `lease_cas_spike_test.rs`.
//!
//! Run with `cargo test -p sqe-coordinator --features test-sqlite --test maintenance_lease_test`.

#![cfg(feature = "test-sqlite")]

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use iceberg::spec::Schema as IcebergSchema;
use iceberg::{Catalog, NamespaceIdent, TableCreation};
use sqe_core::SecretStore;
use sqe_coordinator::maintenance_lease::{release, renew, try_acquire};
use sqe_coordinator::maintenance_log::maintenance_log_arrow_schema;
use sqe_sql::CatalogKind;
use tempfile::TempDir;

const STATE_TABLE: &str = "sqe_system.maintenance_log";
const JOB_KEY: &str = "bench.orders";

/// Build a fresh SQLite-backed `Arc<dyn Catalog>` rooted at `dir`.
async fn sqlite_catalog(dir: &TempDir) -> Arc<dyn Catalog> {
    let location = dir.path().to_str().expect("tempdir path is UTF-8");
    sqe_catalog::mount::build_catalog(location, CatalogKind::Sqlite, &BTreeMap::new(), &SecretStore::new())
        .await
        .expect("sqlite catalog builds")
}

/// Create `sqe_system.maintenance_log` with the fixed schema, via the raw
/// `Catalog` trait (mirrors `maintenance_log_test.rs`).
async fn create_maintenance_log_table(catalog: &Arc<dyn Catalog>) {
    catalog
        .create_namespace(&NamespaceIdent::new("sqe_system".to_string()), HashMap::new())
        .await
        .expect("create sqe_system namespace");

    let arrow_schema = maintenance_log_arrow_schema();
    let iceberg_schema: IcebergSchema =
        iceberg::arrow::arrow_schema_to_schema_auto_assign_ids(&arrow_schema)
            .expect("arrow schema converts to iceberg schema");

    let creation = TableCreation::builder()
        .name("maintenance_log".to_string())
        .schema(iceberg_schema)
        .build();

    catalog
        .create_table(&NamespaceIdent::new("sqe_system".to_string()), creation)
        .await
        .expect("create maintenance_log table");
}

#[tokio::test]
async fn first_ever_claim_bootstraps_and_is_acquired() {
    let dir = tempfile::tempdir().expect("tempdir");
    let catalog = sqlite_catalog(&dir).await;
    create_maintenance_log_table(&catalog).await;

    let handle = try_acquire(&catalog, STATE_TABLE, JOB_KEY, "holder-1", 60, 1_000)
        .await
        .expect("try_acquire succeeds")
        .expect("first-ever claim for a job_key must be acquirable");

    assert_eq!(handle.job_key, JOB_KEY);
    assert_eq!(handle.holder_id, "holder-1");
    assert_eq!(handle.expires_at_ms, 1_000 + 60_000);
    assert!(!handle.claim_path.is_empty());
}

#[tokio::test]
async fn live_claim_denies_a_different_holder() {
    let dir = tempfile::tempdir().expect("tempdir");
    let catalog = sqlite_catalog(&dir).await;
    create_maintenance_log_table(&catalog).await;

    let _h1 = try_acquire(&catalog, STATE_TABLE, JOB_KEY, "holder-1", 60, 1_000)
        .await
        .expect("try_acquire succeeds")
        .expect("holder-1 acquires");

    let denied = try_acquire(&catalog, STATE_TABLE, JOB_KEY, "holder-2", 60, 2_000)
        .await
        .expect("try_acquire succeeds");
    assert!(denied.is_none(), "a live claim by holder-1 must deny holder-2");
}

#[tokio::test]
async fn same_holder_reacquiring_its_own_live_claim_is_idempotent() {
    let dir = tempfile::tempdir().expect("tempdir");
    let catalog = sqlite_catalog(&dir).await;
    create_maintenance_log_table(&catalog).await;

    let h1 = try_acquire(&catalog, STATE_TABLE, JOB_KEY, "holder-1", 60, 1_000)
        .await
        .expect("try_acquire succeeds")
        .expect("holder-1 acquires");

    let h1_again = try_acquire(&catalog, STATE_TABLE, JOB_KEY, "holder-1", 60, 2_000)
        .await
        .expect("try_acquire succeeds")
        .expect("holder-1 re-acquiring its own live claim must succeed (OwnLive)");

    // OwnLive is a read-only fast path: it must not mint a new commit, so
    // the claim path is unchanged and the expiry is NOT bumped (that is
    // what `renew` is for).
    assert_eq!(h1_again.claim_path, h1.claim_path);
    assert_eq!(h1_again.expires_at_ms, h1.expires_at_ms);
}

#[tokio::test]
async fn release_then_a_different_holder_can_acquire() {
    let dir = tempfile::tempdir().expect("tempdir");
    let catalog = sqlite_catalog(&dir).await;
    create_maintenance_log_table(&catalog).await;

    let h1 = try_acquire(&catalog, STATE_TABLE, JOB_KEY, "holder-1", 60, 1_000)
        .await
        .expect("try_acquire succeeds")
        .expect("holder-1 acquires");

    release(h1, &catalog, STATE_TABLE, 2_000)
        .await
        .expect("release succeeds");

    let h2 = try_acquire(&catalog, STATE_TABLE, JOB_KEY, "holder-2", 60, 3_000)
        .await
        .expect("try_acquire succeeds")
        .expect("holder-2 acquires after holder-1 released");
    assert_eq!(h2.holder_id, "holder-2");
}

#[tokio::test]
async fn expired_lease_is_steal_acquirable() {
    let dir = tempfile::tempdir().expect("tempdir");
    let catalog = sqlite_catalog(&dir).await;
    create_maintenance_log_table(&catalog).await;

    // holder-1 claims with a 1-second TTL starting at t=1_000ms, so it
    // expires at t=2_000ms.
    let _h1 = try_acquire(&catalog, STATE_TABLE, JOB_KEY, "holder-1", 1, 1_000)
        .await
        .expect("try_acquire succeeds")
        .expect("holder-1 acquires");

    // Before expiry: holder-2 must be denied.
    let denied = try_acquire(&catalog, STATE_TABLE, JOB_KEY, "holder-2", 60, 1_500)
        .await
        .expect("try_acquire succeeds");
    assert!(denied.is_none(), "must be denied before expiry");

    // At/after expiry: holder-2 steals it.
    let stolen = try_acquire(&catalog, STATE_TABLE, JOB_KEY, "holder-2", 60, 2_000)
        .await
        .expect("try_acquire succeeds")
        .expect("holder-2 must be able to steal an expired lease");
    assert_eq!(stolen.holder_id, "holder-2");
    assert_eq!(stolen.expires_at_ms, 2_000 + 60_000);
}

#[tokio::test]
async fn renew_extends_expiry_and_keeps_other_holders_out() {
    let dir = tempfile::tempdir().expect("tempdir");
    let catalog = sqlite_catalog(&dir).await;
    create_maintenance_log_table(&catalog).await;

    let mut h1 = try_acquire(&catalog, STATE_TABLE, JOB_KEY, "holder-1", 60, 1_000)
        .await
        .expect("try_acquire succeeds")
        .expect("holder-1 acquires");
    assert_eq!(h1.expires_at_ms, 61_000);

    renew(&mut h1, &catalog, STATE_TABLE, 60, 50_000)
        .await
        .expect("renew succeeds while holder-1 still owns the live claim");
    assert_eq!(h1.expires_at_ms, 110_000, "renew must extend from the renew-time now_ms, not the original acquire time");

    // Without the renew this would already have expired (original expiry
    // was 61_000); the renewed expiry (110_000) must still deny holder-2.
    let denied = try_acquire(&catalog, STATE_TABLE, JOB_KEY, "holder-2", 60, 90_000)
        .await
        .expect("try_acquire succeeds");
    assert!(denied.is_none(), "renewed lease must still be live at t=90_000");
}

#[tokio::test]
async fn renew_after_lease_lost_to_a_steal_returns_err() {
    let dir = tempfile::tempdir().expect("tempdir");
    let catalog = sqlite_catalog(&dir).await;
    create_maintenance_log_table(&catalog).await;

    let mut h1 = try_acquire(&catalog, STATE_TABLE, JOB_KEY, "holder-1", 1, 1_000)
        .await
        .expect("try_acquire succeeds")
        .expect("holder-1 acquires");

    // Let it expire and get stolen by holder-2.
    let _h2 = try_acquire(&catalog, STATE_TABLE, JOB_KEY, "holder-2", 60, 5_000)
        .await
        .expect("try_acquire succeeds")
        .expect("holder-2 steals the expired lease");

    let err = renew(&mut h1, &catalog, STATE_TABLE, 60, 6_000)
        .await
        .expect_err("holder-1 must not be able to renew a lease holder-2 now owns");
    let msg = format!("{err}");
    assert!(
        msg.contains("holder-1") || msg.to_lowercase().contains("no longer live") || msg.to_lowercase().contains("held by"),
        "error should explain the lost lease, got: {msg}"
    );
}

#[tokio::test]
async fn concurrent_try_acquire_for_the_same_job_key_exactly_one_wins() {
    let dir = tempfile::tempdir().expect("tempdir");
    let catalog = sqlite_catalog(&dir).await;
    create_maintenance_log_table(&catalog).await;

    // Seed a real "released" sentinel file first (one sequential
    // acquire+release), so the two racers below both start from a state
    // with a genuine live file to CAS against -- the Task 1 primitive's
    // exclusion only engages when there is something to delete-and-replace.
    // (The very first-ever claim on an empty job_key is a documented,
    // accepted exception to exclusion -- see maintenance_lease.rs's module
    // docs -- so it is deliberately not what this test is racing.)
    let seed = try_acquire(&catalog, STATE_TABLE, JOB_KEY, "seed-holder", 60, 500)
        .await
        .expect("seed try_acquire succeeds")
        .expect("seed holder acquires");
    release(seed, &catalog, STATE_TABLE, 600)
        .await
        .expect("seed release succeeds");

    let catalog_a = catalog.clone();
    let catalog_b = catalog.clone();
    let (result_a, result_b) = tokio::join!(
        try_acquire(&catalog_a, STATE_TABLE, JOB_KEY, "racer-a", 60, 1_000),
        try_acquire(&catalog_b, STATE_TABLE, JOB_KEY, "racer-b", 60, 1_000)
    );

    let a = result_a.expect("racer-a's try_acquire must not error");
    let b = result_b.expect("racer-b's try_acquire must not error");

    let winners = [a.is_some(), b.is_some()].iter().filter(|w| **w).count();
    assert_eq!(
        winners, 1,
        "exactly one concurrent try_acquire for the same job_key must return Some, got a={a:?} b={b:?}"
    );
}
