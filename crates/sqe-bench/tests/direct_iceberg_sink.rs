//! Stack-gated integration test for the generic direct-to-Iceberg sink
//! (`sqe_bench::sink::iceberg::run_direct`).
//!
//! Marked `#[ignore]` because it needs a live Iceberg REST catalog and
//! S3-compatible object store; it is not run by plain `cargo test`. Bring
//! up the quickstart benchmark stack (Nessie + RustFS) and run it with
//! `--ignored`:
//!
//! ```bash
//! cd quickstart/benchmark
//! cp .env.example .env  # if present; otherwise the compose defaults apply
//! docker compose up -d rustfs bucket-init nessie
//! cd -
//! cargo test -p sqe-bench --test direct_iceberg_sink -- --ignored
//! ```
//!
//! Any Iceberg REST catalog works, not just Nessie: point the
//! `SQE_TEST_ICEBERG_*` env vars (see the test doc comment below) at a
//! Polaris deployment instead and set the client-credentials pair.

use std::collections::HashMap;

use futures::TryStreamExt;
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent};
use iceberg_catalog_rest::RestCatalogBuilder;

use sqe_bench::generate::{get_generator, GenerateConfig};
use sqe_bench::sink::iceberg::{run_direct, IcebergTarget};

/// Env-driven connection settings, defaulted to match
/// `quickstart/benchmark/docker-compose.yml` (Nessie over RustFS, no auth).
/// Point these at a Polaris deployment (with `SQE_TEST_ICEBERG_CLIENT_ID`
/// / `SQE_TEST_ICEBERG_CLIENT_SECRET` set) to exercise that path instead.
fn test_target() -> IcebergTarget {
    let catalog_uri = std::env::var("SQE_TEST_ICEBERG_CATALOG_URI")
        .unwrap_or_else(|_| "http://localhost:19120/iceberg/".to_string());
    let warehouse =
        std::env::var("SQE_TEST_ICEBERG_WAREHOUSE").unwrap_or_else(|_| "warehouse".to_string());
    let namespace = std::env::var("SQE_TEST_ICEBERG_NAMESPACE")
        .unwrap_or_else(|_| "sqe_bench_it_tpch".to_string());
    let client_id = std::env::var("SQE_TEST_ICEBERG_CLIENT_ID").ok();
    let client_secret = std::env::var("SQE_TEST_ICEBERG_CLIENT_SECRET").ok();
    let s3_endpoint = std::env::var("SQE_TEST_ICEBERG_S3_ENDPOINT")
        .unwrap_or_else(|_| "http://localhost:19000".to_string());
    let s3_access_key =
        std::env::var("SQE_TEST_ICEBERG_S3_ACCESS_KEY").unwrap_or_else(|_| "s3admin".to_string());
    let s3_secret_key = std::env::var("SQE_TEST_ICEBERG_S3_SECRET_KEY")
        .unwrap_or_else(|_| "s3adminpw".to_string());
    let s3_region =
        std::env::var("SQE_TEST_ICEBERG_S3_REGION").unwrap_or_else(|_| "us-east-1".to_string());

    let credential = match (client_id, client_secret) {
        (Some(id), Some(secret)) => Some(format!("{id}:{secret}")),
        _ => None,
    };

    IcebergTarget {
        catalog_uri,
        warehouse,
        namespace,
        credential,
        oauth2_server_uri: None,
        scope: None,
        bearer_token: None,
        s3_endpoint: Some(s3_endpoint),
        s3_access_key: Some(s3_access_key),
        s3_secret_key: Some(s3_secret_key),
        s3_region: Some(s3_region),
        s3_path_style: true,
    }
}

fn test_scale() -> f64 {
    std::env::var("SQE_TEST_ICEBERG_SCALE")
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(0.01)
}

/// Sum of `num_rows()` across every batch a full table scan produces.
async fn count_table_rows(
    catalog: &dyn Catalog,
    table: &TableIdent,
) -> anyhow::Result<u64> {
    let t = catalog.load_table(table).await?;
    let stream = t.scan().build()?.to_arrow().await?;
    let rows = stream
        .try_fold(0u64, |acc, batch| async move { Ok(acc + batch.num_rows() as u64) })
        .await?;
    Ok(rows)
}

async fn current_snapshot_id(
    catalog: &dyn Catalog,
    table: &TableIdent,
) -> anyhow::Result<Option<i64>> {
    let t = catalog.load_table(table).await?;
    Ok(t.metadata().current_snapshot_id())
}

/// Full round trip: `run_direct` writes every tpch table straight into
/// Iceberg, a scan confirms the row counts match the generator's expected
/// counts, and a second `resume=true` run hits the per-table skip path
/// (no new snapshot committed).
///
/// Env vars (all optional, defaulted to the `quickstart/benchmark` compose
/// stack -- Nessie REST catalog over RustFS S3, no auth):
///   SQE_TEST_ICEBERG_CATALOG_URI   (default http://localhost:19120/iceberg/)
///   SQE_TEST_ICEBERG_WAREHOUSE     (default warehouse)
///   SQE_TEST_ICEBERG_NAMESPACE     (default sqe_bench_it_tpch)
///   SQE_TEST_ICEBERG_CLIENT_ID     (unset = no OAuth credential, e.g. Nessie)
///   SQE_TEST_ICEBERG_CLIENT_SECRET (paired with CLIENT_ID, e.g. Polaris)
///   SQE_TEST_ICEBERG_S3_ENDPOINT   (default http://localhost:19000)
///   SQE_TEST_ICEBERG_S3_ACCESS_KEY (default s3admin)
///   SQE_TEST_ICEBERG_S3_SECRET_KEY (default s3adminpw)
///   SQE_TEST_ICEBERG_S3_REGION     (default us-east-1)
///   SQE_TEST_ICEBERG_SCALE         (default 0.01)
#[tokio::test]
#[ignore = "requires a live Iceberg REST catalog + S3-compatible store; run with --ignored \
            (see SQE_TEST_ICEBERG_* env vars in this file's doc comment; defaults match \
            quickstart/benchmark's Nessie/RustFS compose stack)"]
async fn tpch_direct_sink_writes_and_resumes() {
    let target = test_target();
    let scale = test_scale();
    let config = GenerateConfig::resolve(Some(2), None, None).expect("resolve GenerateConfig");
    let gen = get_generator("tpch").expect("tpch generator");
    let target_file_size = 64 * 1024 * 1024;

    // First run: clean slate, no resume. Every table gets written and
    // committed with its `sqe-bench.table.<name>=done` marker.
    run_direct(&target, gen.as_ref(), scale, &config, true, false, target_file_size)
        .await
        .expect("first run_direct should write every table");

    // Build the verify/read-back connection from the exact same property map
    // `run_direct` writes through (URI, warehouse, credential, and all
    // s3_* settings), so a scan failure here reflects a real catalog/storage
    // mismatch rather than an under-specified test harness.
    let verify_catalog = RestCatalogBuilder::default()
        .load("sqe-bench-it", target.catalog_props())
        .await
        .expect("verify catalog connects");
    let verify_catalog: Box<dyn Catalog> = Box::new(verify_catalog);
    let ns = NamespaceIdent::new(target.namespace.clone());

    let mut snapshots_after_first = HashMap::new();
    for table_def in gen.tables() {
        let ident = TableIdent::new(ns.clone(), table_def.name.clone());
        let expected = (table_def.row_count)(scale) as u64;
        let actual = count_table_rows(verify_catalog.as_ref(), &ident)
            .await
            .unwrap_or_else(|e| panic!("scanning {} failed: {e}", table_def.name));
        assert_eq!(
            actual, expected,
            "{}: committed row count should match the generator's expected count",
            table_def.name
        );

        let snap = current_snapshot_id(verify_catalog.as_ref(), &ident)
            .await
            .unwrap_or_else(|e| panic!("loading snapshot id for {} failed: {e}", table_def.name));
        snapshots_after_first.insert(table_def.name.clone(), snap);
    }

    // Second run: resume=true, clean=false. Every table is already marked
    // done, so run_direct must take the skip path: no new snapshot, no
    // duplicated rows.
    run_direct(&target, gen.as_ref(), scale, &config, false, true, target_file_size)
        .await
        .expect("second (resume) run_direct should succeed via the skip path");

    for table_def in gen.tables() {
        let ident = TableIdent::new(ns.clone(), table_def.name.clone());
        let expected = (table_def.row_count)(scale) as u64;

        let actual = count_table_rows(verify_catalog.as_ref(), &ident)
            .await
            .unwrap_or_else(|e| panic!("re-scanning {} failed: {e}", table_def.name));
        assert_eq!(
            actual, expected,
            "{}: resumed row count should be unchanged (no duplicate commit)",
            table_def.name
        );

        let snap = current_snapshot_id(verify_catalog.as_ref(), &ident)
            .await
            .unwrap_or_else(|e| panic!("re-loading snapshot id for {} failed: {e}", table_def.name));
        assert_eq!(
            snap, snapshots_after_first[&table_def.name],
            "{}: resume should skip the table, so the snapshot id must not change",
            table_def.name
        );
    }
}
