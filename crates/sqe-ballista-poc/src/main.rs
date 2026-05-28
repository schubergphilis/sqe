//! SQE-on-ballista vertical-slice PoC.
//!
//! Runs an in-process ballista cluster (`SessionContext::standalone_with_state`)
//! against the existing Polaris + RustFS test stack and submits
//! `SELECT COUNT(*) FROM iceberg.tpch_sf0_1.lineitem`.
//!
//! See `docs/superpowers/specs/2026-05-28-sqe-on-ballista-poc-plan.md` for
//! the verification points (iceberg `TableProvider` reachable from the
//! ballista executor, per-query OIDC bearer reaches the executor, result
//! equals 600,000).
//!
//! Run with the existing test+compare stack up:
//!
//! ```sh
//! docker compose -f docker-compose.test.yml -f docker-compose.compare.yml up -d
//! ./scripts/bootstrap-test.sh
//! cargo run -p sqe-ballista-poc
//! ```
//!
//! Expects `tpch_sf0_1.lineitem` already loaded (SF0.1) via
//! `sqe-bench load tpch --scale 0.1 ...`.

use std::collections::HashMap;
use std::sync::Arc;

mod codec;

use anyhow::{Context, Result};
use ballista::datafusion::{
    execution::SessionStateBuilder,
    prelude::{SessionConfig, SessionContext},
};
use ballista::prelude::{SessionConfigExt, SessionContextExt};
use iceberg::CatalogBuilder;
use iceberg_catalog_rest::{REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalogBuilder};
use iceberg_datafusion::IcebergCatalogProvider;

use crate::codec::{IcebergLogicalCodec, IcebergPhysicalCodec};

const POLARIS_URL: &str = "http://localhost:18181/api/catalog";
const POLARIS_TOKEN_URL: &str = "http://localhost:18181/api/catalog/v1/oauth/tokens";
const POLARIS_CLIENT_ID: &str = "root";
const POLARIS_CLIENT_SECRET: &str = "s3cr3t";
const WAREHOUSE: &str = "test_warehouse";

const S3_ENDPOINT: &str = "http://localhost:19000";
const S3_ACCESS_KEY: &str = "s3admin";
const S3_SECRET_KEY: &str = "s3admin";
const S3_REGION: &str = "us-east-1";

const QUERY: &str = "SELECT COUNT(*) AS n FROM iceberg.tpch_sf0_1.lineitem";
const EXPECTED_COUNT: i64 = 600_000;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG").unwrap_or_else(|_| {
                "info,ballista=info,ballista_core=info,iceberg=info,sqe_ballista_poc=info".into()
            }),
        )
        .init();

    tracing::info!("PoC: SELECT COUNT(*) on tpch_sf0_1.lineitem via ballista standalone");

    // ── Verification point #2 (part 1): obtain a Polaris OAuth token ────
    // The PoC binary plays the role the SQE coordinator would play in
    // production: get the user's bearer, hand it to the ballista cluster.
    // For the spike we use the same client_credentials grant
    // sqe-coordinator already uses.
    let token = fetch_polaris_token().await.context("fetching Polaris token")?;
    tracing::info!(
        "got Polaris token ({} chars), first 12 = {}…",
        token.len(),
        &token[..12.min(token.len())]
    );

    // ── Verification point #1: register iceberg TableProvider ──────────
    // The IcebergCatalogProvider sits in the DataFusion SessionContext
    // that ballista's standalone cluster shares between the in-process
    // scheduler and executor.  In a multi-process deployment this is the
    // moment that would need `override_config_producer` on the executor
    // side — we cheat in this PoC by running standalone.
    let iceberg_catalog = build_iceberg_catalog(&token)
        .await
        .context("building iceberg REST catalog")?;
    let iceberg_provider = Arc::new(
        IcebergCatalogProvider::try_new(iceberg_catalog.clone())
            .await
            .context("wrapping catalog as DataFusion CatalogProvider")?,
    );
    tracing::info!("iceberg catalog ready");

    // ── PoC findings: ballista needs BOTH codecs ───────────────────────
    // iceberg-datafusion ships neither a LogicalExtensionCodec (for the
    // IcebergTableProvider) nor a PhysicalExtensionCodec (for the
    // IcebergTableScan node).  We supply both; each rehydrates iceberg
    // objects from the catalog on the executor side.  See `codec.rs`.
    let logical_codec = Arc::new(IcebergLogicalCodec::new(iceberg_provider.clone()));
    let physical_codec = Arc::new(IcebergPhysicalCodec::new(iceberg_catalog));

    // ── Spin up the in-process ballista cluster ────────────────────────
    let config = SessionConfig::new_with_ballista()
        .with_target_partitions(2)
        .with_ballista_standalone_parallelism(2)
        .with_ballista_logical_extension_codec(logical_codec)
        .with_ballista_physical_extension_codec(physical_codec);

    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .build();

    let ctx = SessionContext::standalone_with_state(state)
        .await
        .context("starting ballista standalone session")?;

    ctx.register_catalog("iceberg", iceberg_provider);
    tracing::info!("ballista standalone context ready, iceberg catalog + codec registered");

    // ── Verification point #3: run the query, check the row count ──────
    tracing::info!("submitting: {QUERY}");
    let df = ctx.sql(QUERY).await.context("planning query")?;

    let batches = df.collect().await.context("executing query")?;

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    if total_rows == 0 {
        anyhow::bail!("query returned zero result rows");
    }
    arrow::util::pretty::print_batches(&batches)?;

    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .context("COUNT(*) column wasn't Int64")?
        .value(0);

    if count != EXPECTED_COUNT {
        anyhow::bail!(
            "verification point #3 FAIL: COUNT(*) = {count}, expected {EXPECTED_COUNT}"
        );
    }

    tracing::info!("verification #3 PASS: COUNT(*) = {count}");
    tracing::info!("PoC end-to-end success");
    Ok(())
}

async fn fetch_polaris_token() -> Result<String> {
    let client = reqwest::Client::new();
    let resp = client
        .post(POLARIS_TOKEN_URL)
        .form(&[
            ("grant_type", "client_credentials"),
            ("client_id", POLARIS_CLIENT_ID),
            ("client_secret", POLARIS_CLIENT_SECRET),
            ("scope", "PRINCIPAL_ROLE:ALL"),
        ])
        .send()
        .await?
        .error_for_status()?;
    let body: serde_json::Value = resp.json().await?;
    body.get("access_token")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| anyhow::anyhow!("Polaris token response had no access_token: {body}"))
}

async fn build_iceberg_catalog(token: &str) -> Result<Arc<dyn iceberg::Catalog>> {
    let mut props: HashMap<String, String> = HashMap::new();
    props.insert(REST_CATALOG_PROP_URI.to_string(), POLARIS_URL.to_string());
    props.insert(REST_CATALOG_PROP_WAREHOUSE.to_string(), WAREHOUSE.to_string());
    // Polaris bearer reuse — same token vendoring SQE does today.
    props.insert("token".to_string(), token.to_string());
    // Object store credentials.  This is the seam that, in the full
    // architecture, ballista's executor-side `override_runtime_producer`
    // would set up with the per-query bearer.  For the standalone PoC
    // they're set once at context construction.
    props.insert("s3.endpoint".to_string(), S3_ENDPOINT.to_string());
    props.insert("s3.access-key-id".to_string(), S3_ACCESS_KEY.to_string());
    props.insert("s3.secret-access-key".to_string(), S3_SECRET_KEY.to_string());
    props.insert("s3.path-style-access".to_string(), "true".to_string());
    props.insert("client.region".to_string(), S3_REGION.to_string());

    let catalog = RestCatalogBuilder::default()
        .load("sqe-ballista-poc".to_string(), props)
        .await
        .map_err(|e| anyhow::anyhow!("RestCatalogBuilder::load: {e}"))?;
    Ok(Arc::new(catalog))
}
