//! A V3 table created, written and read through the AWS Glue Data Catalog
//! backend (`[catalog.backend] type = "glue"`), end to end through SQL.
//!
//! The catalog-level Glue tests in `sqe-catalog` (`backends_integration.rs`)
//! only round-trip a namespace. This one runs CREATE TABLE / INSERT / SELECT
//! through the `QueryHandler`, so the data files land in the Glue warehouse
//! bucket and the metadata carries `format-version: 3`.
//!
//! Needs AWS credentials (standard provider chain, `AWS_PROFILE`), a region
//! and a pre-created warehouse bucket; the local Polaris stack is used only
//! to authenticate the session:
//!
//! ```text
//! set -a; source .env; set +a   # AWS_PROFILE, AWS_REGION, SQE_TEST_GLUE_WAREHOUSE
//! cargo test -p sqe-coordinator --features glue --test it glue_v3_live_e2e -- \
//!     --ignored --nocapture
//! ```
//!
//! Without `SQE_TEST_GLUE_WAREHOUSE` the test skips with the reason.
#![cfg(feature = "glue")]

use std::sync::Arc;

use iceberg::{Catalog, NamespaceIdent, TableIdent};

fn glue_config(region: &str, warehouse: &str) -> sqe_core::SqeConfig {
    let mut config = sqe_core::SqeConfig::load(&crate::common::test_config_path())
        .expect("Failed to load test config");
    config.catalog.backend = sqe_core::config::CatalogBackend::Glue {
        region: region.to_string(),
        warehouse: warehouse.to_string(),
        endpoint: None,
    };
    // The test config points storage at the local RustFS; Glue tables live
    // in real S3 and take credentials from the AWS provider chain.
    config.storage.s3_endpoint = String::new();
    config.storage.s3_region = region.to_string();
    config.storage.s3_access_key = String::new();
    config.storage.s3_secret_key = Default::default();
    config.storage.s3_session_token = Default::default();
    config.storage.s3_path_style = false;
    config.storage.s3_allow_http = false;
    config
}

async fn handler_for(
    config: sqe_core::SqeConfig,
) -> (sqe_core::Session, sqe_coordinator::QueryHandler) {
    crate::common::init_tracing();
    let authenticator = sqe_auth::Authenticator::new(&config.auth)
        .await
        .expect("Failed to create authenticator");
    let session = authenticator
        .authenticate("root", "")
        .await
        .expect("Auth failed for root");
    let query_tracker = Arc::new(sqe_coordinator::query_tracker::QueryTracker::new(
        &config.query_history,
    ));
    let handler = sqe_coordinator::QueryHandler::new(
        Arc::new(sqe_policy::PassthroughEnforcer),
        None,
        config,
        None,
        None,
        None,
        None,
        query_tracker,
        None,
        None, // grant_backend
        None, // lineage observer
        sqe_coordinator::RuntimeCatalogRegistry::default(),
        sqe_core::SecretStore::default(),
    )
    .expect("Failed to create QueryHandler");
    (session, handler)
}

async fn rows(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    sql: &str,
) -> Vec<String> {
    let batches = handler
        .execute(session, sql, None)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    let mut out = Vec::new();
    for batch in &batches {
        for row in 0..batch.num_rows() {
            let cells: Vec<String> = (0..batch.num_columns())
                .map(|c| crate::common::fmt_val(batch.column(c).as_ref(), row))
                .collect();
            out.push(cells.join("|"));
        }
    }
    out
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:aws; SQE_TEST_GLUE_WAREHOUSE"]
async fn v3_table_round_trips_through_glue() {
    let Ok(warehouse) = std::env::var("SQE_TEST_GLUE_WAREHOUSE") else {
        eprintln!(
            "skipping v3_table_round_trips_through_glue: SQE_TEST_GLUE_WAREHOUSE is not set \
             (copy .env.example to .env and source it)"
        );
        return;
    };
    let region = std::env::var("AWS_REGION").unwrap_or_else(|_| "eu-central-1".into());
    let config = glue_config(&region, &warehouse);
    let (session, handler) = handler_for(config.clone()).await;

    // Glue database names: lowercase ASCII, digits and underscore.
    let ns = format!("sqe_glue_v3_{}", uuid::Uuid::new_v4().simple());
    let fq = format!("{ns}.events");
    handler
        .execute(&session, &format!("CREATE SCHEMA {ns}"), None)
        .await
        .expect("CREATE SCHEMA through Glue");

    let result = async {
        handler
            .execute(
                &session,
                &format!(
                    "CREATE TABLE {fq} (id BIGINT, name VARCHAR, ts TIMESTAMP_NS(9)) \
                     TBLPROPERTIES ('format-version' = '3')"
                ),
                None,
            )
            .await
            .expect("CREATE TABLE format-version 3 through Glue");
        handler
            .execute(
                &session,
                &format!(
                    "INSERT INTO {fq} VALUES \
                     (1, 'a', TIMESTAMP '2026-10-22 10:00:00.123456789'), \
                     (2, 'b', TIMESTAMP '2026-10-22 10:00:01.987654321')"
                ),
                None,
            )
            .await
            .expect("INSERT through Glue");
        let read = rows(
            &handler,
            &session,
            &format!("SELECT id, name, CAST(ts AS VARCHAR) FROM {fq} ORDER BY id"),
        )
        .await;

        // Reload the table straight from Glue: the committed metadata must be
        // V3 and the data files must sit under the Glue warehouse.
        let catalog = Arc::new(
            sqe_catalog::SessionCatalog::for_session(&config, None, "")
                .await
                .expect("SessionCatalog over Glue"),
        )
        .as_catalog();
        let table = catalog
            .load_table(&TableIdent::new(
                NamespaceIdent::new(ns.clone()),
                "events".to_string(),
            ))
            .await
            .expect("load_table from Glue");
        let files = rows(
            &handler,
            &session,
            &format!("SELECT file_path FROM table_files('{ns}', 'events')"),
        )
        .await;
        (read, table.metadata().format_version(), files)
    }
    .await;

    // Clean up before asserting, so a failed assertion leaves no Glue
    // database behind.
    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {fq}"), None)
        .await;
    let _ = handler
        .execute(&session, &format!("DROP SCHEMA IF EXISTS {ns}"), None)
        .await;

    let (read, format_version, files) = result;
    assert_eq!(
        read,
        vec![
            "1|a|2026-10-22T10:00:00.123456789".to_string(),
            "2|b|2026-10-22T10:00:01.987654321".to_string(),
        ],
        "rows read back through Glue"
    );
    assert_eq!(format_version, iceberg::spec::FormatVersion::V3);
    assert!(!files.is_empty(), "INSERT committed at least one data file");
    let bucket = warehouse
        .trim_start_matches("s3://")
        .split('/')
        .next()
        .unwrap_or_default()
        .to_string();
    assert!(
        files.iter().all(|f| f.contains(&bucket)),
        "data files under the Glue warehouse bucket: {files:?}"
    );
}
