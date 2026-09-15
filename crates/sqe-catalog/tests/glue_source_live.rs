//! `GlueMetadataSource` against a real AWS Glue catalog.
//!
//! `--ignored` because it needs live credentials and a populated catalog.
//! Run with:
//!
//! ```text
//! eval "$(aws configure export-credentials --format env)"
//! AWS_REGION=eu-central-1 cargo test -p sqe-catalog --features glue \
//!   --test glue_source_live -- --ignored --nocapture
//! ```
//!
//! Point it at a different catalog with `SQE_TEST_GLUE_DATABASE` and
//! `SQE_TEST_GLUE_TABLE`; both default to a table in the survey account.

#![cfg(feature = "glue")]

use sqe_catalog::hive::glue_source::GlueMetadataSource;
use sqe_catalog::hive::source::HiveMetadataSource;

fn region() -> Option<String> {
    std::env::var("AWS_REGION")
        .ok()
        .or_else(|| std::env::var("AWS_DEFAULT_REGION").ok())
}

/// last verified: never in this tree
#[tokio::test]
#[ignore = "needs:aws; live Glue catalog"]
async fn lists_every_database_in_the_region() {
    let src = GlueMetadataSource::from_env(region().as_deref(), None, Vec::new(), Default::default(), None).await;
    let dbs = src.list_databases().await.expect("get_databases");
    assert!(
        dbs.len() > 1,
        "expected a populated catalog, got {dbs:?}. Paging is the thing under test here: a \
         single-page result would not exercise it."
    );
    println!("{} databases", dbs.len());
}

/// last verified: never in this tree
#[tokio::test]
#[ignore = "needs:aws; live Glue catalog"]
async fn a_table_maps_onto_a_manifest_the_provider_can_read() {
    let db = std::env::var("SQE_TEST_GLUE_DATABASE").unwrap_or_else(|_| "claude".to_string());
    let table = std::env::var("SQE_TEST_GLUE_TABLE").unwrap_or_else(|_| "user_usage".to_string());

    let src = GlueMetadataSource::from_env(region().as_deref(), None, Vec::new(), Default::default(), None).await;

    let tables = src.list_tables(&db).await.expect("get_tables");
    assert!(tables.contains(&table), "{db} has no {table}: {tables:?}");

    let m = src.get(&db, &table).await.expect("get_table");
    assert_eq!(m.database_name, db);
    assert_eq!(m.name, table);

    // The three fields the read path cannot work without.
    assert!(
        m.storage_descriptor.location.starts_with("s3://"),
        "location: {}",
        m.storage_descriptor.location
    );
    assert!(
        m.storage_descriptor.location.ends_with('/'),
        "a location without a trailing slash also matches sibling prefixes: {}",
        m.storage_descriptor.location
    );
    assert!(
        m.storage_descriptor
            .serde_info
            .serialization_library
            .is_some(),
        "no SerDe recorded, so the file format cannot be resolved"
    );
    assert!(!m.storage_descriptor.columns.is_empty(), "no columns");

    // Partition keys live in their own list, never in `columns`. The provider
    // appends them at the tail of the query schema and would double them up
    // if Glue reported them in both places.
    let col_names: Vec<&str> = m
        .storage_descriptor
        .columns
        .iter()
        .map(|c| c.name.as_str())
        .collect();
    for key in &m.partition_keys {
        assert!(
            !col_names.contains(&key.name.as_str()),
            "partition key `{}` also appears in columns",
            key.name
        );
    }

    // The manifest must survive the same validation a sidecar one does,
    // or the provider will reject it at load.
    let bytes = serde_json::to_vec(&m).expect("serialize");
    sqe_catalog::hive::manifest::HiveManifest::parse(&bytes, src.limits())
        .expect("a Glue-derived manifest must pass manifest validation");

    println!(
        "{db}.{table}: {} cols, {} partition keys, serde {:?}",
        m.storage_descriptor.columns.len(),
        m.partition_keys.len(),
        m.storage_descriptor.serde_info.serialization_library
    );
}

/// A projected table carries its `projection.*` properties through, which is
/// the only reason it is readable: 192 of the surveyed catalog's projected
/// tables have no partitions registered in Glue at all.
/// last verified: never in this tree
#[tokio::test]
#[ignore = "needs:aws; live Glue catalog"]
async fn projection_properties_survive_the_mapping() {
    let src = GlueMetadataSource::from_env(region().as_deref(), None, Vec::new(), Default::default(), None).await;
    let m = src
        .get("usage_data_raw", "sbp_cloud")
        .await
        .expect("get_table");
    assert_eq!(
        m.parameters.get("projection.enabled").map(String::as_str),
        Some("true"),
        "projection.enabled did not survive: {:?}",
        m.parameters
    );
    assert!(
        m.parameters.contains_key("projection.snapshot_date.range"),
        "the date range is what NOW resolution needs: {:?}",
        m.parameters
    );
}

/// last verified: never in this tree
#[tokio::test]
#[ignore = "needs:aws; live Glue catalog"]
async fn an_absent_table_is_not_found_rather_than_a_transport_error() {
    let src = GlueMetadataSource::from_env(region().as_deref(), None, Vec::new(), Default::default(), None).await;
    let err = src
        .get("claude", "no_such_table_ee7f21")
        .await
        .expect_err("expected NotFound");
    assert!(
        matches!(err, sqe_catalog::hive::HiveError::NotFound(_)),
        "got {err:?}, which the provider would surface as a failure rather than a missing table"
    );
}
