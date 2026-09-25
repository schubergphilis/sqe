//! Finding 611 item 5 / finding 807 item 5: the `compact_file_group` rewrite
//! against a live S3-compatible endpoint and a real Iceberg table.
//!
//! This used to be an `#[ignore]`d unit test inside `src/compaction.rs` that
//! read `SQE_IT_*` variables from production code. It now lives with the other
//! protocol scenarios and skips LOUDLY (a `skipping:` line on stderr, then
//! `Ok`) when the environment is not set, so `cargo test -p sqe-worker --test
//! protocol` needs no services. Against the local `make test-integration`
//! stack (Polaris + RustFS) run:
//!
//! ```text
//! SQE_IT_METADATA_LOCATION=s3://warehouse/ns/tbl/metadata/00002-....metadata.json \
//! SQE_IT_SNAPSHOT_ID=<current-snapshot-id-from-that-metadata.json> \
//! SQE_IT_GROUP_FILE_PATH=s3://warehouse/ns/tbl/data/<some-existing-file>.parquet \
//! SQE_IT_S3_ENDPOINT=http://localhost:9000 \
//! SQE_IT_S3_ACCESS_KEY=... SQE_IT_S3_SECRET_KEY=... \
//!   cargo test -p sqe-worker --test it compact_file_group_against_live_s3 -- --nocapture
//! ```
//!
//! It is not a `make test-integration FILTER=` case yet: that harness runs
//! `cargo test -p sqe-coordinator -- --ignored`, so promoting it means a
//! coordinator-crate test that creates the table through Polaris first and
//! then drives this rewrite; that lives outside `sqe-worker`.

use datafusion::prelude::SessionContext;
use iceberg::spec::{FormatVersion, NestedField, PrimitiveType};
use sqe_compaction::wire::{CompactGroupRequest, S3Conn};
use sqe_worker::compaction::compact_file_group;

struct LiveEnv {
    metadata_location: String,
    snapshot_id: i64,
    group_file_path: String,
    s3: S3Conn,
}

fn live_env() -> Option<LiveEnv> {
    let var = |name: &str| std::env::var(name).ok().filter(|v| !v.is_empty());
    let Some(metadata_location) = var("SQE_IT_METADATA_LOCATION") else {
        eprintln!(
            "skipping compact_file_group_against_live_s3: set SQE_IT_METADATA_LOCATION, \
             SQE_IT_SNAPSHOT_ID, SQE_IT_GROUP_FILE_PATH and SQE_IT_S3_* against the local \
             Polaris + RustFS stack to run it (see this file's header)"
        );
        return None;
    };
    let snapshot_id = var("SQE_IT_SNAPSHOT_ID")
        .expect("SQE_IT_SNAPSHOT_ID must be set when SQE_IT_METADATA_LOCATION is")
        .parse()
        .expect("SQE_IT_SNAPSHOT_ID must be an i64");
    let group_file_path = var("SQE_IT_GROUP_FILE_PATH")
        .expect("SQE_IT_GROUP_FILE_PATH must be set when SQE_IT_METADATA_LOCATION is");
    Some(LiveEnv {
        metadata_location,
        snapshot_id,
        group_file_path,
        s3: S3Conn {
            endpoint: var("SQE_IT_S3_ENDPOINT").unwrap_or_default(),
            region: var("SQE_IT_S3_REGION").unwrap_or_default(),
            access_key: var("SQE_IT_S3_ACCESS_KEY").unwrap_or_default(),
            secret_key: var("SQE_IT_S3_SECRET_KEY").unwrap_or_default(),
            session_token: var("SQE_IT_S3_SESSION_TOKEN").unwrap_or_default(),
            path_style: true,
            allow_http: true,
        },
    })
}

/// Point at a real Iceberg table's metadata.json and S3 credentials and
/// confirm `compact_file_group` returns a `CompactGroupResponse` whose
/// `new_data_files_avro` decodes via `read_data_files_from_avro`.
#[tokio::test]
async fn compact_file_group_against_live_s3() {
    let Some(env) = live_env() else {
        return;
    };

    let request = CompactGroupRequest {
        audience: String::new(),
        expires_at: 0,
        job_id: "protocol-live".to_string(),
        group_id: 0,
        table_ident: "protocol.live.table".to_string(),
        metadata_location: env.metadata_location,
        snapshot_id: env.snapshot_id,
        group_file_paths: vec![env.group_file_path],
        target_file_size_bytes: 128 * 1024 * 1024,
        compression: "zstd".to_string(),
        sort: None,
        s3: env.s3,
    };

    let ctx = SessionContext::new();
    let response = compact_file_group(&ctx, &request, None)
        .await
        .expect("compaction failed");

    // Schema/partition_type here are placeholders: a real run against a
    // table with a different shape should load them from the same
    // metadata.json (`table.metadata().current_schema()` /
    // `table.metadata().default_partition_type()`).
    let schema = iceberg::spec::Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![NestedField::required(
            1,
            "id",
            iceberg::spec::Type::Primitive(PrimitiveType::Long),
        )
        .into()])
        .build()
        .unwrap();
    let partition_type = iceberg::spec::StructType::new(vec![]);
    let decoded = iceberg::spec::read_data_files_from_avro(
        &mut response.new_data_files_avro.as_slice(),
        &schema,
        0,
        &partition_type,
        FormatVersion::V2,
    )
    .expect("avro decode of returned data files must round-trip");
    assert!(
        !decoded.is_empty(),
        "expected at least one rewritten data file"
    );
}
