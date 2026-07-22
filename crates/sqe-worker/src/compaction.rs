//! Worker-side executor for the distributed `compact_file_group` Flight
//! action (Phase 4c Task 3).
//!
//! The coordinator (Task 4, not yet implemented) signs and sends one
//! [`CompactGroupRequest`] per bin-packed file group to a worker's
//! `do_action("compact_file_group")`. This module does the actual rewrite:
//! build a catalog-free `FileIO` from the request's S3 credentials, pin a
//! [`StaticTable`] to the requested `metadata_location`, verify it is still
//! at the snapshot the coordinator planned against, apply deletes while
//! reading the group's files, optionally sort, and write fresh Parquet data
//! files back to the table's data location.
//!
//! Workers never see a catalog token and never commit: the coordinator
//! (Task 4) commits the returned, Avro-encoded `DataFile`s via
//! `RewriteFilesAction` after collecting every group's response.
//!
//! Delete application and the post-delete row cross-check are NOT
//! reimplemented here: both come from `sqe_compaction::rewrite::rewrite_group`
//! (Task 1), the same primitive `CALL system.rewrite_data_files` uses on the
//! coordinator. This module only adapts a worker's inputs (S3 creds instead
//! of a catalog session, one pre-selected group instead of a bin-packed
//! set) to that shared primitive.

use std::collections::HashMap;
use std::sync::Arc;

use futures::{StreamExt, TryStreamExt};

use datafusion::prelude::SessionContext;
use iceberg::io::{FileIO, FileIOBuilder};
use iceberg::scan::FileScanTask;
use iceberg::spec::{
    write_data_files_to_avro, DataContentType, DataFile, DataFileBuilder, ManifestStatus, Struct,
};
use iceberg::table::{StaticTable, Table as IcebergTable};
use iceberg::TableIdent;
use sqe_compaction::wire::{CompactGroupRequest, CompactGroupResponse, S3Conn};
use sqe_compaction::writer::{new_upload_tracker, parse_parquet_compression};
use sqe_compaction::{plan_delete_aware_read, rewrite_group, SortCtx};
use sqe_core::{Result as SqeResult, SqeError};

/// Build a catalog-free `FileIO` from a [`S3Conn`]. Mirrors the S3 property
/// names the coordinator's per-session `RestCatalog` already injects for
/// FileIO configuration (`crates/sqe-catalog/src/rest_catalog.rs`), but talks
/// directly to `iceberg::io::FileIOBuilder` since a worker has no catalog to
/// go through.
///
/// `allow_http` is not translated into a separate property: opendal's S3
/// backend infers the scheme from the endpoint URL itself (`http://` vs
/// `https://`), the same way the executor's own S3 object-store builder
/// treats it (`executor.rs::build_object_store_with_creds`).
fn build_file_io(s3: &S3Conn) -> SqeResult<FileIO> {
    let mut builder = FileIOBuilder::new("s3");
    if !s3.endpoint.is_empty() {
        builder = builder.with_prop(iceberg::io::S3_ENDPOINT, s3.endpoint.clone());
    }
    if !s3.region.is_empty() {
        builder = builder.with_prop(iceberg::io::S3_REGION, s3.region.clone());
    }
    if !s3.access_key.is_empty() {
        builder = builder.with_prop(iceberg::io::S3_ACCESS_KEY_ID, s3.access_key.clone());
    }
    if !s3.secret_key.is_empty() {
        builder = builder.with_prop(iceberg::io::S3_SECRET_ACCESS_KEY, s3.secret_key.clone());
    }
    if !s3.session_token.is_empty() {
        builder = builder.with_prop(iceberg::io::S3_SESSION_TOKEN, s3.session_token.clone());
    }
    if s3.path_style {
        builder = builder.with_prop(iceberg::io::S3_PATH_STYLE_ACCESS, "true");
    }
    builder.build().map_err(|e| {
        SqeError::Execution(format!(
            "compact_file_group: failed to build FileIO from S3 config: {e}"
        ))
    })
}

/// Build a synthetic [`DataFile`] from a single [`FileScanTask`]. Used both
/// to resolve the requested group's data files and to reconstruct delete
/// files from a data file task's attached `deletes`; a data file's
/// `FileScanTask` (built by `plan_files`, before any row-group splitting
/// happens in the reader) carries the manifest entry's exact
/// `record_count`/`file_size_in_bytes`, so this is not a lossy re-derivation.
///
/// Only the fields [`rewrite_group`]/`expected_rows_after_deletes` actually
/// read (`content`, `file_path`, `record_count`) plus enough metadata to
/// build a valid `DataFile` are populated; partition/spec-id are not needed
/// because the worker rewrites a single pre-selected group rather than
/// bin-packing by partition.
fn data_file_from_task(task: &FileScanTask) -> SqeResult<DataFile> {
    DataFileBuilder::default()
        .content(task.data_file_content)
        .file_path(task.data_file_path.clone())
        .file_format(task.data_file_format)
        .record_count(task.record_count.unwrap_or(0))
        .file_size_in_bytes(task.file_size_in_bytes)
        .partition(task.partition.clone().unwrap_or_else(Struct::empty))
        .equality_ids(task.equality_ids.clone())
        .referenced_data_file(task.referenced_data_file.clone())
        .build()
        .map_err(|e| {
            SqeError::Execution(format!(
                "compact_file_group: failed to build data file metadata for '{}': {e}",
                task.data_file_path
            ))
        })
}

/// Resolve every path in `group_file_paths` to its planned [`DataFile`] via
/// `tasks_by_path` (the delete-aware read plan's per-path task map).
///
/// This is the resurrection guard: a data file the coordinator asked us to
/// compact but that is missing from the plan means the worker would read it
/// without knowing which delete files apply, silently resurrecting deleted
/// rows. Fail loud instead, matching `rewrite_group`'s own guard for the
/// same situation (`crates/sqe-compaction/src/rewrite.rs`), which this
/// duplicates at the group-resolution boundary so the error surfaces before
/// any read/write I/O, not partway through a stream.
fn resolve_group_data_files(
    tasks_by_path: &HashMap<String, Vec<FileScanTask>>,
    group_file_paths: &[String],
) -> SqeResult<Vec<DataFile>> {
    group_file_paths
        .iter()
        .map(|path| {
            let tasks = tasks_by_path.get(path).ok_or_else(|| {
                SqeError::Execution(format!(
                    "compact_file_group: requested data file '{path}' is missing from the \
                     delete-aware scan plan; refusing to compact it without confirming its \
                     applicable deletes (resurrection guard)"
                ))
            })?;
            let first = tasks.first().ok_or_else(|| {
                SqeError::Execution(format!(
                    "compact_file_group: data file '{path}' resolved to zero scan tasks"
                ))
            })?;
            data_file_from_task(first)
        })
        .collect()
}

/// True when a manifest entry is a live delete file (position or equality).
/// Copied from `sqe_coordinator::maintenance::is_live_delete_entry`: workers
/// cannot depend on `sqe-coordinator` (the dependency would be a cycle, and
/// that crate also pulls in the catalog bridge/session types workers must
/// never link), so this tiny predicate is duplicated rather than shared.
fn is_live_delete_entry(entry: &iceberg::spec::ManifestEntry) -> bool {
    entry.status() != ManifestStatus::Deleted && entry.content_type() != DataContentType::Data
}

/// Collect the live delete files (position + equality) of the table's
/// current snapshot, by walking its manifest list/manifests directly via
/// `FileIO` (no catalog call). Copied from
/// `sqe_coordinator::maintenance::collect_live_delete_files` for the same
/// reason as [`is_live_delete_entry`]: this is the audited, already-shipped
/// algorithm `CALL system.rewrite_data_files` uses to build the
/// `live_deletes` that `rewrite_group`'s row cross-check depends on, and
/// duplicating a manifest scan is safer than trusting an independent
/// derivation from the read plan to agree with it. A future refactor could
/// promote this to `sqe-compaction` so both call sites share one copy.
async fn collect_live_delete_files(table: &IcebergTable) -> SqeResult<Vec<DataFile>> {
    let metadata_ref = table.metadata_ref();
    let Some(snapshot) = metadata_ref.current_snapshot() else {
        return Ok(vec![]);
    };

    let cache = table.object_cache();
    let manifest_list = cache
        .get_manifest_list(snapshot, &metadata_ref)
        .await
        .map_err(|e| SqeError::Execution(format!("Failed to load manifest list: {e}")))?;

    const CONCURRENCY: usize = 8;
    let manifests: Vec<Arc<iceberg::spec::Manifest>> =
        futures::stream::iter(manifest_list.entries().iter().cloned())
            .map(|mf| {
                let cache = cache.clone();
                async move { cache.get_manifest(&mf).await }
            })
            .buffer_unordered(CONCURRENCY)
            .try_collect()
            .await
            .map_err(|e| SqeError::Execution(format!("Failed to load manifest: {e}")))?;

    Ok(manifests
        .into_iter()
        .flat_map(|m| {
            m.entries()
                .iter()
                .filter(|e| is_live_delete_entry(e))
                .map(|e| e.data_file().clone())
                .collect::<Vec<_>>()
        })
        .collect())
}

/// Parse `table_ident` (`catalog.namespace.table`, dot-separated) into a
/// `TableIdent`. Purely cosmetic identity for the `StaticTable` -- the actual
/// read/write never goes through a catalog, so this can never resolve to the
/// wrong table the way a real catalog lookup could.
fn parse_table_ident(table_ident: &str) -> SqeResult<TableIdent> {
    TableIdent::from_strs(table_ident.split('.')).map_err(|e| {
        SqeError::Execution(format!(
            "compact_file_group: invalid table_ident '{table_ident}': {e}"
        ))
    })
}

/// Rewrite one file group against an already-pinned [`IcebergTable`]. Split
/// out from [`compact_file_group`] so the snapshot-pin assertion and the
/// group-resolution guard are reachable in tests without needing a real
/// `StaticTable::from_metadata_file` load (see the unit tests below); the
/// public entry point below is the thin S3/StaticTable-loading wrapper
/// around this.
async fn compact_pinned_table(
    session_ctx: &SessionContext,
    table: IcebergTable,
    request: &CompactGroupRequest,
) -> SqeResult<CompactGroupResponse> {
    // Snapshot pin assertion. The coordinator planned this group's file list
    // and delete accounting against a specific snapshot; if the table has
    // since advanced (or the metadata_location resolves to a different
    // snapshot than the coordinator observed), the plan is stale and must
    // not be executed. Checked BEFORE any manifest I/O (plan_delete_aware_read
    // below), so a mismatch never triggers a scan.
    let actual_snapshot_id = table.metadata().current_snapshot_id();
    if actual_snapshot_id != Some(request.snapshot_id) {
        return Err(SqeError::Execution(format!(
            "compact_file_group: snapshot mismatch for table '{}': request is pinned to \
             snapshot {}, but '{}' resolves to current snapshot {:?}; the read plan is only \
             valid against the pinned snapshot, refusing to compact",
            request.table_ident, request.snapshot_id, request.metadata_location, actual_snapshot_id
        )));
    }

    let plan = plan_delete_aware_read(&table).await.map_err(|e| {
        SqeError::Execution(format!("compact_file_group: failed to plan delete-aware read: {e}"))
    })?;

    let group = resolve_group_data_files(&plan.tasks_by_path, &request.group_file_paths)?;
    let live_deletes = collect_live_delete_files(&table).await?;

    let arrow_schema: arrow_schema::SchemaRef = Arc::new(
        iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema().as_ref())
            .map_err(|e| {
                SqeError::Execution(format!("compact_file_group: schema conversion failed: {e}"))
            })?,
    );

    let sort_ctx: Option<SortCtx> = request.sort.as_ref().map(|s| SortCtx {
        runtime: session_ctx.runtime_env(),
        spec: s.to_sort_spec(),
    });

    let compression = parse_parquet_compression(&request.compression);
    let tracker = new_upload_tracker();

    let (new_files, _old_files, rows_written) = rewrite_group(
        &table,
        &plan,
        &live_deletes,
        &arrow_schema,
        sort_ctx.as_ref(),
        group,
        compression,
        tracker.clone(),
        request.target_file_size_bytes,
    )
    .await
    .map_err(|e| SqeError::Execution(format!("compact_file_group: rewrite failed: {e}")))?;

    let partition_type = table.metadata().default_partition_type().clone();
    let format_version = table.metadata().format_version();
    let mut avro_buf = Vec::new();
    write_data_files_to_avro(&mut avro_buf, new_files.clone(), &partition_type, format_version)
        .map_err(|e| {
            SqeError::Execution(format!(
                "compact_file_group: failed to encode data files to avro: {e}"
            ))
        })?;

    let uploaded_paths = tracker
        .lock()
        .map_err(|_| {
            SqeError::Execution("compact_file_group: upload tracker mutex poisoned".to_string())
        })?
        .clone();

    let bytes_written: u64 = new_files.iter().map(|f| f.file_size_in_bytes()).sum();

    Ok(CompactGroupResponse {
        group_id: request.group_id,
        new_data_files_avro: avro_buf,
        rows_written,
        bytes_written,
        uploaded_paths,
    })
}

/// Rewrite one file group for a `compact_file_group` action: the worker-side
/// executor for Phase 4c distributed compaction.
///
/// Builds a catalog-free `FileIO` from `request.s3`, pins a `StaticTable` to
/// `request.metadata_location`, and delegates to [`compact_pinned_table`] for
/// the snapshot check, delete-aware rewrite, and Avro encoding. The worker
/// never obtains a catalog token and never commits; the coordinator (Task 4)
/// commits the returned `DataFile`s.
pub async fn compact_file_group(
    session_ctx: &SessionContext,
    request: &CompactGroupRequest,
) -> SqeResult<CompactGroupResponse> {
    let file_io = build_file_io(&request.s3)?;
    let ident = parse_table_ident(&request.table_ident)?;

    let static_table =
        StaticTable::from_metadata_file(&request.metadata_location, ident, file_io)
            .await
            .map_err(|e| {
                SqeError::Execution(format!(
                    "compact_file_group: failed to load table metadata '{}': {e}",
                    request.metadata_location
                ))
            })?;

    compact_pinned_table(session_ctx, static_table.into_table(), request).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg::spec::{DataFileFormat, FormatVersion, NestedField, PrimitiveType};
    use sqe_compaction::wire::SortSpecWire;
    use std::io::Write as _;

    fn sample_task(path: &str, record_count: Option<u64>, size: u64) -> FileScanTask {
        let schema = Arc::new(
            iceberg::spec::Schema::builder()
                .with_schema_id(0)
                .with_fields(vec![
                    NestedField::required(1, "id", iceberg::spec::Type::Primitive(PrimitiveType::Long))
                        .into(),
                ])
                .build()
                .unwrap(),
        );
        FileScanTask {
            file_size_in_bytes: size,
            start: 0,
            length: size,
            record_count,
            data_file_path: path.to_string(),
            referenced_data_file: None,
            data_file_content: DataContentType::Data,
            data_file_format: DataFileFormat::Parquet,
            schema,
            project_field_ids: vec![1],
            predicate: None,
            deletes: vec![],
            sequence_number: 0,
            equality_ids: None,
            partition: Some(Struct::empty()),
            partition_spec: None,
            name_mapping: None,
            case_sensitive: true,
        }
    }

    fn sample_request(group_file_paths: Vec<String>, snapshot_id: i64) -> CompactGroupRequest {
        CompactGroupRequest {
            job_id: "job-1".to_string(),
            group_id: 0,
            table_ident: "catalog.ns.tbl".to_string(),
            metadata_location: "s3://bucket/warehouse/tbl/metadata/v1.metadata.json".to_string(),
            snapshot_id,
            group_file_paths,
            target_file_size_bytes: 128 * 1024 * 1024,
            compression: "zstd".to_string(),
            sort: None,
            s3: sqe_compaction::wire::S3Conn {
                endpoint: String::new(),
                region: String::new(),
                access_key: String::new(),
                secret_key: String::new(),
                session_token: String::new(),
                path_style: false,
                allow_http: false,
            },
        }
    }

    // ---- resolve_group_data_files (resurrection guard) ---------------

    #[test]
    fn resolve_group_data_files_succeeds_for_known_paths() {
        let mut tasks_by_path = HashMap::new();
        tasks_by_path.insert(
            "s3://bucket/data/f1.parquet".to_string(),
            vec![sample_task("s3://bucket/data/f1.parquet", Some(100), 4096)],
        );
        let group = resolve_group_data_files(
            &tasks_by_path,
            &["s3://bucket/data/f1.parquet".to_string()],
        )
        .unwrap();
        assert_eq!(group.len(), 1);
        assert_eq!(group[0].file_path(), "s3://bucket/data/f1.parquet");
        assert_eq!(group[0].record_count(), 100);
        assert_eq!(group[0].file_size_in_bytes(), 4096);
    }

    #[test]
    fn resolve_group_data_files_fails_loud_on_missing_path() {
        // Resurrection guard: a requested path absent from the plan must
        // error out before any read, not silently skip the file (which
        // would resurrect its deleted rows by never applying its deletes).
        let mut tasks_by_path = HashMap::new();
        tasks_by_path.insert(
            "s3://bucket/data/f1.parquet".to_string(),
            vec![sample_task("s3://bucket/data/f1.parquet", Some(100), 4096)],
        );
        let err = resolve_group_data_files(
            &tasks_by_path,
            &[
                "s3://bucket/data/f1.parquet".to_string(),
                "s3://bucket/data/GHOST.parquet".to_string(),
            ],
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("GHOST.parquet"), "error must name the missing path: {msg}");
        assert!(msg.contains("resurrection guard") || msg.contains("missing"), "error must explain the guard: {msg}");
    }

    #[test]
    fn resolve_group_data_files_uses_task_record_count_when_present() {
        let mut tasks_by_path = HashMap::new();
        // record_count is None when a task does not cover the whole file;
        // the resolver must not fabricate a nonzero count in that case.
        tasks_by_path.insert(
            "s3://bucket/data/partial.parquet".to_string(),
            vec![sample_task("s3://bucket/data/partial.parquet", None, 4096)],
        );
        let group = resolve_group_data_files(
            &tasks_by_path,
            &["s3://bucket/data/partial.parquet".to_string()],
        )
        .unwrap();
        assert_eq!(group[0].record_count(), 0);
    }

    // ---- snapshot pin assertion ---------------------------------------

    /// Minimal local (fs, zero S3/network) table with a real current
    /// snapshot, built the same way `iceberg::table::StaticTable`'s own doc
    /// example does: parse a `TableMetadata` and attach it via
    /// `Table::builder()`. The manifest list this snapshot points at does
    /// not need to exist on disk: the snapshot mismatch guard in
    /// `compact_pinned_table` returns before `plan_delete_aware_read` ever
    /// reads it.
    fn local_table_with_snapshot(snapshot_id: i64) -> IcebergTable {
        let json = format!(
            r#"{{
              "format-version": 2,
              "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
              "location": "s3://bucket/test/location",
              "last-sequence-number": 1,
              "last-updated-ms": 1602638573590,
              "last-column-id": 1,
              "current-schema-id": 0,
              "schemas": [
                {{"type": "struct", "schema-id": 0, "fields": [
                  {{"id": 1, "name": "x", "required": true, "type": "long"}}
                ]}}
              ],
              "default-spec-id": 0,
              "partition-specs": [{{"spec-id": 0, "fields": []}}],
              "last-partition-id": 999,
              "default-sort-order-id": 0,
              "sort-orders": [{{"order-id": 0, "fields": []}}],
              "properties": {{}},
              "current-snapshot-id": {snapshot_id},
              "snapshots": [
                {{
                  "snapshot-id": {snapshot_id},
                  "timestamp-ms": 1555100955770,
                  "sequence-number": 1,
                  "summary": {{"operation": "append"}},
                  "manifest-list": "s3://a/b/nonexistent.avro"
                }}
              ],
              "snapshot-log": [
                {{"snapshot-id": {snapshot_id}, "timestamp-ms": 1555100955770}}
              ],
              "metadata-log": []
            }}"#
        );
        let metadata: iceberg::spec::TableMetadata = serde_json::from_str(&json).unwrap();
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        IcebergTable::builder()
            .metadata(metadata)
            .identifier(TableIdent::from_strs(["ns", "tbl"]).unwrap())
            .file_io(file_io)
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn compact_pinned_table_rejects_snapshot_mismatch() {
        let table = local_table_with_snapshot(555);
        let request = sample_request(vec!["s3://bucket/data/f1.parquet".to_string()], 999);
        let ctx = SessionContext::new();
        let err = compact_pinned_table(&ctx, table, &request).await.unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("snapshot mismatch"), "error must name the mismatch: {msg}");
        assert!(msg.contains("999") && msg.contains("555"), "error must cite both ids: {msg}");
    }

    #[tokio::test]
    async fn compact_pinned_table_accepts_matching_snapshot_then_fails_later() {
        // A matching snapshot id must pass the guard; the call still fails
        // downstream because the manifest list is a placeholder path, but
        // the error must NOT be the snapshot-mismatch error.
        let table = local_table_with_snapshot(555);
        let request = sample_request(vec!["s3://bucket/data/f1.parquet".to_string()], 555);
        let ctx = SessionContext::new();
        let err = compact_pinned_table(&ctx, table, &request).await.unwrap_err();
        assert!(
            !err.to_string().contains("snapshot mismatch"),
            "matching snapshot must pass the pin guard: {err}"
        );
    }

    // ---- SortSpecWire wiring (no I/O) ----------------------------------

    #[test]
    fn sort_spec_wire_builds_a_sort_ctx_spec() {
        let wire = SortSpecWire::Columns(vec![("a".to_string(), true)]);
        let spec = wire.to_sort_spec();
        assert_eq!(spec.columns(), vec!["a"]);
    }

    // ---- Avro encode/decode contract for CompactGroupResponse ----------
    //
    // Pins the exact (version, partition_type, partition_spec_id) contract
    // `compact_pinned_table` uses to encode `new_data_files_avro`, so Task 4
    // (the coordinator decoder) has a concrete round-trip to match rather
    // than re-deriving it from the vendored `write_data_files_to_avro` source.

    #[test]
    fn data_files_avro_round_trips_through_write_and_read() {
        let partition_type = iceberg::spec::StructType::new(vec![]);
        let file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("s3://bucket/data/out-0.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .record_count(42)
            .file_size_in_bytes(2048)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .build()
            .unwrap();

        let mut buf = Vec::new();
        write_data_files_to_avro(&mut buf, vec![file.clone()], &partition_type, FormatVersion::V2)
            .unwrap();
        buf.flush().unwrap();

        let schema = iceberg::spec::Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", iceberg::spec::Type::Primitive(PrimitiveType::Long))
                    .into(),
            ])
            .build()
            .unwrap();

        let decoded = iceberg::spec::read_data_files_from_avro(
            &mut buf.as_slice(),
            &schema,
            0,
            &partition_type,
            FormatVersion::V2,
        )
        .unwrap();

        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].file_path(), file.file_path());
        assert_eq!(decoded[0].record_count(), 42);
        assert_eq!(decoded[0].file_size_in_bytes(), 2048);
    }

    // ---- Full end-to-end (ignored: needs a live S3-compatible endpoint) --

    /// Manual run against the docker quickstart stack: point at a real
    /// Iceberg table's metadata.json and RustFS/MinIO credentials, and
    /// confirm `compact_file_group` returns a `Done`-worthy
    /// `CompactGroupResponse` whose `new_data_files_avro` decodes via
    /// `read_data_files_from_avro`.
    ///
    /// Not runnable in this environment (no live S3). To exercise it
    /// manually:
    ///
    /// ```text
    /// SQE_IT_METADATA_LOCATION=s3://warehouse/ns/tbl/metadata/00002-....metadata.json \
    /// SQE_IT_SNAPSHOT_ID=<current-snapshot-id-from-that-metadata.json> \
    /// SQE_IT_GROUP_FILE_PATH=s3://warehouse/ns/tbl/data/<some-existing-file>.parquet \
    /// SQE_IT_S3_ENDPOINT=http://localhost:9000 \
    /// SQE_IT_S3_ACCESS_KEY=... SQE_IT_S3_SECRET_KEY=... \
    ///   cargo test -p sqe-worker --lib compact_file_group_against_live_s3 -- --ignored
    /// ```
    #[tokio::test]
    #[ignore = "requires a live S3-compatible endpoint + a real Iceberg table; run manually"]
    async fn compact_file_group_against_live_s3() {
        let metadata_location = std::env::var("SQE_IT_METADATA_LOCATION")
            .expect("SQE_IT_METADATA_LOCATION must be set for this manual integration test");
        let snapshot_id: i64 = std::env::var("SQE_IT_SNAPSHOT_ID")
            .expect("SQE_IT_SNAPSHOT_ID must be set")
            .parse()
            .expect("SQE_IT_SNAPSHOT_ID must be an i64");
        let group_file_path =
            std::env::var("SQE_IT_GROUP_FILE_PATH").expect("SQE_IT_GROUP_FILE_PATH must be set");

        let request = CompactGroupRequest {
            job_id: "manual-it".to_string(),
            group_id: 0,
            table_ident: "manual.it.table".to_string(),
            metadata_location,
            snapshot_id,
            group_file_paths: vec![group_file_path],
            target_file_size_bytes: 128 * 1024 * 1024,
            compression: "zstd".to_string(),
            sort: None,
            s3: sqe_compaction::wire::S3Conn {
                endpoint: std::env::var("SQE_IT_S3_ENDPOINT").unwrap_or_default(),
                region: std::env::var("SQE_IT_S3_REGION").unwrap_or_default(),
                access_key: std::env::var("SQE_IT_S3_ACCESS_KEY").unwrap_or_default(),
                secret_key: std::env::var("SQE_IT_S3_SECRET_KEY").unwrap_or_default(),
                session_token: std::env::var("SQE_IT_S3_SESSION_TOKEN").unwrap_or_default(),
                path_style: true,
                allow_http: true,
            },
        };

        let ctx = SessionContext::new();
        let response = compact_file_group(&ctx, &request).await.expect("compaction failed");

        // Schema/partition_type here are placeholders: a real manual run
        // should load them from the same table's metadata.json used above
        // (`table.metadata().current_schema()` /
        // `table.metadata().default_partition_type()`) so the decode
        // exercises the actual table shape.
        let schema = iceberg::spec::Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", iceberg::spec::Type::Primitive(PrimitiveType::Long))
                    .into(),
            ])
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
        assert!(!decoded.is_empty(), "expected at least one rewritten data file");
    }
}
