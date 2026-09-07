//! Test-only fixtures shared by the statistics, scan-planning and table
//! provider unit tests (architecture review 313).
//!
//! [`fake_iceberg_table`] writes a real manifest list and two real data
//! manifests through `iceberg::spec::{ManifestWriterBuilder,
//! ManifestListWriter}` into a temp-dir `FileIO`, so every manifest walk in
//! the crate (`compute_table_statistics`, `collect_data_files_for_pruning`,
//! `SqeTableProvider::scan`) runs against bytes the reader actually decodes,
//! not against a hand-built `DataFile` list. No Parquet is written: the
//! fixture stops at metadata, which is all the planning paths read.

use std::collections::HashMap;
use std::path::Path;

use iceberg::spec::{
    DataContentType, DataFile, DataFileBuilder, DataFileFormat, Datum, ManifestListWriter,
    ManifestWriterBuilder, Struct, TableMetadata,
};
use iceberg::table::Table;
use iceberg::{NamespaceIdent, TableIdent};

/// The one snapshot every fixture table has.
pub(crate) const FAKE_SNAPSHOT_ID: i64 = 7_000_000_000_000_001;

/// One data file of the fixture: manifest slot, rows, bytes, `id` bounds,
/// and how many `name` values are null.
#[derive(Debug, Clone, Copy)]
pub(crate) struct FakeFile {
    pub name: &'static str,
    pub manifest: usize,
    pub rows: u64,
    pub bytes: u64,
    pub id_min: i64,
    pub id_max: i64,
    pub name_nulls: u64,
}

/// Three files over two manifests. Totals: 60 rows, 600 bytes, `id` in
/// `1..=60`, five null `name`s.
pub(crate) const FAKE_FILES: [FakeFile; 3] = [
    FakeFile {
        name: "f0.parquet",
        manifest: 0,
        rows: 10,
        bytes: 100,
        id_min: 1,
        id_max: 10,
        name_nulls: 2,
    },
    FakeFile {
        name: "f1.parquet",
        manifest: 0,
        rows: 20,
        bytes: 200,
        id_min: 11,
        id_max: 30,
        name_nulls: 3,
    },
    FakeFile {
        name: "f2.parquet",
        manifest: 1,
        rows: 30,
        bytes: 300,
        id_min: 31,
        id_max: 60,
        name_nulls: 0,
    },
];

pub(crate) const FAKE_TOTAL_ROWS: usize = 60;
pub(crate) const FAKE_TOTAL_BYTES: usize = 600;
pub(crate) const FAKE_NAME_NULLS: usize = 5;

/// Path of one fixture data file under `table`'s location.
pub(crate) fn fake_file_path(table: &Table, file: &FakeFile) -> String {
    format!("{}/data/{}", table.metadata().location(), file.name)
}

fn metadata_json(uuid: &str, location: &str, manifest_list: &str) -> String {
    format!(
        r#"{{
        "format-version": 2,
        "table-uuid": "{uuid}",
        "location": "{location}",
        "last-sequence-number": 1,
        "last-updated-ms": 1700000000000,
        "last-column-id": 2,
        "current-schema-id": 0,
        "schemas": [{{"type": "struct", "schema-id": 0, "fields": [
            {{"id": 1, "name": "id", "required": true, "type": "long"}},
            {{"id": 2, "name": "name", "required": false, "type": "string"}}
        ]}}],
        "default-spec-id": 0,
        "partition-specs": [{{"spec-id": 0, "fields": []}}],
        "last-partition-id": 999,
        "default-sort-order-id": 0,
        "sort-orders": [{{"order-id": 0, "fields": []}}],
        "properties": {{}},
        "current-snapshot-id": {FAKE_SNAPSHOT_ID},
        "snapshots": [{{
            "snapshot-id": {FAKE_SNAPSHOT_ID},
            "sequence-number": 1,
            "timestamp-ms": 1700000000000,
            "schema-id": 0,
            "summary": {{"operation": "append"}},
            "manifest-list": "{manifest_list}"
        }}],
        "snapshot-log": [],
        "metadata-log": []
    }}"#
    )
}

fn data_file(table: &Table, file: &FakeFile) -> DataFile {
    DataFileBuilder::default()
        .partition_spec_id(0)
        .content(DataContentType::Data)
        .file_path(fake_file_path(table, file))
        .file_format(DataFileFormat::Parquet)
        .file_size_in_bytes(file.bytes)
        .record_count(file.rows)
        .partition(Struct::empty())
        .value_counts(HashMap::from([(1, file.rows), (2, file.rows)]))
        .null_value_counts(HashMap::from([(1, 0), (2, file.name_nulls)]))
        .lower_bounds(HashMap::from([
            (1, Datum::long(file.id_min)),
            (2, Datum::string("a")),
        ]))
        .upper_bounds(HashMap::from([
            (1, Datum::long(file.id_max)),
            (2, Datum::string("z")),
        ]))
        .key_metadata(None)
        .build()
        .expect("fixture data file")
}

/// A table whose current snapshot points at a manifest list naming two data
/// manifests holding [`FAKE_FILES`], all written under `dir`. With
/// `with_live_deletes` a third, `deletes` manifest holding one live
/// position-delete file is added, which turns the row count inexact.
///
/// Each call gets a fresh table uuid so the process-global
/// `SNAPSHOT_STATS_CACHE` never serves one test's manifests to another.
pub(crate) async fn fake_iceberg_table(dir: &Path, with_live_deletes: bool) -> Table {
    let location = dir.to_string_lossy().to_string();
    let uuid = uuid::Uuid::new_v4().to_string();
    let manifest_list_path = format!("{location}/metadata/snap-{FAKE_SNAPSHOT_ID}.avro");
    let metadata: TableMetadata =
        serde_json::from_str(&metadata_json(&uuid, &location, &manifest_list_path))
            .expect("fixture metadata");
    let file_io = iceberg::io::FileIOBuilder::new_fs_io()
        .build()
        .expect("fs FileIO");
    let table = Table::builder()
        .metadata(metadata)
        .identifier(TableIdent::new(
            NamespaceIdent::new("sales".to_string()),
            "orders".to_string(),
        ))
        .file_io(file_io)
        .build()
        .expect("fixture table");

    let snapshot = table.metadata().current_snapshot().expect("snapshot");
    let schema = snapshot.schema(table.metadata()).expect("schema");
    let spec = table.metadata().default_partition_spec().as_ref().clone();

    let mut manifests = Vec::new();
    for manifest_index in 0..2 {
        let mut writer = ManifestWriterBuilder::new(
            table
                .file_io()
                .new_output(format!("{location}/metadata/m{manifest_index}.avro"))
                .expect("manifest output"),
            Some(FAKE_SNAPSHOT_ID),
            None,
            schema.clone(),
            spec.clone(),
        )
        .build_v2_data();
        for file in FAKE_FILES.iter().filter(|f| f.manifest == manifest_index) {
            writer
                .add_file(data_file(&table, file), 1)
                .expect("add data file");
        }
        manifests.push(writer.write_manifest_file().await.expect("write manifest"));
    }

    if with_live_deletes {
        let mut writer = ManifestWriterBuilder::new(
            table
                .file_io()
                .new_output(format!("{location}/metadata/m-deletes.avro"))
                .expect("delete manifest output"),
            Some(FAKE_SNAPSHOT_ID),
            None,
            schema,
            spec,
        )
        .build_v2_deletes();
        let delete_file = DataFileBuilder::default()
            .partition_spec_id(0)
            .content(DataContentType::PositionDeletes)
            .file_path(format!("{location}/data/deletes-0.parquet"))
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(40)
            .record_count(2)
            .partition(Struct::empty())
            .key_metadata(None)
            .build()
            .expect("fixture delete file");
        writer.add_file(delete_file, 1).expect("add delete file");
        manifests.push(
            writer
                .write_manifest_file()
                .await
                .expect("write delete manifest"),
        );
    }

    let mut list_writer = ManifestListWriter::v2(
        table
            .file_io()
            .new_output(&manifest_list_path)
            .expect("manifest list output"),
        FAKE_SNAPSHOT_ID,
        None,
        1,
    );
    list_writer
        .add_manifests(manifests.into_iter())
        .expect("add manifests");
    list_writer.close().await.expect("close manifest list");
    table
}

/// A table with the fixture schema and no snapshot at all.
pub(crate) fn fake_snapshotless_table() -> Table {
    let uuid = uuid::Uuid::new_v4().to_string();
    let json = format!(
        r#"{{
        "format-version": 2,
        "table-uuid": "{uuid}",
        "location": "s3://bucket/sales/orders",
        "last-sequence-number": 0,
        "last-updated-ms": 1700000000000,
        "last-column-id": 2,
        "current-schema-id": 0,
        "schemas": [{{"type": "struct", "schema-id": 0, "fields": [
            {{"id": 1, "name": "id", "required": true, "type": "long"}},
            {{"id": 2, "name": "name", "required": false, "type": "string"}}
        ]}}],
        "default-spec-id": 0,
        "partition-specs": [{{"spec-id": 0, "fields": []}}],
        "last-partition-id": 999,
        "default-sort-order-id": 0,
        "sort-orders": [{{"order-id": 0, "fields": []}}],
        "properties": {{}},
        "current-snapshot-id": -1,
        "snapshots": [],
        "snapshot-log": [],
        "metadata-log": []
    }}"#
    );
    let metadata: TableMetadata = serde_json::from_str(&json).expect("fixture metadata");
    let file_io = iceberg::io::FileIOBuilder::new_fs_io()
        .build()
        .expect("fs FileIO");
    Table::builder()
        .metadata(metadata)
        .identifier(TableIdent::new(
            NamespaceIdent::new("sales".to_string()),
            "orders".to_string(),
        ))
        .file_io(file_io)
        .build()
        .expect("fixture table")
}
