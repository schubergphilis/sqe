#![cfg(feature = "hive-external")]

use std::collections::BTreeMap;

use sqe_catalog::hive::manifest::{
    HiveManifest, ManifestLimits, PartitionIndex, PartitionIndexEntry,
    PARTITION_INDEX_FORMAT_VERSION, MANIFEST_VERSION,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

fn spec_of(sql: &str) -> sqe_sql::hive_ddl::HiveTableSpec {
    let stmts = Parser::parse_sql(&GenericDialect {}, sql).expect("parse");
    sqe_sql::hive_ddl::try_extract_hive_table(&stmts[0])
        .expect("extract")
        .expect("external")
}

const DDL: &str = "CREATE EXTERNAL TABLE sales.orders (id BIGINT, amount DOUBLE) \
PARTITIONED BY (dt string) \
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' \
WITH SERDEPROPERTIES ('separatorChar' = ',') \
STORED AS TEXTFILE LOCATION 's3://raw/orders/' \
TBLPROPERTIES ('skip.header.line.count' = '1', 'sqe.secret' = 'partner')";

#[test]
fn round_trips_through_json() {
    let m = HiveManifest::from_spec(&spec_of(DDL), "sales", "jacob", "2026-08-19T10:00:00Z");
    let bytes = m.to_bytes().expect("serialize");
    let back = HiveManifest::parse(&bytes, &ManifestLimits::default()).expect("parse");
    assert_eq!(back.manifest_version, MANIFEST_VERSION);
    assert_eq!(back.table_type, "EXTERNAL_TABLE");
    assert_eq!(back.database_name, "sales");
    assert_eq!(back.name, "orders");
    assert_eq!(back.storage_descriptor.location, "s3://raw/orders/");
    assert_eq!(
        back.storage_descriptor
            .serde_info
            .serialization_library
            .as_deref(),
        Some("org.apache.hadoop.hive.serde2.OpenCSVSerde")
    );
    assert_eq!(back.storage_descriptor.columns.len(), 2);
    assert_eq!(back.partition_keys.len(), 1);
    assert_eq!(back.secret(), Some("partner"));
}

#[test]
fn uses_glue_field_names_so_a_future_glue_backend_is_a_mapping() {
    let m = HiveManifest::from_spec(&spec_of(DDL), "sales", "jacob", "2026-08-19T10:00:00Z");
    let json: serde_json::Value =
        serde_json::from_slice(&m.to_bytes().expect("bytes")).expect("json");
    for key in [
        "manifest_version",
        "table_type",
        "database_name",
        "description",
        "partition_keys",
        "parameters",
        "storage_descriptor",
        "sqe",
    ] {
        assert!(json.get(key).is_some(), "missing top-level key {key}");
    }
    let sd = json.get("storage_descriptor").expect("sd");
    for key in ["location", "input_format", "serde_info", "columns"] {
        assert!(
            sd.get(key).is_some(),
            "missing storage_descriptor key {key}"
        );
    }
    assert!(sd["serde_info"].get("serialization_library").is_some());

    // Descend into GlueColumn: a `#[serde(rename = ...)]` there would change
    // serialize and deserialize together, so a round-trip test alone would
    // stay green while the on-disk JSON silently stopped matching Glue.
    let column = &sd["columns"][0];
    for key in ["name", "type", "comment"] {
        assert!(column.get(key).is_some(), "missing GlueColumn key {key}");
    }

    // Same for the sqe extension block.
    let sqe = json.get("sqe").expect("sqe");
    for key in ["secret", "column_tags", "partition_index", "statistics"] {
        assert!(sqe.get(key).is_some(), "missing sqe key {key}");
    }
}

#[test]
fn a_higher_manifest_version_is_rejected_not_parsed_best_effort() {
    let mut json: serde_json::Value = serde_json::from_slice(
        &HiveManifest::from_spec(&spec_of(DDL), "sales", "j", "t")
            .to_bytes()
            .expect("bytes"),
    )
    .expect("json");
    json["manifest_version"] = serde_json::json!(99);
    let err = HiveManifest::parse(json.to_string().as_bytes(), &ManifestLimits::default())
        .expect_err("must reject");
    assert!(
        format!("{err:?}").contains("99"),
        "error should name the version: {err:?}"
    );
}

#[test]
fn an_unknown_field_inside_a_known_version_is_ignored() {
    let mut json: serde_json::Value = serde_json::from_slice(
        &HiveManifest::from_spec(&spec_of(DDL), "sales", "j", "t")
            .to_bytes()
            .expect("bytes"),
    )
    .expect("json");
    json["future_field_from_a_newer_writer"] = serde_json::json!({"a": 1});
    HiveManifest::parse(json.to_string().as_bytes(), &ManifestLimits::default())
        .expect("a newer writer's extra field must stay readable");
}

#[test]
fn oversized_manifest_is_rejected_before_parsing() {
    let limits = ManifestLimits {
        max_bytes: 64,
        ..ManifestLimits::default()
    };
    let bytes = HiveManifest::from_spec(&spec_of(DDL), "sales", "j", "t")
        .to_bytes()
        .expect("bytes");
    assert!(bytes.len() > 64, "fixture must exceed the cap");
    let err = HiveManifest::parse(&bytes, &limits).expect_err("must reject");
    assert!(
        format!("{err:?}").to_lowercase().contains("size"),
        "got {err:?}"
    );
}

// Not in the brief's test list: added to pin the parse-order requirement
// itself, which `oversized_manifest_is_rejected_before_parsing` above
// cannot distinguish (it feeds a *valid* manifest that happens to be over
// the cap, so it passes whether the size check runs before or after
// deserialization). This one feeds bytes that are both oversized and not
// valid JSON; a size-check-after-deserialize implementation would report a
// JSON error here instead of a size error.
#[test]
fn oversized_garbage_is_rejected_on_size_not_on_deserialize_failure() {
    let limits = ManifestLimits::default();
    let garbage = vec![b'x'; limits.max_bytes + 1];
    let err = HiveManifest::parse(&garbage, &limits).expect_err("must reject");
    assert!(
        format!("{err:?}").to_lowercase().contains("size"),
        "got {err:?}"
    );
}

#[test]
fn too_many_columns_is_rejected() {
    let limits = ManifestLimits {
        max_columns: 1,
        ..ManifestLimits::default()
    };
    let bytes = HiveManifest::from_spec(&spec_of(DDL), "sales", "j", "t")
        .to_bytes()
        .expect("bytes");
    let err = HiveManifest::parse(&bytes, &limits).expect_err("must reject");
    assert!(
        format!("{err:?}").to_lowercase().contains("column"),
        "got {err:?}"
    );
}

#[test]
fn empty_location_is_rejected() {
    let mut m = HiveManifest::from_spec(&spec_of(DDL), "sales", "j", "t");
    m.storage_descriptor.location = String::new();
    let bytes = m.to_bytes().expect("bytes");
    let err = HiveManifest::parse(&bytes, &ManifestLimits::default()).expect_err("must reject");
    assert!(
        format!("{err:?}").to_lowercase().contains("location"),
        "got {err:?}"
    );
}

#[test]
fn empty_name_is_rejected() {
    let mut m = HiveManifest::from_spec(&spec_of(DDL), "sales", "j", "t");
    m.name = String::new();
    let bytes = m.to_bytes().expect("bytes");
    let err = HiveManifest::parse(&bytes, &ManifestLimits::default()).expect_err("must reject");
    assert!(
        format!("{err:?}").contains("name must not be empty"),
        "got {err:?}"
    );
}

#[test]
fn empty_database_name_is_rejected() {
    let mut m = HiveManifest::from_spec(&spec_of(DDL), "sales", "j", "t");
    m.database_name = String::new();
    let bytes = m.to_bytes().expect("bytes");
    let err = HiveManifest::parse(&bytes, &ManifestLimits::default()).expect_err("must reject");
    assert!(
        format!("{err:?}").contains("database_name must not be empty"),
        "got {err:?}"
    );
}

#[test]
fn a_table_type_other_than_external_table_is_rejected() {
    let mut m = HiveManifest::from_spec(&spec_of(DDL), "sales", "j", "t");
    m.table_type = "MANAGED_TABLE".to_string();
    let bytes = m.to_bytes().expect("bytes");
    let err = HiveManifest::parse(&bytes, &ManifestLimits::default()).expect_err("must reject");
    assert!(
        format!("{err:?}").contains("MANAGED_TABLE"),
        "error should name the bad value: {err:?}"
    );
}

#[test]
fn a_column_name_repeated_between_columns_and_partition_keys_is_rejected() {
    let mut m = HiveManifest::from_spec(&spec_of(DDL), "sales", "j", "t");
    // `id` is already a storage_descriptor column; force the partition key
    // to collide with it.
    m.partition_keys[0].name = "id".to_string();
    let bytes = m.to_bytes().expect("bytes");
    let err = HiveManifest::parse(&bytes, &ManifestLimits::default()).expect_err("must reject");
    assert!(
        format!("{err:?}").contains("id"),
        "error should name the duplicated column: {err:?}"
    );
}

fn index_of(m: &mut HiveManifest) -> &mut PartitionIndex {
    m.sqe.partition_index.get_or_insert_with(|| PartitionIndex {
        format_version: PARTITION_INDEX_FORMAT_VERSION,
        entries: Vec::new(),
    })
}

fn entry(dt: &str) -> PartitionIndexEntry {
    PartitionIndexEntry {
        values: BTreeMap::from([("dt".to_string(), dt.to_string())]),
        location: format!("s3://raw/orders/dt={dt}/"),
        file_count: 1,
        total_bytes: 1024,
    }
}

#[test]
fn partition_index_round_trips_through_json() {
    let mut m = HiveManifest::from_spec(&spec_of(DDL), "sales", "j", "t");
    index_of(&mut m).entries = vec![entry("2026-01-02"), entry("2026-01-03")];
    let back = HiveManifest::parse(&m.to_bytes().expect("bytes"), &ManifestLimits::default())
        .expect("parse");
    let idx = back.sqe.partition_index.expect("index survives");
    assert_eq!(idx.format_version, PARTITION_INDEX_FORMAT_VERSION);
    assert_eq!(idx.entries.len(), 2);
    assert_eq!(idx.entries[0].values.get("dt").map(String::as_str), Some("2026-01-02"));
    assert_eq!(idx.entries[0].location, "s3://raw/orders/dt=2026-01-02/");
    assert_eq!(idx.entries[0].file_count, 1);
    assert_eq!(idx.entries[0].total_bytes, 1024);
}

#[test]
fn a_newer_partition_index_format_version_is_rejected() {
    let mut m = HiveManifest::from_spec(&spec_of(DDL), "sales", "j", "t");
    index_of(&mut m).format_version = 99;
    let err = HiveManifest::parse(&m.to_bytes().expect("bytes"), &ManifestLimits::default())
        .expect_err("must reject");
    assert!(
        format!("{err:?}").contains("99"),
        "error should name the version: {err:?}"
    );
}

#[test]
fn an_oversized_partition_index_is_rejected_at_load() {
    let limits = ManifestLimits {
        max_partition_index_entries: 2,
        ..ManifestLimits::default()
    };
    let mut m = HiveManifest::from_spec(&spec_of(DDL), "sales", "j", "t");
    index_of(&mut m).entries = vec![
        entry("2026-01-01"),
        entry("2026-01-02"),
        entry("2026-01-03"),
    ];
    let err = HiveManifest::parse(&m.to_bytes().expect("bytes"), &limits).expect_err("must reject");
    assert!(
        format!("{err:?}").to_lowercase().contains("exceeding the cap"),
        "got {err:?}"
    );
}

#[test]
fn an_entry_whose_value_keys_do_not_match_the_partition_keys_is_rejected() {
    let mut m = HiveManifest::from_spec(&spec_of(DDL), "sales", "j", "t");
    let mut e = entry("2026-01-02");
    e.values.remove("dt");
    index_of(&mut m).entries = vec![e];
    let err = HiveManifest::parse(&m.to_bytes().expect("bytes"), &ManifestLimits::default())
        .expect_err("must reject");
    assert!(
        format!("{err:?}").contains("dt"),
        "error should name the key: {err:?}"
    );

    // And an extra key that is not a declared partition key at all.
    let mut m = HiveManifest::from_spec(&spec_of(DDL), "sales", "j", "t");
    let mut e = entry("2026-01-02");
    e.values.insert("region".to_string(), "eu".to_string());
    index_of(&mut m).entries = vec![e];
    assert!(
        HiveManifest::parse(&m.to_bytes().expect("bytes"), &ManifestLimits::default()).is_err()
    );
}

// The DDL above declares `dt string`. A second table shape declares `dt
// date`, which is what the typed-value tests need.
const DDL_DATE_KEY: &str = "CREATE EXTERNAL TABLE sales.orders (id BIGINT, amount DOUBLE) \
PARTITIONED BY (dt date) \
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' \
STORED AS TEXTFILE LOCATION 's3://raw/orders/'";

#[test]
fn a_partition_value_that_does_not_parse_to_the_declared_type_fails_load() {
    let mut m = HiveManifest::from_spec(&spec_of(DDL_DATE_KEY), "sales", "j", "t");
    index_of(&mut m).entries = vec![entry("not-a-date")];
    let err = HiveManifest::parse(&m.to_bytes().expect("bytes"), &ManifestLimits::default())
        .expect_err("must reject");
    assert!(
        format!("{err:?}").contains("not-a-date"),
        "error should name the value: {err:?}"
    );
}

#[test]
fn a_date_typed_partition_value_that_parses_is_accepted() {
    let mut m = HiveManifest::from_spec(&spec_of(DDL_DATE_KEY), "sales", "j", "t");
    index_of(&mut m).entries = vec![entry("2026-01-02")];
    HiveManifest::parse(&m.to_bytes().expect("bytes"), &ManifestLimits::default())
        .expect("a valid date parses");
}

#[test]
fn an_int_typed_partition_value_must_parse_as_a_number() {
    const INT_KEY: &str = "CREATE EXTERNAL TABLE sales.orders (id BIGINT) \
PARTITIONED BY (bucket int) \
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' \
STORED AS TEXTFILE LOCATION 's3://raw/orders/'";
    let mut bad = HiveManifest::from_spec(&spec_of(INT_KEY), "sales", "j", "t");
    let mut e = entry("42");
    e.values = BTreeMap::from([("bucket".to_string(), "forty-two".to_string())]);
    e.location = "s3://raw/orders/bucket=forty-two/".to_string();
    index_of(&mut bad).entries = vec![e];
    assert!(
        HiveManifest::parse(&bad.to_bytes().expect("bytes"), &ManifestLimits::default()).is_err()
    );
}

#[test]
fn the_default_partition_sentinel_is_valid_for_every_declared_type() {
    let mut m = HiveManifest::from_spec(&spec_of(DDL_DATE_KEY), "sales", "j", "t");
    index_of(&mut m).entries = vec![entry("__HIVE_DEFAULT_PARTITION__")];
    HiveManifest::parse(&m.to_bytes().expect("bytes"), &ManifestLimits::default())
        .expect("the sentinel normalizes to NULL, not a typed parse failure");
}

#[test]
fn an_entry_location_must_not_be_empty() {
    let mut m = HiveManifest::from_spec(&spec_of(DDL), "sales", "j", "t");
    let mut e = entry("2026-01-02");
    e.location = String::new();
    index_of(&mut m).entries = vec![e];
    let err = HiveManifest::parse(&m.to_bytes().expect("bytes"), &ManifestLimits::default())
        .expect_err("must reject");
    assert!(
        format!("{err:?}").to_lowercase().contains("location"),
        "got {err:?}"
    );
}

#[test]
fn planted_malformed_projection_fails_closed_naming_the_property() {
    let mut m = HiveManifest::from_spec(&spec_of(DDL), "sales", "j", "t");
    m.parameters
        .insert("projection.enabled".into(), "true".into());
    m.parameters
        .insert("projection.dt.type".into(), "banana".into());
    let err = HiveManifest::parse(&m.to_bytes().expect("bytes"), &ManifestLimits::default())
        .expect_err("must reject");
    let msg = err.to_string();
    assert!(msg.contains("projection.dt.type"), "{msg}");
}
