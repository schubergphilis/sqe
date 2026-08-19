#![cfg(feature = "hive-external")]

use sqe_catalog::hive::manifest::{HiveManifest, ManifestLimits, MANIFEST_VERSION};
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
