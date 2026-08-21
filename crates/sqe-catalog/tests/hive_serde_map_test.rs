#![cfg(feature = "hive-external")]

use datafusion::arrow::datatypes::DataType;
use sqe_catalog::hive::manifest::HiveManifest;
use sqe_catalog::hive::serde_map::{arrow_type, listing_options, resolve_format, HiveFileFormat};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

fn manifest(ddl: &str) -> HiveManifest {
    let stmts = Parser::parse_sql(&GenericDialect {}, ddl).expect("parse");
    let spec = sqe_sql::hive_ddl::try_extract_hive_table(&stmts[0])
        .expect("extract")
        .expect("external");
    HiveManifest::from_spec(&spec, "db", "owner", "2026-08-19T00:00:00Z")
}

#[test]
fn open_csv_serde_maps_to_csv_with_its_own_property_names() {
    let m = manifest(
        "CREATE EXTERNAL TABLE t (id INT) ROW FORMAT SERDE \
         'org.apache.hadoop.hive.serde2.OpenCSVSerde' WITH SERDEPROPERTIES \
         ('separatorChar' = ';', 'quoteChar' = '|', 'escapeChar' = '\\\\') \
         STORED AS TEXTFILE LOCATION 's3://b/p/'",
    );
    let r = resolve_format(&m).expect("resolve");
    assert!(matches!(r.kind, HiveFileFormat::Csv));
    assert_eq!(r.delimiter, b';');
    assert_eq!(r.quote, Some(b'|'));
}

#[test]
fn csv_header_is_off_by_default_because_hive_textfile_has_none() {
    let m = manifest("CREATE EXTERNAL TABLE t (id INT) STORED AS TEXTFILE LOCATION 's3://b/p/'");
    assert!(!resolve_format(&m).expect("resolve").has_header);
}

#[test]
fn skip_header_line_count_one_turns_the_header_on() {
    let m = manifest(
        "CREATE EXTERNAL TABLE t (id INT) STORED AS TEXTFILE LOCATION 's3://b/p/' \
         TBLPROPERTIES ('skip.header.line.count' = '1')",
    );
    assert!(resolve_format(&m).expect("resolve").has_header);
}

#[test]
fn skip_header_line_count_above_one_is_an_error_not_a_silent_off_by_n() {
    let m = manifest(
        "CREATE EXTERNAL TABLE t (id INT) STORED AS TEXTFILE LOCATION 's3://b/p/' \
         TBLPROPERTIES ('skip.header.line.count' = '3')",
    );
    let err = resolve_format(&m).expect_err("must reject");
    let msg = format!("{err}");
    assert!(msg.contains('3'), "message should name the value: {msg}");
}

#[test]
fn lazy_simple_serde_with_no_delimiter_defaults_to_ctrl_a_like_hive() {
    let m = manifest(
        "CREATE EXTERNAL TABLE t (id INT) ROW FORMAT SERDE \
         'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' STORED AS TEXTFILE \
         LOCATION 's3://b/p/'",
    );
    assert_eq!(resolve_format(&m).expect("resolve").delimiter, 0x01);
}

#[test]
fn each_allow_listed_serde_maps_to_its_format() {
    for (class, want) in [
        ("org.openx.data.jsonserde.JsonSerDe", HiveFileFormat::Json),
        (
            "org.apache.hive.hcatalog.data.JsonSerDe",
            HiveFileFormat::Json,
        ),
        (
            "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
            HiveFileFormat::Parquet,
        ),
    ] {
        let ddl = format!(
            "CREATE EXTERNAL TABLE t (id INT) ROW FORMAT SERDE '{class}' LOCATION 's3://b/p/'"
        );
        let got = resolve_format(&manifest(&ddl)).expect("resolve").kind;
        assert_eq!(
            std::mem::discriminant(&got),
            std::mem::discriminant(&want),
            "{class} mapped wrong"
        );
    }
}

#[test]
fn an_unlisted_serde_is_rejected_by_class_name() {
    let m = manifest(
        "CREATE EXTERNAL TABLE t (id INT) ROW FORMAT SERDE \
         'org.apache.hadoop.hive.serde2.avro.AvroSerDe' LOCATION 's3://b/p/'",
    );
    let msg = format!("{}", resolve_format(&m).expect_err("must reject"));
    assert!(
        msg.contains("AvroSerDe"),
        "message should name the class: {msg}"
    );
}

#[test]
fn orc_is_rejected_by_name_and_names_no_cycle_because_it_is_out_of_scope() {
    let m = manifest("CREATE EXTERNAL TABLE t (id INT) STORED AS ORC LOCATION 's3://b/p/'");
    let msg = format!("{}", resolve_format(&m).expect_err("must reject"));
    assert!(
        msg.to_uppercase().contains("ORC"),
        "message should name ORC: {msg}"
    );
}

#[test]
fn hive_scalar_types_map_to_arrow() {
    for (sql, want) in [
        ("bigint", DataType::Int64),
        ("int", DataType::Int32),
        ("double", DataType::Float64),
        ("string", DataType::Utf8),
        ("boolean", DataType::Boolean),
        ("date", DataType::Date32),
    ] {
        assert_eq!(arrow_type(sql).expect(sql), want, "{sql} mapped wrong");
    }
    assert_eq!(
        arrow_type("decimal(18,2)").expect("decimal"),
        DataType::Decimal128(18, 2)
    );
    assert!(
        arrow_type("array<string>").is_err(),
        "nested types are cycle-2 work"
    );
}

// Team-lead ruling: erroring on a bare `decimal` (no precision or scale) is
// the safer-looking choice and the wrong one, since Hive and Athena both
// default it to decimal(10,0) and Athena parity is the point of this
// feature.
#[test]
fn a_bare_decimal_with_no_precision_defaults_to_hives_ten_zero() {
    assert_eq!(
        arrow_type("decimal").expect("bare decimal"),
        DataType::Decimal128(10, 0)
    );
}

// The tests above only exercise `resolve_format`, which stops at a
// `ResolvedFormat` value. Nothing yet called `listing_options`, so the three
// requirements the brief calls highest-risk (`collect_stat` off, an empty
// `file_extension`, and the header inversion actually reaching DataFusion's
// `CsvFormat`, not just `ResolvedFormat`) had zero coverage: deleting
// `.with_collect_stat(false)`, changing `file_extension` to `.csv`, or
// dropping `.with_has_header(...)` from `listing_options` would all still
// leave every test above green.
#[test]
fn listing_options_wires_collect_stat_extension_and_header_through_to_datafusion() {
    let headerless =
        manifest("CREATE EXTERNAL TABLE t (id INT) STORED AS TEXTFILE LOCATION 's3://b/p/'");
    let r = resolve_format(&headerless).expect("resolve");
    let opts = listing_options(&r, Vec::new()).expect("listing options");

    assert!(
        !opts.collect_stat,
        "statistics collection must stay off: it reads a footer per file"
    );
    assert_eq!(
        opts.file_extension, "",
        "must pick up Hive's extensionless part files"
    );
    assert!(opts.table_partition_cols.is_empty());

    // `Arc<dyn FileFormat>` has no public downcast without pulling in the
    // `as-any` crate as a new direct dependency just for a test, so the
    // header setting is checked through `CsvFormat`'s derived `Debug`
    // (`CsvOptions` itself derives `Debug, Clone, PartialEq`), which is
    // enough to catch `.with_has_header(...)` being dropped from
    // `listing_options`.
    let debug_str = format!("{:?}", opts.format);
    assert!(
        debug_str.contains("has_header: Some(false)"),
        "the Hive-default-off header must reach CsvFormat, not stop at ResolvedFormat: {debug_str}"
    );

    let headered = manifest(
        "CREATE EXTERNAL TABLE t (id INT) STORED AS TEXTFILE LOCATION 's3://b/p/' \
         TBLPROPERTIES ('skip.header.line.count' = '1')",
    );
    let r2 = resolve_format(&headered).expect("resolve");
    let opts2 = listing_options(&r2, Vec::new()).expect("listing options");
    let debug_str2 = format!("{:?}", opts2.format);
    assert!(
        debug_str2.contains("has_header: Some(true)"),
        "skip.header.line.count = 1 must reach CsvFormat too: {debug_str2}"
    );
}

// Team-lead ruling on the ORC gap found while implementing this task: rather
// than have `resolve_format` invent a class name to reject ORC by,
// `manifest.rs::formats_from_stored_as` now records the genuine Hadoop class
// name Hive itself would (`OrcInputFormat`/`OrcOutputFormat`), so the
// manifest never forgets its own declared format. This test pins that
// fidelity directly on the manifest, independent of `resolve_format`'s
// rejection message: it would fail if `formats_from_stored_as` regressed
// back to dropping ORC (`(None, None)`), or if the class names drifted from
// the real Hadoop ones.
#[test]
fn stored_as_orc_records_the_genuine_hadoop_classes_on_the_manifest() {
    let m = manifest("CREATE EXTERNAL TABLE t (id INT) STORED AS ORC LOCATION 's3://b/p/'");
    assert_eq!(
        m.storage_descriptor.input_format.as_deref(),
        Some("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat")
    );
    assert_eq!(
        m.storage_descriptor.output_format.as_deref(),
        Some("org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat")
    );
}

// Same ruling: STORED AS PARQUET with no explicit ROW FORMAT SERDE should
// get Glue's usual ParquetHiveSerDe default, but an explicit ROW FORMAT
// SERDE must still win. This precedence is now load-bearing in two places
// (`resolve_format`'s SerDe-over-STORED-AS match, and this default-attaching
// code in `from_spec`), so it needs its own test rather than relying on
// `resolve_format`'s existing SerDe-precedence tests to catch a regression
// here: those tests never exercise a table that has both an explicit SerDe
// and a `STORED AS` that would otherwise default to a *different* SerDe
// class.
#[test]
fn an_explicit_row_format_serde_beats_the_stored_as_default_serde() {
    let m = manifest(
        "CREATE EXTERNAL TABLE t (id INT) ROW FORMAT SERDE \
         'org.apache.hive.hcatalog.data.JsonSerDe' STORED AS PARQUET LOCATION 's3://b/p/'",
    );
    assert_eq!(
        m.storage_descriptor
            .serde_info
            .serialization_library
            .as_deref(),
        Some("org.apache.hive.hcatalog.data.JsonSerDe"),
        "explicit ROW FORMAT SERDE must not be overwritten by STORED AS PARQUET's default"
    );
    assert!(matches!(
        resolve_format(&m).expect("resolve").kind,
        HiveFileFormat::Json
    ));
}
