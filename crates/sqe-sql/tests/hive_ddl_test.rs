use sqe_sql::hive_ddl::{secret_name, try_extract_hive_table};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

fn spec(sql: &str) -> sqe_sql::hive_ddl::HiveTableSpec {
    let stmts = Parser::parse_sql(&GenericDialect {}, sql).expect("parse");
    try_extract_hive_table(&stmts[0])
        .expect("extract ok")
        .expect("is an external table")
}

fn err(sql: &str) -> String {
    let stmts = Parser::parse_sql(&GenericDialect {}, sql).expect("parse");
    try_extract_hive_table(&stmts[0])
        .expect_err("should reject")
        .to_string()
}

const FULL: &str = "CREATE EXTERNAL TABLE lake.sales.orders \
(id BIGINT, amount DOUBLE COMMENT 'cents') \
PARTITIONED BY (dt string, region string) \
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' \
WITH SERDEPROPERTIES ('separatorChar' = ',') \
STORED AS TEXTFILE LOCATION 's3://raw/orders/' \
TBLPROPERTIES ('skip.header.line.count' = '1', 'sqe.secret' = 'partner')";

#[test]
fn extracts_every_clause_of_the_full_form() {
    let s = spec(FULL);
    assert_eq!(s.catalog.as_deref(), Some("lake"));
    assert_eq!(s.database.as_deref(), Some("sales"));
    assert_eq!(s.name, "orders");
    assert_eq!(
        s.columns
            .iter()
            .map(|c| (c.name.as_str(), c.sql_type.as_str()))
            .collect::<Vec<_>>(),
        vec![("id", "bigint"), ("amount", "double")]
    );
    assert_eq!(s.columns[1].comment.as_deref(), Some("cents"));
    assert_eq!(
        s.partition_keys
            .iter()
            .map(|c| c.name.as_str())
            .collect::<Vec<_>>(),
        vec!["dt", "region"]
    );
    assert_eq!(s.location, "s3://raw/orders/");
    assert_eq!(
        s.serde_class.as_deref(),
        Some("org.apache.hadoop.hive.serde2.OpenCSVSerde")
    );
    assert_eq!(
        s.serde_properties.get("separatorChar").map(String::as_str),
        Some(",")
    );
    assert_eq!(s.stored_as.as_deref(), Some("TEXTFILE"));
    assert_eq!(
        s.table_properties
            .get("skip.header.line.count")
            .map(String::as_str),
        Some("1")
    );
    assert_eq!(secret_name(&s), Some("partner"));
}

#[test]
fn row_format_delimited_becomes_lazy_simple_serde_with_field_delim() {
    let s = spec(
        "CREATE EXTERNAL TABLE t (id INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' \
         STORED AS TEXTFILE LOCATION 's3://b/p/'",
    );
    assert_eq!(
        s.serde_class.as_deref(),
        Some("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe")
    );
    assert_eq!(
        s.serde_properties.get("field.delim").map(String::as_str),
        Some(",")
    );
}

#[test]
fn input_output_format_is_captured() {
    let s = spec(
        "CREATE EXTERNAL TABLE t (id INT) STORED AS INPUTFORMAT \
         'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT \
         'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' LOCATION 's3://b/p/'",
    );
    assert_eq!(
        s.input_format.as_deref(),
        Some("org.apache.hadoop.mapred.TextInputFormat")
    );
    assert!(s.stored_as.is_none());
}

#[test]
fn non_external_create_table_is_not_ours() {
    let stmts = Parser::parse_sql(&GenericDialect {}, "CREATE TABLE t (id INT)").expect("parse");
    assert!(try_extract_hive_table(&stmts[0]).expect("ok").is_none());
}

#[test]
fn missing_location_is_rejected() {
    let msg = err("CREATE EXTERNAL TABLE t (id INT) STORED AS PARQUET");
    assert!(
        msg.contains("LOCATION"),
        "message should name LOCATION, got: {msg}"
    );
}

#[test]
fn unquoted_identifiers_fold_to_lowercase_quoted_ones_do_not() {
    let s = spec(
        "CREATE EXTERNAL TABLE MyTab (ID INT, \"KeepMe\" INT) STORED AS PARQUET LOCATION 's3://b/p/'",
    );
    assert_eq!(s.name, "mytab");
    assert_eq!(s.columns[0].name, "id");
    assert_eq!(s.columns[1].name, "KeepMe");
}

#[test]
fn unknown_sqe_prefixed_property_is_rejected_by_name() {
    let msg = err(
        "CREATE EXTERNAL TABLE t (id INT) STORED AS PARQUET LOCATION 's3://b/p/' \
         TBLPROPERTIES ('sqe.nope' = '1')",
    );
    assert!(
        msg.contains("sqe.nope"),
        "message should name the property, got: {msg}"
    );
}

#[test]
fn unknown_unprefixed_property_is_kept_not_rejected() {
    let s = spec(
        "CREATE EXTERNAL TABLE t (id INT) STORED AS PARQUET LOCATION 's3://b/p/' \
         TBLPROPERTIES ('classification' = 'parquet', 'EXTERNAL' = 'TRUE')",
    );
    assert_eq!(
        s.table_properties.get("classification").map(String::as_str),
        Some("parquet")
    );
}

#[test]
fn partition_key_repeated_in_column_list_is_rejected() {
    let msg = err(
        "CREATE EXTERNAL TABLE t (id INT, dt STRING) PARTITIONED BY (dt STRING) \
         STORED AS PARQUET LOCATION 's3://b/p/'",
    );
    assert!(
        msg.contains("dt"),
        "message should name the duplicated column, got: {msg}"
    );
}
