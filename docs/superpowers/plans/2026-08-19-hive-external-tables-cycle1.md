# Hive External Tables, Cycle 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Query Hive-style external tables (CSV, line-delimited JSON, Parquet) on S3-compatible storage using Athena/Glue `CREATE EXTERNAL TABLE` syntax, in embedded and server mode, with definitions stored in a Glue-shaped sidecar manifest under an attached warehouse root instead of a metastore.

**Architecture:** `sqe-sql` extracts a backend-neutral `HiveTableSpec` from the `CreateTable` AST that sqlparser already produces. `sqe-catalog::hive` turns that spec into a versioned manifest object in the warehouse root, and turns a manifest back into a DataFusion `ListingTable` with hive partition columns. A `HiveCatalogProvider` registers the root so tables resolve as `root.database.table`. Policy sees Hive tables through a new branch in `resolve_policy_key` and a routing `TagSource`.

**Tech Stack:** Rust, DataFusion 54, sqlparser 0.62, `object_store`, `serde_json`, `moka` (caching), `async-trait`.

**Spec:** `docs/superpowers/specs/2026-08-19-hive-external-tables-design.md`

## Global Constraints

- **No emdash, endash, or Unicode arrows** anywhere, including doc comments. Use `->` inside code blocks only.
- **Clippy:** `cargo clippy --all-targets --all-features -- -D warnings` is the repo's stated gate and it does NOT pass on this tree. `crates/sqe-catalog/src/rest_catalog.rs` carries two pre-existing warnings unrelated to this feature: `too_many_arguments` (8/7) at line 1019 and `field_reassign_with_default` at lines 2135-2136. For any task touching `sqe-catalog`, run clippy WITHOUT `-D warnings` and require zero warnings whose path is under `crates/sqe-catalog/src/hive/` or your own test files, against that baseline of exactly two warnings in `rest_catalog.rs`. Do not fix the pre-existing two; they belong in their own MR. For every other crate, `-D warnings` must pass. Never pipe a gate command into `tail`; the exit code you see would be `tail`'s.
- **Never run `cargo fmt --all`.** Main is not rustfmt-clean. Format only files you touched: `cargo fmt -- <paths>`.
- **Feature gate:** all new `sqe-catalog` code sits behind the `hive-external` cargo feature, default-on. Named to avoid confusion with `hms`, which is Hive Metastore for Iceberg tables.
- **Branch:** `feat/hive-external-tables-cycle1`, cut from `docs/hive-external-tables-design`, NOT from main. The spec and this plan live on that branch and main does not have them yet. If the docs branch has already merged, cut from main. Never push to main. One MR.
- **Manifest path:** `<root>/<database>/<table>/_sqe/table.json`
- **Manifest version:** `manifest_version: 1`. Unknown higher version is rejected, unknown fields inside version 1 are ignored.
- **Attach kind keyword:** `hive_external`. `hive` is already bound to `Hms` at `crates/sqe-sql/src/attach.rs:64`.
- **Policy key:** a Hive table's policy namespace is `<root>.<database>`. Iceberg keys are unchanged.
- **Secret:** `TBLPROPERTIES ('sqe.secret' = '<name>')`. There is no `WITH (SECRET = ...)` clause; it does not parse.
- **Formats:** csv, json (line-delimited), parquet. ORC, Avro, SequenceFile, RCFile rejected by name.
- **Types:** scalar columns and scalar partition keys only. `array<...>`, `map<...>`, `struct<...>` rejected by name.
- **CSV header default is OFF** (Hive), which inverts DataFusion's `CsvFormat` default. `skip.header.line.count = 1` turns it on, `0` off, anything above 1 is an error.
- **CSV field delimiter default is `\x01`** (Hive `LazySimpleSerDe`), not comma.
- **Statistics:** `ListingOptions::with_collect_stat(false)`. Size-only estimates. Never read a Parquet footer per file at plan time.
- **No listing cache.** `crates/sqe-catalog/src/lazy_object_store.rs:64` sets `with_list_files_cache_limit(0)` as the #363 fix. Do not re-enable it.

## File Structure

| File | Responsibility |
|---|---|
| Create `crates/sqe-sql/src/hive_ddl.rs` | `HiveTableSpec` and extraction from the `CreateTable` AST. No DataFusion or object-store types |
| Modify `crates/sqe-sql/src/lib.rs` | Export `hive_ddl` |
| Modify `crates/sqe-sql/src/classifier.rs:665` | Route `external: true` to `StatementKind::CreateExternalTable` |
| Modify `crates/sqe-sql/src/attach.rs:47` | Add `CatalogKind::HiveExternal`, keyword `hive_external` |
| Create `crates/sqe-catalog/src/hive/mod.rs` | Module wiring, `HiveError` |
| Create `crates/sqe-catalog/src/hive/manifest.rs` | Glue-shaped manifest types, `from_spec`, validation, caps |
| Create `crates/sqe-catalog/src/hive/serde_map.rs` | SerDe and `STORED AS` allow-list, `ListingOptions` construction |
| Create `crates/sqe-catalog/src/hive/store.rs` | Manifest read, write, delete, list over `ObjectStore`; `check_path` guard |
| Create `crates/sqe-catalog/src/hive/provider.rs` | `HiveCatalogProvider`, `HiveSchemaProvider` |
| Create `crates/sqe-catalog/src/hive/stats.rs` | Size-only statistics |
| Modify `crates/sqe-catalog/src/lib.rs` | `#[cfg(feature = "hive-external")] pub mod hive;` |
| Modify `crates/sqe-catalog/src/mount.rs` | Sibling dispatch: Hive roots return a `CatalogProvider`, not an `iceberg::Catalog` |
| Modify `crates/sqe-policy/src/plan_rewriter.rs:725` | `resolve_policy_key` Hive branch |
| Create `crates/sqe-coordinator/src/hive_tag_source.rs` | `HiveTagSource` and `CatalogRoutedTagSource` |
| Create `crates/sqe-coordinator/src/hive_ddl_handler.rs` | Server-mode DDL handling and statement rejections |
| Modify `crates/sqe-coordinator/src/query_handler.rs:1306` | Dispatch `CreateExternalTable`, branch `Drop` and friends |
| Modify `crates/sqe-coordinator/src/policy_wiring.rs:89` | Wire `CatalogRoutedTagSource` |
| Modify `crates/sqe-cli/src/embedded.rs:478` | Intercept `CreateExternalTable`; add `--hive-root` |
| Create `docs/site/book/src/reference/hive-external-tables.md` | User documentation |

---

### Task 1: HiveTableSpec extraction in sqe-sql

sqlparser already parses the whole Athena form. This task reads the AST it produces into a neutral struct and rejects what cycle 1 does not support. No DataFusion types: `sqe-catalog` depends on `sqe-sql`, never the reverse.

**Files:**
- Create: `crates/sqe-sql/src/hive_ddl.rs`
- Modify: `crates/sqe-sql/src/lib.rs`
- Test: `crates/sqe-sql/tests/hive_ddl_test.rs`

**Interfaces:**
- Consumes: nothing.
- Produces:
  ```rust
  pub struct HiveTableSpec {
      pub catalog: Option<String>,
      pub database: Option<String>,
      pub name: String,
      pub if_not_exists: bool,
      pub comment: Option<String>,
      pub columns: Vec<HiveColumn>,
      pub partition_keys: Vec<HiveColumn>,
      pub location: String,
      pub serde_class: Option<String>,
      pub serde_properties: BTreeMap<String, String>,
      pub stored_as: Option<String>,
      pub input_format: Option<String>,
      pub output_format: Option<String>,
      pub table_properties: BTreeMap<String, String>,
  }
  pub struct HiveColumn { pub name: String, pub sql_type: String, pub comment: Option<String> }
  pub fn try_extract_hive_table(stmt: &sqlparser::ast::Statement) -> Result<Option<HiveTableSpec>, SqeError>;
  pub fn secret_name(spec: &HiveTableSpec) -> Option<&str>;
  ```
  `try_extract_hive_table` returns `Ok(None)` when the statement is not a `CreateTable` with `external: true`. Column and partition-key names are lowercased unless they were quoted. `sql_type` is the sqlparser `DataType` rendered with `to_string()` then lowercased.

- [ ] **Step 1: Write the failing tests**

Create `crates/sqe-sql/tests/hive_ddl_test.rs`:

```rust
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqe_sql::hive_ddl::{secret_name, try_extract_hive_table};

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
        s.columns.iter().map(|c| (c.name.as_str(), c.sql_type.as_str())).collect::<Vec<_>>(),
        vec![("id", "bigint"), ("amount", "double")]
    );
    assert_eq!(s.columns[1].comment.as_deref(), Some("cents"));
    assert_eq!(
        s.partition_keys.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(),
        vec!["dt", "region"]
    );
    assert_eq!(s.location, "s3://raw/orders/");
    assert_eq!(s.serde_class.as_deref(), Some("org.apache.hadoop.hive.serde2.OpenCSVSerde"));
    assert_eq!(s.serde_properties.get("separatorChar").map(String::as_str), Some(","));
    assert_eq!(s.stored_as.as_deref(), Some("TEXTFILE"));
    assert_eq!(s.table_properties.get("skip.header.line.count").map(String::as_str), Some("1"));
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
    assert_eq!(s.serde_properties.get("field.delim").map(String::as_str), Some(","));
}

#[test]
fn input_output_format_is_captured() {
    let s = spec(
        "CREATE EXTERNAL TABLE t (id INT) STORED AS INPUTFORMAT \
         'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT \
         'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' LOCATION 's3://b/p/'",
    );
    assert_eq!(s.input_format.as_deref(), Some("org.apache.hadoop.mapred.TextInputFormat"));
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
    assert!(msg.contains("LOCATION"), "message should name LOCATION, got: {msg}");
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
    assert!(msg.contains("sqe.nope"), "message should name the property, got: {msg}");
}

#[test]
fn unknown_unprefixed_property_is_kept_not_rejected() {
    let s = spec(
        "CREATE EXTERNAL TABLE t (id INT) STORED AS PARQUET LOCATION 's3://b/p/' \
         TBLPROPERTIES ('classification' = 'parquet', 'EXTERNAL' = 'TRUE')",
    );
    assert_eq!(s.table_properties.get("classification").map(String::as_str), Some("parquet"));
}

#[test]
fn partition_key_repeated_in_column_list_is_rejected() {
    let msg = err(
        "CREATE EXTERNAL TABLE t (id INT, dt STRING) PARTITIONED BY (dt STRING) \
         STORED AS PARQUET LOCATION 's3://b/p/'",
    );
    assert!(msg.contains("dt"), "message should name the duplicated column, got: {msg}");
}
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
cargo test -p sqe-sql --test hive_ddl_test 2>&1 | tail -20
```

Expected: compile failure, `unresolved import sqe_sql::hive_ddl`.

- [ ] **Step 3: Implement the module**

Create `crates/sqe-sql/src/hive_ddl.rs`. Key shape:

```rust
//! Extraction of Athena/Glue `CREATE EXTERNAL TABLE` into a neutral spec.
//!
//! sqlparser 0.62 parses the whole Athena form already, in both
//! `GenericDialect` and `HiveDialect`, so this module reads the AST rather
//! than parsing text. It stays free of DataFusion and object-store types:
//! `sqe-catalog` depends on `sqe-sql`, never the reverse.
//!
//! Spec: `docs/superpowers/specs/2026-08-19-hive-external-tables-design.md`

use std::collections::BTreeMap;

use sqlparser::ast::{
    ColumnDef, ColumnOption, CreateTable, CreateTableOptions, Expr, HiveDelimiter,
    HiveDistributionStyle, HiveIOFormat, HiveRowFormat, Ident, SqlOption, Statement, Value,
};
use sqe_core::SqeError;

pub const LAZY_SIMPLE_SERDE: &str = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";

/// Recognised `sqe.`-prefixed table properties. Anything else with that
/// prefix is a typo, and a silently ignored typo is a setting that did not
/// apply.
const KNOWN_SQE_PROPERTIES: &[&str] = &["sqe.secret"];

pub fn try_extract_hive_table(stmt: &Statement) -> Result<Option<HiveTableSpec>, SqeError> {
    let Statement::CreateTable(ct) = stmt else { return Ok(None) };
    if !ct.external {
        return Ok(None);
    }
    Ok(Some(extract(ct)?))
}

fn extract(ct: &CreateTable) -> Result<HiveTableSpec, SqeError> {
    // Name: 1, 2, or 3 parts, rightmost is the table.
    let parts: Vec<String> = ct.name.0.iter().filter_map(|p| p.as_ident().map(ident_text)).collect();
    let (catalog, database, name) = match parts.as_slice() {
        [t] => (None, None, t.clone()),
        [d, t] => (None, Some(d.clone()), t.clone()),
        [c, d, t] => (Some(c.clone()), Some(d.clone()), t.clone()),
        other => {
            return Err(SqeError::InvalidArgument(format!(
                "hive_external: table name must have 1 to 3 parts, got {}",
                other.len()
            )))
        }
    };

    let columns = ct.columns.iter().map(column_of).collect::<Result<Vec<_>, _>>()?;
    if columns.is_empty() {
        return Err(SqeError::InvalidArgument(
            "hive_external: CREATE EXTERNAL TABLE needs an explicit column list; schema              inference is not supported because the declared list is what binds CSV fields              by ordinal"
                .into(),
        ));
    }

    let partition_keys = match &ct.hive_distribution {
        HiveDistributionStyle::PARTITIONED { columns } => {
            columns.iter().map(column_of).collect::<Result<Vec<_>, _>>()?
        }
        _ => Vec::new(),
    };

    // Hive and Athena declare partition keys separately, and a key repeated in
    // the main list would land in the Arrow schema twice.
    for k in &partition_keys {
        if columns.iter().any(|c| c.name == k.name) {
            return Err(SqeError::InvalidArgument(format!(
                "hive_external: `{}` is both a column and a partition key. Hive declares                  partition keys only in PARTITIONED BY; remove it from the column list.",
                k.name
            )));
        }
    }

    // ROW FORMAT: a SERDE class as given, or DELIMITED lowered onto
    // LazySimpleSerDe with the delimiters as its properties.
    let mut serde_properties = BTreeMap::new();
    let mut serde_class = None;
    if let Some(fmts) = &ct.hive_formats {
        match &fmts.row_format {
            Some(HiveRowFormat::SERDE { class }) => serde_class = Some(class.clone()),
            Some(HiveRowFormat::DELIMITED { delimiters }) => {
                serde_class = Some(LAZY_SIMPLE_SERDE.to_string());
                // `HiveRowDelimiter` is a struct, not an enum: it holds a
                // `delimiter: HiveDelimiter` and a `char: Ident`. Match on the
                // inner field, not on the struct name.
                for d in delimiters {
                    let key = match d.delimiter {
                        HiveDelimiter::FieldsTerminatedBy => "field.delim",
                        HiveDelimiter::LinesTerminatedBy => "line.delim",
                        HiveDelimiter::CollectionItemsTerminatedBy => "collection.delim",
                        HiveDelimiter::MapKeysTerminatedBy => "mapkey.delim",
                    };
                    serde_properties.insert(key.to_string(), d.char.value.clone());
                }
            }
            None => {}
        }
        if let Some(props) = &fmts.serde_properties {
            serde_properties.extend(props.iter().filter_map(option_pair));
        }
    }

    // STORED AS, or INPUTFORMAT / OUTPUTFORMAT.
    let mut stored_as = ct.file_format.as_ref().map(|f| f.to_string());
    let mut input_format = None;
    let mut output_format = None;
    match ct.hive_formats.as_ref().and_then(|f| f.storage.as_ref()) {
        Some(HiveIOFormat::FileFormat { format }) => stored_as = Some(format.to_string()),
        Some(HiveIOFormat::IOF { input_format: i, output_format: o }) => {
            stored_as = None;
            input_format = Some(literal_text(i));
            output_format = Some(literal_text(o));
        }
        None => {}
    }

    let location = ct
        .location
        .clone()
        .or_else(|| ct.hive_formats.as_ref().and_then(|f| f.location.clone()))
        .ok_or_else(|| {
            SqeError::InvalidArgument(
                "hive_external: CREATE EXTERNAL TABLE requires LOCATION '<url>'".into(),
            )
        })?;

    // Every option-list flavour flattens into one property map: Athena writes
    // TBLPROPERTIES, and sqlparser reports the shape it saw.
    let table_properties: BTreeMap<String, String> = match &ct.table_options {
        CreateTableOptions::TableProperties(v)
        | CreateTableOptions::Options(v)
        | CreateTableOptions::Plain(v)
        | CreateTableOptions::With(v) => v.iter().filter_map(option_pair).collect(),
        CreateTableOptions::None => BTreeMap::new(),
    };
    for key in table_properties.keys() {
        if key.starts_with("sqe.") && !KNOWN_SQE_PROPERTIES.contains(&key.as_str()) {
            return Err(SqeError::InvalidArgument(format!(
                "hive_external: unknown property `{key}`. Known sqe. properties: {}. A silently                  ignored typo is a setting that did not apply.",
                KNOWN_SQE_PROPERTIES.join(", ")
            )));
        }
    }

    Ok(HiveTableSpec {
        catalog,
        database,
        name,
        if_not_exists: ct.if_not_exists,
        comment: ct.comment.as_ref().map(ToString::to_string),
        columns,
        partition_keys,
        location,
        serde_class,
        serde_properties,
        stored_as,
        input_format,
        output_format,
        table_properties,
    })
}

/// The text of a string-literal `Expr`, which is how sqlparser reports an
/// INPUTFORMAT or OUTPUTFORMAT class.
fn literal_text(e: &Expr) -> String {
    match e {
        Expr::Value(vw) => match &vw.value {
            Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => s.clone(),
            other => other.to_string(),
        },
        other => other.to_string(),
    }
}

/// Lowercase an unquoted identifier, preserve a quoted one. Matches the
/// issue #337 folding decision and Hive/Glue, which lowercase everything.
fn ident_text(i: &Ident) -> String {
    match i.quote_style {
        Some(_) => i.value.clone(),
        None => i.value.to_ascii_lowercase(),
    }
}

/// A rendered sqlparser type, lowercased. `'<'` means a Hive angle-bracket
/// nested type, which neither dialect parses and cycle 1 does not support.
fn column_of(c: &ColumnDef) -> Result<HiveColumn, SqeError> {
    let sql_type = c.data_type.to_string().to_ascii_lowercase();
    if sql_type.contains('<') {
        return Err(SqeError::InvalidArgument(format!(
            "hive_external: nested type `{sql_type}` on column `{}` is not supported in this \
             release. Cycle 2 adds a pre-parse type rewrite. Use a scalar type for now.",
            ident_text(&c.name)
        )));
    }
    Ok(HiveColumn {
        name: ident_text(&c.name),
        sql_type,
        comment: c.options.iter().find_map(|o| match &o.option {
            ColumnOption::Comment(text) => Some(text.clone()),
            _ => None,
        }),
    })
}

/// The literal text of a `SqlOption::KeyValue` value, unquoted.
fn option_pair(o: &SqlOption) -> Option<(String, String)> {
    match o {
        SqlOption::KeyValue { key, value } => {
            let v = match value {
                Expr::Value(vw) => match &vw.value {
                    Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => s.clone(),
                    other => other.to_string(),
                },
                other => other.to_string(),
            };
            Some((key.value.clone(), v))
        }
        _ => None,
    }
}

pub fn secret_name(spec: &HiveTableSpec) -> Option<&str> {
    spec.table_properties.get("sqe.secret").map(String::as_str)
}
```

Every rejection uses `SqeError::InvalidArgument` and names the offending item. `HiveDelimiter`'s variant list is small and may differ in the pinned sqlparser; confirm it with `grep -n "pub enum HiveDelimiter" -A 12 $(ls -d ~/.cargo/registry/src/*/sqlparser-0.62.0)/src/ast/mod.rs` and keep the mapping to Hive property keys as written.

Add to `crates/sqe-sql/src/lib.rs`:

```rust
pub mod hive_ddl;
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
cargo test -p sqe-sql --test hive_ddl_test 2>&1 | tail -20
cargo clippy -p sqe-sql --all-targets -- -D warnings
```

Expected: 9 passed. Clippy clean.

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-sql/src/hive_ddl.rs crates/sqe-sql/src/lib.rs crates/sqe-sql/tests/hive_ddl_test.rs
git commit -m "feat(sql): extract Athena CREATE EXTERNAL TABLE into a neutral spec

sqlparser 0.62 already parses the whole Athena form, so this reads the
CreateTable AST rather than adding a parser. Rejects nested types, an
unknown sqe.-prefixed property, a duplicated partition key, and a missing
LOCATION, each naming the offending item."
```

---

### Task 2: Classifier routing and the hive_external attach kind

**Files:**
- Modify: `crates/sqe-sql/src/classifier.rs:665`
- Modify: `crates/sqe-sql/src/attach.rs:47`
- Test: `crates/sqe-sql/tests/hive_ddl_test.rs` (append)

**Interfaces:**
- Consumes: `hive_ddl::{HiveTableSpec, try_extract_hive_table}` from Task 1.
- Produces: `StatementKind::CreateExternalTable(Box<HiveTableSpec>)` and `CatalogKind::HiveExternal`.

- [ ] **Step 1: Write the failing tests**

Append to `crates/sqe-sql/tests/hive_ddl_test.rs`:

```rust
use sqe_sql::{parse_and_classify, CatalogKind, StatementKind};

#[test]
fn classifier_routes_external_create_away_from_the_iceberg_path() {
    let kind = parse_and_classify(
        "CREATE EXTERNAL TABLE lake.sales.orders (id INT) STORED AS PARQUET LOCATION 's3://b/p/'",
    )
    .expect("classify");
    match kind {
        StatementKind::CreateExternalTable(spec) => assert_eq!(spec.name, "orders"),
        other => panic!("expected CreateExternalTable, got {other:?}"),
    }
}

#[test]
fn classifier_leaves_plain_create_table_on_the_iceberg_path() {
    let kind = parse_and_classify("CREATE TABLE t (id INT)").expect("classify");
    assert!(matches!(kind, StatementKind::CreateTable(_)));
}

#[test]
fn hive_external_is_its_own_attach_kind_and_hive_still_means_hms() {
    assert_eq!(CatalogKind::parse("hive_external"), Some(CatalogKind::HiveExternal));
    assert_eq!(CatalogKind::parse("hive"), Some(CatalogKind::Hms));
    assert_eq!(CatalogKind::HiveExternal.name(), "hive_external");
}
```

- [ ] **Step 2: Run to verify failure**

```bash
cargo test -p sqe-sql --test hive_ddl_test 2>&1 | tail -20
```

Expected: `no variant named CreateExternalTable`, `no variant named HiveExternal`.

- [ ] **Step 3: Implement**

In `crates/sqe-sql/src/classifier.rs`, add the variant near the other DDL kinds:

```rust
    /// `CREATE EXTERNAL TABLE ... LOCATION ...` (Hive/Athena external table).
    /// Routed away from the Iceberg create path, which would otherwise try to
    /// create a managed table in the catalog.
    CreateExternalTable(Box<crate::hive_ddl::HiveTableSpec>),
```

Replace the `Statement::CreateTable` arm at line 665:

```rust
        Statement::CreateTable(ref ct) => {
            // EXTERNAL is checked before CTAS: `CREATE EXTERNAL TABLE ... AS
            // SELECT` is a write, which is cycle 4, and the DDL handler
            // rejects it with that message rather than silently creating an
            // Iceberg table.
            if ct.external {
                if let Some(spec) = crate::hive_ddl::try_extract_hive_table(&stmt)? {
                    return Ok(StatementKind::CreateExternalTable(Box::new(spec)));
                }
            }
            if ct.query.is_some() {
                Ok(StatementKind::Ctas(Box::new(stmt)))
            } else {
                Ok(StatementKind::CreateTable(Box::new(stmt)))
            }
        }
```

In `crates/sqe-sql/src/attach.rs`, add `HiveExternal` to `CatalogKind`, map `"hive_external"` in `parse` (leave `"hive"` mapping to `Hms`), and return `"hive_external"` from `name`.

- [ ] **Step 4: Run to verify passing**

```bash
cargo test -p sqe-sql 2>&1 | tail -15
cargo clippy -p sqe-sql --all-targets -- -D warnings
```

Expected: all `sqe-sql` tests pass, including the pre-existing ones. Clippy clean.

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-sql/src
git commit -m "feat(sql): route CREATE EXTERNAL TABLE and add the hive_external attach kind

TYPE hive already means Hive Metastore for Iceberg tables, so the new
warehouse-root kind is hive_external. EXTERNAL is checked before CTAS so a
CREATE EXTERNAL TABLE AS SELECT gets the cycle-4 message instead of quietly
creating an Iceberg table."
```

---

### Task 3: The manifest: types, validation, caps

The manifest mirrors Glue's `TableInput` so a future Glue or HMS backend is a field mapping. It is also untrusted input: anyone who can write to the warehouse prefix can plant one.

**Files:**
- Create: `crates/sqe-catalog/src/hive/mod.rs`
- Create: `crates/sqe-catalog/src/hive/manifest.rs`
- Modify: `crates/sqe-catalog/src/lib.rs`
- Modify: `crates/sqe-catalog/Cargo.toml`
- Test: `crates/sqe-catalog/tests/hive_manifest_test.rs`

**Interfaces:**
- Consumes: `sqe_sql::hive_ddl::{HiveTableSpec, HiveColumn}` from Task 1.
- Produces:
  ```rust
  pub const MANIFEST_VERSION: u32 = 1;
  pub struct HiveManifest {
      pub manifest_version: u32,
      pub table_type: String,
      pub database_name: String,
      pub name: String,
      pub owner: Option<String>,
      pub created_at: Option<String>,
      pub comment: Option<String>,
      pub partition_keys: Vec<GlueColumn>,
      pub parameters: BTreeMap<String, String>,
      pub storage_descriptor: StorageDescriptor,
      pub sqe: SqeExtensions,
  }
  pub struct StorageDescriptor {
      pub location: String,
      pub input_format: Option<String>,
      pub output_format: Option<String>,
      pub compressed: bool,
      pub serde_info: SerdeInfo,
      pub columns: Vec<GlueColumn>,
      pub sort_columns: Vec<String>,
  }
  pub struct SerdeInfo { pub serialization_library: Option<String>, pub parameters: BTreeMap<String, String> }
  pub struct GlueColumn { pub name: String, pub r#type: String, pub comment: Option<String> }
  pub struct SqeExtensions {
      pub secret: Option<String>,
      pub column_tags: BTreeMap<String, Vec<String>>,
      pub partition_index: Option<serde_json::Value>,
      pub statistics: Option<serde_json::Value>,
  }
  pub struct ManifestLimits { pub max_bytes: usize, pub max_columns: usize, pub max_partition_keys: usize }
  impl HiveManifest {
      pub fn from_spec(spec: &HiveTableSpec, database: &str, owner: &str, created_at: &str) -> Self;
      pub fn parse(bytes: &[u8], limits: &ManifestLimits) -> Result<Self, HiveError>;
      pub fn to_bytes(&self) -> Result<Vec<u8>, HiveError>;
      pub fn secret(&self) -> Option<&str>;
  }
  pub enum HiveError { Validation(String), NotFound(String), Store(String), Forbidden(String) }
  ```
  `ManifestLimits::default()` is `max_bytes: 1 << 20`, `max_columns: 4096`, `max_partition_keys: 32`. All three are config values in Task 6; the default lives here.

- [ ] **Step 1: Write the failing tests**

Create `crates/sqe-catalog/tests/hive_manifest_test.rs`:

```rust
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
        back.storage_descriptor.serde_info.serialization_library.as_deref(),
        Some("org.apache.hadoop.hive.serde2.OpenCSVSerde")
    );
    assert_eq!(back.storage_descriptor.columns.len(), 2);
    assert_eq!(back.partition_keys.len(), 1);
    assert_eq!(back.secret(), Some("partner"));
}

#[test]
fn uses_glue_field_names_so_a_future_glue_backend_is_a_mapping() {
    let m = HiveManifest::from_spec(&spec_of(DDL), "sales", "jacob", "2026-08-19T10:00:00Z");
    let json: serde_json::Value = serde_json::from_slice(&m.to_bytes().expect("bytes")).expect("json");
    for key in ["manifest_version", "table_type", "database_name", "partition_keys",
                "parameters", "storage_descriptor", "sqe"] {
        assert!(json.get(key).is_some(), "missing top-level key {key}");
    }
    let sd = json.get("storage_descriptor").expect("sd");
    for key in ["location", "input_format", "serde_info", "columns"] {
        assert!(sd.get(key).is_some(), "missing storage_descriptor key {key}");
    }
    assert!(sd["serde_info"].get("serialization_library").is_some());
}

#[test]
fn a_higher_manifest_version_is_rejected_not_parsed_best_effort() {
    let mut json: serde_json::Value = serde_json::from_slice(
        &HiveManifest::from_spec(&spec_of(DDL), "sales", "j", "t").to_bytes().expect("bytes"),
    )
    .expect("json");
    json["manifest_version"] = serde_json::json!(99);
    let err = HiveManifest::parse(json.to_string().as_bytes(), &ManifestLimits::default())
        .expect_err("must reject");
    assert!(format!("{err:?}").contains("99"), "error should name the version: {err:?}");
}

#[test]
fn an_unknown_field_inside_a_known_version_is_ignored() {
    let mut json: serde_json::Value = serde_json::from_slice(
        &HiveManifest::from_spec(&spec_of(DDL), "sales", "j", "t").to_bytes().expect("bytes"),
    )
    .expect("json");
    json["future_field_from_a_newer_writer"] = serde_json::json!({"a": 1});
    HiveManifest::parse(json.to_string().as_bytes(), &ManifestLimits::default())
        .expect("a newer writer's extra field must stay readable");
}

#[test]
fn oversized_manifest_is_rejected_before_parsing() {
    let limits = ManifestLimits { max_bytes: 64, ..ManifestLimits::default() };
    let bytes = HiveManifest::from_spec(&spec_of(DDL), "sales", "j", "t").to_bytes().expect("bytes");
    assert!(bytes.len() > 64, "fixture must exceed the cap");
    let err = HiveManifest::parse(&bytes, &limits).expect_err("must reject");
    assert!(format!("{err:?}").to_lowercase().contains("size"), "got {err:?}");
}

#[test]
fn too_many_columns_is_rejected() {
    let limits = ManifestLimits { max_columns: 1, ..ManifestLimits::default() };
    let bytes = HiveManifest::from_spec(&spec_of(DDL), "sales", "j", "t").to_bytes().expect("bytes");
    let err = HiveManifest::parse(&bytes, &limits).expect_err("must reject");
    assert!(format!("{err:?}").to_lowercase().contains("column"), "got {err:?}");
}

#[test]
fn empty_location_is_rejected() {
    let mut m = HiveManifest::from_spec(&spec_of(DDL), "sales", "j", "t");
    m.storage_descriptor.location = String::new();
    let bytes = m.to_bytes().expect("bytes");
    let err = HiveManifest::parse(&bytes, &ManifestLimits::default()).expect_err("must reject");
    assert!(format!("{err:?}").to_lowercase().contains("location"), "got {err:?}");
}
```

- [ ] **Step 2: Run to verify failure**

```bash
cargo test -p sqe-catalog --features hive-external --test hive_manifest_test 2>&1 | tail -20
```

Expected: `unresolved module sqe_catalog::hive`.

- [ ] **Step 3: Implement**

Add to `crates/sqe-catalog/Cargo.toml` under `[features]`:

```toml
# Hive-style external tables (Athena/Glue CREATE EXTERNAL TABLE over CSV,
# line-delimited JSON, and Parquet) with definitions in a sidecar manifest.
# Distinct from `hms`, which is Hive Metastore holding Iceberg tables. Adds no
# new dependencies: the readers are already compiled for the file TVFs.
hive-external = []
```

Add `hive-external` to the `default` list and to `full-backends`.

Add to `crates/sqe-catalog/src/lib.rs`:

```rust
#[cfg(feature = "hive-external")]
pub mod hive;
```

Create `crates/sqe-catalog/src/hive/mod.rs`:

```rust
//! Hive-style external tables with definitions in a sidecar manifest.
//!
//! Spec: `docs/superpowers/specs/2026-08-19-hive-external-tables-design.md`
//!
//! Layout: `<root>/<database>/<table>/_sqe/table.json`. The manifest mirrors
//! Glue's `TableInput`, so storing definitions in Glue or HMS later is a field
//! mapping rather than a redesign.

pub mod manifest;
pub mod provider;
pub mod serde_map;
pub mod stats;
pub mod store;

/// Errors from the Hive external-table path.
///
/// `Forbidden` is separate from `Validation` because a location that fails the
/// `[storage.tvf]` allowlist is a permission answer, not a malformed input, and
/// the two map to different `SqeError` variants at the boundary.
#[derive(Debug, thiserror::Error)]
pub enum HiveError {
    #[error("hive_external: {0}")]
    Validation(String),
    #[error("hive_external: table not found: {0}")]
    NotFound(String),
    #[error("hive_external: object store error: {0}")]
    Store(String),
    #[error("hive_external: forbidden: {0}")]
    Forbidden(String),
}
```

Create `crates/sqe-catalog/src/hive/manifest.rs` with the structs from the Interfaces block. Requirements:

- `#[serde(default)]` on every optional field and on the `sqe` block so an older manifest stays readable. Do NOT use `deny_unknown_fields`: an unknown field from a newer writer must be ignored.
- `parse` order matters: check `bytes.len()` against `max_bytes` FIRST, then deserialize, then check `manifest_version <= MANIFEST_VERSION`, then the count caps, then the field checks. A cap that runs after deserialization does not protect against the oversized input.
- Validation errors: empty `location`, empty `name`, empty `database_name`, a column name repeated between `columns` and `partition_keys`, `table_type` other than `EXTERNAL_TABLE`.
- `from_spec` copies `spec.table_properties` into `parameters` verbatim (so `sqe.secret` and `classification` both survive), puts `spec.serde_properties` into `serde_info.parameters`, and sets `input_format` / `output_format` from the spec, deriving them from `stored_as` when only that was given (`TEXTFILE` gives `org.apache.hadoop.mapred.TextInputFormat`, `PARQUET` gives `org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat`).
- `secret()` reads `parameters["sqe.secret"]`, falling back to `sqe.secret`, so a manifest written by hand either way works.

- [ ] **Step 4: Run to verify passing**

```bash
cargo test -p sqe-catalog --features hive-external --test hive_manifest_test 2>&1 | tail -15
cargo clippy -p sqe-catalog --features hive-external --all-targets -- -D warnings
```

Expected: 7 passed. Clippy clean.

- [ ] **Step 5: Verify the slim build still compiles**

```bash
cargo check -p sqe-catalog --no-default-features --features rest 2>&1 | tail -5
```

Expected: success. The module is feature-gated, so a slim build must not see it.

- [ ] **Step 6: Commit**

```bash
git add crates/sqe-catalog/Cargo.toml crates/sqe-catalog/src/lib.rs crates/sqe-catalog/src/hive crates/sqe-catalog/tests/hive_manifest_test.rs
git commit -m "feat(catalog): Glue-shaped Hive table manifest with validation caps

Field names mirror Glue TableInput and StorageDescriptor so a later Glue or
HMS backend is a mapping. The manifest is untrusted input, so parse checks
the byte cap before deserializing, rejects an unknown higher version rather
than parsing best-effort, and ignores unknown fields inside a known version
so a newer writer stays readable."
```

---

### Task 4: SerDe and format mapping

**Files:**
- Create: `crates/sqe-catalog/src/hive/serde_map.rs`
- Test: `crates/sqe-catalog/tests/hive_serde_map_test.rs`

**Interfaces:**
- Consumes: `HiveManifest` from Task 3.
- Produces:
  ```rust
  pub enum HiveFileFormat { Csv, Json, Parquet }
  pub struct ResolvedFormat {
      pub kind: HiveFileFormat,
      pub delimiter: u8,
      pub quote: Option<u8>,
      pub escape: Option<u8>,
      pub has_header: bool,
      pub file_extension: String,
  }
  pub fn resolve_format(m: &HiveManifest) -> Result<ResolvedFormat, HiveError>;
  pub fn listing_options(r: &ResolvedFormat, partition_cols: Vec<(String, DataType)>) -> Result<ListingOptions, HiveError>;
  pub fn arrow_type(sql_type: &str) -> Result<DataType, HiveError>;
  ```
  `file_extension` is `""` for every format, so a listing picks up Hive's extensionless part files. Callers that want extension filtering set it from a table property, which cycle 1 does not expose.

- [ ] **Step 1: Write the failing tests**

Create `crates/sqe-catalog/tests/hive_serde_map_test.rs`:

```rust
#![cfg(feature = "hive-external")]

use datafusion::arrow::datatypes::DataType;
use sqe_catalog::hive::manifest::HiveManifest;
use sqe_catalog::hive::serde_map::{arrow_type, resolve_format, HiveFileFormat};
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
        ("org.apache.hive.hcatalog.data.JsonSerDe", HiveFileFormat::Json),
        ("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe", HiveFileFormat::Parquet),
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
    assert!(msg.contains("AvroSerDe"), "message should name the class: {msg}");
}

#[test]
fn orc_is_rejected_by_name_and_names_no_cycle_because_it_is_out_of_scope() {
    let m = manifest("CREATE EXTERNAL TABLE t (id INT) STORED AS ORC LOCATION 's3://b/p/'");
    let msg = format!("{}", resolve_format(&m).expect_err("must reject"));
    assert!(msg.to_uppercase().contains("ORC"), "message should name ORC: {msg}");
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
    assert_eq!(arrow_type("decimal(18,2)").expect("decimal"), DataType::Decimal128(18, 2));
    assert!(arrow_type("array<string>").is_err(), "nested types are cycle-2 work");
}
```

- [ ] **Step 2: Run to verify failure**

```bash
cargo test -p sqe-catalog --features hive-external --test hive_serde_map_test 2>&1 | tail -20
```

Expected: `unresolved module serde_map`.

- [ ] **Step 3: Implement**

Create `crates/sqe-catalog/src/hive/serde_map.rs`. Shape:

```rust
//! SerDe and `STORED AS` allow-list, and the `ListingOptions` it produces.
//!
//! No SerDe is implemented. Each allow-listed class maps onto one of the three
//! formats DataFusion already reads. Anything else is rejected by name, because
//! a silently ignored SerDe property changes query results and a wrong answer
//! is worse than a rejected DDL.

use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::json::JsonFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::ListingOptions;

use super::manifest::HiveManifest;
use super::HiveError;

const OPEN_CSV: &str = "org.apache.hadoop.hive.serde2.OpenCSVSerde";
const LAZY_SIMPLE: &str = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";
const OPENX_JSON: &str = "org.openx.data.jsonserde.JsonSerDe";
const HCATALOG_JSON: &str = "org.apache.hive.hcatalog.data.JsonSerDe";
const PARQUET_SERDE: &str = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe";

/// Hive's `LazySimpleSerDe` default field delimiter: ctrl-A, not comma.
/// Athena behaves the same way, so a table declared without a delimiter over
/// comma-separated data is a one-column table in both engines.
const HIVE_DEFAULT_DELIM: u8 = 0x01;

pub fn resolve_format(m: &HiveManifest) -> Result<ResolvedFormat, HiveError> {
    let class = m.storage_descriptor.serde_info.serialization_library.as_deref();
    let props = &m.storage_descriptor.serde_info.parameters;

    // The SerDe wins over STORED AS when both are present, as Hive does.
    let kind = match class {
        Some(OPEN_CSV) | Some(LAZY_SIMPLE) => HiveFileFormat::Csv,
        Some(OPENX_JSON) | Some(HCATALOG_JSON) => HiveFileFormat::Json,
        Some(PARQUET_SERDE) => HiveFileFormat::Parquet,
        Some(other) => {
            return Err(HiveError::Validation(format!(
                "SerDe `{other}` is not supported. Supported: {OPEN_CSV}, {LAZY_SIMPLE}, \
                 {OPENX_JSON}, {HCATALOG_JSON}, {PARQUET_SERDE}"
            )))
        }
        None => kind_from_input_format(m)?,
    };

    // Delimiter precedence: OpenCSVSerde's separatorChar, then Hive's
    // field.delim / serialization.format, then ctrl-A.
    let delimiter = single_byte(props, &["separatorChar", "field.delim", "serialization.format"])?
        .unwrap_or(HIVE_DEFAULT_DELIM);

    let has_header = match m.parameters.get("skip.header.line.count").map(String::as_str) {
        None | Some("0") => false,
        Some("1") => true,
        Some(other) => {
            return Err(HiveError::Validation(format!(
                "skip.header.line.count = {other} is not supported: DataFusion expresses the \
                 setting as a boolean, so only 0 or 1 can be honored. Strip the extra header \
                 lines from the data, or use 1."
            )))
        }
    };

    Ok(ResolvedFormat {
        kind,
        delimiter,
        quote: single_byte(props, &["quoteChar"])?,
        escape: single_byte(props, &["escapeChar", "escape.delim"])?,
        has_header,
        file_extension: String::new(),
    })
}
```

Also implement:

- `kind_from_input_format`: reads `storage_descriptor.input_format`, accepting only `org.apache.hadoop.mapred.TextInputFormat` (csv) and `org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat` (parquet). `parameters["classification"] = "json"` selects json for a `TextInputFormat` table, which is how Glue records a JSON table. Anything else, including an ORC or Avro input format, is rejected with the class or keyword named.
- `single_byte`: reads the first present key, requires exactly one byte, and errors naming the key and value otherwise.
- `arrow_type`: `tinyint`/`smallint`/`int`/`integer`/`bigint`, `float`/`real`, `double`, `boolean`, `string`/`varchar`/`char` (with or without a length, all becoming `Utf8`), `binary`, `date`, `timestamp`, `decimal(p,s)`. A type containing `'<'` or an unknown name is an error naming the type.
- `listing_options`: builds `CsvFormat` with delimiter, quote, escape, and `has_header`; `JsonFormat`; or `ParquetFormat`. Then:

```rust
    Ok(ListingOptions::new(format)
        .with_file_extension(&r.file_extension)
        .with_table_partition_cols(partition_cols)
        // Size-only statistics. Collecting real ones reads a Parquet footer
        // per file at plan time, which is the latency this avoids. Cycle 2's
        // ANALYZE persists real counts into the manifest instead.
        .with_collect_stat(false))
```

- [ ] **Step 4: Run to verify passing**

```bash
cargo test -p sqe-catalog --features hive-external --test hive_serde_map_test 2>&1 | tail -15
cargo clippy -p sqe-catalog --features hive-external --all-targets -- -D warnings
```

Expected: 9 passed. Clippy clean.

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-catalog/src/hive/serde_map.rs crates/sqe-catalog/tests/hive_serde_map_test.rs
git commit -m "feat(catalog): map the Hive SerDe allow-list onto DataFusion formats

Five SerDe classes map onto csv, json, and parquet; anything else is rejected
by class name. Two Hive defaults are restored against DataFusion's: the CSV
header is off unless skip.header.line.count says otherwise, and a missing
field delimiter is ctrl-A rather than comma. Statistics collection is off so
planning never reads a footer per file."
```

---

### Task 5: Manifest store with the location guard

This is the security task. The manifest is untrusted input, so its location is re-validated on every load, not only at create.

**Files:**
- Create: `crates/sqe-catalog/src/hive/store.rs`
- Test: `crates/sqe-catalog/tests/hive_store_test.rs`

**Interfaces:**
- Consumes: `HiveManifest`, `ManifestLimits`, `HiveError` from Task 3.
- Produces:
  ```rust
  pub struct HiveManifestStore {
      root: ListingTableUrl,
      store: Arc<dyn ObjectStore>,
      limits: ManifestLimits,
      tvf: TvfPolicy,
      caller: TvfCaller,
  }
  impl HiveManifestStore {
      pub fn new(root: ListingTableUrl, store: Arc<dyn ObjectStore>, limits: ManifestLimits, tvf: TvfPolicy, caller: TvfCaller) -> Self;
      pub fn manifest_path(&self, database: &str, table: &str) -> Path;
      pub async fn put(&self, m: &HiveManifest, if_not_exists: bool) -> Result<(), HiveError>;
      pub async fn get(&self, database: &str, table: &str) -> Result<HiveManifest, HiveError>;
      pub async fn delete(&self, database: &str, table: &str) -> Result<(), HiveError>;
      pub async fn list_databases(&self) -> Result<Vec<String>, HiveError>;
      pub async fn list_tables(&self, database: &str) -> Result<Vec<String>, HiveError>;
  }
  ```
  `manifest_path` yields `<root_path>/<database>/<table>/_sqe/table.json`. `get` validates `storage_descriptor.location` through `tvf.check_path(location, &caller, false)` and returns `HiveError::Forbidden` on failure. `put` validates the same before writing.

- [ ] **Step 1: Write the failing tests**

Create `crates/sqe-catalog/tests/hive_store_test.rs`:

```rust
#![cfg(feature = "hive-external")]

use std::sync::Arc;

use datafusion::datasource::listing::ListingTableUrl;
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use sqe_catalog::hive::manifest::{HiveManifest, ManifestLimits};
use sqe_catalog::hive::store::HiveManifestStore;
use sqe_core::config::{TvfCaller, TvfPolicy};

fn store_for(dir: &std::path::Path, tvf: TvfPolicy) -> HiveManifestStore {
    let url = ListingTableUrl::parse(format!("file://{}", dir.display())).expect("url");
    let os: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
    HiveManifestStore::new(url, os, ManifestLimits::default(), tvf, TvfCaller::LocalTrusted)
}

/// A policy that allows local paths, which is what the embedded CLI uses.
fn permissive() -> TvfPolicy {
    TvfPolicy { allow_local_paths: true, ..TvfPolicy::default() }
}

fn manifest(location: &str) -> HiveManifest {
    let ddl = format!(
        "CREATE EXTERNAL TABLE sales.orders (id BIGINT) PARTITIONED BY (dt string) \
         STORED AS PARQUET LOCATION '{location}'"
    );
    let stmts = sqlparser::parser::Parser::parse_sql(
        &sqlparser::dialect::GenericDialect {},
        &ddl,
    )
    .expect("parse");
    let spec = sqe_sql::hive_ddl::try_extract_hive_table(&stmts[0])
        .expect("extract")
        .expect("external");
    HiveManifest::from_spec(&spec, "sales", "jacob", "2026-08-19T00:00:00Z")
}

#[tokio::test]
async fn manifest_path_follows_the_documented_layout() {
    let dir = tempfile::tempdir().expect("tmp");
    let s = store_for(dir.path(), permissive());
    let p = s.manifest_path("sales", "orders").to_string();
    assert!(p.ends_with("sales/orders/_sqe/table.json"), "got {p}");
}

#[tokio::test]
async fn put_then_get_round_trips() {
    let dir = tempfile::tempdir().expect("tmp");
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).expect("mkdir");
    let s = store_for(dir.path(), permissive());
    let m = manifest(&format!("file://{}", data.display()));
    s.put(&m, false).await.expect("put");
    let back = s.get("sales", "orders").await.expect("get");
    assert_eq!(back.name, "orders");
}

#[tokio::test]
async fn get_on_a_missing_table_is_not_found() {
    let dir = tempfile::tempdir().expect("tmp");
    let s = store_for(dir.path(), permissive());
    let err = s.get("sales", "nope").await.expect_err("must fail");
    assert!(
        matches!(err, sqe_catalog::hive::HiveError::NotFound(_)),
        "a missing manifest is table-not-found, got {err:?}"
    );
}

#[tokio::test]
async fn a_planted_manifest_pointing_at_a_forbidden_location_is_refused_on_load() {
    let dir = tempfile::tempdir().expect("tmp");
    // Write the manifest with a permissive policy, simulating an attacker who
    // can write to the warehouse prefix directly.
    let planted = manifest("file:///var/run/secrets/kubernetes.io/serviceaccount/token");
    store_for(dir.path(), permissive()).put(&planted, false).await.expect("plant");

    // Read it back with the coordinator's fail-closed policy: local paths off.
    let guarded = store_for(dir.path(), TvfPolicy::default());
    let err = guarded.get("sales", "orders").await.expect_err("must refuse");
    assert!(
        matches!(err, sqe_catalog::hive::HiveError::Forbidden(_)),
        "the location guard must run on every load, not only at create; got {err:?}"
    );
}

#[tokio::test]
async fn put_refuses_a_forbidden_location_up_front() {
    let dir = tempfile::tempdir().expect("tmp");
    let guarded = store_for(dir.path(), TvfPolicy::default());
    let err = guarded
        .put(&manifest("file:///etc/shadow"), false)
        .await
        .expect_err("must refuse");
    assert!(matches!(err, sqe_catalog::hive::HiveError::Forbidden(_)), "got {err:?}");
}

#[tokio::test]
async fn if_not_exists_does_not_overwrite() {
    let dir = tempfile::tempdir().expect("tmp");
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).expect("mkdir");
    let s = store_for(dir.path(), permissive());
    let mut m = manifest(&format!("file://{}", data.display()));
    s.put(&m, false).await.expect("first put");
    m.owner = Some("someone-else".into());
    let err = s.put(&m, true).await.expect_err("must not overwrite");
    assert!(format!("{err}").contains("orders"), "message should name the table: {err}");
    assert_eq!(s.get("sales", "orders").await.expect("get").owner.as_deref(), Some("jacob"));
}

#[tokio::test]
async fn list_tables_finds_manifests_by_prefix() {
    let dir = tempfile::tempdir().expect("tmp");
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).expect("mkdir");
    let s = store_for(dir.path(), permissive());
    for name in ["orders", "customers"] {
        let mut m = manifest(&format!("file://{}", data.display()));
        m.name = name.to_string();
        s.put(&m, false).await.expect("put");
    }
    let mut tables = s.list_tables("sales").await.expect("list");
    tables.sort();
    assert_eq!(tables, vec!["customers".to_string(), "orders".to_string()]);
    assert_eq!(s.list_databases().await.expect("dbs"), vec!["sales".to_string()]);
}
```

Add `tempfile` to `crates/sqe-catalog/[dev-dependencies]` if it is not already there.

- [ ] **Step 2: Run to verify failure**

```bash
cargo test -p sqe-catalog --features hive-external --test hive_store_test 2>&1 | tail -20
```

Expected: `unresolved module store`.

- [ ] **Step 3: Implement**

Create `crates/sqe-catalog/src/hive/store.rs`. Notes that matter:

- `put` with `if_not_exists` uses `PutMode::Create`, and maps `object_store::Error::AlreadyExists` to `HiveError::Validation` naming the table. Where the backing store does not support conditional put it returns a generic error; catch it and fall back to a `head` check plus an unconditional put, logging at `warn` that the create is not atomic on this store. The doc says last-writer-wins there, so behave that way rather than failing.
- `list_databases` and `list_tables` use `list_with_delimiter` so the LIST is scoped to one prefix level instead of walking the whole root.
- `list_tables` returns a table name only when `<db>/<table>/_sqe/table.json` exists. Do not infer a table from a bare directory; a data directory under the root is not a table.
- Every `object_store` error becomes `HiveError::Store` with the object path in the message, because the operator's next step is to look at that object.
- `get` calls `HiveManifest::parse` with `self.limits`, then `self.tvf.check_path(&m.storage_descriptor.location, &self.caller, false)`, mapping a failure to `HiveError::Forbidden`. Do this after parse and before returning, and add a doc comment saying why it cannot be hoisted to create time.

- [ ] **Step 4: Run to verify passing**

```bash
cargo test -p sqe-catalog --features hive-external --test hive_store_test 2>&1 | tail -15
cargo clippy -p sqe-catalog --features hive-external --all-targets -- -D warnings
```

Expected: 7 passed. Clippy clean.

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-catalog/src/hive/store.rs crates/sqe-catalog/tests/hive_store_test.rs crates/sqe-catalog/Cargo.toml
git commit -m "feat(catalog): manifest store, with the location guard on every load

The manifest is untrusted input: anyone who can write to the warehouse prefix
can plant one. So storage_descriptor.location goes through the fail-closed
[storage.tvf] allowlist on load as well as on create, which is what CAT-01
added for the TVFs and what the DDL path would otherwise bypass. A planted
manifest pointing at a service-account token is refused with Forbidden."
```

---

### Task 6: HiveSchemaProvider and HiveCatalogProvider

**Files:**
- Create: `crates/sqe-catalog/src/hive/provider.rs`
- Create: `crates/sqe-catalog/src/hive/stats.rs`
- Test: `crates/sqe-catalog/tests/hive_provider_test.rs`

**Interfaces:**
- Consumes: `HiveManifestStore` (Task 5), `resolve_format` / `listing_options` / `arrow_type` (Task 4).
- Produces:
  ```rust
  pub struct HiveCatalogProvider {
      name: String,
      store: Arc<HiveManifestStore>,
      data_store_factory: Arc<dyn HiveDataStoreFactory>,
      runtime: Arc<RuntimeEnv>,
      manifest_cache: Arc<HiveMetadataCache>,
      table_names_cache: moka::sync::Cache<String, Arc<Vec<String>>>,
  }
  pub struct HiveSchemaProvider {
      database: String,
      store: Arc<HiveManifestStore>,
      data_store_factory: Arc<dyn HiveDataStoreFactory>,
      runtime: Arc<RuntimeEnv>,
      cache: Arc<HiveMetadataCache>,
      table_names_cache: moka::sync::Cache<String, Arc<Vec<String>>>,
  }
  impl HiveCatalogProvider {
      /// Built PER SESSION. `data_store_factory` carries that session's
      /// credentials and `runtime` is that session's `RuntimeEnv`; both are
      /// session-scoped, so the provider must be too.
      pub fn new(
          name: String,
          store: Arc<HiveManifestStore>,
          data_store_factory: Arc<dyn HiveDataStoreFactory>,
          runtime: Arc<RuntimeEnv>,
          manifest_cache: Arc<HiveMetadataCache>,
          cache_ttl: Duration,
      ) -> Self;
      pub fn manifest_cache(&self) -> Arc<HiveMetadataCache>;
  }
  pub trait HiveDataStoreFactory: Send + Sync + std::fmt::Debug {
      /// Build an object store for a table's data location. The implementation
      /// holds the session's credentials; `secret` is a name resolved against
      /// the SecretStore, never material carried in a manifest.
      fn store_for(&self, location: &str, secret: Option<&str>) -> Result<Arc<dyn ObjectStore>, HiveError>;
  }
  pub struct HiveMetadataCache { /* moka: "<db>.<table>" -> HiveManifest */ }
  impl HiveMetadataCache {
      pub fn get(&self, database: &str, table: &str) -> Option<HiveManifest>;
      pub fn insert(&self, database: &str, table: &str, m: HiveManifest);
  }
  ```

  **Two lifetimes, deliberately different.** The provider is per session because
  a Hive read must run as the calling user, and `SchemaProvider::table()` takes
  no session argument, so the credentials have to be baked into the provider
  instance. There is no other place to put them.

  `HiveMetadataCache` is the exception: one process-wide instance, injected into
  every per-session provider. It has to be shared, because the `TagSource` in
  Task 7 is constructed once at `main.rs:398` and is synchronous, so it can only
  read a cache that outlives any one session. Sharing is safe: a manifest's
  content does not depend on who read it, and reading one grants no data access.
  Tags in it are not secrets; the mask decision is still made per user.

  `runtime` is the session's `RuntimeEnv`. `ListingTable` resolves its object
  store from the session registry at scan time, so the store must be registered
  on the same runtime the query executes against. Registering on a fresh runtime
  would produce a scan that cannot find its bucket.

  `HiveCatalogProvider` implements `datafusion::catalog::CatalogProvider`, and
  `HiveSchemaProvider` implements `SchemaProvider`. `table()` inserts the
  manifest into `HiveMetadataCache` before returning, which is the ordering the
  synchronous `TagSource` depends on.

- [ ] **Step 1: Write the failing tests**

Create `crates/sqe-catalog/tests/hive_provider_test.rs`. Fixture builds a partitioned CSV tree on local disk, so no S3 is needed:

```rust
#![cfg(feature = "hive-external")]

use std::sync::Arc;

use datafusion::prelude::SessionContext;

mod hive_fixture;
use hive_fixture::{partitioned_csv_root, register_hive_root};

#[tokio::test]
async fn reads_a_partitioned_csv_table_and_exposes_the_partition_column() {
    let f = partitioned_csv_root().await;
    let ctx = SessionContext::new();
    register_hive_root(&ctx, &f).await;

    let df = ctx.sql("SELECT id, amount, dt FROM lake.sales.orders ORDER BY id").await.expect("plan");
    let batches = df.collect().await.expect("collect");
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 3, "two partitions, three rows");

    let text = datafusion::arrow::util::pretty::pretty_format_batches(&batches)
        .expect("format")
        .to_string();
    assert!(text.contains("2026-01-01"), "partition value must be projected: {text}");
}

#[tokio::test]
async fn a_partition_predicate_prunes_to_one_partition() {
    let f = partitioned_csv_root().await;
    let ctx = SessionContext::new();
    register_hive_root(&ctx, &f).await;

    let batches = ctx
        .sql("SELECT id FROM lake.sales.orders WHERE dt = '2026-01-02'")
        .await
        .expect("plan")
        .collect()
        .await
        .expect("collect");
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 1);

    let plan = ctx
        .sql("EXPLAIN SELECT id FROM lake.sales.orders WHERE dt = '2026-01-02'")
        .await
        .expect("plan")
        .collect()
        .await
        .expect("collect");
    let text = datafusion::arrow::util::pretty::pretty_format_batches(&plan).expect("fmt").to_string();
    assert!(
        !text.contains("dt=2026-01-01"),
        "the pruned partition must not appear in the scan: {text}"
    );
}

#[tokio::test]
async fn csv_binds_by_ordinal_so_a_renamed_column_keeps_its_position() {
    // The declared list is authoritative for CSV. Renaming the second declared
    // column must still read the second field, not search by name.
    let f = partitioned_csv_root().await;
    let ctx = SessionContext::new();
    register_hive_root(&ctx, &f).await;
    let batches = ctx
        .sql("SELECT amount FROM lake.sales.orders WHERE id = 1")
        .await
        .expect("plan")
        .collect()
        .await
        .expect("collect");
    let text = datafusion::arrow::util::pretty::pretty_format_batches(&batches).expect("fmt").to_string();
    assert!(text.contains("10.5"), "ordinal binding must read field 2: {text}");
}

#[tokio::test]
async fn show_tables_lists_the_manifest_backed_table() {
    let f = partitioned_csv_root().await;
    let ctx = SessionContext::new();
    register_hive_root(&ctx, &f).await;
    let batches = ctx
        .sql("SELECT table_name FROM information_schema.tables WHERE table_schema = 'sales'")
        .await
        .expect("plan")
        .collect()
        .await
        .expect("collect");
    let text = datafusion::arrow::util::pretty::pretty_format_batches(&batches).expect("fmt").to_string();
    assert!(text.contains("orders"), "information_schema must show the table: {text}");
}

#[tokio::test]
async fn loading_a_table_populates_the_metadata_cache_before_returning() {
    // The TagSource in the policy path is synchronous, so it can only answer
    // from cache. This asserts the ordering it depends on.
    let f = partitioned_csv_root().await;
    let ctx = SessionContext::new();
    let provider = register_hive_root(&ctx, &f).await;
    assert!(provider.manifest_cache().get("sales", "orders").is_none(), "cold");
    let _ = ctx.sql("SELECT 1 FROM lake.sales.orders LIMIT 1").await.expect("plan");
    assert!(
        provider.manifest_cache().get("sales", "orders").is_some(),
        "planning must have loaded the manifest into cache"
    );
}

#[tokio::test]
async fn hive_default_partition_reads_as_null() {
    let f = hive_fixture::root_with_default_partition().await;
    let ctx = SessionContext::new();
    register_hive_root(&ctx, &f).await;
    let batches = ctx
        .sql("SELECT count(*) AS n FROM lake.sales.orders WHERE dt IS NULL")
        .await
        .expect("plan")
        .collect()
        .await
        .expect("collect");
    let text = datafusion::arrow::util::pretty::pretty_format_batches(&batches).expect("fmt").to_string();
    assert!(text.contains('1'), "__HIVE_DEFAULT_PARTITION__ must read as NULL: {text}");
}
```

Create `crates/sqe-catalog/tests/hive_fixture/mod.rs`:

```rust
//! Shared fixture: a warehouse root on local disk with a partitioned CSV table.
//!
//! Kept in one place because four test files build the same tree, and a
//! divergent fixture is how a passing test starts asserting the wrong shape.

use std::sync::Arc;

use datafusion::prelude::SessionContext;
use sqe_catalog::hive::provider::HiveCatalogProvider;

pub struct Fixture {
    pub dir: tempfile::TempDir,
    pub root: String,
    pub data: String,
}

/// A store on the local filesystem with local paths allowed, which is the
/// embedded CLI's trust model.
fn local_store(root: &str) -> HiveManifestStore {
    HiveManifestStore::new(
        ListingTableUrl::parse(root).expect("root url"),
        Arc::new(object_store::local::LocalFileSystem::new()),
        ManifestLimits::default(),
        TvfPolicy { allow_local_paths: true, ..TvfPolicy::default() },
        TvfCaller::LocalTrusted,
    )
}

/// Write the manifest for a CSV table with a single `dt` partition key and a
/// header row.
async fn put_csv_manifest(root: &str, data: &str) {
    let ddl = format!(
        "CREATE EXTERNAL TABLE sales.orders (id BIGINT, amount DOUBLE)          PARTITIONED BY (dt string) STORED AS CSV LOCATION '{data}'          TBLPROPERTIES ('skip.header.line.count' = '1')"
    );
    let stmts = sqlparser::parser::Parser::parse_sql(
        &sqlparser::dialect::GenericDialect {},
        &ddl,
    )
    .expect("parse");
    let spec = sqe_sql::hive_ddl::try_extract_hive_table(&stmts[0])
        .expect("extract")
        .expect("external");
    let m = HiveManifest::from_spec(&spec, "sales", "test", "2026-08-19T00:00:00Z");
    local_store(root).put(&m, false).await.expect("put manifest");
}

fn write_partition(data: &std::path::Path, dt: &str, rows: &str) {
    let dir = data.join(format!("dt={dt}"));
    std::fs::create_dir_all(&dir).expect("mkdir partition");
    std::fs::write(dir.join("part-0.csv"), format!("id,amount\n{rows}")).expect("write csv");
}

/// `dt=2026-01-01` with two rows and `dt=2026-01-02` with one, plus the
/// manifest under the warehouse root.
pub async fn partitioned_csv_root() -> Fixture {
    let dir = tempfile::tempdir().expect("tmp");
    let data_path = dir.path().join("data");
    std::fs::create_dir_all(&data_path).expect("mkdir data");
    write_partition(&data_path, "2026-01-01", "1,10.5\n2,20.0\n");
    write_partition(&data_path, "2026-01-02", "3,30.5\n");

    let root = format!("file://{}/wh", dir.path().display());
    let data = format!("file://{}", data_path.display());
    put_csv_manifest(&root, &data).await;
    Fixture { dir, root, data }
}

/// Same shape plus a `__HIVE_DEFAULT_PARTITION__` directory, which Hive writes
/// for a NULL partition value.
pub async fn root_with_default_partition() -> Fixture {
    let f = partitioned_csv_root().await;
    let data_path = f.dir.path().join("data");
    write_partition(&data_path, "__HIVE_DEFAULT_PARTITION__", "4,40.5\n");
    f
}

/// Register the root as catalog `lake` and return the provider so a test can
/// inspect its manifest cache.
pub async fn register_hive_root(ctx: &SessionContext, f: &Fixture) -> Arc<HiveCatalogProvider> {
    #[derive(Debug)]
    struct LocalFactory;
    impl sqe_catalog::hive::provider::HiveDataStoreFactory for LocalFactory {
        fn store_for(
            &self,
            _location: &str,
            _secret: Option<&str>,
        ) -> Result<Arc<dyn object_store::ObjectStore>, sqe_catalog::hive::HiveError> {
            Ok(Arc::new(object_store::local::LocalFileSystem::new()))
        }
    }

    let provider = Arc::new(HiveCatalogProvider::new(
        "lake".to_string(),
        Arc::new(local_store(&f.root)),
        Arc::new(LocalFactory),
        std::time::Duration::from_secs(60),
    ));
    ctx.register_catalog("lake", provider.clone());
    provider
}
```

- [ ] **Step 2: Run to verify failure**

```bash
cargo test -p sqe-catalog --features hive-external --test hive_provider_test 2>&1 | tail -20
```

Expected: `unresolved module provider`.

- [ ] **Step 3: Implement the provider**

Create `crates/sqe-catalog/src/hive/provider.rs`. The pieces that need care:

```rust
#[async_trait]
impl SchemaProvider for HiveSchemaProvider {
    // DataFusion's table_names() is synchronous while the manifest store is
    // async, the same constraint `SqeSchemaProvider` documents at
    // `schema_provider.rs:184`. Use the identical bridge: yield the current
    // tokio worker with `block_in_place`, then block on the listing. Do not
    // introduce a second bridging style.
    fn table_names(&self) -> Vec<String> { /* cached list, block_in_place fallback */ }

    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        let manifest = match self.store.get(&self.database, name).await {
            Ok(m) => m,
            Err(HiveError::NotFound(_)) => return Ok(None),
            Err(e) => return Err(DataFusionError::External(Box::new(e))),
        };

        // Cache BEFORE building the table. The policy TagSource is
        // synchronous and can only answer from this cache, and plan rewriting
        // runs after the LogicalPlan is built, which is after this call. If
        // the insert moved below the build and the build failed, a later
        // retry could rewrite against an empty cache and deny.
        self.cache.insert(&self.database, name, manifest.clone());

        let resolved = resolve_format(&manifest).map_err(to_df)?;
        let partition_cols = manifest
            .partition_keys
            .iter()
            .map(|k| arrow_type(&k.r#type).map(|t| (k.name.clone(), t)))
            .collect::<Result<Vec<_>, _>>()
            .map_err(to_df)?;
        let options = listing_options(&resolved, partition_cols).map_err(to_df)?;

        // The declared schema is authoritative. Never infer: for CSV the
        // declared list binds by ordinal, and inferring would silently
        // reorder or retype columns. Partition keys are appended last, which
        // is Hive's column order.
        let schema = declared_schema(&manifest).map_err(to_df)?;

        let data_store = self
            .data_store_factory
            .store_for(&manifest.storage_descriptor.location, manifest.secret())
            .map_err(to_df)?;
        let url = ListingTableUrl::parse(&manifest.storage_descriptor.location)?;
        self.runtime.register_object_store(url.as_ref(), data_store);

        let config = ListingTableConfig::new(url)
            .with_listing_options(options)
            .with_schema(schema);
        Ok(Some(Arc::new(ListingTable::try_new(config)?)))
    }
}
```

`declared_schema` builds an Arrow schema from `storage_descriptor.columns` then `partition_keys`, all nullable except partition keys, which DataFusion marks non-null. Normalize `__HIVE_DEFAULT_PARTITION__` by passing `Some("__HIVE_DEFAULT_PARTITION__")` as the null value for partition parsing; if DataFusion 54 offers no hook, post-process in `partition.rs` by mapping that exact segment to a NULL partition value and add a comment saying which layer does it.

Create `crates/sqe-catalog/src/hive/stats.rs`: a `size_only_statistics(files: &[ObjectMeta], row_width_hint: usize) -> Statistics` returning `num_rows: Precision::Inexact(total_bytes / row_width_hint)` and `total_byte_size: Precision::Exact(total_bytes)`, with `row_width_hint` derived from the declared schema's fixed widths (16 bytes for a variable-width column). Doc comment: this exists because `JoinStrategyRule` and `star_schema_reorder` plan from statistics, and Unknown is what caused the SMJ regression in !724.

- [ ] **Step 4: Run to verify passing**

```bash
cargo test -p sqe-catalog --features hive-external --test hive_provider_test 2>&1 | tail -20
cargo clippy -p sqe-catalog --features hive-external --all-targets -- -D warnings
```

Expected: 6 passed. Clippy clean.

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-catalog/src/hive crates/sqe-catalog/tests/hive_provider_test.rs crates/sqe-catalog/tests/hive_fixture
git commit -m "feat(catalog): Hive catalog and schema providers over the manifest

table() loads the manifest, caches it, then builds a ListingTable with the
declared schema and hive partition columns. The declared schema is never
inferred: for CSV the declared list binds by ordinal, and inferring would
reorder or retype columns, which is the defect class !784 fixed for Iceberg
field ids. The manifest is cached before the table is built because the
policy TagSource is synchronous and can only answer from that cache."
```

---

### Task 7: Policy key and tag-source routing

Without this task every Hive query returns zero rows in any deployment with a tag source, and a grant written for an Iceberg table would govern a Hive table of the same name. Do not defer either half.

**Files:**
- Modify: `crates/sqe-policy/src/plan_rewriter.rs:725`
- Create: `crates/sqe-coordinator/src/hive_tag_source.rs`
- Modify: `crates/sqe-coordinator/src/policy_wiring.rs:89`
- Modify: `crates/sqe-coordinator/src/lib.rs`
- Test: `crates/sqe-policy/src/plan_rewriter.rs` (unit tests, existing module)
- Test: `crates/sqe-coordinator/tests/hive_tag_source_test.rs`

**Interfaces:**
- Consumes: `HiveMetadataCache` from Task 6.
- Produces:
  ```rust
  // sqe-policy
  /// LIVE set of attached Hive warehouse-root names. Interior mutability is
  /// required, not stylistic: `build_policy_enforcer` runs once at
  /// `main.rs:398`, process-wide, while `ATTACH ... TYPE hive_external` is a
  /// runtime statement. A snapshot taken at construction would go stale, the
  /// Iceberg branch would win, and `lake.hr.employees` would silently key as
  /// `("hr", "employees")`. That is the exact escalation the naming rule
  /// exists to prevent, so it must not be reachable through staleness.
  pub struct HiveCatalogs(std::sync::RwLock<std::collections::HashSet<String>>);
  impl HiveCatalogs {
      pub fn new() -> Self;
      pub fn insert(&self, root: &str);
      pub fn remove(&self, root: &str);
      pub fn contains_name(&self, catalog: Option<&str>) -> bool;
      pub fn is_empty(&self) -> bool;
  }
  impl PolicyPlanRewriter { pub fn with_hive_catalogs(self, c: Arc<HiveCatalogs>) -> Self; }

  // sqe-coordinator
  pub struct HiveTagSource { cache: Arc<HiveMetadataCache> }
  pub struct CatalogRoutedTagSource { hive_catalogs: Arc<HiveCatalogs>, hive: Arc<dyn TagSource>, fallback: Arc<dyn TagSource> }
  impl CatalogRoutedTagSource { pub fn new(hive_catalogs: Arc<HiveCatalogs>, hive: Arc<dyn TagSource>, fallback: Arc<dyn TagSource>) -> Self; }
  ```
  `resolve_policy_key` gains a third argument-free branch driven by the rewriter's `hive_catalogs` set, so its signature becomes `fn resolve_policy_key(table_ref: &TableReference, hive_catalogs: &HiveCatalogs) -> Option<(String, String)>`.

- [ ] **Step 1: Write the failing tests**

Append to the test module in `crates/sqe-policy/src/plan_rewriter.rs`:

```rust
    fn hive_only(name: &str) -> HiveCatalogs {
        HiveCatalogs(std::iter::once(name.to_string()).collect())
    }

    #[test]
    fn hive_table_key_carries_the_root_name() {
        let r = datafusion::common::TableReference::full("lake", "sales", "orders");
        assert_eq!(
            resolve_policy_key(&r, &hive_only("lake")),
            Some(("lake.sales".to_string(), "orders".to_string()))
        );
    }

    #[test]
    fn an_iceberg_key_is_unchanged_by_the_hive_branch() {
        // Regression guard: every Ranger policy already written against the
        // hive_database convention keys on the bare namespace. The Hive branch
        // must not touch that.
        let r = datafusion::common::TableReference::full("iceberg", "hr", "employees");
        assert_eq!(
            resolve_policy_key(&r, &hive_only("lake")),
            Some(("hr".to_string(), "employees".to_string()))
        );
        assert_eq!(
            resolve_policy_key(&r, &HiveCatalogs(Default::default())),
            Some(("hr".to_string(), "employees".to_string()))
        );
    }

    #[test]
    fn an_iceberg_grant_cannot_reach_a_hive_table_of_the_same_name() {
        let iceberg = datafusion::common::TableReference::full("iceberg", "hr", "employees");
        let hive = datafusion::common::TableReference::full("lake", "hr", "employees");
        let cats = hive_only("lake");
        assert_ne!(
            resolve_policy_key(&iceberg, &cats),
            resolve_policy_key(&hive, &cats),
            "same-named tables in different catalog kinds must not share a policy key"
        );
    }
```

Create `crates/sqe-coordinator/tests/hive_tag_source_test.rs`:

```rust
use std::collections::HashMap;
use std::sync::Arc;

use sqe_policy::tag_source::TagSource;

/// A stand-in for the Iceberg-backed source: unknown table means unknown tag
/// state, which the rewriter turns into a deny.
#[derive(Debug)]
struct AlwaysUnknown;
impl TagSource for AlwaysUnknown {
    fn column_tags(&self, _c: Option<&str>, _n: &[String], _t: &str) -> Option<HashMap<String, Vec<String>>> {
        None
    }
}

#[test]
fn a_hive_table_is_answered_from_the_manifest_cache_not_denied() {
    let (cache, hive_catalogs) = fixture_with_cached_manifest("lake", "sales", "orders");
    let routed = sqe_coordinator::hive_tag_source::CatalogRoutedTagSource::new(
        hive_catalogs,
        Arc::new(sqe_coordinator::hive_tag_source::HiveTagSource::new(cache)),
        Arc::new(AlwaysUnknown),
    );
    let got = routed.column_tags(Some("lake"), &["sales".to_string()], "orders");
    assert_eq!(
        got,
        Some(HashMap::new()),
        "an untagged Hive table is known-no-tags; None here would deny every Hive query"
    );
}

#[test]
fn a_hive_table_missing_from_the_cache_still_fails_closed() {
    let (cache, hive_catalogs) = fixture_with_cached_manifest("lake", "sales", "orders");
    let routed = sqe_coordinator::hive_tag_source::CatalogRoutedTagSource::new(
        hive_catalogs,
        Arc::new(sqe_coordinator::hive_tag_source::HiveTagSource::new(cache)),
        Arc::new(AlwaysUnknown),
    );
    assert_eq!(
        routed.column_tags(Some("lake"), &["sales".to_string()], "not_loaded"),
        None,
        "a cache miss is unknown tag state, same as the Iceberg source"
    );
}

#[test]
fn an_iceberg_table_still_reaches_the_iceberg_source() {
    let (cache, hive_catalogs) = fixture_with_cached_manifest("lake", "sales", "orders");
    let routed = sqe_coordinator::hive_tag_source::CatalogRoutedTagSource::new(
        hive_catalogs,
        Arc::new(sqe_coordinator::hive_tag_source::HiveTagSource::new(cache)),
        Arc::new(AlwaysUnknown),
    );
    assert_eq!(routed.column_tags(Some("iceberg"), &["hr".to_string()], "employees"), None);
}

#[test]
fn an_unknown_catalog_routes_to_the_fail_closed_source_not_to_empty() {
    // A permissive default in a dispatcher is a silent mask bypass.
    let (cache, hive_catalogs) = fixture_with_cached_manifest("lake", "sales", "orders");
    let routed = sqe_coordinator::hive_tag_source::CatalogRoutedTagSource::new(
        hive_catalogs,
        Arc::new(sqe_coordinator::hive_tag_source::HiveTagSource::new(cache)),
        Arc::new(AlwaysUnknown),
    );
    assert_eq!(routed.column_tags(None, &["sales".to_string()], "orders"), None);
    assert_eq!(routed.column_tags(Some("who_is_this"), &["sales".to_string()], "orders"), None);
}

#[test]
fn a_root_attached_after_the_rewriter_was_built_still_gets_the_hive_key() {
    // The regression guard for the staleness path. The enforcer is built once
    // process-wide; ATTACH happens later. If the set were snapshotted, this
    // key would come back as ("sales", "orders") and the table would inherit
    // any grant written for an Iceberg namespace named `sales`.
    let cats = Arc::new(sqe_policy::plan_rewriter::HiveCatalogs::default());
    let r = datafusion::common::TableReference::full("lake", "sales", "orders");
    assert_eq!(
        sqe_policy::plan_rewriter::resolve_policy_key_for_test(&r, &cats),
        Some(("sales".to_string(), "orders".to_string())),
        "before ATTACH the root is unknown, so the Iceberg branch is correct"
    );
    cats.insert("lake");
    assert_eq!(
        sqe_policy::plan_rewriter::resolve_policy_key_for_test(&r, &cats),
        Some(("lake.sales".to_string(), "orders".to_string())),
        "after ATTACH the same reference must key under the root"
    );
}

#[test]
fn a_root_attached_after_construction_reaches_the_hive_tag_source() {
    let (cache, cats) = fixture_with_cached_manifest_unattached("lake", "sales", "orders");
    let routed = sqe_coordinator::hive_tag_source::CatalogRoutedTagSource::new(
        cats.clone(),
        Arc::new(sqe_coordinator::hive_tag_source::HiveTagSource::new(cache)),
        Arc::new(AlwaysUnknown),
    );
    assert_eq!(
        routed.column_tags(Some("lake"), &["sales".to_string()], "orders"),
        None,
        "not attached yet, so the fail-closed source answers"
    );
    cats.insert("lake");
    assert_eq!(
        routed.column_tags(Some("lake"), &["sales".to_string()], "orders"),
        Some(HashMap::new()),
        "a router that snapshotted the set at construction would still deny here"
    );
}

#[test]
fn tags_recorded_in_the_manifest_are_returned() {
    let (cache, hive_catalogs) = fixture_with_tagged_manifest("lake", "sales", "orders", "ssn", "PII");
    let routed = sqe_coordinator::hive_tag_source::CatalogRoutedTagSource::new(
        hive_catalogs,
        Arc::new(sqe_coordinator::hive_tag_source::HiveTagSource::new(cache)),
        Arc::new(AlwaysUnknown),
    );
    let got = routed.column_tags(Some("lake"), &["sales".to_string()], "orders").expect("known");
    assert_eq!(got.get("ssn").map(Vec::as_slice), Some(&["PII".to_string()][..]));
}
```

Write the three `fixture_*` helpers inline in the test file: build a `HiveManifest` from a DDL string, set `sqe.column_tags` for the tagged case, insert it into a fresh `HiveMetadataCache`, and return that plus an `Arc<HiveCatalogs>`. `fixture_with_cached_manifest` returns a set that already contains the root; `fixture_with_cached_manifest_unattached` returns an empty one so the test can attach mid-flight.

`resolve_policy_key` is private, so expose `resolve_policy_key_for_test` from `sqe-policy` under `#[doc(hidden)]` rather than making the real function public. The key rule is internal; only the test needs the door.

- [ ] **Step 2: Run to verify failure**

```bash
cargo test -p sqe-policy resolve_policy_key 2>&1 | tail -20
cargo test -p sqe-coordinator --test hive_tag_source_test 2>&1 | tail -20
```

Expected: `resolve_policy_key` takes 1 argument, not 2. `unresolved module hive_tag_source`.

- [ ] **Step 3: Implement**

In `crates/sqe-policy/src/plan_rewriter.rs`:

```rust
/// Live set of catalog names that hold Hive external tables rather than
/// Iceberg tables. Empty in every Iceberg-only deployment, which is why the
/// Hive branch below cannot change an existing key.
///
/// Read through a lock on every key resolution rather than snapshotted at
/// construction. The rewriter is built once, process-wide, at `main.rs:398`;
/// `ATTACH ... TYPE hive_external` happens later, per session. A stale set
/// sends a Hive table down the Iceberg branch, where it would inherit any
/// grant written for a same-named Iceberg namespace.
#[derive(Debug, Default)]
pub struct HiveCatalogs(std::sync::RwLock<std::collections::HashSet<String>>);

impl HiveCatalogs {
    pub fn insert(&self, root: &str) {
        // A poisoned lock here would mean a panic while holding it, and
        // failing closed is not an option because the caller has no error
        // path. Clear the poison and continue: the set is a name registry,
        // not state that can be left half-written.
        self.0.write().unwrap_or_else(|e| e.into_inner()).insert(root.to_string());
    }

    pub fn remove(&self, root: &str) {
        self.0.write().unwrap_or_else(|e| e.into_inner()).remove(root);
    }

    pub fn contains_name(&self, catalog: Option<&str>) -> bool {
        let Some(c) = catalog else { return false };
        self.0.read().unwrap_or_else(|e| e.into_inner()).contains(c)
    }

    pub fn is_empty(&self) -> bool {
        self.0.read().unwrap_or_else(|e| e.into_inner()).is_empty()
    }
}

fn resolve_policy_key(
    table_ref: &datafusion::common::TableReference,
    hive_catalogs: &HiveCatalogs,
) -> Option<(String, String)> {
    let table = table_ref.table();
    if table.is_empty() {
        return None;
    }

    let namespace = match table_ref.schema() {
        Some(schema) if !schema.is_empty() => schema.to_string(),
        _ => "default".to_string(),
    };

    // A Hive external table's key carries the warehouse-root name, so a grant
    // written for the Iceberg namespace `sales` cannot reach `lake.sales`.
    // Whoever can write a manifest picks the Hive table's name and its
    // LOCATION, so a shared key would be privilege escalation.
    //
    // The asymmetry is deliberate: adding the catalog to the Iceberg key would
    // invalidate every Ranger policy already written against the
    // `hive_database` convention. Making both symmetric is its own change.
    if hive_catalogs.contains_name(table_ref.catalog()) {
        let root = table_ref.catalog().unwrap_or_default();
        return Some((format!("{root}.{namespace}"), table.to_string()));
    }

    Some((namespace, table.to_string()))
}
```

Thread `hive_catalogs: Arc<HiveCatalogs>` through `PolicyPlanRewriter` with a `with_hive_catalogs` builder defaulting to empty, and update the two call sites (`plan_rewriter.rs:154` and the diagnostic path that documents the same key).

Create `crates/sqe-coordinator/src/hive_tag_source.rs`:

```rust
//! Tag sources for Hive external tables, and the dispatcher in front of them.
//!
//! Without this module every Hive query is denied. `plan_rewriter.rs` calls
//! `TagSource::column_tags` for every non-reserved TableScan and turns `None`
//! into a `lit(false)` row filter, and `CacheTagSource` returns `None` for
//! anything absent from the Iceberg metadata cache, which a Hive table always
//! is.
//!
//! Spec: `docs/superpowers/specs/2026-08-19-hive-external-tables-design.md`

/// Reads `sqe.column_tags` from a cached manifest.
///
/// `column_tags` is synchronous, so this can only answer from cache. The
/// ordering it relies on: plan rewriting runs after LogicalPlan construction,
/// which has already called `HiveSchemaProvider::table()` and cached the
/// manifest. A cache miss is reported as unknown, never as no-tags.
impl TagSource for HiveTagSource {
    fn column_tags(&self, _c: Option<&str>, namespace: &[String], table: &str)
        -> Option<HashMap<String, Vec<String>>>
    {
        let database = namespace.last()?;
        let manifest = self.cache.get(database, table)?;
        Some(manifest.sqe.column_tags.clone().into_iter().collect())
    }
}

/// Routes by catalog name. An unmatched or unknown catalog goes to the
/// fallback, which is the fail-closed Iceberg source. It never returns
/// `Some(empty)` of its own accord: a permissive default in a dispatcher is a
/// silent mask bypass, and this is where a future catalog kind would introduce
/// one.
impl TagSource for CatalogRoutedTagSource {
    fn column_tags(&self, catalog: Option<&str>, namespace: &[String], table: &str)
        -> Option<HashMap<String, Vec<String>>>
    {
        if self.hive_catalogs.contains_name(catalog) {
            self.hive.column_tags(catalog, namespace, table)
        } else {
            self.fallback.column_tags(catalog, namespace, table)
        }
    }
}
```

In `crates/sqe-coordinator/src/policy_wiring.rs:89`, wrap the existing source instead of replacing it:

```rust
            if let Some(cache) = table_cache {
                let iceberg: Arc<dyn TagSource> =
                    Arc::new(crate::tag_source_impl::CacheTagSource::new(Arc::new(cache)));
                // Wrap unconditionally, and let the router read the live set.
                // Deciding here on `is_empty()` would bake in whatever was
                // attached at process start, and a later ATTACH would never
                // reach the Hive source. When the set is empty the router
                // delegates every call to `iceberg`, so an Iceberg-only
                // deployment behaves identically.
                let tag_src: Arc<dyn TagSource> = Arc::new(CatalogRoutedTagSource::new(
                    hive_catalogs.clone(),
                    Arc::new(HiveTagSource::new(hive_manifest_cache.clone())),
                    iceberg,
                ));
                rewriter = rewriter
                    .with_tag_source(tag_src)
                    .with_hive_catalogs(hive_catalogs.clone());
            }
```

`build_policy_enforcer` gains two parameters: `hive_catalogs: Arc<HiveCatalogs>` and `hive_manifest_cache: Arc<HiveMetadataCache>`. Both are created in `main.rs` before the call at line 398 and are the same instances the ATTACH handler and every per-session provider receive. Construct them once and pass the same `Arc` everywhere; two instances would look correct and route nothing.

- [ ] **Step 4: Run to verify passing**

```bash
cargo test -p sqe-policy 2>&1 | tail -15
cargo test -p sqe-coordinator --test hive_tag_source_test 2>&1 | tail -15
cargo clippy -p sqe-policy -p sqe-coordinator --all-targets --all-features -- -D warnings
```

Expected: all `sqe-policy` tests pass, including the pre-existing `resolve_policy_key` cases. 5 new coordinator tests pass. Clippy clean.

Note: `drop_secret_in_use_by_attached_catalog_errors` fails on a plain `cargo test -p sqe-coordinator` because it needs the `sql-sqlite` feature. That failure is pre-existing and unrelated. Do not try to fix it.

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-policy/src/plan_rewriter.rs crates/sqe-coordinator/src/hive_tag_source.rs crates/sqe-coordinator/src/policy_wiring.rs crates/sqe-coordinator/src/main.rs crates/sqe-coordinator/src/lib.rs crates/sqe-coordinator/tests/hive_tag_source_test.rs
git commit -m "feat(policy): give Hive tables their own policy key and tag route

resolve_policy_key dropped the catalog, so a grant on Iceberg hr.employees
would have governed a Hive lake.hr.employees, and whoever can write a
manifest picks that name and its LOCATION. A Hive key now carries the root
name; the Iceberg key is unchanged and pinned by a test, because making both
symmetric would invalidate existing Ranger policies.

CatalogRoutedTagSource stops the second failure mode: the Iceberg-backed
source returns None for any Hive table, and the rewriter turns None into
lit(false), so every Hive query would have returned zero rows. An unmatched
catalog routes to the fail-closed source, never to known-no-tags."
```

---

### Task 8: ATTACH and the embedded root flag

**Files:**
- Modify: `crates/sqe-catalog/src/mount.rs`
- Modify: `crates/sqe-core/src/config.rs`
- Modify: `crates/sqe-cli/src/main.rs`
- Modify: `crates/sqe-cli/src/embedded.rs`
- Test: `crates/sqe-catalog/tests/hive_mount_test.rs`

**Interfaces:**
- Consumes: `HiveCatalogProvider` (Task 6), `CatalogKind::HiveExternal` (Task 2).
- Produces:
  ```rust
  // sqe-catalog::mount
  /// Validate a root and build the shared, session-independent half.
  /// Returns the manifest store, which every session's provider borrows.
  pub async fn build_hive_root(
      location: &str,
      options: &BTreeMap<String, OptionValue>,
      storage: &StorageConfig,
      caller: TvfCaller,
  ) -> Result<Arc<HiveManifestStore>, String>;

  /// Build the per-session provider over a shared root. `credentials` and
  /// `runtime` are this session's, which is what makes a Hive read run as the
  /// calling user. `manifest_cache` is the process-wide instance from Task 7.
  pub fn session_hive_catalog(
      name: &str,
      root: Arc<HiveManifestStore>,
      credentials: SessionStorageCredentials,
      secrets: Arc<dyn SecretStore>,
      runtime: Arc<RuntimeEnv>,
      manifest_cache: Arc<HiveMetadataCache>,
      cache_ttl: Duration,
  ) -> Arc<HiveCatalogProvider>;

  // sqe-core::config
  pub struct HiveRootConfig { pub name: String, pub location: String, pub secret: Option<String> }
  pub struct HiveConfig { pub roots: Vec<HiveRootConfig>, pub manifest_max_bytes: usize, pub manifest_max_columns: usize, pub manifest_max_partition_keys: usize }
  ```
  These are siblings of `build_catalog`, not variants of it: `build_catalog` returns `Arc<dyn iceberg::Catalog>`, which a Hive root is not.

  The split is the credential model. A root is validated and its manifest store
  built once, because the manifest is the same object for everyone. The provider
  is built per session, because `SchemaProvider::table()` takes no session and
  the calling user's credentials therefore have to live in the provider
  instance. Building the provider once at ATTACH time would make every user read
  as whoever attached, which is the service-account model this engine rejects.

  `SessionStorageCredentials` is the session's existing vended-credential
  handle; reuse whatever `session_context.rs` already threads to the Iceberg
  scan path rather than introducing a parallel type.

- [ ] **Step 1: Write the failing tests**

Create `crates/sqe-catalog/tests/hive_mount_test.rs`:

```rust
#![cfg(feature = "hive-external")]

use std::collections::BTreeMap;

use sqe_core::config::StorageConfig;
use sqe_core::InMemorySecretStore;

#[tokio::test]
async fn attach_rejects_a_root_location_that_fails_the_allowlist() {
    // Default storage config denies local paths, matching the coordinator.
    let err = sqe_catalog::mount::build_hive_root(
        "file:///etc",
        &BTreeMap::new(),
        &StorageConfig::default(),
        sqe_core::config::TvfCaller::Remote,
    )
    .await
    .expect_err("must refuse");
    assert!(err.to_lowercase().contains("etc") || err.to_lowercase().contains("local"), "got {err}");
}

#[tokio::test]
async fn attach_accepts_an_s3_root_without_touching_the_network() {
    let storage = StorageConfig {
        tvf: sqe_core::config::TvfPolicy {
            allowed_object_store_prefixes: vec!["s3://bucket/".to_string()],
            ..Default::default()
        },
        ..StorageConfig::default()
    };
    // ATTACH must not list or HEAD: a root is registered lazily, so attaching
    // an empty or not-yet-created prefix has to succeed.
    sqe_catalog::mount::build_hive_root(
        "s3://bucket/wh/",
        &BTreeMap::new(),
        &storage,
        sqe_core::config::TvfCaller::Remote,
    )
    .await
    .expect("attach should not require the prefix to exist");
}

#[test]
fn parse_and_classify_accepts_the_hive_external_attach_form() {
    let kind = sqe_sql::parse_and_classify(
        "ATTACH 's3://bucket/wh/' AS lake (TYPE hive_external)",
    )
    .expect("classify");
    match kind {
        sqe_sql::StatementKind::Attach(a) => {
            assert_eq!(a.name, "lake");
            assert_eq!(a.kind, sqe_sql::CatalogKind::HiveExternal);
            assert_eq!(a.location, "s3://bucket/wh/");
        }
        other => panic!("expected Attach, got {other:?}"),
    }
}
```

- [ ] **Step 2: Run to verify failure**

```bash
cargo test -p sqe-catalog --features hive-external --test hive_mount_test 2>&1 | tail -20
```

Expected: `build_hive_root` not found.

- [ ] **Step 3: Implement**

- `build_hive_root` in `mount.rs`: validate `location` through `storage.tvf.check_path`, build the `HiveManifestStore` with limits from config, and return it. Never LIST or HEAD at attach time, so attaching a prefix that does not exist yet works. Also insert the root name into the shared `HiveCatalogs` from Task 7, and remove it on `DETACH`; a root registered for reads but missing from that set is the staleness bug by another route.
- `session_hive_catalog` in `mount.rs`: build a `HiveDataStoreFactory` closing over the session credentials and the `SecretStore`, then construct the provider with the session's `RuntimeEnv` and the process-wide manifest cache. The factory resolves `manifest.secret()` against the `SecretStore` when present and falls back to session credentials when absent.
- In `sqe-core/src/config.rs`, add `HiveConfig` with the three caps defaulting to `1048576`, `4096`, and `32`, plus `roots`. Wire it under `[hive]` in the TOML, with `[[hive.roots]]` entries.
- In `sqe-cli/src/main.rs`, add `--hive-root NAME=URL` as a repeatable arg, documented like `--catalog`, and note in the help text that it can be combined with any catalog mode because a Hive root is not an Iceberg catalog.
- In `embedded.rs`, register each root before the REPL starts.

- [ ] **Step 4: Run to verify passing**

```bash
cargo test -p sqe-catalog --features hive-external --test hive_mount_test 2>&1 | tail -15
cargo build -p sqe-cli --profile dev-release 2>&1 | tail -5
cargo clippy -p sqe-catalog -p sqe-core -p sqe-cli --all-targets --all-features -- -D warnings
```

Expected: 3 passed, `sqe-cli` builds, clippy clean.

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-catalog/src/mount.rs crates/sqe-core/src/config.rs crates/sqe-cli/src crates/sqe-catalog/tests/hive_mount_test.rs
git commit -m "feat: attach a Hive warehouse root by SQL, config, or CLI flag

build_hive_root and session_hive_catalog are siblings of build_catalog rather
than variants: build_catalog returns an iceberg::Catalog, and a Hive root is a
DataFusion CatalogProvider. The split is the credential model. The root and
its manifest store are shared, because the manifest is the same object for
everyone; the provider is per session, because SchemaProvider::table() takes no
session and the calling user's credentials therefore have to live in the
provider instance. ATTACH validates the root through the allowlist but never
lists or HEADs, so attaching a prefix that does not exist yet works."
```

---

### Task 9: Embedded-mode DDL

**Files:**
- Modify: `crates/sqe-cli/src/embedded.rs:478`
- Test: `crates/sqe-cli/src/embedded.rs` (test module at the bottom, following the existing `embedded_client_*` pattern)

**Interfaces:**
- Consumes: `StatementKind::CreateExternalTable` (Task 2), `HiveManifestStore` (Task 5), registered roots (Task 8).
- Produces: `EmbeddedClient::embedded_handle_create_external_table(&mut self, spec: &HiveTableSpec) -> Result<QueryResult, ...>` and `embedded_handle_drop_hive_table`.

- [ ] **Step 1: Write the failing tests**

Append to the test module in `crates/sqe-cli/src/embedded.rs`:

```rust
    /// Build an embedded client with a Hive root on a temp dir holding a
    /// partitioned CSV tree. Returns the client and the data path.
    async fn client_with_hive_root() -> (EmbeddedClient, tempfile::TempDir) { /* ... */ }

    #[tokio::test]
    async fn create_external_table_then_select_round_trips_in_embedded_mode() {
        let (mut c, dir) = client_with_hive_root().await;
        let data = dir.path().join("data");
        let sql = format!(
            "CREATE EXTERNAL TABLE lake.sales.orders (id BIGINT, amount DOUBLE) \
             PARTITIONED BY (dt string) STORED AS CSV LOCATION 'file://{}' \
             TBLPROPERTIES ('skip.header.line.count' = '1')",
            data.display()
        );
        c.execute(&sql).await.expect("create");
        let r = c.execute("SELECT count(*) AS n FROM lake.sales.orders").await.expect("select");
        assert!(format!("{r:?}").contains('3'), "expected 3 rows, got {r:?}");
    }

    #[tokio::test]
    async fn the_definition_survives_a_new_client_because_it_is_in_the_root() {
        let (mut c, dir) = client_with_hive_root().await;
        let data = dir.path().join("data");
        c.execute(&format!(
            "CREATE EXTERNAL TABLE lake.sales.orders (id BIGINT, amount DOUBLE) \
             PARTITIONED BY (dt string) STORED AS CSV LOCATION 'file://{}' \
             TBLPROPERTIES ('skip.header.line.count' = '1')",
            data.display()
        ))
        .await
        .expect("create");

        // A fresh client over the same root, which is the whole point of the
        // sidecar manifest: no shared process state, no metastore.
        let mut c2 = client_over_root(dir.path()).await;
        let r = c2.execute("SELECT count(*) AS n FROM lake.sales.orders").await.expect("select");
        assert!(format!("{r:?}").contains('3'), "definition did not survive: {r:?}");
    }

    #[tokio::test]
    async fn drop_removes_the_manifest_and_leaves_the_data() {
        let (mut c, dir) = client_with_hive_root().await;
        let data = dir.path().join("data");
        c.execute(&format!(
            "CREATE EXTERNAL TABLE lake.sales.orders (id BIGINT) STORED AS CSV LOCATION 'file://{}'",
            data.display()
        ))
        .await
        .expect("create");
        c.execute("DROP TABLE lake.sales.orders").await.expect("drop");
        assert!(c.execute("SELECT 1 FROM lake.sales.orders").await.is_err(), "table should be gone");
        assert!(
            data.join("dt=2026-01-01/a.csv").exists(),
            "DROP TABLE must never delete data"
        );
    }

    #[tokio::test]
    async fn show_create_table_renders_athena_compatible_ddl() {
        let (mut c, dir) = client_with_hive_root().await;
        let data = dir.path().join("data");
        c.execute(&format!(
            "CREATE EXTERNAL TABLE lake.sales.orders (id BIGINT, amount DOUBLE) \
             PARTITIONED BY (dt string) STORED AS CSV LOCATION 'file://{}'",
            data.display()
        ))
        .await
        .expect("create");
        let r = c.execute("SHOW CREATE TABLE lake.sales.orders").await.expect("show");
        let text = format!("{r:?}");
        for needle in ["CREATE EXTERNAL TABLE", "PARTITIONED BY", "LOCATION"] {
            assert!(text.contains(needle), "missing {needle} in: {text}");
        }
    }

    #[tokio::test]
    async fn create_external_table_as_select_fails_at_the_parser_not_at_our_handler() {
        // Corrected from the original plan text, which assumed this shape
        // reaches the Hive DDL handler. It does not, and the guarantee is
        // structural rather than dialect-specific: sqlparser dispatches
        // CREATE EXTERNAL TABLE to its own `parse_create_external_table`
        // (parser/mod.rs:6394), which never calls `parse_query` and has no
        // `AS` path at all, so `ct.query` is unconditionally None whenever
        // `ct.external` is true, in every dialect. No handler can ever see it. The test pins where the boundary actually is
        // instead of asserting a message no code produces. The
        // EXTERNAL-before-CTAS ordering in the classifier stays as defensive
        // code for a future sqlparser that does accept the shape.
        let (mut c, dir) = client_with_hive_root().await;
        let err = c
            .execute(&format!(
                "CREATE EXTERNAL TABLE lake.sales.t STORED AS CSV LOCATION 'file://{}' AS SELECT 1",
                dir.path().display()
            ))
            .await
            .expect_err("must reject");
        let msg = format!("{err}");
        assert!(
            msg.contains("AS") || msg.to_lowercase().contains("parse"),
            "expected a parser-level rejection, got: {msg}"
        );
    }

    #[tokio::test]
    async fn creating_a_table_in_a_root_that_is_not_attached_names_the_missing_root() {
        let (mut c, _dir) = client_with_hive_root().await;
        let err = c
            .execute("CREATE EXTERNAL TABLE nosuch.sales.t (id INT) STORED AS CSV LOCATION 'file:///tmp/'")
            .await
            .expect_err("must reject");
        assert!(format!("{err}").contains("nosuch"), "should name the root: {err}");
    }
```

Write `client_with_hive_root` and `client_over_root` with real code: build the CSV tree exactly as the `sqe-catalog` fixture does, then construct the client with the root registered.

- [ ] **Step 2: Run to verify failure**

```bash
cargo test -p sqe-cli create_external_table 2>&1 | tail -20
```

Expected: the DDL reaches `ctx.sql()` and fails, or the handler does not exist.

- [ ] **Step 3: Implement**

In `embedded.rs`, add arms to the existing interception block at line 478:

```rust
                sqe_sql::StatementKind::CreateExternalTable(spec) => {
                    return self.embedded_handle_create_external_table(&spec).await;
                }
```

`embedded_handle_create_external_table` resolves the root from `spec.catalog` (erroring with the root named when it is not attached), defaults `database` to the session schema when absent, builds the manifest with `owner` from the local user and `created_at` from `chrono::Utc::now()`, and calls `store.put(&m, spec.if_not_exists)`. Then it invalidates the root's `table_names` cache so `SHOW TABLES` sees the new table immediately.

`DROP TABLE` needs a branch before the existing drop path: when the resolved catalog is a Hive root, call `store.delete`. Reject `PURGE` naming it.

`SHOW CREATE TABLE` renders from the manifest: column list, `PARTITIONED BY`, `ROW FORMAT SERDE` with `WITH SERDEPROPERTIES` when a class is recorded, `STORED AS`, `LOCATION`, and `TBLPROPERTIES`. Round-trip requirement: the rendered DDL must re-parse through `try_extract_hive_table` and produce an equal `HiveTableSpec`. Add that as an assertion in the `show_create_table` test.

`CREATE EXTERNAL TABLE ... AS SELECT` reaches the handler because the classifier checks `external` before `query`. Reject it naming cycle 4.

- [ ] **Step 4: Run to verify passing**

```bash
cargo test -p sqe-cli 2>&1 | tail -15
cargo clippy -p sqe-cli --all-targets --all-features -- -D warnings
```

Expected: 6 new tests pass, existing `sqe-cli` tests unaffected. Clippy clean.

- [ ] **Step 5: Manual check in the real CLI**

```bash
cargo build -p sqe-cli --profile dev-release
mkdir -p /tmp/hive-probe/data/dt=2026-01-01
printf 'id,amount\n1,10.5\n2,20.0\n' > /tmp/hive-probe/data/dt=2026-01-01/a.csv
target/dev-release/sqe-cli --embedded --memory --hive-root lake=file:///tmp/hive-probe/wh -e \
  "CREATE EXTERNAL TABLE lake.sales.orders (id BIGINT, amount DOUBLE) PARTITIONED BY (dt string) STORED AS CSV LOCATION 'file:///tmp/hive-probe/data' TBLPROPERTIES ('skip.header.line.count'='1')"
target/dev-release/sqe-cli --embedded --memory --hive-root lake=file:///tmp/hive-probe/wh -e \
  "SELECT * FROM lake.sales.orders"
cat /tmp/hive-probe/wh/sales/orders/_sqe/table.json
```

Expected: two rows with `dt` populated from the second command, proving the definition persisted across processes, and a readable Glue-shaped manifest.

- [ ] **Step 6: Commit**

```bash
git add crates/sqe-cli/src/embedded.rs
git commit -m "feat(cli): Hive external table DDL in embedded mode

CREATE EXTERNAL TABLE, DROP TABLE, and SHOW CREATE TABLE against a warehouse
root. A definition written by one process is readable by the next, which is
the point of the sidecar manifest. DROP removes the manifest and never the
data. SHOW CREATE TABLE round-trips back through the extractor."
```

---

### Task 10: Server-mode DDL and statement dispatch

**Files:**
- Create: `crates/sqe-coordinator/src/hive_ddl_handler.rs`
- Modify: `crates/sqe-coordinator/src/query_handler.rs:1306`
- Modify: `crates/sqe-coordinator/src/session_context.rs`
- Test: `crates/sqe-coordinator/tests/hive_ddl_test.rs`

**Interfaces:**
- Consumes: everything from Tasks 2, 5, 6, 7, 8.
- Produces:
  ```rust
  pub struct HiveDdlHandler { /* roots by name, audit sink */ }
  impl HiveDdlHandler {
      pub fn resolve_table_kind(&self, table_ref: &TableReference) -> TableKind;
      pub async fn create_external_table(&self, session: &Session, spec: &HiveTableSpec) -> Result<(), SqeError>;
      pub async fn drop_table(&self, session: &Session, table_ref: &TableReference, purge: bool) -> Result<(), SqeError>;
      pub fn reject_unsupported(&self, statement: &str, cycle: u8) -> SqeError;
  }
  pub enum TableKind { Iceberg, Hive { root: String, database: String, table: String } }
  ```
  `resolve_table_kind` is the single branch point. No other handler grows its own detection.

- [ ] **Step 1: Write the failing tests**

Create `crates/sqe-coordinator/tests/hive_ddl_test.rs` with one test per row of the spec's statement dispatch table:

```rust
/// Every rejection must name `hive_external` and the cycle that adds support,
/// so an operator reads what to do rather than only what failed. One case per
/// row of the dispatch table in the spec, which is how the table stays true.
#[tokio::test]
async fn unsupported_statements_are_rejected_by_name_with_their_cycle() {
    let h = handler_with_root("lake").await;
    let cases = [
        ("ANALYZE lake.sales.orders", 2u8),
        ("ALTER TABLE lake.sales.orders ADD PARTITION (dt='2026-01-01')", 2),
        ("MSCK REPAIR TABLE lake.sales.orders", 2),
        ("SHOW PARTITIONS lake.sales.orders", 2),
        ("ALTER TABLE lake.sales.orders SET TAGS ('ssn' = 'PII')", 3),
        ("INSERT INTO lake.sales.orders VALUES (1, 2.0, '2026-01-01')", 4),
        // No CTAS row: `CREATE EXTERNAL TABLE ... AS SELECT` does not parse,
        // so it never reaches dispatch. Task 9 pins that boundary instead.
        ("DELETE FROM lake.sales.orders WHERE id = 1", 4),
        ("UPDATE lake.sales.orders SET amount = 1 WHERE id = 1", 4),
        ("TRUNCATE TABLE lake.sales.orders", 4),
        ("ALTER TABLE lake.sales.orders RENAME TO lake.sales.o2", 0),
        ("COMMENT ON TABLE lake.sales.orders IS 'x'", 0),
    ];
    for (sql, cycle) in cases {
        let err = h.execute_for_test(sql).await.expect_err(sql);
        let msg = format!("{err}");
        assert!(msg.contains("hive_external"), "{sql}: message should name the feature: {msg}");
        if cycle > 0 {
            assert!(msg.contains(&cycle.to_string()), "{sql}: should name cycle {cycle}: {msg}");
        }
    }
}

#[tokio::test]
async fn supported_statements_reach_the_hive_path_not_the_iceberg_path() {
    let h = handler_with_root("lake").await;
    for sql in [
        "SHOW TABLES FROM lake.sales",
        "DESCRIBE lake.sales.orders",
        "SHOW CREATE TABLE lake.sales.orders",
    ] {
        h.execute_for_test(sql).await.unwrap_or_else(|e| panic!("{sql} should work: {e}"));
    }
}

#[tokio::test]
async fn an_iceberg_table_name_still_routes_to_iceberg() {
    let h = handler_with_root("lake").await;
    assert!(matches!(
        h.resolve_table_kind(&"iceberg.hr.employees".into()),
        sqe_coordinator::hive_ddl_handler::TableKind::Iceberg
    ));
}

#[tokio::test]
async fn create_external_table_emits_an_audit_event_with_the_policy_name() {
    let h = handler_with_root("lake").await;
    h.execute_for_test(
        "CREATE EXTERNAL TABLE lake.sales.orders (id BIGINT) STORED AS PARQUET LOCATION 's3://raw/orders/'",
    )
    .await
    .expect("create");
    let events = h.audit_events_for_test();
    assert!(
        events.iter().any(|e| e.contains("lake.sales.orders")),
        "audit_resources only sees DataFusion CreateExternalTable plans, and the server path \
         produces none, so the handler must emit its own: {events:?}"
    );
}

#[tokio::test]
async fn a_hive_query_is_not_denied_when_a_tag_source_is_active() {
    // The end-to-end guard for the lit(false) failure mode. Without the
    // routing tag source this returns zero rows rather than erroring, which is
    // why the assertion is on the row count.
    let h = handler_with_root_and_tag_source("lake").await;
    let rows = h.query_rows_for_test("SELECT count(*) AS n FROM lake.sales.orders").await.expect("query");
    assert_eq!(rows, 3, "a tag source must not deny an untagged Hive table");
}
```

Write `handler_with_root`, `handler_with_root_and_tag_source`, `execute_for_test`, `query_rows_for_test`, and `audit_events_for_test` as real helpers over the existing coordinator test scaffolding. Use `RUST_MIN_STACK=33554432` if any of them hit the 2MB default stack.

- [ ] **Step 2: Run to verify failure**

```bash
RUST_MIN_STACK=33554432 cargo test -p sqe-coordinator --test hive_ddl_test 2>&1 | tail -20
```

Expected: `unresolved module hive_ddl_handler`.

- [ ] **Step 3: Implement**

- `hive_ddl_handler.rs` with `resolve_table_kind` consulting the registered Hive roots, `create_external_table` (mirroring the embedded handler, sharing the manifest-building code via `sqe-catalog`, not by copy), `drop_table`, and `reject_unsupported(statement, cycle)` producing `SqeError::Unsupported` with a message of the form: `hive_external: <statement> is not supported for Hive external tables in this release; cycle <n> adds it`.
- In `query_handler.rs`, add the `CreateExternalTable` arm next to the existing `CreateTable` arm at line 1306. For every statement kind in the spec's dispatch table, branch on `resolve_table_kind` and call `reject_unsupported` for the Hive case, leaving the Iceberg path untouched.
- Emit the audit event from `create_external_table` and `drop_table` using the same three-part name the policy key uses, so audit and policy agree on identity.
- In `session_context.rs`, call `session_hive_catalog` for every configured or attached root while building each user's session, passing that session's credentials and `RuntimeEnv`. The shared `HiveManifestStore`, `HiveCatalogs`, and `HiveMetadataCache` come from the process-wide set built in `main.rs`.
- `ATTACH` mid-session registers the provider on the current session context and inserts the name into `HiveCatalogs`, then calls `invalidate_session_cache(&username)` so the next statement rebuilds against the new root.

- [ ] **Step 4: Run to verify passing**

```bash
RUST_MIN_STACK=33554432 cargo test -p sqe-coordinator --test hive_ddl_test 2>&1 | tail -20
cargo clippy -p sqe-coordinator --all-targets --all-features -- -D warnings
```

Expected: 6 passed. Clippy clean.

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-coordinator/src/hive_ddl_handler.rs crates/sqe-coordinator/src/query_handler.rs crates/sqe-coordinator/src/session_context.rs crates/sqe-coordinator/tests/hive_ddl_test.rs
git commit -m "feat(coordinator): server-mode Hive DDL and one branch point for dispatch

resolve_table_kind is the single place that decides Iceberg or Hive, so no
handler grows its own detection. Every unsupported statement is rejected
naming hive_external and the cycle that adds it, instead of falling through
to the Iceberg path and emitting a confusing error. The handler emits its own
audit events because audit_resources only sees DataFusion
CreateExternalTable plans and the server path produces none."
```

---

### Task 11: Integration test on object storage

**Files:**
- Create: `crates/sqe-coordinator/tests/it/hive_external_e2e.rs`
- Modify: `crates/sqe-coordinator/tests/it/main.rs`
- Modify: `Makefile`

**Interfaces:**
- Consumes: the whole feature.
- Produces: nothing consumed by later tasks.

- [ ] **Step 1: Write the failing test**

Create `crates/sqe-coordinator/tests/it/hive_external_e2e.rs` following the existing integration-test shape (fixed-port shared stack, RustFS for S3):

```rust
//! End-to-end Hive external tables against real object storage.
//!
//! Local only. There is no CI equivalent: issue #387 removed the dind jobs, so
//! `make test-integration` is the gate.

#[tokio::test]
#[ignore = "requires the local Polaris + RustFS stack"]
async fn hive_external_table_round_trips_on_object_storage() {
    let stack = common::shared_stack().await;
    upload_partitioned_csv(&stack, "raw/orders").await;

    let mut c = stack.client().await;
    c.execute("ATTACH 's3://warehouse/wh/' AS lake (TYPE hive_external)").await.expect("attach");
    c.execute(
        "CREATE EXTERNAL TABLE lake.sales.orders (id BIGINT, amount DOUBLE) \
         PARTITIONED BY (dt string) STORED AS CSV LOCATION 's3://raw/orders/' \
         TBLPROPERTIES ('skip.header.line.count' = '1')",
    )
    .await
    .expect("create");

    assert!(c.execute("SHOW TABLES FROM lake.sales").await.expect("show").contains("orders"));
    assert_eq!(count_of(&mut c, "SELECT count(*) FROM lake.sales.orders").await, 3);
    assert_eq!(
        count_of(&mut c, "SELECT count(*) FROM lake.sales.orders WHERE dt = '2026-01-02'").await,
        1
    );

    // A second session sees the table with no DDL replay: the definition is in
    // the root, not in session state.
    let mut c2 = stack.client().await;
    c2.execute("ATTACH 's3://warehouse/wh/' AS lake (TYPE hive_external)").await.expect("attach");
    assert_eq!(count_of(&mut c2, "SELECT count(*) FROM lake.sales.orders").await, 3);

    c.execute("DROP TABLE lake.sales.orders").await.expect("drop");
    assert!(object_exists(&stack, "raw/orders/dt=2026-01-01/a.csv").await, "data must survive DROP");
    assert!(!object_exists(&stack, "wh/sales/orders/_sqe/table.json").await, "manifest must be gone");
}

#[tokio::test]
#[ignore = "requires the local Polaris + RustFS stack"]
async fn parquet_and_json_tables_read_on_object_storage() { /* same shape, both formats */ }
```

- [ ] **Step 2: Run to verify failure**

```bash
make test-integration FILTER=hive_external 2>&1 | tail -30
```

Expected: the test does not exist yet, then fails on the missing feature.

- [ ] **Step 3: Implement the helpers and register the module**

Add `mod hive_external_e2e;` to `crates/sqe-coordinator/tests/it/main.rs`. Write `upload_partitioned_csv`, `count_of`, and `object_exists` against the existing stack helpers.

- [ ] **Step 4: Run to verify passing**

```bash
make test-integration FILTER=hive_external 2>&1 | tail -30
```

Expected: 2 passed. If the run reports a port conflict, tear the stack down with `make test-integration-down` first.

- [ ] **Step 5: Run the whole suite to check for regressions**

```bash
make test-integration 2>&1 | tail -30
```

Expected: the pre-existing baseline plus the 2 new tests. One known red test (#431) is expected; classify by test name, never by pass count.

- [ ] **Step 6: Commit**

```bash
git add crates/sqe-coordinator/tests/it Makefile
git commit -m "test: end-to-end Hive external tables on object storage

Covers ATTACH, CREATE EXTERNAL TABLE, SHOW TABLES, a partitioned read, a
partition-pruned read, a second session reading the table with no DDL replay,
and DROP TABLE leaving the data in place."
```

---

### Task 12: Documentation and project state

**Files:**
- Create: `docs/site/book/src/reference/hive-external-tables.md`
- Modify: `docs/site/book/src/SUMMARY.md`
- Modify: `README.md`
- Modify: `nextsteps.md`
- Modify: `sqe.toml.example`

- [ ] **Step 1: Write the reference page**

`docs/site/book/src/reference/hive-external-tables.md` covers: attaching a root (SQL, TOML, CLI flag), the supported DDL with a worked Athena example, the SerDe allow-list table, the four semantics decisions, the five deviations from Athena, the `sqe.secret` property, what `DROP TABLE` does and does not delete, and a "performance" section stating plainly that cycle 1 lists on every query, that pinning leading partition columns with equality is what prunes the listing today, and that compressed CSV and JSON are one file per thread so files should be sized accordingly.

Follow the voice rules: no emdash, no endash, no Unicode arrows, no AI tells, short sentences carrying the weight.

- [ ] **Step 2: Verify the style gate**

```bash
grep -n '—\|–\|→' docs/site/book/src/reference/hive-external-tables.md || echo "clean"
./scripts/leak-scan.sh 2>&1 | tail -5
```

Expected: no character hits, no leaks. `docs/site/` must be publish-clean.

- [ ] **Step 3: Update project state**

- `README.md`: add Hive external tables to the roadmap checklist, marked done for the read path with the four later cycles listed as open.
- `nextsteps.md`: update the status line and move the NEXT pointer to cycle 2 (partition DDL, partition index, projection).
- `sqe.toml.example`: add a commented `[hive]` block with a `[[hive.roots]]` example and the three manifest caps at their defaults.
- `docs/site/book/src/SUMMARY.md`: link the new page.

- [ ] **Step 4: Final full-workspace verification**

```bash
cargo build --all 2>&1 | tail -5
cargo test --all 2>&1 | tail -20
cargo clippy --all-targets --all-features -- -D warnings
cargo check -p sqe-catalog --no-default-features --features rest 2>&1 | tail -3
```

Run each on its own line. Never pipe a gate into `tail` and then chain with `&&`: the exit code you would read is `tail`'s, which has produced false greens in this repo before.

Expected: build clean, tests pass apart from the known `sql-sqlite`-gated coordinator test, clippy clean, slim build clean.

- [ ] **Step 5: Commit and open the MR**

```bash
git add README.md nextsteps.md sqe.toml.example docs/site
git commit -m "docs: reference page for Hive external tables, and project state

States the performance shape honestly: cycle 1 lists on every query, an
equality-pinned leading partition column is what prunes the listing today,
and compressed CSV or JSON is one file per thread."
git push -u origin feat/hive-external-tables-cycle1
glab mr create --fill --yes
```

Do not use `--auto-merge`: it merges immediately with no pipeline running. Push the merge commit and let the pipeline run on it, because an MR's green pipeline never builds the merge result.

---

## Self-Review

**Spec coverage.** Every section of the spec maps to a task: SQL surface to Tasks 1 and 2; SerDe allow-list and semantics decisions to Task 4; manifest format, versioning, and untrusted-input handling to Tasks 3 and 5; concurrency to Task 5; warehouse root to Task 8; providers and execution to Task 6; credentials to Tasks 6 and 8; statement dispatch to Task 10; resource naming and tag routing to Task 7; fast query to Tasks 4, 6, and 12; error handling to every task's rejection tests; testing to Tasks 1 through 11; rollback to Task 3's feature gate plus Task 7's Iceberg-key regression test.

**Two spec items deliberately spread rather than given their own task.** Statistics live in Task 6 with the provider that consumes them, because a `Statistics` value with no provider to attach it to cannot be tested. Audit lives in Task 10 with the handler that emits it.

**Known gap, stated rather than hidden.** The spec's `sqe.partition_index` and `sqe.statistics` slots are defined in Task 3 and left `None` for cycle 1. That is intentional: they are cycle-2 fields, and the reason they exist now is that a manifest in object storage read by mixed engine versions is a wire format.

**Two lifetimes, called out because getting them wrong is silent.** The
`HiveManifestStore`, `HiveCatalogs`, and `HiveMetadataCache` are process-wide
single instances created in `main.rs`; the `HiveCatalogProvider` is per session
so a read runs as the calling user. Passing a second instance of any of the
three would compile, look right, and route nothing: the tag source would read a
cache no provider writes to, or the policy key would consult a set ATTACH never
updates. Task 8 and Task 10 both say to pass the same `Arc`.

**Type consistency check.** `HiveTableSpec`, `HiveColumn`, `HiveManifest`, `StorageDescriptor`, `SerdeInfo`, `GlueColumn`, `SqeExtensions`, `ManifestLimits`, `HiveError`, `ResolvedFormat`, `HiveFileFormat`, `HiveManifestStore`, `HiveCatalogProvider`, `HiveSchemaProvider`, `HiveMetadataCache`, `HiveDataStoreFactory`, `HiveCatalogs`, `HiveTagSource`, `CatalogRoutedTagSource`, `HiveDdlHandler`, and `TableKind` are each defined once, in the task that creates them, and referenced with the same spelling afterwards. `resolve_policy_key` gains its second parameter in Task 7 and every call site is updated in that same task.
