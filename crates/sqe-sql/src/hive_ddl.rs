//! Extraction of Athena/Glue `CREATE EXTERNAL TABLE` into a neutral spec.
//!
//! sqlparser 0.62 parses the full Athena clause set already, in both
//! `GenericDialect` and `HiveDialect`, so this module reads the AST rather
//! than parsing text. It stays free of DataFusion and object-store types:
//! `sqe-catalog` depends on `sqe-sql`, never the reverse.
//!
//! Hive angle-bracket nested types are not uniformly supported by either
//! dialect: `ARRAY<...>` and `STRUCT<...>` parse fine and reach `column_of`
//! below, which rejects them by name; `MAP<...>` does not parse at all under
//! `GenericDialect` or `HiveDialect` (only `SparkDialect` accepts the
//! angle-bracket map form), so it fails inside `Parser::parse_sql` before
//! this module ever sees a `CreateTable` AST.
//!
//! Spec: `docs/superpowers/specs/2026-08-19-hive-external-tables-design.md`

use std::collections::BTreeMap;

use sqe_core::{SqeError, SqeErrorCode};
use sqlparser::ast::{
    ColumnDef, ColumnOption, CreateTable, CreateTableOptions, Expr, HiveDelimiter,
    HiveDistributionStyle, HiveIOFormat, HiveRowFormat, Ident, SqlOption, Statement, Value,
};

pub const LAZY_SIMPLE_SERDE: &str = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";

/// Recognised `sqe.`-prefixed table properties. Anything else with that
/// prefix is a typo, and a silently ignored typo is a setting that did not
/// apply.
const KNOWN_SQE_PROPERTIES: &[&str] = &["sqe.secret"];

/// A backend-neutral column: sqlparser's `ColumnDef` boiled down to the
/// three facts that matter for a Hive-style external table.
///
/// `PartialEq` is derived for the embedded-mode `SHOW CREATE TABLE`
/// round-trip check: the rendered DDL is re-parsed through
/// `try_extract_hive_table` and the resulting spec compared for equality
/// against the spec it was rendered from.
#[derive(Debug, PartialEq)]
pub struct HiveColumn {
    pub name: String,
    pub sql_type: String,
    pub comment: Option<String>,
}

/// Neutral extraction of an Athena/Glue `CREATE EXTERNAL TABLE`, independent
/// of both sqlparser's AST shape and any DataFusion/object-store type.
///
/// `PartialEq` is derived for the same reason as on [`HiveColumn`]: the
/// embedded-mode `SHOW CREATE TABLE` round-trip test needs to assert spec
/// equality, not a substring match on the rendered DDL text.
#[derive(Debug, PartialEq)]
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

/// `Ok(None)` when `stmt` is not a `CREATE EXTERNAL TABLE`; `Err` when it is
/// one but uses a clause this release does not support.
pub fn try_extract_hive_table(stmt: &Statement) -> Result<Option<HiveTableSpec>, SqeError> {
    let Statement::CreateTable(ct) = stmt else {
        return Ok(None);
    };
    if !ct.external {
        return Ok(None);
    }
    Ok(Some(extract(ct)?))
}

fn extract(ct: &CreateTable) -> Result<HiveTableSpec, SqeError> {
    // Name: 1, 2, or 3 parts, rightmost is the table.
    let parts: Vec<String> = ct
        .name
        .0
        .iter()
        .filter_map(|p| p.as_ident().map(ident_text))
        .collect();
    let (catalog, database, name) = match parts.as_slice() {
        [t] => (None, None, t.clone()),
        [d, t] => (None, Some(d.clone()), t.clone()),
        [c, d, t] => (Some(c.clone()), Some(d.clone()), t.clone()),
        other => {
            return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
                "hive_external: table name must have 1 to 3 parts, got {}",
                other.len()
            )))
        }
    };

    let columns = ct
        .columns
        .iter()
        .map(column_of)
        .collect::<Result<Vec<_>, _>>()?;
    if columns.is_empty() {
        return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, 
            "hive_external: CREATE EXTERNAL TABLE needs an explicit column list; schema \
             inference is not supported because the declared list is what binds CSV fields \
             by ordinal",
        ));
    }

    let partition_keys = match &ct.hive_distribution {
        HiveDistributionStyle::PARTITIONED { columns } => columns
            .iter()
            .map(column_of)
            .collect::<Result<Vec<_>, _>>()?,
        _ => Vec::new(),
    };

    // Hive and Athena declare partition keys separately, and a key repeated in
    // the main list would land in the Arrow schema twice.
    for k in &partition_keys {
        if columns.iter().any(|c| c.name == k.name) {
            return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
                "hive_external: `{}` is both a column and a partition key. Hive declares \
                 partition keys only in PARTITIONED BY; remove it from the column list.",
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
                        HiveDelimiter::FieldsEscapedBy => "escape.delim",
                        HiveDelimiter::NullDefinedAs => "serialization.null.format",
                    };
                    serde_properties.insert(key.to_string(), decode_hive_escape(&d.char.value));
                }
            }
            None => {}
        }
        if let Some(props) = &fmts.serde_properties {
            serde_properties.extend(
                props
                    .iter()
                    .filter_map(option_pair)
                    .map(|(k, v)| (k, decode_hive_escape(&v))),
            );
        }
    }

    // STORED AS, or INPUTFORMAT / OUTPUTFORMAT.
    let mut stored_as = ct.file_format.as_ref().map(|f| f.to_string());
    let mut input_format = None;
    let mut output_format = None;
    match ct.hive_formats.as_ref().and_then(|f| f.storage.as_ref()) {
        Some(HiveIOFormat::FileFormat { format }) => stored_as = Some(format.to_string()),
        Some(HiveIOFormat::IOF {
            input_format: i,
            output_format: o,
        }) => {
            stored_as = None;
            input_format = Some(literal_text(i));
            output_format = Some(literal_text(o));
        }
        // Spark's `USING <format>` syntax. Not part of the Athena/Hive form
        // this release targets; leave whatever `ct.file_format` already gave.
        Some(HiveIOFormat::Using { .. }) | None => {}
    }

    let location = ct
        .location
        .clone()
        .or_else(|| ct.hive_formats.as_ref().and_then(|f| f.location.clone()))
        .ok_or_else(|| {
            SqeError::with_code(SqeErrorCode::ExecutionFailed, 
                "hive_external: CREATE EXTERNAL TABLE requires LOCATION '<url>'",
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
            return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
                "hive_external: unknown property `{key}`. Known sqe. properties: {}. A silently \
                 ignored typo is a setting that did not apply.",
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
        return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
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

/// Decode a Hive/Java-style backslash escape in a SerDe or `ROW FORMAT
/// DELIMITED` property value.
///
/// sqlparser-rs's `GenericDialect` tokenizer treats backslash as a literal
/// character inside a single-quoted string, unlike Hive's own DDL lexer,
/// which processes these escapes before the SerDe or `LazySimpleSerDe`
/// delimiter code ever sees the value. Decoding here, rather than in
/// `sqe-catalog`'s `serde_map`, keeps the manifest holding the byte Hive and
/// Glue would actually store: `escapeChar = '\\'` (Hive's own escape for a
/// literal backslash) must persist as one backslash character, not two, or
/// a manifest round-trip would hold a value no real Hive deployment would
/// ever produce.
///
/// Only a sequence this function actually recognizes is collapsed: `\\`,
/// `\t`, `\n`, `\r`, `\0`, and Hive's own three-digit octal escape `\NNN`
/// (octal `000` through `377`, i.e. byte values 0-255) -- `FIELDS
/// TERMINATED BY '\001'` for Hive's ctrl-A default is the single most
/// common form of this in real DDL. A value this function does not
/// recognize, including one that is already a single character, or a lone
/// backslash followed by a character with no defined escape, passes
/// through unchanged: a manifest written by hand or by real Glue must keep
/// meaning what it already means, and a false-positive decode would rewrite
/// a property nobody asked to have rewritten.
///
/// Octal values above `\177` (127) decode to a Unicode codepoint in the
/// Latin-1 supplement (`char::from` is a lossless byte-to-codepoint
/// mapping), which needs two bytes to encode as UTF-8. `serde_map::
/// single_byte` downstream still requires exactly one byte, so those values
/// are correctly decoded here but rejected there -- the same outcome Hive
/// itself would have when persisting that property as UTF-8-encoded text.
/// Every byte value Hive actually uses for a delimiter (control characters
/// 000-037, and printable ASCII) is inside the one-byte range.
fn decode_hive_escape(value: &str) -> String {
    let bytes = value.as_bytes();
    match bytes {
        [b'\\', b'\\'] => "\\".to_string(),
        [b'\\', b't'] => "\t".to_string(),
        [b'\\', b'n'] => "\n".to_string(),
        [b'\\', b'r'] => "\r".to_string(),
        [b'\\', b'0'] => "\0".to_string(),
        [b'\\', d0, d1, d2] if is_octal_triplet(*d0, *d1, *d2) => {
            let n = (d0 - b'0') as u32 * 64 + (d1 - b'0') as u32 * 8 + (d2 - b'0') as u32;
            char::from(n as u8).to_string()
        }
        _ => value.to_string(),
    }
}

/// Three octal digits (`000` through `377`, Hive's actual escape range: byte
/// values 0-255). `is_octal_digit` alone would accept `\777`, which is not a
/// valid Hive octal escape and would silently truncate to a wrong byte via
/// `as u8`; this rejects anything whose value would exceed 255 so it falls
/// through to the pass-through arm instead.
fn is_octal_triplet(d0: u8, d1: u8, d2: u8) -> bool {
    is_octal_digit(d0) && is_octal_digit(d1) && is_octal_digit(d2) && d0 <= b'3'
}

fn is_octal_digit(b: u8) -> bool {
    (b'0'..=b'7').contains(&b)
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
