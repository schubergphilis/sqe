//! The Trino Iceberg connector's table properties on `CREATE TABLE` and
//! CTAS (#897, openspec `trino-table-properties`).
//!
//! An unquoted key in a `WITH (...)` list is a Trino property: dbt-trino and
//! every Trino client write them so. SQE honours each one or refuses it; an
//! unknown key is an error, so no property is stored and then ignored.
//! Quoted keys (`'write.delete.mode'`) and every `TBLPROPERTIES` key are
//! Iceberg properties and pass through. The front door joins a
//! `TBLPROPERTIES` list beside `WITH` into the `WITH` list with its keys
//! quoted, so the rule holds for the joined form too.

use iceberg::spec::{
    NullOrder, Schema as IcebergSchema, SortDirection, SortField, SortOrder, Transform,
    UnboundPartitionSpec,
};
use iceberg::table::Table;
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableRequirement, TableUpdate};
use sqe_core::{SqeError, SqeErrorCode};
use sqlparser::ast::{
    CreateTableOptions, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Ident, SqlOption,
    Value,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::{Parser, ParserError};
use sqlparser::tokenizer::Token;

use super::{build_partition_spec, create_table_at_location};
use crate::ident::fold_unquoted_ident;

/// The Trino properties SQE honours.
const SUPPORTED: &[&str] = &[
    "comment",
    "extra_properties",
    "format",
    "format_version",
    "gc_enabled",
    "location",
    "parquet_bloom_filter_columns",
    "parquet_writer_row_group_size",
    "partitioning",
    "sorted_by",
];

/// The Trino properties SQE refuses, and why.
const REFUSED: &[(&str, &str)] = &[
    (
        "compression_codec",
        "SQE takes the Parquet codec from the session property \
         `iceberg.compression_codec` or the server configuration",
    ),
    (
        "target_max_file_size",
        "SQE's writer does not read a per-table target file size",
    ),
    (
        "max_commit_retry",
        "SQE retries a commit a fixed number of times",
    ),
    (
        "delete_after_commit_enabled",
        "SQE does not manage metadata files; set the Iceberg property \
         'write.metadata.delete-after-commit.enabled' (quoted) for the catalog",
    ),
    (
        "max_previous_versions",
        "SQE does not manage metadata files; set the Iceberg property \
         'write.metadata.previous-versions-max' (quoted) for the catalog",
    ),
    (
        "data_location",
        "SQE writes data files under the table location",
    ),
    (
        "object_store_layout_enabled",
        "SQE's writer has no object-store file layout",
    ),
    ("orc_bloom_filter_columns", "SQE writes Parquet only"),
    ("orc_bloom_filter_fpp", "SQE writes Parquet only"),
];

/// A `CREATE TABLE` option list with its Trino properties resolved.
#[derive(Debug, Default)]
pub(crate) struct TrinoTableOptions {
    /// The option list the rest of the create path reads: Iceberg
    /// properties, the Trino keys mapped to theirs, plus `format_version`
    /// and `comment`, which it already handles.
    options: Vec<SqlOption>,
    partitioning: Option<Vec<String>>,
    sorted_by: Option<Vec<String>>,
    location: Option<String>,
}

fn invalid(msg: impl Into<String>) -> SqeError {
    SqeError::with_code(SqeErrorCode::InvalidArguments, msg.into())
}

/// A quoted `'key' = 'value'` option: an Iceberg property.
fn iceberg_option(key: impl Into<String>, value: impl Into<String>) -> SqlOption {
    SqlOption::KeyValue {
        key: Ident::with_quote('\'', key),
        value: Expr::value(Value::SingleQuotedString(value.into())),
    }
}

impl TrinoTableOptions {
    /// Resolve the options of a `CREATE TABLE`. `partitioned_by`: the
    /// statement has a `PARTITIONED BY` clause, which `partitioning` would
    /// contradict. Every error is raised here, before anything is planned or
    /// created, except a column the schema does not have.
    pub(crate) fn parse(
        options: &CreateTableOptions,
        partitioned_by: bool,
    ) -> sqe_core::Result<Self> {
        let mut out = Self::default();
        let CreateTableOptions::With(items) = options else {
            out.options = super::create_table_options_as_slice(options).to_vec();
            return Ok(out);
        };
        for opt in items {
            match opt {
                SqlOption::KeyValue { key, value } if key.quote_style.is_none() => {
                    out.trino_option(&key.value.to_ascii_lowercase(), opt, value)?;
                }
                other => out.options.push(other.clone()),
            }
        }
        if partitioned_by && out.partitioning.is_some() {
            return Err(invalid(
                "the `partitioning` table property and PARTITIONED BY cannot both be given",
            ));
        }
        Ok(out)
    }

    /// Resolve the options of `ALTER TABLE ... SET PROPERTIES` (or `SET
    /// TBLPROPERTIES`). The front door quotes every `SET PROPERTIES` key
    /// (#338), so a Trino property is known here by its name; none of them
    /// is an Iceberg property name, which are dotted. Every other key stays
    /// an Iceberg property, as before. `location` cannot change, and Trino's
    /// `p = DEFAULT` reset is refused.
    pub(crate) fn parse_alter(items: &[SqlOption]) -> sqe_core::Result<Self> {
        let mut out = Self::default();
        for opt in items {
            let SqlOption::KeyValue { key, value } = opt else {
                out.options.push(opt.clone());
                continue;
            };
            if matches!(value, Expr::Identifier(id)
                if id.quote_style.is_none() && id.value.eq_ignore_ascii_case("default"))
            {
                return Err(invalid(format!(
                    "SET PROPERTIES {} = DEFAULT is not supported; set the value explicitly",
                    key.value
                )));
            }
            // As in CREATE, a quoted key (every `SET TBLPROPERTIES` key) is an
            // Iceberg property. An unquoted one is Trino vocabulary when SQE
            // knows the name; any other stays a property, as `SET PROPERTIES`
            // always stored it.
            let name = key.value.to_ascii_lowercase();
            let trino = key.quote_style.is_none()
                && (SUPPORTED.contains(&name.as_str()) || REFUSED.iter().any(|(k, _)| *k == name));
            if trino && name == "location" {
                return Err(invalid(
                    "ALTER TABLE cannot change the table property `location`",
                ));
            }
            if trino {
                out.trino_option(&name, opt, value)?;
            } else {
                out.options.push(opt.clone());
            }
        }
        Ok(out)
    }

    /// True when `partitioning` or `sorted_by` was given.
    pub(crate) fn changes_layout(&self) -> bool {
        self.partitioning.is_some() || self.sorted_by.is_some()
    }

    /// The table updates `partitioning` (partition evolution) and `sorted_by`
    /// (a new default sort order) ask of `table`, with the requirements that
    /// pin what they were derived from. Nothing for a property not given or
    /// already in place.
    pub(crate) fn alter_updates(
        &self,
        table: &Table,
    ) -> sqe_core::Result<(Vec<TableUpdate>, Vec<TableRequirement>)> {
        let meta = table.metadata();
        let schema = meta.current_schema();
        let mut updates = Vec::new();
        let mut requirements = Vec::new();
        if self.partitioning.is_some() {
            let wanted = self
                .partition_spec(None, schema)?
                .map_or_else(Vec::new, |s| s.fields().to_vec());
            let key = |source_id: i32, transform: Transform| (source_id, transform);
            let live: Vec<_> = meta
                .default_partition_spec()
                .fields()
                .iter()
                .map(|f| key(f.source_id, f.transform))
                .collect();
            if live
                != wanted
                    .iter()
                    .map(|f| key(f.source_id, f.transform))
                    .collect::<Vec<_>>()
            {
                // A field some spec already had keeps its id and name, as
                // Iceberg's own spec evolution does; a new one takes the next
                // id after the last assigned (1000 on a table never partitioned).
                let mut next_id = (meta.last_partition_id() + 1).max(1000);
                let mut fields = Vec::with_capacity(wanted.len());
                for mut field in wanted {
                    let earlier = meta
                        .partition_specs_iter()
                        .flat_map(|s| s.fields())
                        .find(|f| f.source_id == field.source_id && f.transform == field.transform);
                    match earlier {
                        Some(f) => {
                            field.field_id = Some(f.field_id);
                            field.name = f.name.clone();
                        }
                        None => {
                            // `bucket(id, 8)` after `bucket(id, 4)` would reuse
                            // the name `id_bucket` for another field; Iceberg
                            // names it `id_bucket_8`.
                            let taken = meta
                                .partition_specs_iter()
                                .flat_map(|s| s.fields())
                                .any(|f| f.name == field.name);
                            if taken {
                                field.name = match field.transform {
                                    Transform::Bucket(n) | Transform::Truncate(n) => {
                                        format!("{}_{n}", field.name)
                                    }
                                    _ => format!("{}_{next_id}", field.name),
                                };
                            }
                            field.field_id = Some(next_id);
                            next_id += 1;
                        }
                    }
                    fields.push(field);
                }
                requirements.push(TableRequirement::DefaultSpecIdMatch {
                    default_spec_id: meta.default_partition_spec_id(),
                });
                // The catalog reuses a spec equal to one the table has and
                // then has no "last added" spec, so an earlier layout is made
                // the default by its id.
                let same = |s: &iceberg::spec::PartitionSpec| {
                    s.fields().len() == fields.len()
                        && s.fields().iter().zip(&fields).all(|(a, b)| {
                            Some(a.field_id) == b.field_id
                                && a.source_id == b.source_id
                                && a.transform == b.transform
                                && a.name == b.name
                        })
                };
                if let Some(spec) = meta.partition_specs_iter().find(|s| same(s)) {
                    updates.push(TableUpdate::SetDefaultSpec {
                        spec_id: spec.spec_id(),
                    });
                } else {
                    let spec_id = meta
                        .partition_specs_iter()
                        .map(|s| s.spec_id())
                        .max()
                        .unwrap_or(0)
                        + 1;
                    let spec = UnboundPartitionSpec::builder()
                        .with_spec_id(spec_id)
                        .add_partition_fields(fields)
                        .map_err(|e| invalid(format!("partitioning: {e}")))?
                        .build();
                    updates.push(TableUpdate::AddSpec { spec });
                    updates.push(TableUpdate::SetDefaultSpec { spec_id: -1 });
                    requirements.push(TableRequirement::LastAssignedPartitionIdMatch {
                        last_assigned_partition_id: meta.last_partition_id(),
                    });
                }
            }
        }
        if !self.keeps_sort_order(table, schema) {
            let order_id = meta
                .sort_orders_iter()
                .map(|o| o.order_id)
                .max()
                .unwrap_or(0)
                + 1;
            let order = self
                .sort_order_with_id(schema, order_id)?
                .unwrap_or_else(SortOrder::unsorted_order);
            // As for specs: an order the table has is made the default by id.
            let key = |o: &SortOrder| -> Vec<_> {
                o.fields
                    .iter()
                    .map(|f| (f.source_id, f.transform, f.direction, f.null_order))
                    .collect()
            };
            requirements.push(TableRequirement::DefaultSortOrderIdMatch {
                default_sort_order_id: meta.default_sort_order_id(),
            });
            match meta.sort_orders_iter().find(|o| key(o) == key(&order)) {
                Some(existing) => updates.push(TableUpdate::SetDefaultSortOrder {
                    sort_order_id: existing.order_id,
                }),
                None => {
                    updates.push(TableUpdate::AddSortOrder { sort_order: order });
                    updates.push(TableUpdate::SetDefaultSortOrder { sort_order_id: -1 });
                }
            }
        }
        if !updates.is_empty() {
            requirements.push(TableRequirement::UuidMatch { uuid: meta.uuid() });
            requirements.push(TableRequirement::CurrentSchemaIdMatch {
                current_schema_id: meta.current_schema_id(),
            });
        }
        Ok((updates, requirements))
    }

    fn trino_option(&mut self, key: &str, opt: &SqlOption, value: &Expr) -> sqe_core::Result<()> {
        let bad = |want: &str| {
            invalid(format!(
                "table property `{key}` must be {want}, got {value}"
            ))
        };
        match key {
            "format_version" | "comment" => self.options.push(opt.clone()),
            "partitioning" => {
                let items = string_array(value).ok_or_else(|| bad("an ARRAY of strings"))?;
                for item in &items {
                    trino_partition_expr(item)?;
                }
                self.partitioning = Some(items);
            }
            "sorted_by" => {
                let items = string_array(value).ok_or_else(|| bad("an ARRAY of strings"))?;
                for item in &items {
                    sort_item(item)?;
                }
                self.sorted_by = Some(items);
            }
            "location" => {
                let location = string(value).ok_or_else(|| bad("a string"))?;
                self.location = Some(location.trim_end_matches('/').to_string());
            }
            "format" => {
                let format = string(value).ok_or_else(|| bad("a string"))?;
                if !format.eq_ignore_ascii_case("parquet") {
                    return Err(invalid(format!(
                        "table property `format` = '{format}' is not supported: \
                         SQE writes Parquet only"
                    )));
                }
            }
            "extra_properties" => {
                for (k, v) in string_map(value)
                    .ok_or_else(|| bad("MAP(ARRAY['key', ...], ARRAY['value', ...]) of strings"))?
                {
                    let reserved = iceberg::spec::TableProperties::RESERVED_PROPERTIES
                        .iter()
                        .chain(&["format_version"])
                        .any(|r| k.eq_ignore_ascii_case(r));
                    if reserved {
                        return Err(invalid(format!(
                            "extra_properties cannot set the reserved property '{k}'; \
                             use the `format_version` table property for the format version"
                        )));
                    }
                    self.options.push(iceberg_option(k, v));
                }
            }
            "gc_enabled" => {
                let enabled = boolean(value).ok_or_else(|| bad("true or false"))?;
                self.options
                    .push(iceberg_option("gc.enabled", enabled.to_string()));
            }
            "parquet_bloom_filter_columns" => {
                for column in string_array(value).ok_or_else(|| bad("an ARRAY of strings"))? {
                    let column = column_name(&column, "parquet_bloom_filter_columns")?;
                    self.options.push(iceberg_option(
                        format!("write.parquet.bloom-filter-enabled.column.{column}"),
                        "true",
                    ));
                }
            }
            "parquet_writer_row_group_size" => {
                let bytes = data_size(value).ok_or_else(|| bad("a data size such as '128MB'"))?;
                self.options.push(iceberg_option(
                    "write.parquet.row-group-size-bytes",
                    bytes.to_string(),
                ));
            }
            other => {
                return Err(match REFUSED.iter().find(|(k, _)| *k == other) {
                    Some((_, why)) => {
                        invalid(format!("table property `{other}` is not supported: {why}"))
                    }
                    None => invalid(format!(
                        "unknown table property `{other}`. Supported: {}. Quote a key \
                         ('{other}') to store it as an Iceberg property",
                        SUPPORTED.join(", ")
                    )),
                });
            }
        }
        Ok(())
    }

    /// The option list the create path reads (see the field).
    pub(crate) fn options(&self) -> &[SqlOption] {
        &self.options
    }

    /// The table location the statement asked for.
    pub(crate) fn location(&self) -> Option<String> {
        self.location.clone()
    }

    /// The partition spec: Trino's `partitioning`, else `PARTITIONED BY`.
    pub(crate) fn partition_spec(
        &self,
        partitioned_by: Option<&Expr>,
        schema: &IcebergSchema,
    ) -> sqe_core::Result<Option<UnboundPartitionSpec>> {
        let Some(items) = &self.partitioning else {
            return build_partition_spec(partitioned_by, schema);
        };
        if items.is_empty() {
            return Ok(None);
        }
        let exprs = items
            .iter()
            .map(|item| trino_partition_expr(item))
            .collect::<sqe_core::Result<Vec<_>>>()?;
        build_partition_spec(Some(&Expr::Tuple(exprs)), schema)
    }

    /// The default sort order `sorted_by` asks for, order id 1. Metadata
    /// only: SQE's writer does not sort, and its data files claim no order.
    pub(crate) fn sort_order(&self, schema: &IcebergSchema) -> sqe_core::Result<Option<SortOrder>> {
        self.sort_order_with_id(schema, 1)
    }

    fn sort_order_with_id(
        &self,
        schema: &IcebergSchema,
        order_id: i64,
    ) -> sqe_core::Result<Option<SortOrder>> {
        let Some(items) = self.sorted_by.as_ref().filter(|i| !i.is_empty()) else {
            return Ok(None);
        };
        let mut order = SortOrder::builder();
        order.with_order_id(order_id);
        for item in items {
            let (column, direction, null_order) = sort_item(item)?;
            let field = schema.field_by_name(&column).ok_or_else(|| {
                invalid(format!("sorted_by references unknown column '{column}'"))
            })?;
            order.with_sort_field(
                SortField::builder()
                    .source_id(field.id)
                    .transform(Transform::Identity)
                    .direction(direction)
                    .null_order(null_order)
                    .build(),
            );
        }
        order
            .build(schema)
            .map(Some)
            .map_err(|e| invalid(format!("sorted_by: {e}")))
    }

    /// An in-place `CREATE OR REPLACE` keeps the live sort order: true when
    /// that is the one `sorted_by` asks for, or `sorted_by` is not given.
    pub(crate) fn keeps_sort_order(&self, live: &Table, schema: &IcebergSchema) -> bool {
        if self.sorted_by.is_none() {
            return true;
        }
        let Ok(wanted) = self.sort_order(schema) else {
            return false;
        };
        let live_meta = live.metadata();
        let named = |order: &SortOrder, schema: &IcebergSchema| -> Vec<_> {
            order
                .fields
                .iter()
                .map(|f| {
                    let name = schema.name_by_field_id(f.source_id).map(str::to_string);
                    (name, f.transform, f.direction, f.null_order)
                })
                .collect()
        };
        let wanted = wanted.map_or_else(Vec::new, |o| named(&o, schema));
        wanted == named(live_meta.default_sort_order(), live_meta.current_schema())
    }

    /// `CREATE OR REPLACE` keeps the live table, so a `location` must be its
    /// own; a different one is refused, never ignored.
    pub(crate) fn check_replace_location(&self, live: &Table) -> sqe_core::Result<()> {
        let live_location = live.metadata().location().trim_end_matches('/');
        match &self.location {
            Some(location) if location != live_location => Err(invalid(format!(
                "CREATE OR REPLACE cannot move the table to '{location}': it is at \
                 '{live_location}'"
            ))),
            _ => Ok(()),
        }
    }

    /// A schema-changing `CREATE OR REPLACE` stages a new table, which cannot
    /// take the live table's location.
    pub(crate) fn refuse_staged_location(&self) -> sqe_core::Result<()> {
        if self.location.is_none() {
            return Ok(());
        }
        Err(invalid(
            "this CREATE OR REPLACE changes the schema, partitioning or sort order, so the \
             replacement is a new table and cannot keep the `location`; drop the property",
        ))
    }

    /// Create the table. A `location` the user gave is sent as it is: when
    /// the catalog refuses it, the statement fails rather than creating the
    /// table elsewhere (D4).
    pub(crate) async fn create_table(
        &self,
        catalog: &dyn Catalog,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> iceberg::Result<Table> {
        if self.location.is_some() {
            catalog.create_table(namespace, creation).await
        } else {
            create_table_at_location(catalog, namespace, creation).await
        }
    }
}

fn string(value: &Expr) -> Option<String> {
    match value {
        Expr::Value(v) => match &v.value {
            Value::SingleQuotedString(s) => Some(s.clone()),
            _ => None,
        },
        _ => None,
    }
}

fn string_array(value: &Expr) -> Option<Vec<String>> {
    match value {
        Expr::Array(array) => array.elem.iter().map(string).collect(),
        _ => None,
    }
}

/// `MAP(ARRAY['k', ...], ARRAY['v', ...])` as key/value pairs.
fn string_map(value: &Expr) -> Option<Vec<(String, String)>> {
    let Expr::Function(f) = value else {
        return None;
    };
    let name = f.name.0.last()?.as_ident()?;
    let FunctionArguments::List(list) = &f.args else {
        return None;
    };
    let arrays = list
        .args
        .iter()
        .map(|a| match a {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => string_array(e),
            _ => None,
        })
        .collect::<Option<Vec<_>>>()?;
    match arrays.as_slice() {
        [keys, values] if name.value.eq_ignore_ascii_case("map") && keys.len() == values.len() => {
            Some(keys.iter().cloned().zip(values.iter().cloned()).collect())
        }
        _ => None,
    }
}

fn boolean(value: &Expr) -> Option<bool> {
    match value {
        Expr::Value(v) => match &v.value {
            Value::Boolean(b) => Some(*b),
            Value::SingleQuotedString(s) => s.to_ascii_lowercase().parse().ok(),
            _ => None,
        },
        _ => None,
    }
}

/// A Trino data size (`'128MB'`, `'1.5GB'`, a byte count) in bytes. The
/// units are binary, as in Trino: `1kB` is 1024 bytes.
fn data_size(value: &Expr) -> Option<u64> {
    let text = match value {
        Expr::Value(v) => match &v.value {
            Value::Number(n, _) => n.clone(),
            Value::SingleQuotedString(s) => s.clone(),
            _ => return None,
        },
        _ => return None,
    };
    let text = text.trim();
    let split = text
        .find(|c: char| !(c.is_ascii_digit() || c == '.'))
        .unwrap_or(text.len());
    let (number, unit) = text.split_at(split);
    let number: f64 = number.parse().ok()?;
    let power = match unit.trim().to_ascii_uppercase().as_str() {
        "" | "B" => 0,
        "KB" => 1,
        "MB" => 2,
        "GB" => 3,
        "TB" => 4,
        "PB" => 5,
        _ => return None,
    };
    let bytes = number * 1024f64.powi(power);
    (bytes >= 1.0 && bytes.fract() == 0.0 && bytes <= u64::MAX as f64).then_some(bytes as u64)
}

/// Parse all of `text` with `parse`; trailing input is an error.
fn parse_whole<T>(
    text: &str,
    what: &str,
    parse: impl FnOnce(&mut Parser) -> Result<T, ParserError>,
) -> sqe_core::Result<T> {
    let dialect = GenericDialect {};
    let fail = |e: String| invalid(format!("{what}: cannot read '{text}': {e}"));
    let mut parser = Parser::new(&dialect)
        .try_with_sql(text)
        .map_err(|e| fail(e.to_string()))?;
    let parsed = parse(&mut parser).map_err(|e| fail(e.to_string()))?;
    if parser.peek_token().token != Token::EOF {
        return Err(fail("unexpected trailing input".to_string()));
    }
    Ok(parsed)
}

/// A column name as Trino writes it in a property string: unquoted names
/// fold to lower case, `"Quoted"` ones keep their case.
fn column_name(text: &str, what: &str) -> sqe_core::Result<String> {
    let ident = parse_whole(text, what, |p| p.parse_identifier())?;
    Ok(fold_unquoted_ident(&ident))
}

/// A Trino `partitioning` item as the expression `PARTITIONED BY` takes:
/// `bucket(col, n)` / `truncate(col, n)` swap to Spark's argument order and
/// column names fold as SQL identifiers do.
fn trino_partition_expr(item: &str) -> sqe_core::Result<Expr> {
    let bad = |why: &str| invalid(format!("partitioning '{item}': {why}"));
    let column = |e: &Expr| match e {
        Expr::Identifier(id) => Some(Expr::Identifier(Ident::with_quote(
            '"',
            fold_unquoted_ident(id),
        ))),
        _ => None,
    };
    match parse_whole(item, "partitioning", |p| p.parse_expr())? {
        Expr::Function(mut f) => {
            let name = f
                .name
                .0
                .last()
                .and_then(|p| p.as_ident())
                .map(|i| i.value.to_ascii_lowercase())
                .unwrap_or_default();
            let FunctionArguments::List(list) = &mut f.args else {
                return Err(bad("expected a transform such as bucket(column, 16)"));
            };
            let mut args = Vec::new();
            for arg in &list.args {
                match arg {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => args.push(e.clone()),
                    _ => return Err(bad("expected plain arguments")),
                }
            }
            let number =
                |e: &Expr| matches!(e, Expr::Value(v) if matches!(v.value, Value::Number(..)));
            let rewritten = match (name.as_str(), args.as_slice()) {
                ("bucket" | "truncate", [col, n]) if number(n) => {
                    let col = column(col).ok_or_else(|| bad("the first argument is the column"))?;
                    vec![n.clone(), col]
                }
                ("bucket" | "truncate", _) => {
                    return Err(bad(&format!(
                        "Trino takes {name}(column, n), the column first"
                    )))
                }
                ("year" | "month" | "day" | "hour" | "void", [col]) => {
                    vec![column(col).ok_or_else(|| bad("the argument is a column"))?]
                }
                _ => {
                    return Err(bad(
                        "expected a column, year/month/day/hour(column), bucket(column, n), \
                         truncate(column, n) or void(column)",
                    ))
                }
            };
            list.args = rewritten
                .into_iter()
                .map(|e| FunctionArg::Unnamed(FunctionArgExpr::Expr(e)))
                .collect();
            Ok(Expr::Function(f))
        }
        Expr::Identifier(id) => Ok(Expr::Identifier(Ident::new(fold_unquoted_ident(&id)))),
        _ => Err(bad("expected a column or a transform")),
    }
}

/// A `sorted_by` item: `column [ASC | DESC] [NULLS FIRST | NULLS LAST]`.
/// Without NULLS, ascending puts nulls first and descending last, as in
/// Trino and Iceberg.
fn sort_item(item: &str) -> sqe_core::Result<(String, SortDirection, NullOrder)> {
    let parsed = parse_whole(item, "sorted_by", |p| p.parse_order_by_expr())?;
    let Expr::Identifier(id) = &parsed.expr else {
        return Err(invalid(format!(
            "sorted_by '{item}': expected a column, optionally followed by ASC or DESC \
             and NULLS FIRST or NULLS LAST"
        )));
    };
    let asc = parsed.options.asc.unwrap_or(true);
    let nulls_first = parsed.options.nulls_first.unwrap_or(asc);
    Ok((
        fold_unquoted_ident(id),
        if asc {
            SortDirection::Ascending
        } else {
            SortDirection::Descending
        },
        if nulls_first {
            NullOrder::First
        } else {
            NullOrder::Last
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::super::{create_table_options_as_slice, create_table_properties};
    use super::*;
    use iceberg::spec::{FormatVersion, NestedField, PrimitiveType, Type};
    use sqlparser::ast::Statement;
    use std::sync::Arc;

    fn create(sql: &str) -> sqlparser::ast::CreateTable {
        let stmt = Parser::parse_sql(&GenericDialect {}, sql)
            .expect("parse")
            .remove(0);
        let Statement::CreateTable(ct) = stmt else {
            panic!("not a CREATE TABLE");
        };
        ct
    }

    fn parse(with: &str) -> sqe_core::Result<TrinoTableOptions> {
        let ct = create(&format!("CREATE TABLE t (id BIGINT) WITH ({with})"));
        TrinoTableOptions::parse(&ct.table_options, ct.partition_by.is_some())
    }

    fn err(with: &str) -> String {
        parse(with).expect_err(with).to_string()
    }

    fn schema() -> IcebergSchema {
        let field =
            |id, name: &str, t| Arc::new(NestedField::optional(id, name, Type::Primitive(t)));
        IcebergSchema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                field(1, "id", PrimitiveType::Long),
                field(2, "region", PrimitiveType::String),
                field(3, "ts", PrimitiveType::Timestamp),
                field(4, "Mixed", PrimitiveType::Int),
            ])
            .build()
            .unwrap()
    }

    #[test]
    fn trino_keys_map_to_iceberg_properties() {
        let opts = parse(
            "format_version = 3, comment = 'c', gc_enabled = false, format = 'PARQUET', \
             parquet_bloom_filter_columns = ARRAY['id', '\"Mixed\"', 'Region'], \
             parquet_writer_row_group_size = '64MB', \
             extra_properties = MAP(ARRAY['x.y', 'write.merge.mode'], ARRAY['z', 'merge-on-read']), \
             'q.k' = 'v', location = 's3://b/w/t/', \
             partitioning = ARRAY['region'], sorted_by = ARRAY['id']",
        )
        .unwrap();
        let props = create_table_properties(false, FormatVersion::V2, opts.options());
        let get = |k: &str| props.get(k).map(String::as_str);
        assert_eq!(get("gc.enabled"), Some("false"));
        assert_eq!(
            get("write.parquet.bloom-filter-enabled.column.id"),
            Some("true")
        );
        assert_eq!(
            get("write.parquet.bloom-filter-enabled.column.Mixed"),
            Some("true")
        );
        assert_eq!(
            get("write.parquet.bloom-filter-enabled.column.region"),
            Some("true")
        );
        assert_eq!(get("write.parquet.row-group-size-bytes"), Some("67108864"));
        assert_eq!(get("x.y"), Some("z"));
        assert_eq!(get("write.merge.mode"), Some("merge-on-read"));
        assert_eq!(get("q.k"), Some("v"));
        assert_eq!(get("comment"), Some("c"));
        for gone in [
            "format",
            "location",
            "partitioning",
            "sorted_by",
            "format_version",
        ] {
            assert_eq!(get(gone), None, "{gone} is not a stored property");
        }
        assert_eq!(opts.location().as_deref(), Some("s3://b/w/t"));
        assert_eq!(
            super::super::new_table_format_version(opts.options(), None, FormatVersion::V2)
                .unwrap(),
            FormatVersion::V3
        );

        // The translated list prints as SQL that reads back as the same list.
        let printed = opts
            .options()
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        let again = parse(&printed).unwrap();
        assert_eq!(again.options(), opts.options());
    }

    #[test]
    fn unknown_refused_and_malformed_keys_are_errors() {
        let unknown = err("foo = 'bar'");
        assert!(
            unknown.contains("unknown table property `foo`"),
            "{unknown}"
        );
        assert!(unknown.contains("partitioning, sorted_by"), "{unknown}");
        assert!(unknown.contains("('foo')"), "{unknown}");
        let refused = err("compression_codec = 'ZSTD'");
        assert!(refused.contains("iceberg.compression_codec"), "{refused}");
        for key in REFUSED.iter().map(|(k, _)| k) {
            assert!(
                err(&format!("{key} = 'x'")).contains("is not supported"),
                "{key}"
            );
        }
        assert!(err("format = 'ORC'").contains("Parquet only"));
        assert!(err("partitioning = 'region'").contains("ARRAY of strings"));
        assert!(err("gc_enabled = 'maybe'").contains("true or false"));
        assert!(err("parquet_writer_row_group_size = '12 parsecs'").contains("data size"));
        assert!(
            err("extra_properties = MAP(ARRAY['format-version'], ARRAY['3'])").contains("reserved")
        );
        assert!(err("extra_properties = MAP(ARRAY['a', 'b'], ARRAY['1'])").contains("MAP("));
        assert!(err("sorted_by = ARRAY['id sideways']").contains("sorted_by"));
        assert!(err("partitioning = ARRAY['bucket(4, id)']").contains("column first"));
        assert!(err("partitioning = ARRAY['region; DROP TABLE t']").contains("partitioning"));
        // Quoted keys are Iceberg properties, whatever their name.
        assert!(parse("'foo' = 'bar', 'partitioning' = 'x'").is_ok());
        // `partitioning` and PARTITIONED BY contradict each other.
        let ct = create("CREATE TABLE t (id BIGINT) WITH (partitioning = ARRAY['id'])");
        let both = TrinoTableOptions::parse(&ct.table_options, true)
            .expect_err("both")
            .to_string();
        assert!(both.contains("cannot both be given"), "{both}");
    }

    #[test]
    fn data_sizes_are_binary_units() {
        let size = |s: &str| data_size(&Expr::value(Value::SingleQuotedString(s.into())));
        assert_eq!(size("128MB"), Some(128 << 20));
        assert_eq!(size("1kB"), Some(1024));
        assert_eq!(size("1.5GB"), Some(3 << 29));
        assert_eq!(size("4096"), Some(4096));
        assert_eq!(size("0B"), None);
        assert_eq!(size("1.5B"), None);
        assert_eq!(size("MB"), None);
    }

    #[test]
    fn partitioning_takes_trino_argument_order() {
        let opts = parse(
            "partitioning = ARRAY['Region', 'bucket(id, 4)', 'truncate(region, 2)', \
             'month(ts)', '\"Mixed\"']",
        )
        .unwrap();
        let spec = opts
            .partition_spec(None, &schema())
            .unwrap()
            .expect("a spec");
        let got: Vec<_> = spec
            .fields()
            .iter()
            .map(|f| (f.source_id, f.transform))
            .collect();
        assert_eq!(
            got,
            vec![
                (2, Transform::Identity),
                (1, Transform::Bucket(4)),
                (2, Transform::Truncate(2)),
                (3, Transform::Month),
                (4, Transform::Identity),
            ]
        );
        assert!(parse("partitioning = ARRAY[]")
            .unwrap()
            .partition_spec(None, &schema())
            .unwrap()
            .is_none());
        let missing = parse("partitioning = ARRAY['nope']").unwrap();
        assert!(missing.partition_spec(None, &schema()).is_err());
    }

    #[test]
    fn sorted_by_builds_the_default_sort_order() {
        let opts = parse("sorted_by = ARRAY['id DESC', 'region', 'ts ASC NULLS LAST', '\"Mixed\" DESC NULLS FIRST']")
            .unwrap();
        let order = opts.sort_order(&schema()).unwrap().expect("an order");
        assert_eq!(order.order_id, 1);
        let got: Vec<_> = order
            .fields
            .iter()
            .map(|f| (f.source_id, f.direction, f.null_order))
            .collect();
        assert_eq!(
            got,
            vec![
                (1, SortDirection::Descending, NullOrder::Last),
                (2, SortDirection::Ascending, NullOrder::First),
                (3, SortDirection::Ascending, NullOrder::Last),
                (4, SortDirection::Descending, NullOrder::First),
            ]
        );
        let missing = parse("sorted_by = ARRAY['nope']").unwrap();
        assert!(missing.sort_order(&schema()).is_err());
        assert!(parse("comment = 'c'")
            .unwrap()
            .sort_order(&schema())
            .unwrap()
            .is_none());
    }

    #[test]
    fn other_option_forms_pass_through() {
        let ct = create("CREATE TABLE t (id BIGINT) TBLPROPERTIES ('a' = '1')");
        let opts = TrinoTableOptions::parse(&ct.table_options, false).unwrap();
        assert_eq!(
            opts.options(),
            create_table_options_as_slice(&ct.table_options)
        );
    }

    /// The SET TBLPROPERTIES list the front door makes of `SET PROPERTIES`.
    fn alter(set: &str) -> sqe_core::Result<TrinoTableOptions> {
        let sql = format!("ALTER TABLE t SET TBLPROPERTIES ({set})");
        let stmt = Parser::parse_sql(&GenericDialect {}, &sql)
            .expect("parse")
            .remove(0);
        let Statement::AlterTable(at) = stmt else {
            panic!("not an ALTER TABLE");
        };
        let Some(sqlparser::ast::AlterTableOperation::SetTblProperties { table_properties }) =
            at.operations.first()
        else {
            panic!("not SET TBLPROPERTIES");
        };
        TrinoTableOptions::parse_alter(table_properties)
    }

    #[test]
    fn alter_knows_unquoted_trino_keys() {
        let opts = alter(
            r#"partitioning = ARRAY['bucket(id, 4)'], sorted_by = ARRAY['ts DESC'],
               extra_properties = MAP(ARRAY['owner'], ARRAY['etl']),
               gc_enabled = false, "write.delete.mode" = 'merge-on-read', custom = 'x'"#,
        )
        .unwrap();
        assert!(opts.changes_layout());
        let props = create_table_properties(false, FormatVersion::V2, opts.options());
        assert_eq!(props.get("owner").map(String::as_str), Some("etl"));
        assert_eq!(props.get("gc.enabled").map(String::as_str), Some("false"));
        assert_eq!(
            props.get("write.delete.mode").map(String::as_str),
            Some("merge-on-read")
        );
        // An unknown unquoted key stays a property, as SET PROPERTIES stored it.
        assert_eq!(props.get("custom").map(String::as_str), Some("x"));
        assert!(!props.contains_key("partitioning"));
        assert!(!alter("comment = 'c'").unwrap().changes_layout());
    }

    #[test]
    fn alter_quoted_keys_stay_iceberg_properties() {
        // SET TBLPROPERTIES quotes every key; those keep their old meaning.
        let opts = alter(
            r#"'location' = 's3://b/t', 'sorted_by' = 'x', "max_commit_retry" = '4',
               'gc_enabled' = 'true'"#,
        )
        .unwrap();
        assert!(!opts.changes_layout());
        let props = create_table_properties(false, FormatVersion::V2, opts.options());
        for key in ["location", "sorted_by", "max_commit_retry", "gc_enabled"] {
            assert!(props.contains_key(key), "{key}: {props:?}");
        }
        assert!(!props.contains_key("gc.enabled"));
    }

    #[test]
    fn alter_refuses_default_location_and_refused_keys() {
        for (set, want) in [
            ("sorted_by = DEFAULT", "DEFAULT is not supported"),
            (
                r#""write.delete.mode" = DEFAULT"#,
                "DEFAULT is not supported",
            ),
            (
                "location = 's3://b/t'",
                "cannot change the table property `location`",
            ),
            ("max_commit_retry = 4", "not supported"),
            ("partitioning = 'id'", "must be an ARRAY of strings"),
        ] {
            let e = alter(set).expect_err(set).to_string();
            assert!(e.contains(want), "{set}: {e}");
        }
    }
}
