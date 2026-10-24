//! Row-group pruning on shredded variant fields (sqe#870).
//!
//! A shredded variant column stores each shredded field as its own Parquet
//! leaf, `v.typed_value.<f>.typed_value`, with ordinary min/max statistics.
//! Those leaves carry no Iceberg field id, so an Iceberg predicate cannot
//! name them. This module reads a filter such as
//! `variant_get(v, '$.a', 'bigint') > 10`, finds the leaf for `$.a` in each
//! file's Parquet schema and drops the row groups whose statistics exclude
//! the comparison.
//!
//! A row group is only pruned when its statistics prove that every row reads
//! the field from the typed leaf (or reads it as missing). A row whose value
//! sits in a residual `value` column is unshredded: its field could hold
//! anything, so the leaf's min/max say nothing about it. Each rule below
//! needs a statistic the writer may omit; a missing one keeps the row group.
//!
//! The filter itself stays in the plan (`Inexact`), so pruning only ever
//! skips rows the filter would have rejected.

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{Expr, Operator};
use datafusion::physical_plan::metrics::Count;
use datafusion::scalar::ScalarValue;
use iceberg::arrow::RowGroupPruner;
use parquet::basic::{
    ColumnOrder, ConvertedType, LogicalType, Repetition, SortOrder, Type as PhysicalType,
};
use parquet::file::metadata::{ParquetMetaData, RowGroupMetaData};
use parquet::file::statistics::Statistics;
use parquet::schema::types::SchemaDescriptor;

/// The SQL type a `variant_get` call reads the field as, for the types
/// whose Parquet statistics order matches SQL order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Target {
    Int32,
    Int64,
    Utf8,
    Date32,
    Boolean,
}

impl Target {
    fn parse(name: &str) -> Option<Self> {
        match name.trim().to_ascii_lowercase().as_str() {
            "int" | "integer" | "int32" => Some(Self::Int32),
            "bigint" | "long" | "int64" => Some(Self::Int64),
            "string" | "varchar" | "text" | "utf8" => Some(Self::Utf8),
            "date" | "date32" => Some(Self::Date32),
            "boolean" | "bool" => Some(Self::Boolean),
            _ => None,
        }
    }

    fn is_integer(self) -> bool {
        matches!(self, Self::Int32 | Self::Int64)
    }
}

/// The literal side of the comparison, in the domain the leaf's statistics
/// are compared in.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Bound {
    Int(i64),
    Bytes(Vec<u8>),
    Bool(bool),
}

/// The comparison, with the field on the left.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Cmp {
    Eq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

impl Cmp {
    fn from_op(op: Operator) -> Option<Self> {
        match op {
            Operator::Eq => Some(Self::Eq),
            Operator::Lt => Some(Self::Lt),
            Operator::LtEq => Some(Self::LtEq),
            Operator::Gt => Some(Self::Gt),
            Operator::GtEq => Some(Self::GtEq),
            _ => None,
        }
    }

    /// `lit <op> field` as `field <op'> lit`.
    fn flip(self) -> Self {
        match self {
            Self::Eq => Self::Eq,
            Self::Lt => Self::Gt,
            Self::LtEq => Self::GtEq,
            Self::Gt => Self::Lt,
            Self::GtEq => Self::LtEq,
        }
    }

    /// True when no value in `[min, max]` can satisfy `value <op> c`.
    fn excludes(self, min: &Bound, max: &Bound, c: &Bound) -> bool {
        match self {
            Self::Eq => c < min || c > max,
            Self::Lt => min >= c,
            Self::LtEq => min > c,
            Self::Gt => max <= c,
            Self::GtEq => max < c,
        }
    }
}

/// One `variant_get(col, '$.path', 'type') <op> literal` filter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VariantPrune {
    column: String,
    field_id: Option<i32>,
    path: Vec<String>,
    target: Target,
    cmp: Cmp,
    value: Bound,
}

impl VariantPrune {
    /// Reads a filter in the shape this module prunes on, or `None`.
    ///
    /// The variant argument must be a bare column of `schema` holding
    /// variant storage: a policy guard (`CASE WHEN ... THEN v END`) or a
    /// masked column never matches.
    pub fn from_expr(expr: &Expr, schema: &Schema) -> Option<Self> {
        let Expr::BinaryExpr(binary) = expr else {
            return None;
        };
        let cmp = Cmp::from_op(binary.op)?;
        let (call, literal, cmp) = match (&*binary.left, &*binary.right) {
            (Expr::Literal(lit, _), other) => (other, lit, cmp.flip()),
            (other, Expr::Literal(lit, _)) => (other, lit, cmp),
            _ => return None,
        };
        let (call, widened) = match call {
            // `variant_get(.., 'int') > 5` coerces to `CAST(.. AS Int64) > 5`.
            // Widening an integer keeps its order.
            Expr::Cast(cast) if cast.field.data_type() == &DataType::Int64 => (&*cast.expr, true),
            other => (other, false),
        };
        let Expr::ScalarFunction(ScalarFunction { func, args }) = call else {
            return None;
        };
        if !matches!(func.name(), "variant_get" | "try_variant_get") || args.len() != 3 {
            return None;
        }
        let Expr::Column(column) = &args[0] else {
            return None;
        };
        let field = schema.field_with_name(&column.name).ok()?;
        if !sqe_core::variant::is_variant_storage(field.data_type()) {
            return None;
        }
        let path = parse_path(string_literal(&args[1])?)?;
        let target = Target::parse(string_literal(&args[2])?)?;
        if widened && !target.is_integer() {
            return None;
        }
        Some(Self {
            column: column.name.clone(),
            field_id: None,
            path,
            target,
            cmp,
            value: bound_of(literal, target)?,
        })
    }

    /// The column's Iceberg field id, used to find its Parquet group even
    /// after a rename.
    pub fn with_field_id(mut self, iceberg_schema: &iceberg::spec::Schema) -> Self {
        self.field_id = iceberg_schema.field_by_name(&self.column).map(|f| f.id);
        self
    }

    /// True when the row group cannot hold a row that passes the filter.
    fn excludes(&self, metadata: &ParquetMetaData, row_group: &RowGroupMetaData) -> bool {
        let schema = metadata.file_metadata().schema_descr();
        let Some(leaves) = Leaves::find(schema, self.field_id, &self.column, &self.path) else {
            return false;
        };
        let Some(typed) = leaf_stats(row_group, leaves.typed) else {
            return false;
        };
        let rows = row_group.num_rows().max(0) as u64;
        let all_null = |leaf: Option<usize>| match leaf {
            // An absent residual column holds nothing.
            None => true,
            Some(i) => leaf_stats(row_group, i).and_then(Statistics::null_count_opt) == Some(rows),
        };
        let fields_typed = leaves.field_residuals.iter().all(|&r| all_null(r));
        let Some(typed_nulls) = typed.null_count_opt() else {
            return false;
        };
        // A: the leaf is null only where the whole variant is null.
        let rule_a = leaf_stats(row_group, leaves.metadata)
            .and_then(Statistics::null_count_opt)
            .is_some_and(|m| m == typed_nulls);
        // H: no row fell back to the top-level residual (the definition
        // level "variant present, typed_value null"), and no field on the
        // path was written as a residual.
        let rule_h = fields_typed
            && match leaves.top_typed_null_level {
                // A required typed_value is never null.
                None => true,
                Some(level) => row_group
                    .column(leaves.typed)
                    .definition_level_histogram()
                    .is_some_and(|h| h.values().get(level) == Some(&0)),
            };
        // B: no residual at the top level or on the path.
        let rule_b = fields_typed && all_null(leaves.top_residual);
        if !(rule_a || rule_h || rule_b) {
            return false;
        }
        if typed_nulls == rows {
            // Every row reads the field as NULL.
            return true;
        }
        let descr = schema.column(leaves.typed);
        let column_order = metadata.file_metadata().column_order(leaves.typed);
        match bounds(typed, &descr, column_order, self.target) {
            Some((min, max)) => self.cmp.excludes(&min, &max, &self.value),
            None => false,
        }
    }
}

/// The Parquet leaves one shredded field path reads.
struct Leaves {
    typed: usize,
    metadata: usize,
    /// `v.value`; `None` when the column was not written.
    top_residual: Option<usize>,
    /// `<field>.value` for each step of the path.
    field_residuals: Vec<Option<usize>>,
    /// The definition level of a row whose variant is present but whose
    /// top-level `typed_value` is null: 1 under an optional column, 0 under
    /// a required one. `None` when `typed_value` is required.
    top_typed_null_level: Option<usize>,
}

impl Leaves {
    fn find(
        schema: &SchemaDescriptor,
        field_id: Option<i32>,
        name: &str,
        path: &[String],
    ) -> Option<Self> {
        let roots = schema.root_schema().get_fields();
        let root_type = field_id
            .and_then(|id| {
                roots.iter().find(|t| {
                    let info = t.get_basic_info();
                    info.has_id() && info.id() == id
                })
            })
            .or_else(|| roots.iter().find(|t| t.name() == name))?;
        let optional = |t: &parquet::schema::types::Type| {
            let info = t.get_basic_info();
            !info.has_repetition() || info.repetition() != Repetition::REQUIRED
        };
        let top_typed = root_type
            .is_group()
            .then(|| root_type.get_fields())
            .and_then(|f| f.iter().find(|t| t.name() == "typed_value"))?;
        let top_typed_null_level = optional(top_typed).then_some(usize::from(optional(root_type)));
        let root = root_type.name().to_string();
        let leaf = |parts: &[&str]| {
            schema.columns().iter().position(|c| {
                c.path()
                    .parts()
                    .iter()
                    .map(String::as_str)
                    .eq(parts.iter().copied())
            })
        };
        let mut prefix: Vec<&str> = vec![&root, "typed_value"];
        let mut field_residuals = Vec::with_capacity(path.len());
        for step in path {
            prefix.push(step);
            let mut residual = prefix.clone();
            residual.push("value");
            field_residuals.push(leaf(&residual));
            prefix.push("typed_value");
        }
        Some(Self {
            typed: leaf(&prefix)?,
            metadata: leaf(&[&root, "metadata"])?,
            top_residual: leaf(&[&root, "value"]),
            field_residuals,
            top_typed_null_level,
        })
    }
}

fn leaf_stats(row_group: &RowGroupMetaData, leaf: usize) -> Option<&Statistics> {
    row_group.column(leaf).statistics()
}

/// The leaf's min and max in the target's domain, when the leaf's Parquet
/// type reads as the target without conversion.
fn bounds(
    stats: &Statistics,
    descr: &parquet::schema::types::ColumnDescriptor,
    order: ColumnOrder,
    target: Target,
) -> Option<(Bound, Bound)> {
    if stats.is_min_max_deprecated() {
        return None;
    }
    let logical = descr.logical_type_ref();
    let signed = match logical {
        None => true,
        Some(LogicalType::Integer(i)) => i.is_signed,
        Some(_) => false,
    };
    let plain_int = signed
        && matches!(
            descr.converted_type(),
            ConvertedType::NONE
                | ConvertedType::INT_8
                | ConvertedType::INT_16
                | ConvertedType::INT_32
                | ConvertedType::INT_64
        );
    match (target, descr.physical_type(), stats) {
        (Target::Int32 | Target::Int64, PhysicalType::INT32, Statistics::Int32(s)) if plain_int => {
            Some((
                Bound::Int((*s.min_opt()?).into()),
                Bound::Int((*s.max_opt()?).into()),
            ))
        }
        (Target::Int64, PhysicalType::INT64, Statistics::Int64(s)) if plain_int => {
            Some((Bound::Int(*s.min_opt()?), Bound::Int(*s.max_opt()?)))
        }
        // Reading an INT64 as `int` errors (or is NULL) for a value out of
        // range; only prune when every value fits.
        (Target::Int32, PhysicalType::INT64, Statistics::Int64(s)) if plain_int => {
            let (min, max) = (*s.min_opt()?, *s.max_opt()?);
            let fits = |v: i64| i32::try_from(v).is_ok();
            (fits(min) && fits(max)).then_some((Bound::Int(min), Bound::Int(max)))
        }
        (Target::Date32, PhysicalType::INT32, Statistics::Int32(s))
            if matches!(logical, Some(LogicalType::Date)) =>
        {
            Some((
                Bound::Int((*s.min_opt()?).into()),
                Bound::Int((*s.max_opt()?).into()),
            ))
        }
        (Target::Boolean, PhysicalType::BOOLEAN, Statistics::Boolean(s)) => {
            Some((Bound::Bool(*s.min_opt()?), Bound::Bool(*s.max_opt()?)))
        }
        // Byte-wise order is SQL string order. A truncated min is still a
        // lower bound and a truncated max is rounded up, so both bound the
        // values.
        (Target::Utf8, PhysicalType::BYTE_ARRAY, Statistics::ByteArray(s))
            if (matches!(logical, Some(LogicalType::String))
                || descr.converted_type() == ConvertedType::UTF8)
                && order == ColumnOrder::TYPE_DEFINED_ORDER(SortOrder::UNSIGNED) =>
        {
            Some((
                Bound::Bytes(s.min_bytes_opt()?.to_vec()),
                Bound::Bytes(s.max_bytes_opt()?.to_vec()),
            ))
        }
        _ => None,
    }
}

fn string_literal(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Literal(
            ScalarValue::Utf8(Some(s))
            | ScalarValue::LargeUtf8(Some(s))
            | ScalarValue::Utf8View(Some(s)),
            _,
        ) => Some(s),
        _ => None,
    }
}

/// `$.a.b` as `["a", "b"]`. Array steps and quoted names are not shredded
/// object fields and are refused.
fn parse_path(path: &str) -> Option<Vec<String>> {
    let rest = path.trim().strip_prefix('$')?;
    if rest.is_empty() {
        return Some(Vec::new());
    }
    let rest = rest.strip_prefix('.')?;
    let steps: Vec<String> = rest.split('.').map(str::to_string).collect();
    let plain = |s: &String| !s.is_empty() && !s.contains(['[', ']', '\'', '"', '*', ' ']);
    steps.iter().all(plain).then_some(steps)
}

fn bound_of(literal: &ScalarValue, target: Target) -> Option<Bound> {
    let int = |v: Option<i64>| v.map(Bound::Int);
    match (target, literal) {
        (Target::Int32 | Target::Int64, ScalarValue::Int8(v)) => int(v.map(i64::from)),
        (Target::Int32 | Target::Int64, ScalarValue::Int16(v)) => int(v.map(i64::from)),
        (Target::Int32 | Target::Int64, ScalarValue::Int32(v)) => int(v.map(i64::from)),
        (Target::Int32 | Target::Int64, ScalarValue::Int64(v)) => int(*v),
        (Target::Int32 | Target::Int64, ScalarValue::UInt8(v)) => int(v.map(i64::from)),
        (Target::Int32 | Target::Int64, ScalarValue::UInt16(v)) => int(v.map(i64::from)),
        (Target::Int32 | Target::Int64, ScalarValue::UInt32(v)) => int(v.map(i64::from)),
        (Target::Date32, ScalarValue::Date32(v)) => int(v.map(i64::from)),
        (Target::Boolean, ScalarValue::Boolean(v)) => v.map(Bound::Bool),
        (
            Target::Utf8,
            ScalarValue::Utf8(v) | ScalarValue::LargeUtf8(v) | ScalarValue::Utf8View(v),
        ) => v.as_ref().map(|s| Bound::Bytes(s.as_bytes().to_vec())),
        _ => None,
    }
}

/// True when the filter is one this module prunes on.
pub fn is_variant_prune_filter(expr: &Expr, schema: &Schema) -> bool {
    VariantPrune::from_expr(expr, schema).is_some()
}

/// Drops the row groups any of the filters excludes, for the reader hook.
#[derive(Debug)]
pub struct VariantRowGroupPruner {
    prunes: Arc<[VariantPrune]>,
    pruned: Count,
}

impl VariantRowGroupPruner {
    pub fn new(prunes: Arc<[VariantPrune]>, pruned: Count) -> Self {
        Self { prunes, pruned }
    }
}

impl RowGroupPruner for VariantRowGroupPruner {
    fn select(&self, metadata: &ParquetMetaData, candidates: Vec<usize>) -> Vec<usize> {
        let before = candidates.len();
        let kept: Vec<usize> = candidates
            .into_iter()
            .filter(|&i| {
                let row_group = metadata.row_group(i);
                !self.prunes.iter().any(|p| p.excludes(metadata, row_group))
            })
            .collect();
        self.pruned.add(before - kept.len());
        kept
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{
        Array, ArrayRef, BooleanArray, RecordBatch, StringArray, StructArray,
    };
    use datafusion::arrow::compute::kernels::{nullif::nullif, zip::zip};
    use datafusion::arrow::datatypes::{Field, Fields};
    use datafusion::common::Column;
    use datafusion::execution::FunctionRegistry;
    use datafusion::logical_expr::{BinaryExpr, Cast};
    use datafusion::prelude::SessionContext;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use parquet_variant_compute::{json_to_variant, shred_variant};

    use super::*;

    fn schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            sqe_core::variant::field("v", true),
        ])
    }

    fn get(func: &str, column: &str, path: &str, ty: &str) -> Expr {
        let ctx = SessionContext::new();
        sqe_core::variant_get::register_variant_functions(&ctx);
        Expr::ScalarFunction(ScalarFunction {
            func: ctx.udf(func).expect("registered"),
            args: vec![
                Expr::Column(Column::from_name(column)),
                Expr::Literal(ScalarValue::Utf8(Some(path.into())), None),
                Expr::Literal(ScalarValue::Utf8(Some(ty.into())), None),
            ],
        })
    }

    fn cmp(left: Expr, op: Operator, right: Expr) -> Expr {
        Expr::BinaryExpr(BinaryExpr::new(Box::new(left), op, Box::new(right)))
    }

    fn int(v: i64) -> Expr {
        Expr::Literal(ScalarValue::Int64(Some(v)), None)
    }

    fn text(v: &str) -> Expr {
        Expr::Literal(ScalarValue::Utf8(Some(v.into())), None)
    }

    fn a_bigint(op: Operator, v: i64) -> Expr {
        cmp(get("variant_get", "v", "$.a", "bigint"), op, int(v))
    }

    #[test]
    fn reads_the_supported_shapes() {
        let s = schema();
        let p = VariantPrune::from_expr(
            &cmp(
                get("variant_get", "v", "$.a.b", "bigint"),
                Operator::Gt,
                int(5),
            ),
            &s,
        )
        .expect("bigint >");
        assert_eq!(p.path, vec!["a".to_string(), "b".to_string()]);
        assert_eq!(
            (p.target, p.cmp, p.value),
            (Target::Int64, Cmp::Gt, Bound::Int(5))
        );

        // Literal on the left flips the operator.
        let p = VariantPrune::from_expr(
            &cmp(
                int(5),
                Operator::Lt,
                get("try_variant_get", "v", "$.a", "bigint"),
            ),
            &s,
        )
        .expect("5 < get");
        assert_eq!(p.cmp, Cmp::Gt);

        // `int` widened to Int64 by coercion.
        let widened = Expr::Cast(Cast::new(
            Box::new(get("variant_get", "v", "$.a", "int")),
            DataType::Int64,
        ));
        assert!(VariantPrune::from_expr(&cmp(widened, Operator::Eq, int(5)), &s).is_some());
    }

    #[test]
    fn refuses_other_shapes() {
        let s = schema();
        let refused = [
            // `!=` cannot prune on min/max.
            a_bigint(Operator::NotEq, 5),
            // Not a variant column.
            cmp(
                get("variant_get", "id", "$.a", "bigint"),
                Operator::Eq,
                int(5),
            ),
            // Array steps and quoted names are not shredded object fields.
            cmp(
                get("variant_get", "v", "$.a[0]", "bigint"),
                Operator::Eq,
                int(5),
            ),
            cmp(
                get("variant_get", "v", "$['a']", "bigint"),
                Operator::Eq,
                int(5),
            ),
            // Float statistics order differs from SQL order (NaN, -0.0).
            cmp(
                get("variant_get", "v", "$.a", "double"),
                Operator::Eq,
                Expr::Literal(ScalarValue::Float64(Some(1.0)), None),
            ),
            // Literal of another type.
            cmp(
                get("variant_get", "v", "$.a", "string"),
                Operator::Eq,
                int(5),
            ),
            Expr::BinaryExpr(BinaryExpr::new(
                Box::new(get("variant_get", "v", "$.a", "bigint")),
                Operator::Eq,
                Box::new(Expr::Literal(ScalarValue::Int64(None), None)),
            )),
        ];
        for expr in refused {
            assert!(VariantPrune::from_expr(&expr, &s).is_none(), "{expr}");
        }
    }

    /// `rows` (JSON, one per row) as variant column `v`, shredded as
    /// `typed` and written `per_group` rows per row group. A row listed in
    /// `misfit` keeps its whole value unshredded, as the writer stores a row
    /// that does not fit the file's shredding plan.
    fn shredded_file(
        rows: &[Option<&str>],
        typed: Fields,
        per_group: usize,
        misfit: &[usize],
    ) -> Arc<ParquetMetaData> {
        shredded_file_with(rows, typed, per_group, misfit, true)
    }

    /// `nullable` false writes `v` as a required group, which shifts every
    /// definition level below it down by one.
    fn shredded_file_with(
        rows: &[Option<&str>],
        typed: Fields,
        per_group: usize,
        misfit: &[usize],
        nullable: bool,
    ) -> Arc<ParquetMetaData> {
        let json: ArrayRef = Arc::new(StringArray::from(rows.to_vec()));
        let variants = json_to_variant(&json).expect("json");
        let shredded = shred_variant(&variants, &DataType::Struct(typed))
            .expect("shred")
            .into_inner();
        let column: ArrayRef = if misfit.is_empty() {
            Arc::new(shredded)
        } else {
            let mask: BooleanArray = (0..rows.len()).map(|r| Some(misfit.contains(&r))).collect();
            let (fields, mut children, nulls) = shredded.into_parts();
            let at = |name: &str| fields.iter().position(|f| f.name() == name).expect(name);
            let (value, typed_value) = (at("value"), at("typed_value"));
            let original = datafusion::arrow::compute::cast(
                variants.value_column(),
                children[value].data_type(),
            )
            .expect("cast");
            children[value] = zip(&mask, &original, &children[value]).expect("zip");
            children[typed_value] = nullif(children[typed_value].as_ref(), &mask).expect("nullif");
            Arc::new(StructArray::try_new(fields, children, nulls).expect("struct"))
        };
        let field = Field::new("v", column.data_type().clone(), nullable);
        let batch =
            RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![column]).expect("batch");
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(per_group))
            .build();
        let mut out = Vec::new();
        let mut writer =
            ArrowWriter::try_new(&mut out, batch.schema(), Some(props)).expect("writer");
        writer.write(&batch).expect("write");
        writer.close().expect("close");
        let reader =
            ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(out)).expect("read");
        Arc::clone(reader.metadata())
    }

    fn fields(name: &str, ty: DataType) -> Fields {
        Fields::from(vec![Field::new(name, ty, true)])
    }

    fn kept(meta: &ParquetMetaData, filter: Expr) -> Vec<usize> {
        let prune = VariantPrune::from_expr(&filter, &schema()).expect("prunable filter");
        let pruned = Count::new();
        let pruner = VariantRowGroupPruner::new(Arc::from(vec![prune]), pruned.clone());
        let all = meta.num_row_groups();
        let kept = pruner.select(meta, (0..all).collect());
        assert_eq!(pruned.value(), all - kept.len());
        kept
    }

    fn ints(values: &[i64]) -> Vec<String> {
        values.iter().map(|v| format!(r#"{{"a": {v}}}"#)).collect()
    }

    #[test]
    fn prunes_groups_outside_the_range() {
        let rows = ints(&[1, 2, 3, 4, 5, 6, 7, 8]);
        let rows: Vec<Option<&str>> = rows.iter().map(|r| Some(r.as_str())).collect();
        let meta = shredded_file(&rows, fields("a", DataType::Int64), 2, &[]);
        assert_eq!(meta.num_row_groups(), 4);
        assert_eq!(kept(&meta, a_bigint(Operator::Gt, 6)), vec![3]);
        assert_eq!(kept(&meta, a_bigint(Operator::GtEq, 6)), vec![2, 3]);
        assert_eq!(kept(&meta, a_bigint(Operator::Lt, 3)), vec![0]);
        assert_eq!(kept(&meta, a_bigint(Operator::LtEq, 3)), vec![0, 1]);
        assert_eq!(kept(&meta, a_bigint(Operator::Eq, 4)), vec![1]);
        assert_eq!(
            kept(&meta, a_bigint(Operator::Eq, 100)),
            Vec::<usize>::new()
        );
        // `int` reads an INT64 leaf whose values all fit.
        let as_int = cmp(
            get("try_variant_get", "v", "$.a", "int"),
            Operator::Gt,
            int(6),
        );
        assert_eq!(kept(&meta, as_int), vec![3]);
    }

    #[test]
    fn missing_fields_and_null_variants_read_as_null() {
        let rows = [
            Some(r#"{"a": 1}"#),
            Some("{}"),
            None,
            Some(r#"{"b": 2}"#),
            None,
            None,
        ];
        let meta = shredded_file(&rows, fields("a", DataType::Int64), 2, &[]);
        // Group 0 holds a=1 and a row without `a`; groups 1 and 2 have no `a`.
        assert_eq!(kept(&meta, a_bigint(Operator::Gt, 5)), Vec::<usize>::new());
        assert_eq!(kept(&meta, a_bigint(Operator::Eq, 1)), vec![0]);
    }

    #[test]
    fn keeps_groups_with_unshredded_values() {
        // `a` is a string in row 2: it lands in the field's residual column,
        // whose bytes the typed leaf's statistics do not describe.
        let rows = [
            Some(r#"{"a": 1}"#),
            Some(r#"{"a": 2}"#),
            Some(r#"{"a": "500"}"#),
            Some(r#"{"a": 4}"#),
            Some(r#"{"a": 5}"#),
            Some(r#"{"a": 6}"#),
        ];
        let meta = shredded_file(&rows, fields("a", DataType::Int64), 2, &[]);
        assert_eq!(kept(&meta, a_bigint(Operator::Gt, 100)), vec![1]);

        // A non-object row is unshredded at the top level.
        let rows = [
            Some(r#"{"a": 1}"#),
            Some("7"),
            Some(r#"{"a": 3}"#),
            Some(r#"{"a": 4}"#),
        ];
        let meta = shredded_file(&rows, fields("a", DataType::Int64), 2, &[]);
        assert_eq!(kept(&meta, a_bigint(Operator::Gt, 100)), vec![0]);

        // A row that did not fit the file's plan keeps its whole value in
        // `v.value`, though its own shredding would have produced a typed `a`.
        let rows = [
            Some(r#"{"a": 1}"#),
            Some(r#"{"a": 999}"#),
            Some(r#"{"a": 3}"#),
            Some(r#"{"a": 4}"#),
        ];
        let meta = shredded_file(&rows, fields("a", DataType::Int64), 2, &[1]);
        assert_eq!(kept(&meta, a_bigint(Operator::Gt, 100)), vec![0]);

        // The same on a NOT NULL column, where "no top-level typed_value" is
        // definition level 0, not 1.
        let meta = shredded_file_with(&rows, fields("a", DataType::Int64), 2, &[1], false);
        assert_eq!(kept(&meta, a_bigint(Operator::Gt, 100)), vec![0]);
    }

    #[test]
    fn keeps_groups_when_the_leaf_type_does_not_match() {
        let rows = [
            Some(r#"{"a": "x"}"#),
            Some(r#"{"a": "y"}"#),
            Some(r#"{"a": "m"}"#),
            Some(r#"{"a": "n"}"#),
        ];
        let meta = shredded_file(&rows, fields("a", DataType::Utf8), 2, &[]);
        // A string leaf read as bigint: no bounds in that domain.
        assert_eq!(kept(&meta, a_bigint(Operator::Eq, 5)), vec![0, 1]);
        let as_string = cmp(
            get("variant_get", "v", "$.a", "string"),
            Operator::Eq,
            text("n"),
        );
        assert_eq!(kept(&meta, as_string), vec![1]);

        // An INT64 leaf read as `int` with a value out of range: reading it
        // fails (or is NULL), so the bounds do not apply.
        let rows = ints(&[1, 3_000_000_000, 5, 6]);
        let rows: Vec<Option<&str>> = rows.iter().map(|r| Some(r.as_str())).collect();
        let meta = shredded_file(&rows, fields("a", DataType::Int64), 2, &[]);
        let as_int = cmp(
            get("try_variant_get", "v", "$.a", "int"),
            Operator::Lt,
            int(0),
        );
        assert_eq!(kept(&meta, as_int), vec![0]);
    }

    #[test]
    fn prunes_on_a_nested_path() {
        let rows = [
            Some(r#"{"o": {"b": 1}}"#),
            Some(r#"{"o": {"b": 2}}"#),
            Some(r#"{"o": {"b": 30}}"#),
            Some(r#"{"o": {"b": 40}}"#),
        ];
        let inner = DataType::Struct(fields("b", DataType::Int64));
        let meta = shredded_file(&rows, fields("o", inner), 2, &[]);
        let filter = cmp(
            get("variant_get", "v", "$.o.b", "bigint"),
            Operator::GtEq,
            int(10),
        );
        assert_eq!(kept(&meta, filter), vec![1]);
    }

    #[test]
    fn an_unshredded_file_is_kept() {
        let rows = ints(&[1, 2]);
        let json: ArrayRef = Arc::new(StringArray::from(
            rows.iter().map(|r| Some(r.as_str())).collect::<Vec<_>>(),
        ));
        let column: ArrayRef = Arc::new(json_to_variant(&json).expect("json").into_inner());
        assert!(column.len() == 2);
        let field = Field::new("v", column.data_type().clone(), true);
        let batch =
            RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![column]).expect("batch");
        let mut out = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut out, batch.schema(), None).expect("writer");
        writer.write(&batch).expect("write");
        writer.close().expect("close");
        let meta = Arc::clone(
            ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(out))
                .expect("read")
                .metadata(),
        );
        assert_eq!(kept(&meta, a_bigint(Operator::Gt, 100)), vec![0]);
    }
}
