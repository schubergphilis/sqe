//! `variant_get` / `try_variant_get`: typed field access on a variant (#857).
//!
//! `variant_get(v, '$.a.b[0]', 'int')` reads one field of a variant column
//! straight out of its binary encoding with arrow's `variant_get` kernel, so
//! the value is never rendered as JSON text first (the `json_*` functions do
//! that; see `variant_json_args`). The shape follows Spark:
//!
//! - the path is `$`, then `.field`, `['field']` or `[index]` steps;
//! - the optional third argument names the result type (`int`, `bigint`,
//!   `double`, `decimal(10,2)`, `string`, `date`, `timestamp`, ...). Without
//!   it the result is itself a variant;
//! - a path that does not exist is NULL. A value that does not convert to
//!   the type is an error for `variant_get` and NULL for `try_variant_get`.

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef};
use datafusion::arrow::compute::CastOptions;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, TimeUnit};
use datafusion::common::{exec_err, plan_err, DataFusionError, Result as DFResult, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion::prelude::SessionContext;
use parquet_variant_compute::GetOptions;

/// Register `variant_get` and `try_variant_get`.
pub fn register_variant_functions(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::new_from_impl(VariantGet::new(false)));
    ctx.register_udf(ScalarUDF::new_from_impl(VariantGet::new(true)));
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct VariantGet {
    /// `try_variant_get`: a value that does not convert is NULL.
    safe: bool,
    signature: Signature,
}

impl VariantGet {
    fn new(safe: bool) -> Self {
        Self {
            safe,
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

/// The literal string argument `i`, or a planning error naming it.
fn literal_arg<'a>(
    name: &str,
    scalars: &[Option<&'a ScalarValue>],
    i: usize,
    what: &str,
) -> DFResult<&'a str> {
    match scalars.get(i).copied().flatten() {
        Some(
            ScalarValue::Utf8(Some(s))
            | ScalarValue::LargeUtf8(Some(s))
            | ScalarValue::Utf8View(Some(s)),
        ) => Ok(s.as_str()),
        _ => plan_err!("{name}: the {what} must be a string literal"),
    }
}

/// A Spark-style JSON path (`$`, `$.a.b`, `$['a'][0]`) as the path syntax of
/// arrow's `VariantPath` (`a.b`, `['a'][0]`).
fn variant_path_text(path: &str) -> DFResult<&str> {
    let Some(rest) = path.trim().strip_prefix('$') else {
        return plan_err!("variant_get: path {path:?} must start with '$'");
    };
    Ok(rest.strip_prefix('.').unwrap_or(rest))
}

/// The Arrow type a Spark/SQL type name reads as. Arrow's own names
/// (`Int32`, `Utf8`, ...) are accepted too.
fn parse_type_name(name: &str) -> DFResult<DataType> {
    let lower = name.trim().to_ascii_lowercase();
    let data_type = match lower.as_str() {
        "boolean" | "bool" => DataType::Boolean,
        "tinyint" | "byte" => DataType::Int8,
        "smallint" | "short" => DataType::Int16,
        "int" | "integer" => DataType::Int32,
        "bigint" | "long" => DataType::Int64,
        "float" | "real" => DataType::Float32,
        "double" => DataType::Float64,
        "string" | "varchar" | "text" => DataType::Utf8,
        "binary" | "varbinary" => DataType::Binary,
        "date" => DataType::Date32,
        "timestamp" | "timestamp_ltz" => {
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        }
        "timestamp_ntz" => DataType::Timestamp(TimeUnit::Microsecond, None),
        "decimal" => DataType::Decimal128(10, 0),
        other => {
            if let Some(args) = other
                .strip_prefix("decimal(")
                .and_then(|r| r.strip_suffix(')'))
            {
                let mut parts = args.split(',').map(str::trim);
                let precision = parts.next().and_then(|p| p.parse::<u8>().ok());
                let scale = match parts.next() {
                    Some(s) => s.parse::<i8>().ok(),
                    None => Some(0),
                };
                match (precision, scale, parts.next()) {
                    (Some(p), Some(s), None) if (1..=38).contains(&p) && s >= 0 && s as u8 <= p => {
                        DataType::Decimal128(p, s)
                    }
                    _ => return plan_err!("variant_get: invalid type {name:?}"),
                }
            } else {
                match name.trim().parse::<DataType>() {
                    Ok(t) => t,
                    Err(_) => return plan_err!("variant_get: unknown type {name:?}"),
                }
            }
        }
    };
    Ok(data_type)
}

impl ScalarUDFImpl for VariantGet {
    fn name(&self) -> &str {
        if self.safe {
            "try_variant_get"
        } else {
            "variant_get"
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> DFResult<Vec<DataType>> {
        if !(2..=3).contains(&arg_types.len()) {
            return plan_err!(
                "{}(variant, path [, type]) takes 2 or 3 arguments, got {}",
                self.name(),
                arg_types.len()
            );
        }
        if !crate::variant::is_variant_storage(&arg_types[0]) && arg_types[0] != DataType::Null {
            return plan_err!(
                "{}: the first argument must be a VARIANT, got {}",
                self.name(),
                arg_types[0]
            );
        }
        let mut out = vec![arg_types[0].clone()];
        out.extend(arg_types[1..].iter().map(|_| DataType::Utf8));
        Ok(out)
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Err(DataFusionError::Internal(
            "variant_get: return_field_from_args is used instead".to_string(),
        ))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> DFResult<FieldRef> {
        variant_path_text(literal_arg(self.name(), args.scalar_arguments, 1, "path")?)?;
        let field = if args.scalar_arguments.len() == 3 {
            let type_name = literal_arg(self.name(), args.scalar_arguments, 2, "type")?;
            Field::new(self.name(), parse_type_name(type_name)?, true)
        } else {
            crate::variant::field(self.name(), true)
        };
        Ok(Arc::new(field))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let path = match &args.args[1] {
            ColumnarValue::Scalar(
                ScalarValue::Utf8(Some(s))
                | ScalarValue::LargeUtf8(Some(s))
                | ScalarValue::Utf8View(Some(s)),
            ) => s.clone(),
            _ => return exec_err!("{}: the path must be a string literal", self.name()),
        };
        let as_type = args.return_field.data_type().clone();
        let typed = args.args.len() == 3;

        let (input, scalar) = match &args.args[0] {
            ColumnarValue::Array(a) => (Arc::clone(a), false),
            ColumnarValue::Scalar(s) => (s.to_array()?, true),
        };
        let result = if input.data_type() == &DataType::Null {
            datafusion::arrow::array::new_null_array(&as_type, input.len())
        } else {
            let to_df = |e| DataFusionError::ArrowError(Box::new(e), None);
            let options = GetOptions {
                path: variant_path_text(&path)?.try_into().map_err(to_df)?,
                as_type: typed.then(|| Arc::new(Field::new("value", as_type.clone(), true))),
                cast_options: CastOptions {
                    safe: self.safe,
                    ..Default::default()
                },
            };
            let got = parquet_variant_compute::variant_get(&input, options).map_err(to_df)?;
            if typed {
                got
            } else {
                as_storage(got.as_ref()).map_err(to_df)?
            }
        };
        Ok(if scalar {
            ColumnarValue::Scalar(ScalarValue::try_from_array(&result, 0)?)
        } else {
            ColumnarValue::Array(result)
        })
    }
}

/// A variant result in the unshredded storage the return field declares.
fn as_storage(array: &dyn Array) -> Result<ArrayRef, datafusion::arrow::error::ArrowError> {
    let target = crate::variant::storage_type();
    if crate::variant::is_shredded_storage(array.data_type()) {
        crate::variant::unshred(array, &target)
    } else {
        crate::variant::from_arrow(array, &target)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow;

    /// A session with the variant functions and a table `t(id, v)` of
    /// variants built from JSON text.
    async fn session(rows: &[Option<&str>]) -> SessionContext {
        let ctx = SessionContext::new();
        register_variant_functions(&ctx);
        let text: ArrayRef = Arc::new(arrow::array::StringArray::from(rows.to_vec()));
        let variant =
            crate::variant::from_arrow(text.as_ref(), &crate::variant::storage_type()).unwrap();
        let ids: ArrayRef = Arc::new(arrow::array::Int32Array::from_iter_values(
            1..=rows.len() as i32,
        ));
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            crate::variant::field("v", true),
        ]));
        let batch = arrow::record_batch::RecordBatch::try_new(schema, vec![ids, variant]).unwrap();
        ctx.register_batch("t", batch).unwrap();
        ctx
    }

    /// Every value of the single column of `sql`, a variant as its JSON text
    /// and NULL as `NULL`, plus the column's type.
    async fn column(ctx: &SessionContext, sql: &str) -> (DataType, Vec<String>) {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let mut out = Vec::new();
        let mut data_type = DataType::Null;
        for batch in &batches {
            let mut col = Arc::clone(batch.column(0));
            data_type = col.data_type().clone();
            if crate::variant::is_variant_storage(col.data_type()) {
                col = crate::variant::to_json(col.as_ref()).unwrap();
            }
            for r in 0..col.len() {
                out.push(if col.is_null(r) {
                    "NULL".to_string()
                } else {
                    arrow::util::display::array_value_to_string(&col, r).unwrap()
                });
            }
        }
        (data_type, out)
    }

    const DOC: &str = r#"{"a": {"b": [10, 20]}, "s": "x", "n": 7, "d": 1.25, "t": true}"#;

    #[tokio::test]
    async fn typed_paths() {
        let ctx = session(&[Some(DOC), None]).await;
        let cases = [
            ("'$.n', 'int'", DataType::Int32, "7"),
            ("'$.n', 'bigint'", DataType::Int64, "7"),
            ("'$.a.b[1]', 'int'", DataType::Int32, "20"),
            ("'$[''a''][''b''][0]', 'int'", DataType::Int32, "10"),
            ("'$.s', 'string'", DataType::Utf8, "x"),
            ("'$.d', 'double'", DataType::Float64, "1.25"),
            ("'$.d', 'decimal(5,2)'", DataType::Decimal128(5, 2), "1.25"),
            ("'$.t', 'boolean'", DataType::Boolean, "true"),
        ];
        for (args, data_type, want) in cases {
            let sql = format!("SELECT variant_get(v, {args}) FROM t ORDER BY id");
            assert_eq!(
                column(&ctx, &sql).await,
                (data_type, vec![want.to_string(), "NULL".to_string()]),
                "{sql}"
            );
        }
    }

    #[tokio::test]
    async fn without_a_type_the_result_is_a_variant() {
        let ctx = session(&[Some(DOC)]).await;
        let (data_type, values) = column(&ctx, "SELECT variant_get(v, '$.a') FROM t").await;
        assert!(
            crate::variant::is_variant_storage(&data_type),
            "{data_type}"
        );
        assert_eq!(values, [r#"{"b":[10,20]}"#]);
        let (_, values) = column(&ctx, "SELECT variant_get(v, '$') FROM t").await;
        assert_eq!(
            values,
            [r#"{"a":{"b":[10,20]},"d":1.25,"n":7,"s":"x","t":true}"#]
        );
    }

    #[tokio::test]
    async fn missing_path_is_null() {
        let ctx = session(&[Some(DOC)]).await;
        for args in ["'$.nope', 'int'", "'$.a.b[5]', 'int'", "'$.nope'"] {
            let sql = format!("SELECT variant_get(v, {args}) FROM t");
            assert_eq!(column(&ctx, &sql).await.1, ["NULL"], "{sql}");
        }
    }

    #[tokio::test]
    async fn a_value_that_does_not_convert_errors_unless_try() {
        let ctx = session(&[Some(DOC)]).await;
        let err = ctx
            .sql("SELECT variant_get(v, '$.s', 'int') FROM t")
            .await
            .unwrap()
            .collect()
            .await
            .expect_err("'x' is not an int");
        assert!(!err.to_string().is_empty());
        assert_eq!(
            column(&ctx, "SELECT try_variant_get(v, '$.s', 'int') FROM t")
                .await
                .1,
            ["NULL"]
        );
    }

    #[tokio::test]
    async fn bad_arguments_are_planning_errors() {
        let ctx = session(&[Some(DOC)]).await;
        for (sql, want) in [
            (
                "SELECT variant_get(v, 'a.b', 'int') FROM t",
                "must start with '$'",
            ),
            (
                "SELECT variant_get(v, '$.a', 'nonsense') FROM t",
                "unknown type",
            ),
            (
                "SELECT variant_get(v, '$.a', 'decimal(50,2)') FROM t",
                "invalid type",
            ),
            ("SELECT variant_get(id, '$.a') FROM t", "must be a VARIANT"),
            (
                "SELECT variant_get(v, concat('$', '.a')) FROM t",
                "string literal",
            ),
        ] {
            let err = match ctx.sql(sql).await {
                Ok(df) => df.collect().await.expect_err(sql).to_string(),
                Err(e) => e.to_string(),
            };
            assert!(err.contains(want), "{sql}: {err}");
        }
    }
}
