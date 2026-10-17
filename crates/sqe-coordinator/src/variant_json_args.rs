//! Let the JSON functions read a variant column (#849).
//!
//! On a format v3 table a `JSON` column is an Iceberg variant, which
//! DataFusion sees as `Struct { metadata, value }`. The `json_*` functions
//! (Trino's `json_extract` family and the `datafusion-functions-json` set)
//! take JSON text and refuse that struct at planning time.
//! [`accept_variant_in_json_functions`] re-registers each of them behind a
//! wrapper that renders a variant argument as its JSON text and hands the
//! text to the original function, so `json_extract(j, '$.a')` reads the same
//! on a v2 string column and on a v3 variant column.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{DataFusionError, Result as DFResult, ScalarValue};
use datafusion::logical_expr::type_coercion::functions::fields_with_udf;
use datafusion::logical_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
};
use datafusion::prelude::SessionContext;

/// Re-register every scalar function named `json*` so it also takes variant
/// arguments. Idempotent: a wrapped function is not wrapped again.
pub fn accept_variant_in_json_functions(ctx: &SessionContext) {
    let mut json_fns: Vec<Arc<ScalarUDF>> = ctx
        .state()
        .scalar_functions()
        .values()
        .filter(|f| f.name().starts_with("json"))
        .filter(|f| {
            let imp: &dyn std::any::Any = f.inner().as_ref();
            !imp.is::<VariantJsonArgs>()
        })
        .cloned()
        .collect();
    json_fns.sort_by(|a, b| a.name().cmp(b.name()));
    json_fns.dedup_by(|a, b| a.name() == b.name());
    for f in json_fns {
        ctx.register_udf(ScalarUDF::new_from_impl(VariantJsonArgs::new(
            f.as_ref().clone(),
        )));
    }
}

/// A JSON function whose variant arguments arrive as JSON text.
#[derive(Debug, PartialEq, Eq, Hash)]
struct VariantJsonArgs {
    inner: ScalarUDF,
    signature: Signature,
}

impl VariantJsonArgs {
    fn new(inner: ScalarUDF) -> Self {
        // User-defined, so planning asks `coerce_types` below, which keeps a
        // variant argument as it is instead of casting the struct to text.
        let signature = Signature::user_defined(inner.signature().volatility);
        Self { inner, signature }
    }
}

/// `field` with a variant type replaced by the JSON text it is read as.
fn as_text_field(field: &FieldRef) -> FieldRef {
    if sqe_core::variant::is_variant_storage(field.data_type()) {
        Arc::new(Field::new(
            field.name(),
            DataType::Utf8,
            field.is_nullable(),
        ))
    } else {
        Arc::clone(field)
    }
}

fn as_text_type(data_type: &DataType) -> DataType {
    if sqe_core::variant::is_variant_storage(data_type) {
        DataType::Utf8
    } else {
        data_type.clone()
    }
}

fn to_json_text(value: ColumnarValue) -> DFResult<ColumnarValue> {
    let is_variant = sqe_core::variant::is_variant_storage(&value.data_type());
    if !is_variant {
        return Ok(value);
    }
    let to_json = |array: &dyn arrow::array::Array| {
        sqe_core::variant::to_json(array)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
    };
    Ok(match value {
        ColumnarValue::Array(array) => ColumnarValue::Array(to_json(array.as_ref())?),
        ColumnarValue::Scalar(scalar) => {
            let text = to_json(scalar.to_array()?.as_ref())?;
            ColumnarValue::Scalar(ScalarValue::try_from_array(&text, 0)?)
        }
    })
}

impl ScalarUDFImpl for VariantJsonArgs {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn aliases(&self) -> &[String] {
        self.inner.aliases()
    }

    fn display_name(&self, args: &[Expr]) -> DFResult<String> {
        #[expect(deprecated)]
        self.inner.inner().display_name(args)
    }

    fn schema_name(&self, args: &[Expr]) -> DFResult<String> {
        self.inner.inner().schema_name(args)
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> DFResult<Vec<DataType>> {
        let text_fields: Vec<FieldRef> = arg_types
            .iter()
            .map(|t| Arc::new(Field::new("arg", as_text_type(t), true)))
            .collect();
        let coerced = fields_with_udf(&text_fields, &self.inner)?;
        Ok(arg_types
            .iter()
            .zip(coerced)
            .map(|(original, coerced)| {
                if sqe_core::variant::is_variant_storage(original) {
                    original.clone()
                } else {
                    coerced.data_type().clone()
                }
            })
            .collect())
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        let text: Vec<DataType> = arg_types.iter().map(as_text_type).collect();
        self.inner.return_type(&text)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> DFResult<FieldRef> {
        let arg_fields: Vec<FieldRef> = args.arg_fields.iter().map(as_text_field).collect();
        self.inner.return_field_from_args(ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: args.scalar_arguments,
        })
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs {
            args: values,
            arg_fields,
            number_rows,
            return_field,
            config_options,
        } = args;
        let values = values
            .into_iter()
            .map(to_json_text)
            .collect::<DFResult<Vec<_>>>()?;
        self.inner.invoke_with_args(ScalarFunctionArgs {
            args: values,
            arg_fields: arg_fields.iter().map(as_text_field).collect(),
            number_rows,
            return_field,
            config_options,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A session with the JSON functions and a one-row table `t(v)` where
    /// `v` is a variant holding `{"a": {"b": 2}, "s": "x"}`.
    async fn session() -> SessionContext {
        let mut ctx = SessionContext::new();
        sqe_trino_functions::register_trino_functions(&ctx);
        datafusion_functions_json::register_all(&mut ctx).unwrap();
        accept_variant_in_json_functions(&ctx);

        let text: arrow::array::ArrayRef = Arc::new(arrow::array::StringArray::from(vec![Some(
            r#"{"a": {"b": 2}, "s": "x"}"#,
        )]));
        let variant =
            sqe_core::variant::from_arrow(text.as_ref(), &sqe_core::variant::storage_type())
                .unwrap();
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
            "v",
            variant.data_type().clone(),
            true,
        )]));
        let batch = arrow::record_batch::RecordBatch::try_new(schema, vec![variant]).unwrap();
        ctx.register_batch("t", batch).unwrap();
        ctx
    }

    async fn one(ctx: &SessionContext, sql: &str) -> String {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let col = batches[0].column(0);
        arrow::util::display::array_value_to_string(col, 0).unwrap()
    }

    #[tokio::test]
    async fn json_functions_read_a_variant_as_json_text() {
        let ctx = session().await;
        assert_eq!(
            one(&ctx, "SELECT json_extract(v, '$.a') FROM t").await,
            r#"{"b":2}"#
        );
        assert_eq!(
            one(&ctx, "SELECT json_extract_scalar(v, '$.s') FROM t").await,
            "x"
        );
        assert_eq!(
            one(&ctx, "SELECT json_get_int(v, 'a', 'b') FROM t").await,
            "2"
        );
        assert_eq!(one(&ctx, "SELECT json_get_str(v, 's') FROM t").await, "x");
    }

    #[tokio::test]
    async fn json_functions_still_take_text() {
        let ctx = session().await;
        assert_eq!(
            one(&ctx, r#"SELECT json_extract_scalar('{"s": "y"}', '$.s')"#).await,
            "y"
        );
        assert_eq!(
            one(&ctx, r#"SELECT json_get_int('{"n": 7}', 'n')"#).await,
            "7"
        );
    }

    #[tokio::test]
    async fn wrapping_twice_is_a_no_op() {
        let ctx = session().await;
        accept_variant_in_json_functions(&ctx);
        assert_eq!(
            one(&ctx, "SELECT json_get_int(v, 'a', 'b') FROM t").await,
            "2"
        );
    }
}
