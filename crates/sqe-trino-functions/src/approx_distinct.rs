//! `approx_distinct` over `double` and `real`, as in Trino.
//!
//! DataFusion's `approx_distinct` rejects floating-point input on purpose,
//! because `NaN` payloads and `-0.0` / `0.0` hash as different values.
//! Trino accepts `double` and `real` and counts every `NaN` as one value and
//! both zeros as one value. This shadows the built-in (Trino semantics): a
//! float is mapped to the bits of its `double` value, with `NaN` and `-0.0`
//! made canonical first, and the wrapped aggregate counts those as `bigint`.
//! A `real` is widened to `double` exactly, so distinct `real` values stay
//! distinct. Every other input type goes to the wrapped aggregate unchanged.

use std::sync::Arc;

use arrow::array::{ArrayRef, AsArray, BooleanArray, Int64Array};
use arrow::compute::cast;
use arrow::datatypes::{DataType, FieldRef, Float64Type};
use datafusion::common::{Result, ScalarValue};
use datafusion::functions_aggregate::approx_distinct::approx_distinct_udaf;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::AggregateOrderSensitivity;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Documentation, EmitTo, GroupsAccumulator,
    ReversedUDAF, SetMonotonicity, Signature,
};
use datafusion::prelude::SessionContext;

/// Register `approx_distinct` with float support, wrapping the context's own.
pub fn register_approx_distinct(ctx: &SessionContext) {
    let state = ctx.state();
    let inner = state
        .aggregate_functions()
        .get("approx_distinct")
        .map_or_else(|| (*approx_distinct_udaf()).clone(), |a| (**a).clone());
    ctx.register_udaf(AggregateUDF::new_from_impl(FloatApproxDistinct { inner }));
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct FloatApproxDistinct {
    inner: AggregateUDF,
}

fn is_float(t: &DataType) -> bool {
    matches!(t, DataType::Float32 | DataType::Float64)
}

/// The same fields with every float declared as the `bigint` it is counted as.
fn bits_fields(fields: &[FieldRef]) -> Vec<FieldRef> {
    fields
        .iter()
        .map(|f| match is_float(f.data_type()) {
            true => Arc::new(f.as_ref().clone().with_data_type(DataType::Int64)),
            false => Arc::clone(f),
        })
        .collect()
}

fn with_bits<T>(args: AccumulatorArgs, f: impl FnOnce(AccumulatorArgs) -> Result<T>) -> Result<T> {
    let fields = bits_fields(args.expr_fields);
    f(AccumulatorArgs {
        expr_fields: &fields,
        ..args
    })
}

/// One bit pattern per value as Trino compares them: every `NaN` is the same
/// value, and so are `-0.0` and `0.0`.
fn canonical_bits(x: f64) -> i64 {
    let x = if x.is_nan() {
        f64::NAN
    } else if x == 0.0 {
        0.0
    } else {
        x
    };
    x.to_bits() as i64
}

/// Floats as their canonical bits (NULL stays NULL); other arrays unchanged.
fn to_bits(values: &[ArrayRef]) -> Result<Vec<ArrayRef>> {
    values
        .iter()
        .map(|v| {
            if !is_float(v.data_type()) {
                return Ok(Arc::clone(v));
            }
            let wide = cast(v, &DataType::Float64)?;
            let bits: Int64Array = wide.as_primitive::<Float64Type>().unary(canonical_bits);
            Ok(Arc::new(bits) as ArrayRef)
        })
        .collect()
}

impl AggregateUDFImpl for FloatApproxDistinct {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn aliases(&self) -> &[String] {
        self.inner.aliases()
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.inner.return_type(arg_types)
    }

    fn default_value(&self, data_type: &DataType) -> Result<ScalarValue> {
        self.inner.default_value(data_type)
    }

    fn is_nullable(&self) -> bool {
        self.inner.is_nullable()
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        if !args.expr_fields.iter().any(|f| is_float(f.data_type())) {
            return self.inner.accumulator(args);
        }
        let inner = with_bits(args, |a| self.inner.accumulator(a))?;
        Ok(Box::new(BitsAccumulator { inner }))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        let fields = bits_fields(args.input_fields);
        self.inner.state_fields(StateFieldsArgs {
            input_fields: &fields,
            ..args
        })
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        with_bits(args, |a| Ok(self.inner.groups_accumulator_supported(a))).unwrap_or(false)
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        if !args.expr_fields.iter().any(|f| is_float(f.data_type())) {
            return self.inner.create_groups_accumulator(args);
        }
        let inner = with_bits(args, |a| self.inner.create_groups_accumulator(a))?;
        Ok(Box::new(BitsGroups { inner }))
    }

    fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        self.inner.inner().order_sensitivity()
    }

    fn set_monotonicity(&self, data_type: &DataType) -> SetMonotonicity {
        self.inner.inner().set_monotonicity(data_type)
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        self.inner.reverse_udf()
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.inner.documentation()
    }
}

#[derive(Debug)]
struct BitsAccumulator {
    inner: Box<dyn Accumulator>,
}

impl Accumulator for BitsAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.inner.update_batch(&to_bits(values)?)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        self.inner.evaluate()
    }

    fn size(&self) -> usize {
        self.inner.size()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.inner.state()
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.inner.merge_batch(states)
    }
}

struct BitsGroups {
    inner: Box<dyn GroupsAccumulator>,
}

impl GroupsAccumulator for BitsGroups {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        let bits = to_bits(values)?;
        self.inner
            .update_batch(&bits, group_indices, opt_filter, total_num_groups)
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        self.inner.evaluate(emit_to)
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        self.inner.state(emit_to)
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> Result<()> {
        self.inner
            .merge_batch(values, group_indices, total_num_groups)
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        self.inner.convert_to_state(&to_bits(values)?, opt_filter)
    }

    fn size(&self) -> usize {
        self.inner.size()
    }
}
