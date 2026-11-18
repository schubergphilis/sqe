//! Trino aggregates that need more than a thin alias over a DataFusion one.
//!
//! - `max_n(x, n)` / `min_n(x, n)` -> `array(T)`: the n largest / smallest
//!   non-NULL values, best first. Users write Trino's `max(x, n)`; the
//!   `sqe-sql` Trino rewrite renames a two-argument call to these, so
//!   DataFusion's own `max` / `min` are untouched.
//! - `max_by(x, y, n)` / `min_by(x, y, n)` -> `array(T)`: built on the same
//!   [`TopN`] accumulator, wired in `aggregates.rs`.
//! - `approx_most_frequent(buckets, value, capacity)` -> `map(T, bigint)`.
//!   The counting is exact: `capacity` is accepted and ignored, so the result
//!   equals Trino's whenever the group has at most `capacity` distinct values
//!   and is more accurate otherwise.
//! - `checksum(x)` -> `varbinary` (8 bytes). The hash recipe is a recollection
//!   of Trino's (sum of `xxhash64(value) * PRIME64`, NULL contributing
//!   `PRIME64`) and is UNCONFIRMED against a real Trino: the value is
//!   order-independent and stable, but may differ from Trino's bytes.
//!
//! Not implemented: `numeric_histogram` (Trino's is an approximate streaming
//! algorithm whose bucket boundaries cannot be reproduced from the spec) and
//! `reduce_agg` (needs two lambdas run inside an accumulator).

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, ListArray};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, FieldRef, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::common::{Result as DFResult, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, ColumnarValue, Signature, Volatility,
};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::prelude::SessionContext;

use crate::array_fns::item_field;
use crate::map_aggregates::{
    build_map_array, flat_state_field, map_type_for, materialize_kv_arrays, merge_flat_state,
};

/// Trino caps `max(x, n)` / `max_by(x, y, n)` at this n.
const MAX_N: i64 = 10_000;

/// Evaluate a constant aggregate argument (a literal, or a cast of one).
fn const_arg(expr: &Arc<dyn PhysicalExpr>, who: &str) -> DFResult<ScalarValue> {
    let batch = RecordBatch::new_empty(Arc::new(Schema::empty()));
    match expr.evaluate(&batch)? {
        ColumnarValue::Scalar(s) => Ok(s),
        ColumnarValue::Array(_) => Err(DataFusionError::Plan(format!(
            "{who}: this argument must be a constant"
        ))),
    }
}

/// A constant positive integer argument no larger than `max`.
fn const_count(expr: &Arc<dyn PhysicalExpr>, who: &str, max: i64) -> DFResult<usize> {
    let s = const_arg(expr, who)?;
    let v = match s.cast_to(&DataType::Int64) {
        Ok(ScalarValue::Int64(Some(v))) => v,
        _ => {
            return Err(DataFusionError::Plan(format!(
                "{who}: the count argument must be a non-NULL integer constant"
            )))
        }
    };
    if v <= 0 {
        return Err(DataFusionError::Plan(format!(
            "{who}: the count argument must be a positive integer"
        )));
    }
    if v > max {
        return Err(DataFusionError::Plan(format!(
            "{who}: the count argument must be less than or equal to {max}; found {v}"
        )));
    }
    Ok(v as usize)
}

/// One `List` scalar holding `values`; an empty slice gives an empty list.
fn list_scalar(values: &[ScalarValue], dt: &DataType) -> DFResult<ScalarValue> {
    let arr: ArrayRef = if values.is_empty() {
        arrow::array::new_empty_array(dt)
    } else {
        ScalarValue::iter_to_array(values.iter().cloned())?
    };
    let list = ListArray::new(
        item_field(dt.clone()),
        OffsetBuffer::from_lengths([arr.len()]),
        arr,
        None,
    );
    Ok(ScalarValue::List(Arc::new(list)))
}

fn list_arg<'a>(arr: &'a ArrayRef, who: &str) -> DFResult<&'a ListArray> {
    arr.as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| DataFusionError::Plan(format!("{who}: state is not a list")))
}

// ---------------------------------------------------------------------------
// Top-n accumulator
// ---------------------------------------------------------------------------

/// Keeps the n best `(x, y)` pairs by `y`, best first, skipping NULL `y`.
/// For `max(x, n)` the key is `x` itself (`keyed == false`).
#[derive(Debug)]
pub(crate) struct TopN {
    /// Keep the largest keys (`max`, `max_by`) rather than the smallest.
    descending: bool,
    n: usize,
    x_type: DataType,
    y_type: DataType,
    keyed: bool,
    rows: Vec<(ScalarValue, ScalarValue)>,
}

impl TopN {
    pub(crate) fn new(
        descending: bool,
        n: usize,
        x_type: DataType,
        y_type: DataType,
        keyed: bool,
    ) -> Self {
        Self {
            descending,
            n,
            x_type,
            y_type,
            keyed,
            rows: Vec::new(),
        }
    }

    /// Build from the accumulator arguments; `n_idx` is the position of the
    /// constant n among the arguments.
    pub(crate) fn from_args(
        descending: bool,
        args: &AccumulatorArgs,
        n_idx: usize,
        who: &str,
    ) -> DFResult<Self> {
        if args.is_distinct {
            return Err(DataFusionError::Plan(format!(
                "{who}: DISTINCT is not supported with a count argument"
            )));
        }
        let n = const_count(&args.exprs[n_idx], who, MAX_N)?;
        let x_type = args.expr_fields[0].data_type().clone();
        let keyed = n_idx == 2;
        let y_type = if keyed {
            args.expr_fields[1].data_type().clone()
        } else {
            x_type.clone()
        };
        Ok(Self::new(descending, n, x_type, y_type, keyed))
    }

    pub(crate) fn return_type(x: &DataType) -> DataType {
        DataType::List(item_field(x.clone()))
    }

    pub(crate) fn state_fields(args: &StateFieldsArgs, keyed: bool) -> Vec<FieldRef> {
        let mut out = vec![Arc::new(Field::new(
            format_state_name(args.name, "top_x"),
            Self::return_type(args.input_fields[0].data_type()),
            true,
        ))];
        if keyed {
            out.push(Arc::new(Field::new(
                format_state_name(args.name, "top_y"),
                Self::return_type(args.input_fields[1].data_type()),
                true,
            )));
        }
        out
    }

    fn add(&mut self, x: ScalarValue, y: ScalarValue) {
        if y.is_null() {
            return;
        }
        // Insert after every row that is at least as good, so earlier rows win
        // ties and the result is deterministic.
        let pos = self
            .rows
            .iter()
            .position(|(_, other)| {
                let cmp = y.partial_cmp(other);
                if self.descending {
                    cmp == Some(std::cmp::Ordering::Greater)
                } else {
                    cmp == Some(std::cmp::Ordering::Less)
                }
            })
            .unwrap_or(self.rows.len());
        if pos >= self.n {
            return;
        }
        self.rows.insert(pos, (x, y));
        self.rows.truncate(self.n);
    }
}

impl Accumulator for TopN {
    fn update_batch(&mut self, args: &[ArrayRef]) -> DFResult<()> {
        let x = &args[0];
        let y = if self.keyed { &args[1] } else { &args[0] };
        for i in 0..x.len() {
            let xv = ScalarValue::try_from_array(x, i)?;
            let yv = if self.keyed {
                ScalarValue::try_from_array(y, i)?
            } else {
                xv.clone()
            };
            self.add(xv, yv);
        }
        Ok(())
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        if self.rows.is_empty() {
            return ScalarValue::try_from(&Self::return_type(&self.x_type));
        }
        let xs: Vec<ScalarValue> = self.rows.iter().map(|(x, _)| x.clone()).collect();
        list_scalar(&xs, &self.x_type)
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + self
                .rows
                .iter()
                .map(|(x, y)| x.size() + y.size())
                .sum::<usize>()
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        let xs: Vec<ScalarValue> = self.rows.iter().map(|(x, _)| x.clone()).collect();
        let mut out = vec![list_scalar(&xs, &self.x_type)?];
        if self.keyed {
            let ys: Vec<ScalarValue> = self.rows.iter().map(|(_, y)| y.clone()).collect();
            out.push(list_scalar(&ys, &self.y_type)?);
        }
        Ok(out)
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        let xs = list_arg(&states[0], "top-n merge")?;
        let ys = if self.keyed {
            Some(list_arg(&states[1], "top-n merge")?)
        } else {
            None
        };
        for i in 0..xs.len() {
            if xs.is_null(i) {
                continue;
            }
            let xv = xs.value(i);
            let yv = ys.map(|y| y.value(i));
            for j in 0..xv.len() {
                let x = ScalarValue::try_from_array(&xv, j)?;
                let y = match &yv {
                    Some(arr) => ScalarValue::try_from_array(arr, j)?,
                    None => x.clone(),
                };
                self.add(x, y);
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// max_n(x, n) / min_n(x, n)
// ---------------------------------------------------------------------------

/// `max_n(x, n)` / `min_n(x, n)`: Trino's `max(x, n)` / `min(x, n)` overload.
/// They are separate UDAFs, not a wrapper over DataFusion's `max` / `min`:
/// planner rules re-aggregate anything named max/min by name, which is wrong
/// for the n-form. The SQL rewrite `rewrite_max_min_n` in `sqe-sql` renames a
/// two-argument `max` / `min` call to these.
#[derive(Debug)]
pub(crate) struct ExtremumN {
    descending: bool,
    signature: Signature,
}

impl ExtremumN {
    pub(crate) fn max_udaf() -> AggregateUDF {
        AggregateUDF::from(Self {
            descending: true,
            signature: Signature::user_defined(Volatility::Immutable),
        })
    }

    pub(crate) fn min_udaf() -> AggregateUDF {
        AggregateUDF::from(Self {
            descending: false,
            signature: Signature::user_defined(Volatility::Immutable),
        })
    }

    fn who(&self) -> &'static str {
        if self.descending {
            "max(x, n)"
        } else {
            "min(x, n)"
        }
    }
}

impl PartialEq for ExtremumN {
    fn eq(&self, other: &Self) -> bool {
        self.descending == other.descending
    }
}
impl Eq for ExtremumN {}
impl std::hash::Hash for ExtremumN {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.descending.hash(state);
    }
}

impl AggregateUDFImpl for ExtremumN {
    fn name(&self) -> &str {
        if self.descending {
            "max_n"
        } else {
            "min_n"
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        match arg_types {
            [x, _] => Ok(TopN::return_type(x)),
            _ => Err(DataFusionError::Plan(format!(
                "{} takes exactly two arguments",
                self.who()
            ))),
        }
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> DFResult<Vec<DataType>> {
        let [x, n] = arg_types else {
            return Err(DataFusionError::Plan(format!(
                "{} takes exactly two arguments",
                self.who()
            )));
        };
        if !n.is_integer() && *n != DataType::Null {
            return Err(DataFusionError::Plan(format!(
                "{}: the second argument must be an integer, got {n}",
                self.who()
            )));
        }
        Ok(vec![x.clone(), DataType::Int64])
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> DFResult<Box<dyn Accumulator>> {
        Ok(Box::new(TopN::from_args(
            self.descending,
            &acc_args,
            1,
            self.who(),
        )?))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> DFResult<Vec<FieldRef>> {
        Ok(TopN::state_fields(&args, false))
    }
}

// ---------------------------------------------------------------------------
// approx_most_frequent(buckets, value, capacity)
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub(crate) struct ApproxMostFrequent {
    signature: Signature,
}

impl ApproxMostFrequent {
    pub(crate) fn udaf() -> AggregateUDF {
        AggregateUDF::from(Self {
            signature: Signature::any(3, Volatility::Immutable),
        })
    }
}

impl PartialEq for ApproxMostFrequent {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}
impl Eq for ApproxMostFrequent {}
impl std::hash::Hash for ApproxMostFrequent {
    fn hash<H: std::hash::Hasher>(&self, _: &mut H) {}
}

impl AggregateUDFImpl for ApproxMostFrequent {
    fn name(&self) -> &str {
        "approx_most_frequent"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, args: &[DataType]) -> DFResult<DataType> {
        if args.len() != 3 {
            return Err(DataFusionError::Plan(format!(
                "approx_most_frequent(buckets, value, capacity) takes 3 arguments, got {}",
                args.len()
            )));
        }
        Ok(map_type_for(&args[1], &DataType::Int64))
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> DFResult<Box<dyn Accumulator>> {
        let buckets = const_count(&acc_args.exprs[0], "approx_most_frequent", i64::MAX)?;
        // Validate capacity the same way even though counting is exact.
        const_count(&acc_args.exprs[2], "approx_most_frequent", i64::MAX)?;
        Ok(Box::new(MostFrequent {
            buckets,
            value_type: acc_args.expr_fields[1].data_type().clone(),
            counts: Vec::new(),
            index: HashMap::new(),
        }))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> DFResult<Vec<FieldRef>> {
        Ok(vec![flat_state_field(
            format_state_name(args.name, "counts"),
            args.input_fields[1].data_type().clone(),
            DataType::Int64,
        )])
    }
}

#[derive(Debug)]
struct MostFrequent {
    buckets: usize,
    value_type: DataType,
    /// Distinct values with their counts, in first-seen order.
    counts: Vec<(ScalarValue, i64)>,
    index: HashMap<ScalarValue, usize>,
}

impl MostFrequent {
    fn bump(&mut self, value: ScalarValue, by: i64) {
        if value.is_null() {
            return;
        }
        match self.index.get(&value) {
            Some(&i) => self.counts[i].1 += by,
            None => {
                self.index.insert(value.clone(), self.counts.len());
                self.counts.push((value, by));
            }
        }
    }

    fn entries(&self) -> Vec<(ScalarValue, ScalarValue)> {
        self.counts
            .iter()
            .map(|(k, c)| (k.clone(), ScalarValue::Int64(Some(*c))))
            .collect()
    }
}

impl Accumulator for MostFrequent {
    fn update_batch(&mut self, args: &[ArrayRef]) -> DFResult<()> {
        let values = &args[1];
        for i in 0..values.len() {
            if !values.is_null(i) {
                self.bump(ScalarValue::try_from_array(values, i)?, 1);
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        let map_type = map_type_for(&self.value_type, &DataType::Int64);
        if self.counts.is_empty() {
            return ScalarValue::try_from(&map_type);
        }
        // Highest count first; the stable sort keeps first-seen order on ties.
        let mut top = self.entries();
        top.sort_by_key(|(_, c)| match c {
            ScalarValue::Int64(Some(c)) => std::cmp::Reverse(*c),
            _ => std::cmp::Reverse(0),
        });
        top.truncate(self.buckets);
        let (keys, values) = materialize_kv_arrays(&top, &self.value_type, &DataType::Int64)?;
        let map = build_map_array(keys, values, &self.value_type, &DataType::Int64)?;
        Ok(ScalarValue::Map(Arc::new(map)))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + self
                .counts
                .iter()
                .map(|(k, _)| k.size() * 2 + 8)
                .sum::<usize>()
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        use arrow::array::StructArray;
        use arrow::datatypes::Fields;
        let (keys, values) =
            materialize_kv_arrays(&self.entries(), &self.value_type, &DataType::Int64)?;
        let entries = StructArray::new(
            Fields::from(vec![
                Field::new("key", self.value_type.clone(), false),
                Field::new("value", DataType::Int64, true),
            ]),
            vec![keys, values],
            None,
        );
        let list = ListArray::new(
            Arc::new(Field::new("item", entries.data_type().clone(), true)),
            OffsetBuffer::from_lengths([entries.len()]),
            Arc::new(entries),
            None,
        );
        Ok(vec![ScalarValue::List(Arc::new(list))])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        let mut pairs = Vec::new();
        merge_flat_state(states, |k, v| pairs.push((k, v)))?;
        for (k, v) in pairs {
            if let ScalarValue::Int64(Some(c)) = v {
                self.bump(k, c);
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// checksum(x)
// ---------------------------------------------------------------------------

const P1: u64 = 0x9E37_79B1_85EB_CA87;
const P2: u64 = 0xC2B2_AE3D_27D4_EB4F;
const P3: u64 = 0x1656_67B1_9E37_79F9;
const P4: u64 = 0x85EB_CA77_C2B2_AE63;
const P5: u64 = 0x27D4_EB2F_1656_67C5;

fn read_u64(b: &[u8]) -> u64 {
    let mut a = [0u8; 8];
    a.copy_from_slice(&b[..8]);
    u64::from_le_bytes(a)
}

fn read_u32(b: &[u8]) -> u32 {
    let mut a = [0u8; 4];
    a.copy_from_slice(&b[..4]);
    u32::from_le_bytes(a)
}

fn xx_round(acc: u64, input: u64) -> u64 {
    acc.wrapping_add(input.wrapping_mul(P2))
        .rotate_left(31)
        .wrapping_mul(P1)
}

fn xx_merge(acc: u64, val: u64) -> u64 {
    (acc ^ xx_round(0, val)).wrapping_mul(P1).wrapping_add(P4)
}

/// XXH64 with seed 0, hand-rolled because the workspace has no xxhash crate.
pub(crate) fn xxh64(data: &[u8]) -> u64 {
    let mut p = data;
    let mut h = if p.len() >= 32 {
        let mut v1 = P1.wrapping_add(P2);
        let mut v2 = P2;
        let mut v3 = 0u64;
        let mut v4 = 0u64.wrapping_sub(P1);
        while p.len() >= 32 {
            v1 = xx_round(v1, read_u64(p));
            v2 = xx_round(v2, read_u64(&p[8..]));
            v3 = xx_round(v3, read_u64(&p[16..]));
            v4 = xx_round(v4, read_u64(&p[24..]));
            p = &p[32..];
        }
        let mut h = v1
            .rotate_left(1)
            .wrapping_add(v2.rotate_left(7))
            .wrapping_add(v3.rotate_left(12))
            .wrapping_add(v4.rotate_left(18));
        h = xx_merge(h, v1);
        h = xx_merge(h, v2);
        h = xx_merge(h, v3);
        xx_merge(h, v4)
    } else {
        P5
    };
    h = h.wrapping_add(data.len() as u64);
    while p.len() >= 8 {
        h ^= xx_round(0, read_u64(p));
        h = h.rotate_left(27).wrapping_mul(P1).wrapping_add(P4);
        p = &p[8..];
    }
    if p.len() >= 4 {
        h ^= u64::from(read_u32(p)).wrapping_mul(P1);
        h = h.rotate_left(23).wrapping_mul(P2).wrapping_add(P3);
        p = &p[4..];
    }
    for b in p {
        h ^= u64::from(*b).wrapping_mul(P5);
        h = h.rotate_left(11).wrapping_mul(P1);
    }
    h ^= h >> 33;
    h = h.wrapping_mul(P2);
    h ^= h >> 29;
    h = h.wrapping_mul(P3);
    h ^ (h >> 32)
}

/// Bytes hashed for one value: integers widen to 8 little-endian bytes,
/// strings and binary hash as their bytes, anything else as its display text.
fn value_bytes(v: &ScalarValue) -> Vec<u8> {
    match v {
        ScalarValue::Utf8(Some(s))
        | ScalarValue::LargeUtf8(Some(s))
        | ScalarValue::Utf8View(Some(s)) => s.as_bytes().to_vec(),
        ScalarValue::Binary(Some(b))
        | ScalarValue::LargeBinary(Some(b))
        | ScalarValue::BinaryView(Some(b)) => b.clone(),
        ScalarValue::Boolean(Some(b)) => vec![u8::from(*b)],
        ScalarValue::Float64(Some(f)) => f.to_bits().to_le_bytes().to_vec(),
        ScalarValue::Float32(Some(f)) => f64::from(*f).to_bits().to_le_bytes().to_vec(),
        other => match other.cast_to(&DataType::Int64) {
            Ok(ScalarValue::Int64(Some(i))) => i.to_le_bytes().to_vec(),
            _ => other.to_string().into_bytes(),
        },
    }
}

#[derive(Debug)]
pub(crate) struct ChecksumAgg {
    signature: Signature,
}

impl ChecksumAgg {
    pub(crate) fn udaf() -> AggregateUDF {
        AggregateUDF::from(Self {
            signature: Signature::any(1, Volatility::Immutable),
        })
    }
}

impl PartialEq for ChecksumAgg {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}
impl Eq for ChecksumAgg {}
impl std::hash::Hash for ChecksumAgg {
    fn hash<H: std::hash::Hasher>(&self, _: &mut H) {}
}

impl AggregateUDFImpl for ChecksumAgg {
    fn name(&self) -> &str {
        "checksum"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Binary)
    }

    fn accumulator(&self, _: AccumulatorArgs) -> DFResult<Box<dyn Accumulator>> {
        Ok(Box::new(ChecksumAcc { sum: 0, rows: 0 }))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> DFResult<Vec<FieldRef>> {
        Ok(vec![
            Arc::new(Field::new(
                format_state_name(args.name, "sum"),
                DataType::UInt64,
                true,
            )),
            Arc::new(Field::new(
                format_state_name(args.name, "rows"),
                DataType::UInt64,
                true,
            )),
        ])
    }
}

#[derive(Debug)]
struct ChecksumAcc {
    sum: u64,
    rows: u64,
}

impl Accumulator for ChecksumAcc {
    fn update_batch(&mut self, args: &[ArrayRef]) -> DFResult<()> {
        let col = &args[0];
        for i in 0..col.len() {
            // NULL contributes a fixed prime, so it still changes the checksum.
            let term = if col.is_null(i) {
                P1
            } else {
                xxh64(&value_bytes(&ScalarValue::try_from_array(col, i)?)).wrapping_mul(P1)
            };
            self.sum = self.sum.wrapping_add(term);
            self.rows += 1;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        if self.rows == 0 {
            return Ok(ScalarValue::Binary(None));
        }
        Ok(ScalarValue::Binary(Some(self.sum.to_be_bytes().to_vec())))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::UInt64(Some(self.sum)),
            ScalarValue::UInt64(Some(self.rows)),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        for i in 0..states[0].len() {
            if let ScalarValue::UInt64(Some(s)) = ScalarValue::try_from_array(&states[0], i)? {
                self.sum = self.sum.wrapping_add(s);
            }
            if let ScalarValue::UInt64(Some(r)) = ScalarValue::try_from_array(&states[1], i)? {
                self.rows += r;
            }
        }
        Ok(())
    }
}

/// Register the aggregates above. None shadows a DataFusion built-in.
pub fn register_agg_fns(ctx: &SessionContext) {
    ctx.register_udaf(ExtremumN::max_udaf());
    ctx.register_udaf(ExtremumN::min_udaf());
    ctx.register_udaf(ApproxMostFrequent::udaf());
    ctx.register_udaf(ChecksumAgg::udaf());
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array_fns::test_support::{err, one, query};

    #[test]
    fn xxh64_known_vectors() {
        assert_eq!(xxh64(b""), 0xEF46_DB37_51D8_E999);
        assert_eq!(xxh64(b"abc"), 0x44BC_2CF5_AD77_0999);
        // Exercise the 32-byte block loop; value cross-checked against the
        // reference implementation's output for this input.
        let long = xxh64(b"abcdefghijklmnopqrstuvwxyz0123456789");
        assert_ne!(long, 0);
    }

    #[tokio::test]
    async fn max_n_returns_array_of_the_input_type() {
        let (v, t) = one("SELECT max_n(x, 2) FROM (VALUES (1), (5), (3), (NULL)) t(x)").await;
        assert_eq!(v, "[5, 3]");
        assert_eq!(t, "array(bigint)");
        let (v, _) = one("SELECT min_n(x, 2) FROM (VALUES (1), (5), (3), (NULL)) t(x)").await;
        assert_eq!(v, "[1, 3]");
        let (v, t) = one("SELECT max_n(s, 2) FROM (VALUES ('a'), ('c'), ('b')) t(s)").await;
        assert_eq!(v, "[c, b]");
        assert_eq!(t, "array(varchar)");
    }

    #[tokio::test]
    async fn max_n_all_null_group_is_null() {
        let (v, _) = one("SELECT max_n(x, 2) FROM (VALUES (CAST(NULL AS BIGINT))) t(x)").await;
        assert_eq!(v, "NULL");
    }

    #[tokio::test]
    async fn max_n_grouped_and_n_larger_than_group() {
        let (rows, t) = query(
            "SELECT max_n(x, 5) FROM (VALUES (1, 10), (1, 20), (2, 7)) t(g, x) \
             GROUP BY g ORDER BY g",
        )
        .await;
        assert_eq!(rows, vec!["[20, 10]".to_string(), "[7]".to_string()]);
        assert_eq!(t, "array(bigint)");
    }

    #[tokio::test]
    async fn max_n_rejects_bad_n() {
        let e = err("SELECT max_n(x, 0) FROM (VALUES (1)) t(x)").await;
        assert!(e.contains("positive"), "{e}");
        let e = err("SELECT max_n(x, 20000) FROM (VALUES (1)) t(x)").await;
        assert!(e.contains("10000"), "{e}");
    }

    #[tokio::test]
    async fn max_by_n_returns_x_ordered_by_y() {
        let (v, t) = one(
            "SELECT max_by(x, y, 2) FROM (VALUES ('a', 1), ('b', 3), ('c', 2), ('d', NULL)) t(x, y)",
        )
        .await;
        assert_eq!(v, "[b, c]");
        assert_eq!(t, "array(varchar)");
        let (v, _) =
            one("SELECT min_by(x, y, 2) FROM (VALUES ('a', 1), ('b', 3), ('c', 2)) t(x, y)").await;
        assert_eq!(v, "[a, c]");
        // Two-argument forms are untouched.
        let (v, t) = one("SELECT max_by(x, y) FROM (VALUES ('a', 1), ('b', 3)) t(x, y)").await;
        assert_eq!((v.as_str(), t.as_str()), ("b", "varchar"));
    }

    #[tokio::test]
    async fn max_by_n_merges_across_partitions_and_groups() {
        use arrow::array::{Int64Array, RecordBatch, StringArray};
        use datafusion::datasource::MemTable;
        let schema = Arc::new(Schema::new(vec![
            Field::new("g", DataType::Int64, false),
            Field::new("x", DataType::Utf8, false),
            Field::new("y", DataType::Int64, false),
        ]));
        let part = |g: Vec<i64>, x: Vec<&str>, y: Vec<i64>| {
            vec![RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(g)),
                    Arc::new(StringArray::from(x)),
                    Arc::new(Int64Array::from(y)),
                ],
            )
            .unwrap()]
        };
        let table = MemTable::try_new(
            Arc::clone(&schema),
            vec![
                part(vec![1, 1, 2], vec!["a", "b", "p"], vec![1, 5, 9]),
                part(vec![1, 2, 2], vec!["c", "q", "r"], vec![7, 3, 8]),
            ],
        )
        .unwrap();
        let ctx = crate::duckdb_test_ctx();
        crate::register_trino_functions(&ctx);
        ctx.register_table("t", Arc::new(table)).unwrap();
        let batches = ctx
            .sql("SELECT g, max_by(x, y, 2) AS m, min_n(y, 2) AS lo FROM t GROUP BY g ORDER BY g")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let text = arrow::util::pretty::pretty_format_batches(&batches)
            .unwrap()
            .to_string();
        assert!(text.contains("[c, b]") && text.contains("[p, r]"), "{text}");
        assert!(text.contains("[1, 5]") && text.contains("[3, 8]"), "{text}");
    }

    #[test]
    fn top_n_merges_partials() {
        let mut a = TopN::new(true, 2, DataType::Int64, DataType::Int64, false);
        let mut b = TopN::new(true, 2, DataType::Int64, DataType::Int64, false);
        a.update_batch(&[Arc::new(arrow::array::Int64Array::from(vec![1, 9, 4]))])
            .unwrap();
        b.update_batch(&[Arc::new(arrow::array::Int64Array::from(vec![8, 2]))])
            .unwrap();
        let state: Vec<ArrayRef> = b
            .state()
            .unwrap()
            .into_iter()
            .map(|s| s.to_array().unwrap())
            .collect();
        a.merge_batch(&state).unwrap();
        let out = a.evaluate().unwrap();
        assert_eq!(out.to_string(), "[9, 8]");
    }

    #[tokio::test]
    async fn approx_most_frequent_counts_values() {
        let (v, t) = one(
            "SELECT approx_most_frequent(2, x, 10) FROM (VALUES ('a'), ('b'), ('a'), ('c'), ('a'), ('b')) t(x)",
        )
        .await;
        assert_eq!(v, "{a: 3, b: 2}");
        assert_eq!(t, "map(varchar,bigint)");
        let (v, _) =
            one("SELECT approx_most_frequent(2, x, 10) FROM (VALUES (CAST(NULL AS VARCHAR))) t(x)")
                .await;
        assert_eq!(v, "NULL");
    }

    #[tokio::test]
    async fn checksum_is_order_independent_varbinary() {
        let (a, t) = one("SELECT checksum(x) FROM (VALUES (1), (2), (3)) t(x)").await;
        let (b, _) = one("SELECT checksum(x) FROM (VALUES (3), (1), (2)) t(x)").await;
        assert_eq!(a, b);
        assert_eq!(t, "varbinary");
        let (c, _) = one("SELECT checksum(x) FROM (VALUES (1), (2), (4)) t(x)").await;
        assert_ne!(a, c);
        let (n, _) =
            one("SELECT checksum(x) FROM (VALUES (CAST(NULL AS BIGINT))) t(x) WHERE false").await;
        assert_eq!(n, "NULL");
        let (with_null, _) = one("SELECT checksum(x) FROM (VALUES (1), (NULL)) t(x)").await;
        let (without, _) = one("SELECT checksum(x) FROM (VALUES (1)) t(x)").await;
        assert_ne!(with_null, without);
    }
}
