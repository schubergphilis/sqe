//! "First row per partition" (`ROW_NUMBER() OVER (PARTITION BY p ORDER BY o)
//! = 1`) as a hash aggregate instead of a window over a full sort.
//!
//! DataFusion 55 plans the window as a sort of the whole input by `p, o`,
//! then numbers the rows and drops all but the first of each partition. For
//! "latest row per customer" over 150M rows and 15M customers that sort is
//! the whole query (9 s, 4.3 GB spilled), while a hash aggregate over the
//! same rows takes under 4 s. Its `WindowTopN` rewrite does not match
//! `rn = 1` and keeps one heap per key without spilling; the rework that
//! fixes it lands after 55.
//!
//! [`TopRowNumberRule`] turns `Filter(rn = 1, Window(ROW_NUMBER() ...))` into
//! `Projection(Aggregate(GROUP BY p, top_row(o..., payload...)))`. The
//! aggregate keeps, per group, the ordering key of the best row so far in
//! Arrow row format, so a comparison is one `memcmp`, and that row's other
//! columns, also row-encoded. A column that is itself an `ORDER BY` key is
//! not stored twice: it is decoded from the key. Running inside DataFusion's
//! hash aggregate, it gets the partial and final stages, memory accounting
//! and spilling for free. The projection rebuilds the window's output schema
//! (the row number is the constant 1), so nothing above the filter changes.
//!
//! The rewrite declines anything it cannot keep exactly: another window
//! expression in the same node, a predicate that keeps more than the first
//! row, a volatile key, or a column that is not fixed width (strings, lists:
//! the per-group buffers are flat). Those plans run as before. Among rows
//! that tie on every `ORDER BY` key the first one seen wins, which is as
//! arbitrary as the window's choice.

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::array::{
    new_null_array, Array, ArrayRef, AsArray, BooleanArray, FixedSizeBinaryArray, StructArray,
    UInt32Array,
};
use datafusion::arrow::buffer::{Buffer, NullBuffer};
use datafusion::arrow::compute::{take_arrays, SortOptions};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Fields};
use datafusion::arrow::row::{RowConverter, SortField};
use datafusion::common::tree_node::Transformed;
use datafusion::common::{internal_err, Column, Result, ScalarValue};
use datafusion::functions::core::expr_fn::get_field;
use datafusion::logical_expr::expr::{AggregateFunction, WindowFunction, WindowFunctionParams};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::logical_plan::{Aggregate, Filter, Projection};
use datafusion::logical_expr::utils::{conjunction, split_conjunction};
use datafusion::logical_expr::{
    lit, Accumulator, AggregateUDF, AggregateUDFImpl, BinaryExpr, EmitTo, Expr, ExprSchemable,
    GroupsAccumulator, LogicalPlan, Operator, Signature, Volatility, WindowFunctionDefinition,
};
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};

/// Name of the aggregate, and of its output column in the rewritten plan.
const TOP_ROW: &str = "__sqe_top_row";

/// Rewrites a filter keeping only `ROW_NUMBER() = 1` over a partitioned
/// window into a hash aggregate. See the module docs.
#[derive(Debug, Default)]
pub struct TopRowNumberRule;

impl OptimizerRule for TopRowNumberRule {
    fn name(&self) -> &str {
        "sqe_top_row_number"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let LogicalPlan::Filter(filter) = &plan else {
            return Ok(Transformed::no(plan));
        };
        match rewrite_filter(filter)? {
            Some(rewritten) => Ok(Transformed::yes(rewritten)),
            None => Ok(Transformed::no(plan)),
        }
    }
}

fn rewrite_filter(filter: &Filter) -> Result<Option<LogicalPlan>> {
    // The window, directly or under a projection that only picks columns
    // (what projection pruning leaves when the query reads a subset).
    let (window, picked) = match filter.input.as_ref() {
        LogicalPlan::Window(window) => (window, None),
        LogicalPlan::Projection(projection)
            if projection.expr.iter().all(|e| matches!(e, Expr::Column(_))) =>
        {
            match projection.input.as_ref() {
                LogicalPlan::Window(window) => (window, Some(&projection.expr)),
                _ => return Ok(None),
            }
        }
        _ => return Ok(None),
    };
    let [Expr::WindowFunction(function)] = window.window_expr.as_slice() else {
        return Ok(None);
    };
    let WindowFunction { fun, params } = function.as_ref();
    let WindowFunctionDefinition::WindowUDF(udwf) = fun else {
        return Ok(None);
    };
    let WindowFunctionParams {
        args,
        partition_by,
        order_by,
        filter: window_filter,
        distinct,
        ..
    } = params;
    if udwf.name() != "row_number"
        || !args.is_empty()
        || partition_by.is_empty()
        || order_by.is_empty()
        || window_filter.is_some()
        || *distinct
        || partition_by.iter().any(Expr::is_volatile)
        || order_by.iter().any(|sort| sort.expr.is_volatile())
    {
        return Ok(None);
    }

    let input = &window.input;
    let input_schema = input.schema();
    if window.schema.fields().len() != input_schema.fields().len() + 1 {
        return Ok(None);
    }
    let (rn_qualifier, rn_field) = window.schema.qualified_field(input_schema.fields().len());
    let rn = Column::new(rn_qualifier.cloned(), rn_field.name());

    // Exactly one conjunct may mention the row number, and it must keep only
    // row 1; the rest stay in a filter above the aggregate.
    let (first_row, others): (Vec<&Expr>, Vec<&Expr>) = split_conjunction(&filter.predicate)
        .into_iter()
        .partition(|conjunct| conjunct.column_refs().contains(&rn));
    if first_row.len() != 1 || !keeps_only_row_one(first_row[0], &rn) {
        return Ok(None);
    }

    let mut group_by: Vec<Expr> = Vec::with_capacity(partition_by.len());
    for key in partition_by {
        if !group_by.contains(key) {
            group_by.push(key.clone());
        }
    }
    let key_columns: HashSet<&Column> = group_by
        .iter()
        .filter_map(|key| match key {
            Expr::Column(column) => Some(column),
            _ => None,
        })
        .collect();

    let mut agg_args = Vec::with_capacity(order_by.len() + input_schema.fields().len());
    let mut order = Vec::with_capacity(order_by.len());
    let mut key_types = Vec::with_capacity(order_by.len());
    for sort in order_by {
        key_types.push(sort.expr.get_type(input_schema.as_ref())?);
        order.push(SortOptions {
            descending: !sort.asc,
            nulls_first: sort.nulls_first,
        });
        agg_args.push(sort.expr.clone());
    }
    // Every non-partition column comes back in the struct. A column that is
    // also an ORDER BY key is decoded from the stored key; the others are
    // stored once more, as the payload.
    let mut fields = Vec::with_capacity(input_schema.fields().len());
    let mut sources = Vec::with_capacity(input_schema.fields().len());
    let mut stored = Vec::new();
    for (i, (qualifier, field)) in input_schema.iter().enumerate() {
        let column = Column::new(qualifier.cloned(), field.name());
        if key_columns.contains(&column) {
            continue;
        }
        fields.push(Field::new(
            payload_name(i),
            field.data_type().clone(),
            field.is_nullable(),
        ));
        let column = Expr::Column(column);
        match order_by.iter().position(|sort| sort.expr == column) {
            Some(key) => sources.push(Source::Key(key)),
            None => {
                sources.push(Source::Stored(stored.len()));
                stored.push(field.data_type().clone());
                agg_args.push(column);
            }
        }
    }
    let Some(top_row) = TopRow::try_new(key_types, order, stored, fields.into(), sources)? else {
        return Ok(None);
    };

    let aggregate = Expr::AggregateFunction(AggregateFunction::new_udf(
        Arc::new(AggregateUDF::new_from_impl(top_row)),
        agg_args,
        false,
        None,
        vec![],
        None,
    ))
    .alias(TOP_ROW);
    let aggregate = Aggregate::try_new(Arc::clone(input), group_by.clone(), vec![aggregate])?;

    // Rebuild the window's output: the partition columns from the group
    // keys, every other column from the kept row, and row number 1.
    let mut exprs = Vec::with_capacity(window.schema.fields().len());
    for (i, (qualifier, field)) in input_schema.iter().enumerate() {
        let column = Column::new(qualifier.cloned(), field.name());
        if key_columns.contains(&column) {
            exprs.push(Expr::Column(column));
        } else {
            exprs.push(
                get_field(Expr::Column(Column::from_name(TOP_ROW)), payload_name(i))
                    .alias_qualified(qualifier.cloned(), field.name()),
            );
        }
    }
    exprs.push(
        lit(ScalarValue::new_one(rn_field.data_type())?)
            .alias_qualified(rn_qualifier.cloned(), rn_field.name()),
    );
    let projection = Projection::try_new(exprs, Arc::new(LogicalPlan::Aggregate(aggregate)))?;
    if projection
        .schema
        .has_equivalent_names_and_types(&window.schema)
        .is_err()
    {
        return Ok(None);
    }
    let mut projection = LogicalPlan::Projection(projection);
    if let Some(picked) = picked {
        projection =
            LogicalPlan::Projection(Projection::try_new(picked.clone(), Arc::new(projection))?);
    }
    Ok(Some(match conjunction(others.into_iter().cloned()) {
        Some(predicate) => LogicalPlan::Filter(Filter::try_new(predicate, Arc::new(projection))?),
        None => projection,
    }))
}

fn payload_name(input_index: usize) -> String {
    format!("c{input_index}")
}

/// `rn = 1`, `1 = rn`, `rn <= 1`, `rn < 2` and their mirrors, with `rn`
/// possibly cast. Anything else may keep more rows than the first.
fn keeps_only_row_one(expr: &Expr, rn: &Column) -> bool {
    let Expr::BinaryExpr(BinaryExpr { left, op, right }) = expr else {
        return false;
    };
    let (literal, op) = if is_row_number(left, rn) {
        (right.as_ref(), *op)
    } else if is_row_number(right, rn) {
        match op.swap() {
            Some(op) => (left.as_ref(), op),
            None => return false,
        }
    } else {
        return false;
    };
    let Expr::Literal(value, _) = literal else {
        return false;
    };
    if !value.data_type().is_integer() {
        return false;
    }
    let Ok(ScalarValue::Int64(Some(value))) = value.cast_to(&DataType::Int64) else {
        return false;
    };
    matches!(
        (op, value),
        (Operator::Eq | Operator::LtEq, 1) | (Operator::Lt, 2)
    )
}

fn is_row_number(expr: &Expr, rn: &Column) -> bool {
    match expr {
        Expr::Column(column) => column == rn,
        Expr::Cast(cast) => is_row_number(&cast.expr, rn),
        Expr::TryCast(cast) => is_row_number(&cast.expr, rn),
        _ => false,
    }
}

/// Where a field of the output struct comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Source {
    /// The n-th `ORDER BY` key, decoded from the stored key.
    Key(usize),
    /// The n-th stored payload column.
    Stored(usize),
}

/// `top_row(o_1..o_m, p_1..p_n)`: per group, the row that sorts first by
/// `o` under `order`, as a struct of `fields` drawn from `o` and `p` by
/// `sources`. Its state is the encoded key and, when there are `p`
/// columns, the encoded payload: fixed-size binary columns.
#[derive(Debug, PartialEq, Eq, Hash)]
struct TopRow {
    signature: Signature,
    key_types: Vec<DataType>,
    order: Vec<SortOptions>,
    stored: Vec<DataType>,
    fields: Fields,
    sources: Vec<Source>,
    key_width: usize,
    /// 0 when nothing is stored besides the key.
    payload_width: usize,
}

impl TopRow {
    /// `None` when a key or stored column is not fixed width.
    fn try_new(
        key_types: Vec<DataType>,
        order: Vec<SortOptions>,
        stored: Vec<DataType>,
        fields: Fields,
        sources: Vec<Source>,
    ) -> Result<Option<Self>> {
        let fixed = |t: &DataType| t.is_primitive() || matches!(t, DataType::Boolean);
        if key_types.is_empty()
            || fields.is_empty()
            || fields.len() != sources.len()
            || !key_types.iter().all(fixed)
            || !stored.iter().all(fixed)
        {
            return Ok(None);
        }
        let mut arg_types = key_types.clone();
        arg_types.extend(stored.iter().cloned());
        let mut top_row = Self {
            signature: Signature::exact(arg_types, Volatility::Immutable),
            key_types,
            order,
            stored,
            fields,
            sources,
            key_width: 0,
            payload_width: 0,
        };
        let (key, payload) = top_row.converters()?;
        top_row.key_width = row_width(&key, &top_row.key_types)?;
        if let Some(payload) = payload {
            top_row.payload_width = row_width(&payload, &top_row.stored)?;
        }
        Ok(Some(top_row))
    }

    fn converters(&self) -> Result<(RowConverter, Option<RowConverter>)> {
        let key = RowConverter::new(
            self.key_types
                .iter()
                .zip(&self.order)
                .map(|(t, options)| SortField::new_with_options(t.clone(), *options))
                .collect(),
        )?;
        let payload = if self.stored.is_empty() {
            None
        } else {
            Some(RowConverter::new(
                self.stored
                    .iter()
                    .map(|t| SortField::new(t.clone()))
                    .collect(),
            )?)
        };
        Ok((key, payload))
    }

    fn groups(&self) -> Result<TopRowGroups> {
        let (key, payload) = self.converters()?;
        Ok(TopRowGroups {
            n_keys: self.key_types.len(),
            key_converter: key,
            key_width: self.key_width,
            payload_converter: payload,
            payload_width: self.payload_width,
            fields: self.fields.clone(),
            sources: self.sources.clone(),
            keys: Vec::new(),
            payloads: Vec::new(),
            has: Vec::new(),
            candidates: Vec::new(),
        })
    }

    fn state_types(&self) -> Result<Vec<DataType>> {
        let width = |w: usize| match i32::try_from(w) {
            Ok(w) => Ok(DataType::FixedSizeBinary(w)),
            Err(_) => internal_err!("{TOP_ROW}: row too wide"),
        };
        let mut types = vec![width(self.key_width)?];
        if self.payload_width > 0 {
            types.push(width(self.payload_width)?);
        }
        Ok(types)
    }
}

/// Encoded width of one row of `types`; the same for every row, null or
/// not, since every type is fixed width.
fn row_width(converter: &RowConverter, types: &[DataType]) -> Result<usize> {
    let nulls: Vec<ArrayRef> = types.iter().map(|t| new_null_array(t, 1)).collect();
    Ok(converter.convert_columns(&nulls)?.row(0).as_ref().len())
}

impl AggregateUDFImpl for TopRow {
    fn name(&self) -> &str {
        TOP_ROW
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Struct(self.fields.clone()))
    }

    fn return_field(&self, _arg_fields: &[FieldRef]) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(
            TOP_ROW,
            DataType::Struct(self.fields.clone()),
            false,
        )))
    }

    fn is_nullable(&self) -> bool {
        false
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        let names = ["key", "row"];
        Ok(self
            .state_types()?
            .into_iter()
            .zip(names)
            .map(|(t, part)| Arc::new(Field::new(format!("{}[{part}]", args.name), t, true)))
            .collect())
    }

    fn accumulator(&self, _args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(TopRowSingle(self.groups()?)))
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        true
    }

    fn create_groups_accumulator(
        &self,
        _args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        Ok(Box::new(self.groups()?))
    }
}

/// Per group: the encoded ordering key of the best row so far, and that
/// row's encoded payload, in flat buffers of fixed stride.
#[derive(Debug)]
struct TopRowGroups {
    n_keys: usize,
    key_converter: RowConverter,
    key_width: usize,
    payload_converter: Option<RowConverter>,
    payload_width: usize,
    fields: Fields,
    sources: Vec<Source>,
    keys: Vec<u8>,
    payloads: Vec<u8>,
    has: Vec<bool>,
    /// Scratch for `update_batch`: (group, input row) of every improvement.
    candidates: Vec<(usize, u32)>,
}

impl TopRowGroups {
    fn resize(&mut self, groups: usize) {
        if self.has.len() < groups {
            self.keys.resize(groups * self.key_width, 0);
            self.payloads.resize(groups * self.payload_width, 0);
            self.has.resize(groups, false);
        }
    }

    /// Keeps `key` for `group` if it sorts strictly before the current one.
    fn offer(&mut self, group: usize, key: &[u8]) -> Result<bool> {
        if key.len() != self.key_width {
            return internal_err!(
                "{TOP_ROW}: key of {} bytes, expected {}",
                key.len(),
                self.key_width
            );
        }
        let slot = &mut self.keys[group * self.key_width..(group + 1) * self.key_width];
        if self.has[group] && key >= &*slot {
            return Ok(false);
        }
        slot.copy_from_slice(key);
        self.has[group] = true;
        Ok(true)
    }

    fn set_payload(&mut self, group: usize, row: &[u8]) -> Result<()> {
        if row.len() != self.payload_width {
            return internal_err!(
                "{TOP_ROW}: row of {} bytes, expected {}",
                row.len(),
                self.payload_width
            );
        }
        self.payloads[group * self.payload_width..(group + 1) * self.payload_width]
            .copy_from_slice(row);
        Ok(())
    }

    fn emit_len(&self, emit_to: EmitTo) -> usize {
        match emit_to {
            EmitTo::All => self.has.len(),
            EmitTo::First(n) => n.min(self.has.len()),
        }
    }

    fn decode(converter: &RowConverter, bytes: &[u8], width: usize) -> Result<Vec<ArrayRef>> {
        let parser = converter.parser();
        Ok(converter.convert_rows(bytes.chunks_exact(width).map(|row| parser.parse(row)))?)
    }

    /// The kept rows of the first `n` groups, as a struct column.
    fn values(&self, n: usize) -> Result<ArrayRef> {
        if self.has[..n].iter().any(|has| !has) {
            return internal_err!("{TOP_ROW}: a group without a row");
        }
        let keys = if self.sources.iter().any(|s| matches!(s, Source::Key(_))) {
            Self::decode(
                &self.key_converter,
                &self.keys[..n * self.key_width],
                self.key_width,
            )?
        } else {
            Vec::new()
        };
        let stored = match &self.payload_converter {
            Some(converter) => Self::decode(
                converter,
                &self.payloads[..n * self.payload_width],
                self.payload_width,
            )?,
            None => Vec::new(),
        };
        let columns = self
            .sources
            .iter()
            .map(|source| match *source {
                Source::Key(i) => Arc::clone(&keys[i]),
                Source::Stored(i) => Arc::clone(&stored[i]),
            })
            .collect();
        Ok(Arc::new(StructArray::try_new(
            self.fields.clone(),
            columns,
            None,
        )?))
    }

    /// The state of the first `n` groups: encoded keys and payloads.
    fn state_arrays(&self, n: usize) -> Result<Vec<ArrayRef>> {
        let nulls = NullBuffer::from(self.has[..n].to_vec());
        let column = |bytes: &[u8], width: usize| -> Result<ArrayRef> {
            Ok(Arc::new(FixedSizeBinaryArray::try_new(
                width as i32,
                Buffer::from(bytes.to_vec()),
                Some(nulls.clone()),
            )?))
        };
        let mut state = vec![column(&self.keys[..n * self.key_width], self.key_width)?];
        if self.payload_width > 0 {
            state.push(column(
                &self.payloads[..n * self.payload_width],
                self.payload_width,
            )?);
        }
        Ok(state)
    }

    /// Drops the first `n` groups; the rest move to the front.
    fn drain(&mut self, n: usize) {
        if n >= self.has.len() {
            self.keys = Vec::new();
            self.payloads = Vec::new();
            self.has = Vec::new();
        } else {
            self.keys.drain(..n * self.key_width);
            self.payloads.drain(..n * self.payload_width);
            self.has.drain(..n);
        }
    }

    fn encode(
        converter: &RowConverter,
        columns: &[ArrayRef],
        width: usize,
        nulls: Option<NullBuffer>,
    ) -> Result<ArrayRef> {
        let rows = converter.convert_columns(columns)?;
        let mut bytes = Vec::with_capacity(rows.num_rows() * width);
        for row in rows.iter() {
            let row = row.as_ref();
            if row.len() != width {
                return internal_err!("{TOP_ROW}: row of {} bytes, expected {width}", row.len());
            }
            bytes.extend_from_slice(row);
        }
        Ok(Arc::new(FixedSizeBinaryArray::try_new(
            width as i32,
            Buffer::from(bytes),
            nulls,
        )?))
    }
}

impl GroupsAccumulator for TopRowGroups {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.resize(total_num_groups);
        let (key_columns, payload_columns) = values.split_at(self.n_keys);
        let keys = self.key_converter.convert_columns(key_columns)?;
        let mut candidates = std::mem::take(&mut self.candidates);
        candidates.clear();
        for (i, &group) in group_indices.iter().enumerate() {
            if let Some(filter) = opt_filter {
                if !filter.is_valid(i) || !filter.value(i) {
                    continue;
                }
            }
            if self.offer(group, keys.row(i).as_ref())? {
                candidates.push((group, i as u32));
            }
        }
        // Encode the payload of the improving rows only. Later improvements
        // of a group overwrite earlier ones, so the payload left is the one
        // of the key left.
        let rows = match &self.payload_converter {
            Some(converter) if !candidates.is_empty() => {
                let indices = UInt32Array::from_iter_values(candidates.iter().map(|&(_, row)| row));
                let taken = take_arrays(payload_columns, &indices, None)?;
                Some(converter.convert_columns(&taken)?)
            }
            _ => None,
        };
        if let Some(rows) = rows {
            for (row, &(group, _)) in rows.iter().zip(&candidates) {
                self.set_payload(group, row.as_ref())?;
            }
        }
        self.candidates = candidates;
        Ok(())
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> Result<()> {
        self.resize(total_num_groups);
        let expected = if self.payload_width > 0 { 2 } else { 1 };
        if values.len() != expected {
            return internal_err!(
                "{TOP_ROW}: expected {expected} state columns, got {}",
                values.len()
            );
        }
        let keys = values[0].as_fixed_size_binary();
        let payloads = values.get(1).map(|v| v.as_fixed_size_binary());
        for (i, &group) in group_indices.iter().enumerate() {
            if keys.is_valid(i) && self.offer(group, keys.value(i))? {
                if let Some(payloads) = payloads {
                    self.set_payload(group, payloads.value(i))?;
                }
            }
        }
        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let n = self.emit_len(emit_to);
        let values = self.values(n)?;
        self.drain(n);
        Ok(values)
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let n = self.emit_len(emit_to);
        let state = self.state_arrays(n)?;
        self.drain(n);
        Ok(state)
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        let (key_columns, payload_columns) = values.split_at(self.n_keys);
        let nulls = opt_filter.map(|filter| {
            NullBuffer::from_iter((0..filter.len()).map(|i| filter.is_valid(i) && filter.value(i)))
        });
        let mut state = vec![Self::encode(
            &self.key_converter,
            key_columns,
            self.key_width,
            nulls.clone(),
        )?];
        if let Some(converter) = &self.payload_converter {
            state.push(Self::encode(
                converter,
                payload_columns,
                self.payload_width,
                nulls,
            )?);
        }
        Ok(state)
    }

    fn size(&self) -> usize {
        self.keys.capacity()
            + self.payloads.capacity()
            + self.has.capacity()
            + self.candidates.capacity() * std::mem::size_of::<(usize, u32)>()
            + self.key_converter.size()
            + self
                .payload_converter
                .as_ref()
                .map_or(0, RowConverter::size)
    }
}

/// The same aggregate for one group, for plans without a groups
/// accumulator.
#[derive(Debug)]
struct TopRowSingle(TopRowGroups);

impl Accumulator for TopRowSingle {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let rows = values.first().map_or(0, |v| v.len());
        self.0.update_batch(values, &vec![0; rows], None, 1)
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let rows = states.first().map_or(0, |v| v.len());
        self.0.merge_batch(states, &vec![0; rows], 1)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if !self.0.has.first().copied().unwrap_or(false) {
            return ScalarValue::try_new_null(&DataType::Struct(self.0.fields.clone()));
        }
        ScalarValue::try_from_array(&self.0.values(1)?, 0)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.0.resize(1);
        self.0
            .state_arrays(1)?
            .iter()
            .map(|array| ScalarValue::try_from_array(array, 0))
            .collect()
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.0.size()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{
        Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
    };
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::datasource::MemTable;
    use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
    use datafusion::physical_plan::{collect, displayable, ExecutionPlan};
    use datafusion::prelude::{SessionConfig, SessionContext};

    fn table() -> Arc<MemTable> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("k", DataType::Int64, true),
            Field::new("g", DataType::Int32, false),
            Field::new("ts", DataType::Int64, true),
            Field::new("v", DataType::Float64, true),
            Field::new("s", DataType::Utf8, false),
        ]));
        let batch = |range: std::ops::Range<i64>| {
            let ids: Vec<i64> = range.collect();
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(ids.clone())),
                    Arc::new(Int64Array::from_iter(
                        ids.iter().map(|id| (id % 101 != 0).then_some(id % 37)),
                    )),
                    Arc::new(Int32Array::from_iter_values(
                        ids.iter().map(|id| (id % 3) as i32),
                    )),
                    Arc::new(Int64Array::from_iter(
                        ids.iter()
                            .map(|id| (id % 53 != 0).then_some((id * 7919) % 1000)),
                    )),
                    Arc::new(Float64Array::from_iter(
                        ids.iter()
                            .map(|id| (id % 11 != 0).then_some(*id as f64 * 0.5)),
                    )),
                    Arc::new(StringArray::from_iter_values(
                        ids.iter().map(|id| format!("s{id}")),
                    )),
                ],
            )
            .unwrap()
        };
        // Several batches in two partitions, so partial and final stages
        // both merge.
        let partitions = vec![
            vec![batch(0..300), batch(300..500)],
            vec![batch(500..800), batch(800..1000)],
        ];
        Arc::new(MemTable::try_new(schema, partitions).unwrap())
    }

    fn config() -> SessionConfig {
        super::super::engine_session_config()
            .with_target_partitions(4)
            .with_batch_size(64)
    }

    fn ctx(rewrite: bool) -> SessionContext {
        let ctx = if rewrite {
            super::super::engine_context(config(), Arc::new(RuntimeEnv::default()))
        } else {
            SessionContext::new_with_config(config())
        };
        ctx.register_table("t", table()).unwrap();
        ctx
    }

    async fn run(ctx: &SessionContext, sql: &str) -> (String, String) {
        let df = ctx.sql(sql).await.unwrap();
        let plan = df.clone().create_physical_plan().await.unwrap();
        let shown = displayable(plan.as_ref()).indent(false).to_string();
        let batches = df.collect().await.unwrap();
        (pretty_format_batches(&batches).unwrap().to_string(), shown)
    }

    /// Same rows as DataFusion's own window, and the rewrite ran or not.
    async fn assert_same(sql: &str, expect_rewrite: bool) {
        let (want, _) = run(&ctx(false), sql).await;
        let (got, plan) = run(&ctx(true), sql).await;
        assert_eq!(
            plan.contains(super::TOP_ROW),
            expect_rewrite,
            "rewrite expectation for {sql}\n{plan}"
        );
        assert_eq!(got, want, "{sql}\n{plan}");
    }

    fn first_row(order: &str, keep: &str) -> String {
        format!(
            "SELECT id, k, g, ts, v FROM (SELECT id, k, g, ts, v, ROW_NUMBER() OVER \
             (PARTITION BY k ORDER BY {order}) AS rn FROM t) x WHERE {keep} \
             ORDER BY k NULLS FIRST"
        )
    }

    #[tokio::test]
    async fn first_row_matches_the_window() {
        for order in [
            "ts DESC, id DESC",
            "ts DESC NULLS LAST, id",
            "ts ASC NULLS FIRST, id",
            "ts, id DESC",
            "v DESC, id",
            "g, ts DESC NULLS LAST, id",
            "ts + 1 DESC, id",
        ] {
            for keep in [
                "rn = 1",
                "1 = rn",
                "rn <= 1",
                "rn < 2",
                "CAST(rn AS INT) = 1",
            ] {
                assert_same(&first_row(order, keep), true).await;
            }
        }
    }

    #[tokio::test]
    async fn other_conjuncts_and_the_row_number_column_survive() {
        assert_same(&first_row("ts DESC, id", "rn = 1 AND v > 100"), true).await;
        assert_same(
            "SELECT k, g, id, rn FROM (SELECT id, k, g, ROW_NUMBER() OVER \
             (PARTITION BY k, g ORDER BY id DESC) AS rn FROM t) x WHERE rn = 1 \
             ORDER BY k NULLS FIRST, g",
            true,
        )
        .await;
        // A partition key that is an expression, not a column.
        assert_same(
            "SELECT id, ts FROM (SELECT id, ts, ROW_NUMBER() OVER \
             (PARTITION BY id % 7 ORDER BY ts DESC NULLS LAST, id) AS rn FROM t) x \
             WHERE rn = 1 ORDER BY id",
            true,
        )
        .await;
    }

    /// The query reads a subset of the columns, so a projection sits
    /// between the filter and the window; and every kept column is a key or
    /// an ORDER BY key, so nothing is stored besides the key.
    #[tokio::test]
    async fn pruned_columns_and_key_only_state() {
        assert_same(
            "SELECT k, v FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY k ORDER BY ts DESC, \
             id) AS rn FROM t) x WHERE rn = 1 ORDER BY k NULLS FIRST",
            true,
        )
        .await;
        // Only the partition key is read: the rewritten aggregate has no
        // output left, and the optimizer reduces it to a DISTINCT.
        assert_same(
            "SELECT k FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY k ORDER BY ts DESC, id) \
             AS rn FROM t) x WHERE rn = 1 ORDER BY k NULLS FIRST",
            false,
        )
        .await;
        assert_same(
            "SELECT k, ts, id FROM (SELECT k, ts, id, ROW_NUMBER() OVER (PARTITION BY k \
             ORDER BY ts DESC NULLS LAST, id DESC) AS rn FROM t) x WHERE rn = 1 \
             ORDER BY k NULLS FIRST",
            true,
        )
        .await;
    }

    #[tokio::test]
    async fn anything_else_keeps_the_window() {
        for keep in [
            "rn <= 2",
            "rn = 2",
            "rn = 1 OR v > 100",
            "rn > 0 AND rn = 1",
        ] {
            assert_same(&first_row("ts DESC, id", keep), false).await;
        }
        // A string payload column.
        assert_same(
            "SELECT s FROM (SELECT s, ROW_NUMBER() OVER (PARTITION BY k ORDER BY id) AS rn \
             FROM t) x WHERE rn = 1 ORDER BY s",
            false,
        )
        .await;
        // No partition.
        assert_same(
            "SELECT id FROM (SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM t) x \
             WHERE rn = 1",
            false,
        )
        .await;
        assert_same(
            "SELECT id FROM (SELECT id, k, RANK() OVER (PARTITION BY k ORDER BY id) AS r \
             FROM t) x WHERE r = 1 ORDER BY id",
            false,
        )
        .await;
    }

    /// With ties on every ORDER BY key the kept row is arbitrary, as in the
    /// window, but it is one row per partition and it does sort first.
    #[tokio::test]
    async fn ties_keep_one_row_that_sorts_first() {
        let sql = "SELECT g, k, ts FROM (SELECT g, k, ts, ROW_NUMBER() OVER \
                   (PARTITION BY g ORDER BY k DESC NULLS LAST) AS rn FROM t) x WHERE rn = 1";
        let ctx = ctx(true);
        let (_, plan) = run(&ctx, sql).await;
        assert!(plan.contains(super::TOP_ROW), "{plan}");
        let (want, _) = run(&ctx, "SELECT g, max(k) AS k FROM t GROUP BY g ORDER BY g").await;
        let (got, _) = run(&ctx, &format!("SELECT g, k FROM ({sql}) y ORDER BY g")).await;
        assert_eq!(got, want);
    }

    fn spill_count(plan: &Arc<dyn ExecutionPlan>) -> usize {
        plan.metrics().and_then(|m| m.spill_count()).unwrap_or(0)
            + plan
                .children()
                .iter()
                .map(|c| spill_count(c))
                .sum::<usize>()
    }

    /// Many groups in a small memory pool: the aggregate spills instead of
    /// failing, and still keeps the right rows.
    #[tokio::test]
    async fn spills_under_a_small_pool() {
        let sql = "SELECT k, ts, v FROM (SELECT k, ts, v, ROW_NUMBER() OVER \
                   (PARTITION BY k ORDER BY ts DESC) AS rn FROM \
                   (SELECT value % 200000 AS k, value AS ts, value * 2 AS v \
                    FROM generate_series(1, 2000000))) x WHERE rn = 1";
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_limit(16 * 1024 * 1024, 1.0)
            .build_arc()
            .unwrap();
        let ctx = super::super::engine_context(
            super::super::engine_session_config().with_target_partitions(2),
            runtime,
        );
        let df = ctx.sql(sql).await.unwrap();
        let plan = df.create_physical_plan().await.unwrap();
        let shown = displayable(plan.as_ref()).indent(false).to_string();
        assert!(shown.contains(super::TOP_ROW), "{shown}");
        let batches = collect(Arc::clone(&plan), ctx.task_ctx()).await.unwrap();
        assert!(spill_count(&plan) > 0, "no spill\n{shown}");
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 200_000);
        // Latest row of k is ts = 1_800_000 + k (k = 0 -> 2_000_000).
        for batch in &batches {
            let k = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let ts = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let v = batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for i in 0..batch.num_rows() {
                let k = k.value(i);
                let want = if k == 0 { 2_000_000 } else { 1_800_000 + k };
                assert_eq!(ts.value(i), want);
                assert_eq!(v.value(i), want * 2);
            }
        }
    }
}
