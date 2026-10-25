// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! SQE PATCH (v3-shredded-variant): shred variant columns on write when the
//! table sets `write.parquet.shred-variants`, top-level or nested in a
//! struct, list or map.
//!
//! The shredding schema is inferred per data file from its first batch:
//! a column whose values are all objects shreds every field that has one
//! type in every row (recursing into nested objects); a column whose values
//! are all one primitive type shreds that type. Arrays, decimals, floats,
//! timestamps and mixed fields stay in the binary `value`.
//!
//! `shred_variant` casts leniently (a boolean or a whole double fits an
//! integer `typed_value`), which would change the value read back. A row
//! that holds a value of another type at a shredded path is therefore
//! written unshredded: its whole value goes to `value` and its top-level
//! `typed_value` is NULL, which the shredding spec allows.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{
    Array, ArrayRef, BooleanArray, LargeListArray, ListArray, MapArray, RecordBatch, StructArray,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use parquet::variant::{Variant, VariantArray, shred_variant};

use crate::{Error, ErrorKind, Result};

/// Most fields shredded in one variant value, so a wide document does not
/// turn into thousands of Parquet leaves. A nested variant (one per list
/// element type, map value type or struct member) has its own budget.
const MAX_SHREDDED_FIELDS: usize = 64;

const VARIANT_EXTENSION: &str = "arrow.parquet.variant";

/// The type every non-null value seen at one path shares.
#[derive(Clone, Debug, PartialEq)]
enum Shape {
    Unseen,
    Int,
    Double,
    Str,
    Bool,
    Date,
    Object(BTreeMap<String, Shape>),
    Mixed,
}

fn primitive_shape(value: &Variant<'_, '_>) -> Shape {
    match value {
        Variant::Int8(_) | Variant::Int16(_) | Variant::Int32(_) | Variant::Int64(_) => Shape::Int,
        Variant::Double(_) => Shape::Double,
        Variant::String(_) | Variant::ShortString(_) => Shape::Str,
        Variant::BooleanTrue | Variant::BooleanFalse => Shape::Bool,
        Variant::Date(_) => Shape::Date,
        _ => Shape::Mixed,
    }
}

/// `shape` widened by one more value.
fn observe(shape: Shape, value: &Variant<'_, '_>) -> Shape {
    if let Variant::Null = value {
        return shape;
    }
    // Objects win: a non-object row next to object rows is written to the
    // residual `value`, so it must not stop the object fields shredding.
    if let Variant::Object(object) = value {
        let mut fields = match shape {
            Shape::Object(fields) => fields,
            _ => BTreeMap::new(),
        };
        for (name, field) in object.iter() {
            let seen = fields.remove(name).unwrap_or(Shape::Unseen);
            fields.insert(name.to_string(), observe(seen, &field));
        }
        return Shape::Object(fields);
    }
    let here = primitive_shape(value);
    match shape {
        Shape::Unseen => here,
        Shape::Object(fields) => Shape::Object(fields),
        seen if seen == here => seen,
        _ => Shape::Mixed,
    }
}

/// What one variant column is shredded into: only the paths that are
/// written to `typed_value`.
#[derive(Clone, Debug, PartialEq)]
enum Plan {
    Primitive(Shape),
    Object(BTreeMap<String, Plan>),
}

impl Plan {
    fn from_shape(shape: &Shape, budget: &mut usize) -> Option<Plan> {
        match shape {
            Shape::Unseen | Shape::Mixed => None,
            Shape::Object(fields) => {
                let mut planned = BTreeMap::new();
                for (name, field) in fields {
                    if *budget == 0 {
                        break;
                    }
                    if let Some(plan) = Plan::from_shape(field, budget) {
                        if matches!(plan, Plan::Primitive(_)) {
                            *budget -= 1;
                        }
                        planned.insert(name.clone(), plan);
                    }
                }
                (!planned.is_empty()).then_some(Plan::Object(planned))
            }
            primitive => Some(Plan::Primitive(primitive.clone())),
        }
    }

    /// The `as_type` argument of `shred_variant`.
    fn shredding_type(&self) -> DataType {
        match self {
            Plan::Primitive(Shape::Int) => DataType::Int64,
            Plan::Primitive(Shape::Double) => DataType::Float64,
            Plan::Primitive(Shape::Str) => DataType::Utf8,
            Plan::Primitive(Shape::Bool) => DataType::Boolean,
            Plan::Primitive(Shape::Date) => DataType::Date32,
            Plan::Primitive(other) => unreachable!("not a shredded shape: {other:?}"),
            Plan::Object(fields) => DataType::Struct(
                fields
                    .iter()
                    .map(|(name, plan)| Field::new(name, plan.shredding_type(), true))
                    .collect(),
            ),
        }
    }

    /// Whether `value` shreds under this plan without changing its type.
    fn fits(&self, value: &Variant<'_, '_>) -> bool {
        match (self, value) {
            (_, Variant::Null) => true,
            (Plan::Object(fields), Variant::Object(object)) => object
                .iter()
                .all(|(name, field)| fields.get(name).is_none_or(|plan| plan.fits(&field))),
            // A non-object goes to `value` whole.
            (Plan::Object(_), _) => true,
            (Plan::Primitive(shape), value) => primitive_shape(value) == *shape,
        }
    }
}

/// Where the variants in one column are, and how each is shredded.
#[derive(Debug)]
enum Node {
    /// A variant value, shredded as `Plan` into `typed_value` of the type.
    Variant(Plan, DataType),
    /// A struct; `None` for a member with nothing to shred.
    Struct(Vec<Option<Node>>),
    /// A list (or large list) of elements.
    List(Box<Node>),
    /// A map; the node of its entries struct.
    Map(Box<Node>),
}

impl Node {
    /// The shredding of `array`, a column declared as `field`, or `None`
    /// when no variant in it has anything to shred.
    fn infer(field: &Field, array: &ArrayRef) -> Result<Option<Node>> {
        if is_variant(field) {
            return infer_variant(array);
        }
        let node = match field.data_type() {
            DataType::Struct(fields) => {
                let Some(array) = array.as_struct_opt() else {
                    return Ok(None);
                };
                let members = fields
                    .iter()
                    .zip(array.columns())
                    .map(|(f, column)| Node::infer(f, column))
                    .collect::<Result<Vec<_>>>()?;
                if members.iter().all(Option::is_none) {
                    return Ok(None);
                }
                Node::Struct(members)
            }
            DataType::List(element) => match array.as_list_opt::<i32>() {
                Some(list) => match Node::infer(element, list.values())? {
                    Some(node) => Node::List(Box::new(node)),
                    None => return Ok(None),
                },
                None => return Ok(None),
            },
            DataType::LargeList(element) => match array.as_list_opt::<i64>() {
                Some(list) => match Node::infer(element, list.values())? {
                    Some(node) => Node::List(Box::new(node)),
                    None => return Ok(None),
                },
                None => return Ok(None),
            },
            DataType::Map(entries, _) => match array.as_map_opt() {
                Some(map) => {
                    let entries_array: ArrayRef = Arc::new(map.entries().clone());
                    match Node::infer(entries, &entries_array)? {
                        Some(node) => Node::Map(Box::new(node)),
                        None => return Ok(None),
                    }
                }
                None => return Ok(None),
            },
            _ => return Ok(None),
        };
        Ok(Some(node))
    }

    /// `array` with the variants this node covers shredded. Every rebuilt
    /// field keeps its name, nullability and metadata (the field id).
    fn shred(&self, array: &ArrayRef) -> Result<ArrayRef> {
        match self {
            Node::Variant(plan, as_type) => shred_column(array, plan, as_type),
            Node::Struct(members) => {
                let (fields, mut children, nulls) = array.as_struct().clone().into_parts();
                let mut rebuilt: Vec<Arc<Field>> = fields.iter().cloned().collect();
                for (i, member) in members.iter().enumerate() {
                    if let Some(node) = member {
                        children[i] = node.shred(&children[i])?;
                        rebuilt[i] = Arc::new(
                            fields[i]
                                .as_ref()
                                .clone()
                                .with_data_type(children[i].data_type().clone()),
                        );
                    }
                }
                Ok(Arc::new(
                    StructArray::try_new(rebuilt.into(), children, nulls).map_err(to_err)?,
                ))
            }
            Node::List(node) => match array.data_type() {
                DataType::List(element) => {
                    let list = array.as_list::<i32>();
                    let values = node.shred(list.values())?;
                    let element = with_type(element, values.data_type());
                    Ok(Arc::new(
                        ListArray::try_new(
                            element,
                            list.offsets().clone(),
                            values,
                            list.nulls().cloned(),
                        )
                        .map_err(to_err)?,
                    ))
                }
                DataType::LargeList(element) => {
                    let list = array.as_list::<i64>();
                    let values = node.shred(list.values())?;
                    let element = with_type(element, values.data_type());
                    Ok(Arc::new(
                        LargeListArray::try_new(
                            element,
                            list.offsets().clone(),
                            values,
                            list.nulls().cloned(),
                        )
                        .map_err(to_err)?,
                    ))
                }
                other => Err(Error::new(
                    ErrorKind::Unexpected,
                    format!("A shredded variant list is {other}, not a list"),
                )),
            },
            Node::Map(node) => {
                let DataType::Map(entries_field, sorted) = array.data_type() else {
                    return Err(Error::new(
                        ErrorKind::Unexpected,
                        format!("A shredded variant map is {}, not a map", array.data_type()),
                    ));
                };
                let map = array.as_map();
                let entries: ArrayRef = Arc::new(map.entries().clone());
                let entries = node.shred(&entries)?;
                let entries_field = with_type(entries_field, entries.data_type());
                Ok(Arc::new(
                    MapArray::try_new(
                        entries_field,
                        map.offsets().clone(),
                        entries.as_struct().clone(),
                        map.nulls().cloned(),
                        *sorted,
                    )
                    .map_err(to_err)?,
                ))
            }
        }
    }
}

fn with_type(field: &Arc<Field>, data_type: &DataType) -> Arc<Field> {
    Arc::new(field.as_ref().clone().with_data_type(data_type.clone()))
}

/// The shredding of one variant `array`, or `None` when it is shredded
/// already or nothing in it has one type.
fn infer_variant(array: &ArrayRef) -> Result<Option<Node>> {
    let variants = VariantArray::try_new(array.as_ref()).map_err(to_err)?;
    if variants.typed_value_column().is_some() {
        return Ok(None);
    }
    let mut shape = Shape::Unseen;
    for row in 0..variants.len() {
        if variants.is_valid(row) {
            shape = observe(shape, &variants.value(row));
        }
    }
    Ok(
        Plan::from_shape(&shape, &mut MAX_SHREDDED_FIELDS.clone()).map(|plan| {
            let as_type = plan.shredding_type();
            Node::Variant(plan, as_type)
        }),
    )
}

/// One file's shredding: which columns, and the schema the file is written
/// with.
#[derive(Debug)]
pub(crate) struct VariantShredder {
    columns: Vec<(usize, Node)>,
    schema: SchemaRef,
}

fn is_variant(field: &Field) -> bool {
    field.extension_type_name() == Some(VARIANT_EXTENSION)
}

impl VariantShredder {
    /// The shredding for a file with `schema` whose first batch is
    /// `batch`, or `None` when no variant column has anything to shred.
    pub(crate) fn infer(schema: &SchemaRef, batch: &RecordBatch) -> Result<Option<Self>> {
        let mut columns = Vec::new();
        for (index, field) in schema.fields().iter().enumerate() {
            if let Some(node) = Node::infer(field, batch.column(index))? {
                columns.push((index, node));
            }
        }
        if columns.is_empty() {
            return Ok(None);
        }
        let mut shredder = Self {
            columns,
            schema: schema.clone(),
        };
        // The file schema is the shape `shred_variant` produces.
        let first = shredder.shred_columns(batch)?;
        let fields: Vec<_> = schema
            .fields()
            .iter()
            .zip(&first)
            .map(|(field, column)| {
                Arc::new(
                    field
                        .as_ref()
                        .clone()
                        .with_data_type(column.data_type().clone()),
                )
            })
            .collect();
        shredder.schema = Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()));
        Ok(Some(shredder))
    }

    /// The schema every shredded batch of this file has.
    pub(crate) fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// `batch` with its variant columns shredded.
    pub(crate) fn shred(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        let columns = self.shred_columns(batch)?;
        RecordBatch::try_new(self.schema.clone(), columns).map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                "A shredded variant batch does not match the file schema",
            )
            .with_source(e)
        })
    }

    fn shred_columns(&self, batch: &RecordBatch) -> Result<Vec<ArrayRef>> {
        let mut columns = batch.columns().to_vec();
        for (index, node) in &self.columns {
            columns[*index] = node.shred(batch.column(*index))?;
        }
        Ok(columns)
    }
}

fn shred_column(column: &ArrayRef, plan: &Plan, as_type: &DataType) -> Result<ArrayRef> {
    let variants = VariantArray::try_new(column.as_ref()).map_err(to_err)?;
    let misfit: BooleanArray = (0..variants.len())
        .map(|row| Some(variants.is_valid(row) && !plan.fits(&variants.value(row))))
        .collect();
    let shredded = shred_variant(&variants, as_type)
        .map_err(to_err)?
        .into_inner();
    if misfit.true_count() == 0 {
        return Ok(Arc::new(shredded));
    }
    // A row whose value would change type keeps its whole value unshredded.
    let (fields, mut children, nulls) = shredded.into_parts();
    let position = |name: &str| {
        fields.iter().position(|f| f.name() == name).ok_or_else(|| {
            Error::new(
                ErrorKind::Unexpected,
                format!("A shredded variant has no `{name}` field"),
            )
        })
    };
    let value = position("value")?;
    let typed_value = position("typed_value")?;
    let original =
        arrow_cast::cast(variants.value_column(), children[value].data_type()).map_err(to_err)?;
    children[value] =
        arrow_select::zip::zip(&misfit, &original, &children[value]).map_err(to_err)?;
    children[typed_value] =
        arrow_select::nullif::nullif(children[typed_value].as_ref(), &misfit).map_err(to_err)?;
    Ok(Arc::new(
        StructArray::try_new(fields, children, nulls).map_err(to_err)?,
    ))
}

fn to_err(e: arrow_schema::ArrowError) -> Error {
    Error::new(ErrorKind::Unexpected, "Failed to shred a variant column").with_source(e)
}

#[cfg(test)]
mod tests {
    use arrow_array::StringArray;
    use parquet::variant::{json_to_variant, unshred_variant, variant_to_json};

    use super::*;

    fn variant_batch(rows: &[Option<&str>]) -> RecordBatch {
        let json: ArrayRef = Arc::new(StringArray::from(rows.to_vec()));
        let variants = json_to_variant(&json).unwrap();
        let field = variants.field("v");
        let column: ArrayRef = Arc::new(variants.into_inner());
        RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![column]).unwrap()
    }

    fn json_rows(column: &ArrayRef) -> Vec<Option<String>> {
        let variants = VariantArray::try_new(column.as_ref()).unwrap();
        // `unshred_variant` writes BinaryView; `variant_to_json` takes Binary.
        let plain = unshred_variant(&variants).unwrap().into_inner();
        let columns = ["metadata", "value"].map(|name| {
            arrow_cast::cast(plain.column_by_name(name).unwrap(), &DataType::Binary).unwrap()
        });
        let fields: Vec<_> = ["metadata", "value"]
            .iter()
            .map(|name| Field::new(*name, DataType::Binary, true))
            .collect();
        let plain: ArrayRef = Arc::new(
            StructArray::try_new(fields.into(), columns.to_vec(), plain.nulls().cloned()).unwrap(),
        );
        let json = variant_to_json(&plain).unwrap();
        json.iter().map(|v| v.map(str::to_string)).collect()
    }

    fn typed_value(column: &ArrayRef) -> ArrayRef {
        VariantArray::try_new(column.as_ref())
            .unwrap()
            .typed_value_column()
            .expect("shredded")
            .clone()
    }

    #[test]
    fn consistent_objects_shred_their_fields() {
        let rows = [
            Some(r#"{"a": 1, "s": "x", "o": {"b": true}}"#),
            Some(r#"{"a": 300, "s": "y", "extra": [1]}"#),
            Some("[1, 2]"),
            None,
        ];
        let batch = variant_batch(&rows);
        let shredder = VariantShredder::infer(&batch.schema(), &batch)
            .unwrap()
            .expect("shreds");
        let out = shredder.shred(&batch).unwrap();
        let DataType::Struct(fields) = typed_value(out.column(0)).data_type().clone() else {
            panic!("object typed_value");
        };
        let names: Vec<_> = fields.iter().map(|f| f.name().as_str()).collect();
        // `extra` holds an array, so it stays in `value`.
        assert_eq!(names, ["a", "o", "s"]);
        assert_eq!(json_rows(out.column(0)), json_rows(batch.column(0)));
        assert_eq!(out.schema(), shredder.schema());
    }

    #[test]
    fn a_value_of_another_type_is_not_cast() {
        let batch = variant_batch(&[Some(r#"{"a": 1}"#), Some(r#"{"a": 2}"#)]);
        let shredder = VariantShredder::infer(&batch.schema(), &batch)
            .unwrap()
            .expect("shreds");
        // Later batches of the same file: a boolean and a double at `a`
        // would cast into the integer typed_value.
        let later = variant_batch(&[
            Some(r#"{"a": true}"#),
            Some(r#"{"a": 2.0e0}"#),
            Some(r#"{"a": "3"}"#),
            Some(r#"{"a": 4}"#),
        ]);
        let out = shredder.shred(&later).unwrap();
        assert_eq!(json_rows(out.column(0)), json_rows(later.column(0)));
        // A row that does not fit is written whole to `value`.
        let typed = typed_value(out.column(0));
        assert_eq!(
            (0..4).map(|r| typed.is_valid(r)).collect::<Vec<_>>(),
            [false, false, false, true]
        );
    }

    #[test]
    fn primitive_columns_shred_and_mixed_ones_do_not() {
        let ints = variant_batch(&[Some("1"), Some("2"), None]);
        let shredder = VariantShredder::infer(&ints.schema(), &ints)
            .unwrap()
            .expect("shreds");
        let out = shredder.shred(&ints).unwrap();
        assert_eq!(typed_value(out.column(0)).data_type(), &DataType::Int64);
        assert_eq!(json_rows(out.column(0)), json_rows(ints.column(0)));

        let mixed = variant_batch(&[Some("1"), Some(r#""x""#), Some("[1]")]);
        assert!(
            VariantShredder::infer(&mixed.schema(), &mixed)
                .unwrap()
                .is_none()
        );
        let all_null = variant_batch(&[None, Some("null")]);
        assert!(
            VariantShredder::infer(&all_null.schema(), &all_null)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn nested_shredded_variants_shred_in_structs_lists_and_maps() {
        let rows = [Some(r#"{"a": 1}"#), Some(r#"{"a": 2}"#), None];
        let flat = variant_batch(&rows);
        let variant_field = flat.schema().field(0).clone();
        let variant = flat.column(0).clone();
        let id = |field: Field, id: &str| {
            let mut metadata = field.metadata().clone();
            metadata.insert("PARQUET:field_id".to_string(), id.to_string());
            field.with_metadata(metadata)
        };

        // `s STRUCT<v VARIANT>`
        let member = id(variant_field.clone(), "2");
        let s: ArrayRef = Arc::new(
            StructArray::try_new(vec![member.clone()].into(), vec![variant.clone()], None).unwrap(),
        );
        // `l ARRAY<VARIANT>`, one element per row
        let element = Arc::new(id(variant_field.clone().with_name("element"), "4"));
        let offsets = arrow_buffer::OffsetBuffer::from_lengths([1; 3]);
        let l: ArrayRef = Arc::new(
            ListArray::try_new(element, offsets.clone(), variant.clone(), None).unwrap(),
        );
        // `m MAP<STRING, VARIANT>`, one entry per row
        let keys: ArrayRef = Arc::new(StringArray::from(vec!["k"; 3]));
        let entry_fields = vec![
            id(Field::new("key", DataType::Utf8, false), "6"),
            id(variant_field.clone().with_name("value"), "7"),
        ];
        let entries = StructArray::try_new(entry_fields.into(), vec![keys, variant], None).unwrap();
        let entries_field = Arc::new(Field::new("key_value", entries.data_type().clone(), false));
        let m: ArrayRef =
            Arc::new(MapArray::try_new(entries_field, offsets, entries, None, false).unwrap());

        let schema = Arc::new(Schema::new(vec![
            id(Field::new("s", s.data_type().clone(), true), "1"),
            id(Field::new("l", l.data_type().clone(), true), "3"),
            id(Field::new("m", m.data_type().clone(), true), "5"),
        ]));
        let batch = RecordBatch::try_new(schema.clone(), vec![s, l, m]).unwrap();
        let shredder = VariantShredder::infer(&schema, &batch)
            .unwrap()
            .expect("shreds");
        let out = shredder.shred(&batch).unwrap();
        assert_eq!(out.schema(), shredder.schema());

        let s = out.column(0).as_struct();
        let l = out.column(1).as_list::<i32>();
        let m = out.column(2).as_map();
        let shredded = [
            s.column(0).clone(),
            l.values().clone(),
            m.values().clone(),
        ];
        for column in &shredded {
            let DataType::Struct(fields) = typed_value(column).data_type().clone() else {
                panic!("object typed_value");
            };
            assert_eq!(fields[0].name(), "a");
            assert_eq!(json_rows(column), json_rows(flat.column(0)));
        }
        // The rebuilt fields keep their field ids and the extension.
        let DataType::Struct(members) = out.schema().field(0).data_type().clone() else {
            panic!("struct");
        };
        assert_eq!(members[0].metadata(), member.metadata());
        let DataType::List(element) = out.schema().field(1).data_type().clone() else {
            panic!("list");
        };
        assert_eq!(element.metadata().get("PARQUET:field_id").unwrap(), "4");
        assert!(is_variant(&element));
        let DataType::Map(entries, _) = out.schema().field(2).data_type().clone() else {
            panic!("map");
        };
        let DataType::Struct(entry_fields) = entries.data_type().clone() else {
            panic!("entries");
        };
        assert_eq!(entry_fields[1].metadata().get("PARQUET:field_id").unwrap(), "7");
        assert!(is_variant(&entry_fields[1]));
    }
}
