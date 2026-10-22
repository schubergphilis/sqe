//! Iceberg V3 `unknown` fields in Arrow (#859).
//!
//! An `unknown` field holds only NULLs and the spec never writes it to a
//! data file, so a file's struct lacks the members the table declares as
//! `unknown`. Arrow has `DataType::Null` for them. [`fill_unknown`] puts the
//! missing members back so a decoded column matches the declared type at
//! every depth, not only at the top level.

use std::sync::Arc;

use datafusion::arrow::array::{
    new_null_array, Array, ArrayRef, AsArray, FixedSizeListArray, LargeListArray, ListArray,
    MapArray, StructArray,
};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::arrow::error::ArrowError;

/// The Arrow field-metadata key the parquet reader puts a field id under.
const FIELD_ID_KEY: &str = "PARQUET:field_id";

/// True when `data_type` is `unknown` or holds an `unknown` field at any depth.
pub fn contains_unknown(data_type: &DataType) -> bool {
    match data_type {
        DataType::Null => true,
        DataType::Struct(fields) => fields.iter().any(|f| contains_unknown(f.data_type())),
        DataType::List(f)
        | DataType::LargeList(f)
        | DataType::ListView(f)
        | DataType::LargeListView(f)
        | DataType::FixedSizeList(f, _)
        | DataType::Map(f, _) => contains_unknown(f.data_type()),
        _ => false,
    }
}

/// Rebuild `array` as `target`: a struct member the file lacks becomes NULLs
/// (an `unknown` member, or any nullable member added later), and lists and
/// maps are rebuilt around their filled values. Members are matched by
/// parquet field id when both sides carry one, else by name. Anything else
/// that differs is cast, as a top-level promoted column is.
pub fn fill_unknown(array: &ArrayRef, target: &DataType) -> Result<ArrayRef, ArrowError> {
    if array.data_type() == target {
        return Ok(Arc::clone(array));
    }
    if array.data_type() == &DataType::Null || target == &DataType::Null {
        return Ok(new_null_array(target, array.len()));
    }
    match target {
        DataType::Struct(fields) => {
            let Some(source) = array.as_struct_opt() else {
                return cast(array, target);
            };
            let DataType::Struct(source_fields) = source.data_type() else {
                return cast(array, target);
            };
            let mut children = Vec::with_capacity(fields.len());
            for field in fields {
                let found = match field_id(field) {
                    Some(id) if source_fields.iter().all(|f| field_id(f).is_some()) => {
                        source_fields.iter().position(|f| field_id(f) == Some(id))
                    }
                    _ => source_fields.iter().position(|f| f.name() == field.name()),
                };
                children.push(match found {
                    Some(idx) => fill_unknown(source.column(idx), field.data_type())?,
                    None if field.is_nullable() => new_null_array(field.data_type(), array.len()),
                    None => {
                        return Err(ArrowError::InvalidArgumentError(format!(
                            "struct member `{}` is missing from the data file but is NOT NULL",
                            field.name()
                        )))
                    }
                });
            }
            Ok(Arc::new(StructArray::try_new(
                fields.clone(),
                children,
                source.nulls().cloned(),
            )?))
        }
        DataType::List(field) => {
            let Some(source) = array.as_list_opt::<i32>() else {
                return cast(array, target);
            };
            let values = fill_unknown(source.values(), field.data_type())?;
            Ok(Arc::new(ListArray::try_new(
                Arc::clone(field),
                source.offsets().clone(),
                values,
                source.nulls().cloned(),
            )?))
        }
        DataType::LargeList(field) => {
            let Some(source) = array.as_list_opt::<i64>() else {
                return cast(array, target);
            };
            let values = fill_unknown(source.values(), field.data_type())?;
            Ok(Arc::new(LargeListArray::try_new(
                Arc::clone(field),
                source.offsets().clone(),
                values,
                source.nulls().cloned(),
            )?))
        }
        DataType::FixedSizeList(field, size) => {
            let Some(source) = array.as_fixed_size_list_opt() else {
                return cast(array, target);
            };
            let values = fill_unknown(source.values(), field.data_type())?;
            Ok(Arc::new(FixedSizeListArray::try_new(
                Arc::clone(field),
                *size,
                values,
                source.nulls().cloned(),
            )?))
        }
        DataType::Map(field, sorted) => {
            let Some(source) = array.as_map_opt() else {
                return cast(array, target);
            };
            let entries: ArrayRef = Arc::new(source.entries().clone());
            let entries = fill_unknown(&entries, field.data_type())?;
            Ok(Arc::new(MapArray::try_new(
                Arc::clone(field),
                source.offsets().clone(),
                entries.as_struct().clone(),
                source.nulls().cloned(),
                *sorted,
            )?))
        }
        _ => cast(array, target),
    }
}

fn field_id(field: &Field) -> Option<i32> {
    field.metadata().get(FIELD_ID_KEY)?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int32Array, Int32Builder, ListBuilder};
    use datafusion::arrow::datatypes::Fields;

    fn with_id(field: Field, id: i32) -> Field {
        field.with_metadata([(FIELD_ID_KEY.to_string(), id.to_string())].into())
    }

    #[test]
    fn contains_unknown_at_any_depth() {
        let nested = DataType::Struct(Fields::from(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("u", DataType::Null, true),
        ]));
        assert!(contains_unknown(&DataType::Null));
        assert!(contains_unknown(&nested));
        assert!(contains_unknown(&DataType::List(Arc::new(Field::new(
            "element", nested, true
        )))));
        assert!(!contains_unknown(&DataType::Int32));
        assert!(!contains_unknown(&DataType::Struct(Fields::from(vec![
            Field::new("a", DataType::Int32, true)
        ]))));
    }

    #[test]
    fn struct_member_missing_from_the_file_reads_as_null() {
        let file_fields = Fields::from(vec![with_id(Field::new("a", DataType::Int32, true), 3)]);
        let file: ArrayRef = Arc::new(StructArray::new(
            file_fields,
            vec![Arc::new(Int32Array::from(vec![10, 20]))],
            None,
        ));
        let target = DataType::Struct(Fields::from(vec![
            with_id(Field::new("a", DataType::Int32, true), 3),
            with_id(Field::new("u", DataType::Null, true), 4),
        ]));
        let out = fill_unknown(&file, &target).unwrap();
        assert_eq!(out.data_type(), &target);
        let out = out.as_struct();
        assert_eq!(
            out.column(0)
                .as_primitive::<datafusion::arrow::datatypes::Int32Type>()
                .values(),
            &[10, 20]
        );
        assert_eq!(out.column(1).logical_null_count(), 2);
    }

    #[test]
    fn struct_inside_a_list_is_filled() {
        let file_struct = Fields::from(vec![Field::new("a", DataType::Int32, true)]);
        let mut builder = ListBuilder::new(datafusion::arrow::array::StructBuilder::from_fields(
            file_struct.clone(),
            0,
        ))
        .with_field(Arc::new(Field::new(
            "element",
            DataType::Struct(file_struct),
            true,
        )));
        for v in [1, 2] {
            let s = builder.values();
            s.field_builder::<Int32Builder>(0).unwrap().append_value(v);
            s.append(true);
        }
        builder.append(true);
        builder.append(false);
        let file: ArrayRef = Arc::new(builder.finish());

        let target_struct = DataType::Struct(Fields::from(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("u", DataType::Null, true),
        ]));
        let target = DataType::List(Arc::new(Field::new("element", target_struct, true)));
        let out = fill_unknown(&file, &target).unwrap();
        assert_eq!(out.data_type(), &target);
        assert_eq!(out.len(), 2);
        assert!(out.is_null(1));
        let values = out.as_list::<i32>().value(0);
        assert_eq!(values.as_struct().column(1).logical_null_count(), 2);
    }

    #[test]
    fn missing_required_member_is_an_error() {
        let file: ArrayRef = Arc::new(StructArray::new(
            Fields::from(vec![Field::new("a", DataType::Int32, true)]),
            vec![Arc::new(Int32Array::from(vec![1]))],
            None,
        ));
        let target = DataType::Struct(Fields::from(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, false),
        ]));
        assert!(fill_unknown(&file, &target).is_err());
    }
}
