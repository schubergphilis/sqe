//! Iceberg V3 `variant` values in Arrow (#849).
//!
//! Iceberg stores a variant as `Struct { metadata: Binary, value: Binary }`
//! with the canonical `arrow.parquet.variant` extension type on the field.
//! DataFusion keeps field metadata through plain column references but drops
//! it through most expressions, so detection falls back to the storage
//! shape: a struct whose fields are `metadata` and `value` (and, for a
//! shredded file, `typed_value`), both binary.
//!
//! Clients have no variant type, so the encoders render a variant as its
//! JSON text, and the write path parses JSON text into a variant.

use std::sync::Arc;

use datafusion::arrow::array::{
    make_array, Array, ArrayRef, RecordBatch, RecordBatchOptions, StringBuilder,
};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::error::ArrowError;
use parquet_variant_compute::{cast_to_variant, json_to_variant, VariantArray};
use parquet_variant_json::VariantToJson;

/// The canonical Arrow extension name of a variant field.
pub const EXTENSION_NAME: &str = "arrow.parquet.variant";

/// The Arrow field-metadata key that carries an extension type name.
const EXTENSION_KEY: &str = "ARROW:extension:name";

/// The Arrow storage Iceberg reads and writes for a variant column.
pub fn storage_type() -> DataType {
    DataType::Struct(
        vec![
            Field::new("metadata", DataType::Binary, false),
            Field::new("value", DataType::Binary, false),
        ]
        .into(),
    )
}

/// A variant column `name`: the storage struct tagged with the extension.
pub fn field(name: impl Into<String>, nullable: bool) -> Field {
    Field::new(name, storage_type(), nullable)
        .with_metadata([(EXTENSION_KEY.to_string(), EXTENSION_NAME.to_string())].into())
}

/// True when `field` holds variant values, by extension or by shape.
pub fn is_variant_field(field: &Field) -> bool {
    field.metadata().get(EXTENSION_KEY).map(String::as_str) == Some(EXTENSION_NAME)
        || is_variant_storage(field.data_type())
}

/// True when `data_type` is variant storage: `metadata` and `value` binary
/// fields, plus `typed_value` when the file was shredded.
pub fn is_variant_storage(data_type: &DataType) -> bool {
    let DataType::Struct(fields) = data_type else {
        return false;
    };
    let binary = |name: &str| {
        fields.iter().any(|f| {
            f.name() == name
                && matches!(
                    f.data_type(),
                    DataType::Binary | DataType::LargeBinary | DataType::BinaryView
                )
        })
    };
    let known = fields
        .iter()
        .all(|f| matches!(f.name().as_str(), "metadata" | "value" | "typed_value"));
    known && binary("metadata") && (binary("value") || fields.len() == 2)
}

/// `field` with the variant extension put back on every variant-shaped
/// field, at any depth.
///
/// iceberg-rust recognises a variant column only by the extension, which a
/// CTAS output schema has lost wherever an expression produced the column;
/// without it the storage struct would be created as a plain struct.
pub fn tag(field: &Field) -> Field {
    if is_variant_storage(field.data_type()) {
        let mut metadata = field.metadata().clone();
        metadata.insert(EXTENSION_KEY.to_string(), EXTENSION_NAME.to_string());
        return field.clone().with_metadata(metadata);
    }
    let retag = |f: &Arc<Field>| Arc::new(tag(f));
    let data_type = match field.data_type() {
        DataType::Struct(fields) => DataType::Struct(fields.iter().map(retag).collect()),
        DataType::List(f) => DataType::List(retag(f)),
        DataType::LargeList(f) => DataType::LargeList(retag(f)),
        DataType::FixedSizeList(f, n) => DataType::FixedSizeList(retag(f), *n),
        DataType::Map(f, sorted) => DataType::Map(retag(f), *sorted),
        _ => return field.clone(),
    };
    field.clone().with_data_type(data_type)
}

/// True when `field` holds a variant at any depth.
pub fn contains_variant(field: &Field) -> bool {
    if is_variant_field(field) {
        return true;
    }
    match field.data_type() {
        DataType::Struct(fields) => fields.iter().any(|f| contains_variant(f)),
        DataType::List(f) | DataType::LargeList(f) | DataType::FixedSizeList(f, _) => {
            contains_variant(f)
        }
        DataType::Map(f, _) => contains_variant(f),
        _ => false,
    }
}

/// Every value of a variant `array` as JSON text; a NULL stays NULL.
pub fn to_json(array: &dyn Array) -> Result<ArrayRef, ArrowError> {
    let variants = VariantArray::try_new(array)?;
    let mut out = StringBuilder::with_capacity(variants.len(), variants.len() * 16);
    for i in 0..variants.len() {
        if variants.is_null(i) {
            out.append_null();
        } else {
            out.append_value(variants.try_value(i)?.to_json_string()?);
        }
    }
    Ok(Arc::new(out.finish()))
}

/// The JSON text of the variant at `row` of `array`, `None` when NULL.
pub fn row_to_json(array: &dyn Array, row: usize) -> Result<Option<String>, ArrowError> {
    if array.is_null(row) {
        return Ok(None);
    }
    let variants = VariantArray::try_new(&array.slice(row, 1))?;
    Ok(Some(variants.try_value(0)?.to_json_string()?))
}

/// Fold a shredded variant `input` (with `typed_value`) back into the
/// unshredded storage `target`, so a file a shredding writer produced reads
/// with the table's declared type.
pub fn unshred(input: &dyn Array, target: &DataType) -> Result<ArrayRef, ArrowError> {
    let variants = parquet_variant_compute::unshred_variant(&VariantArray::try_new(input)?)?;
    let storage: ArrayRef = Arc::new(variants.into_inner());
    if storage.data_type() == target {
        Ok(storage)
    } else {
        cast(&storage, target)
    }
}

/// True when `data_type` is variant storage with a `typed_value` group.
pub fn is_shredded_storage(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Struct(fields) if fields.iter().any(|f| f.name() == "typed_value"))
        && is_variant_storage(data_type)
}

/// `batch` with every top-level shredded variant column unshredded to
/// [`storage_type`]; the batch itself when it has none.
pub fn unshred_batch(batch: &RecordBatch) -> Result<RecordBatch, ArrowError> {
    let schema = batch.schema();
    if !schema
        .fields()
        .iter()
        .any(|f| is_shredded_storage(f.data_type()))
    {
        return Ok(batch.clone());
    }
    let columns = schema
        .fields()
        .iter()
        .zip(batch.columns())
        .map(|(field, column)| {
            if is_shredded_storage(field.data_type()) {
                unshred(column.as_ref(), &storage_type())
            } else {
                Ok(Arc::clone(column))
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    RecordBatch::try_new_with_options(
        unshred_schema(&schema),
        columns,
        &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )
}

/// `schema` with every top-level shredded variant field declared as the
/// unshredded [`storage_type`], the schema [`unshred_batch`] produces.
pub fn unshred_schema(schema: &Schema) -> Arc<Schema> {
    let fields: Vec<Arc<Field>> = schema
        .fields()
        .iter()
        .map(|f| {
            if is_shredded_storage(f.data_type()) {
                Arc::new(tag(&f.as_ref().clone().with_data_type(storage_type())))
            } else {
                Arc::clone(f)
            }
        })
        .collect();
    Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
}

/// Convert `input` to variant storage of type `target`.
///
/// Text is parsed as JSON, so `'{"a": 1}'` stores an object and invalid JSON
/// is an error, never a silent NULL; quote a value (`'"text"'`) to store a
/// JSON string. A variant input is re-cast to the target storage, and any
/// other Arrow type becomes the variant of its value.
pub fn from_arrow(input: &dyn Array, target: &DataType) -> Result<ArrayRef, ArrowError> {
    let variants = match input.data_type() {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            json_to_variant(&make_array(input.to_data()))?
        }
        other if is_variant_storage(other) => VariantArray::try_new(input)?,
        _ => cast_to_variant(input)?,
    };
    let storage: ArrayRef = Arc::new(variants.into_inner());
    if storage.data_type() == target {
        Ok(storage)
    } else {
        cast(&storage, target)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{AsArray, Int64Array, StringArray};

    fn json_of(input: &dyn Array) -> Vec<Option<String>> {
        let stored = from_arrow(input, &storage_type()).unwrap();
        assert_eq!(stored.data_type(), &storage_type());
        let text = to_json(stored.as_ref()).unwrap();
        text.as_string::<i32>()
            .iter()
            .map(|v| v.map(str::to_string))
            .collect()
    }

    #[test]
    fn json_text_round_trips_through_variant() {
        let input = StringArray::from(vec![
            Some(r#"{"a": 1, "b": [true, null, "x"]}"#),
            None,
            Some("null"),
            Some(r#""text""#),
            Some("1.5"),
        ]);
        assert_eq!(
            json_of(&input),
            vec![
                Some(r#"{"a":1,"b":[true,null,"x"]}"#.to_string()),
                None,
                Some("null".to_string()),
                Some(r#""text""#.to_string()),
                Some("1.5".to_string()),
            ]
        );
    }

    #[test]
    fn invalid_json_is_an_error() {
        let input = StringArray::from(vec!["{not json"]);
        assert!(from_arrow(&input, &storage_type()).is_err());
    }

    #[test]
    fn non_text_values_become_their_variant() {
        let input = Int64Array::from(vec![Some(7), None]);
        assert_eq!(json_of(&input), vec![Some("7".to_string()), None]);
    }

    #[test]
    fn shape_detection() {
        assert!(is_variant_storage(&storage_type()));
        assert!(is_variant_field(&field("v", true)));
        let other = DataType::Struct(vec![Field::new("metadata", DataType::Utf8, true)].into());
        assert!(!is_variant_storage(&other));
        assert!(!is_variant_storage(&DataType::Binary));
    }

    #[test]
    fn tag_marks_nested_variants() {
        let bare = Field::new("v", storage_type(), true);
        assert_eq!(tag(&bare), field("v", true));
        let list = Field::new_list("l", Field::new("element", storage_type(), true), true);
        let tagged = tag(&list);
        let DataType::List(element) = tagged.data_type() else {
            panic!("list expected")
        };
        assert_eq!(
            element.metadata().get(EXTENSION_KEY).unwrap(),
            EXTENSION_NAME
        );
        assert!(contains_variant(&list));
        assert!(!contains_variant(&Field::new("s", DataType::Utf8, true)));
    }

    #[test]
    fn shredded_schema_reads_as_unshredded_storage() {
        let shredded = DataType::Struct(
            vec![
                Field::new("metadata", DataType::Binary, false),
                Field::new("value", DataType::Binary, true),
                Field::new("typed_value", DataType::Int8, true),
            ]
            .into(),
        );
        assert!(is_shredded_storage(&shredded));
        assert!(!is_shredded_storage(&storage_type()));
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("v", shredded, true),
        ]);
        let unshredded = unshred_schema(&schema);
        assert_eq!(unshredded.field(0), schema.field(0));
        assert_eq!(unshredded.field(1), &field("v", true));
    }

    #[test]
    fn row_to_json_reads_one_value() {
        let stored =
            from_arrow(&StringArray::from(vec![Some("[1]"), None]), &storage_type()).unwrap();
        assert_eq!(
            row_to_json(stored.as_ref(), 0).unwrap().as_deref(),
            Some("[1]")
        );
        assert_eq!(row_to_json(stored.as_ref(), 1).unwrap(), None);
    }
}
