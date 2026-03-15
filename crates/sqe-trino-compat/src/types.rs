use arrow_schema::DataType;

pub fn arrow_to_trino_type(dt: &DataType) -> String {
    match dt {
        DataType::Boolean => "boolean".to_string(),
        DataType::Int8 => "tinyint".to_string(),
        DataType::Int16 => "smallint".to_string(),
        DataType::Int32 => "integer".to_string(),
        DataType::Int64 => "bigint".to_string(),
        DataType::Float32 => "real".to_string(),
        DataType::Float64 => "double".to_string(),
        DataType::Utf8 | DataType::LargeUtf8 => "varchar".to_string(),
        DataType::Binary | DataType::LargeBinary => "varbinary".to_string(),
        DataType::Date32 | DataType::Date64 => "date".to_string(),
        DataType::Timestamp(_, _) => "timestamp".to_string(),
        DataType::Decimal128(p, s) => format!("decimal({p},{s})"),
        DataType::Decimal256(p, s) => format!("decimal({p},{s})"),
        other => format!("{other:?}"),
    }
}

pub fn arrow_value_to_json(
    array: &dyn arrow_array::Array,
    row: usize,
) -> serde_json::Value {
    use arrow_array::*;

    if array.is_null(row) {
        return serde_json::Value::Null;
    }

    match array.data_type() {
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            serde_json::Value::Bool(arr.value(row))
        }
        DataType::Int8 => {
            let arr = array.as_any().downcast_ref::<Int8Array>().unwrap();
            serde_json::json!(arr.value(row))
        }
        DataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
            serde_json::json!(arr.value(row))
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            serde_json::json!(arr.value(row))
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            serde_json::json!(arr.value(row))
        }
        DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
            serde_json::json!(arr.value(row))
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            serde_json::json!(arr.value(row))
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            serde_json::Value::String(arr.value(row).to_string())
        }
        DataType::LargeUtf8 => {
            let arr = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            serde_json::Value::String(arr.value(row).to_string())
        }
        _ => {
            serde_json::Value::String(
                arrow::util::display::array_value_to_string(array, row).unwrap_or_default()
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_arrow_to_trino_type_basic() {
        assert_eq!(arrow_to_trino_type(&DataType::Int64), "bigint");
        assert_eq!(arrow_to_trino_type(&DataType::Utf8), "varchar");
        assert_eq!(arrow_to_trino_type(&DataType::Boolean), "boolean");
        assert_eq!(arrow_to_trino_type(&DataType::Float64), "double");
        assert_eq!(arrow_to_trino_type(&DataType::Int32), "integer");
    }

    #[test]
    fn test_arrow_to_trino_type_timestamp() {
        assert_eq!(
            arrow_to_trino_type(&DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None)),
            "timestamp"
        );
    }

    #[test]
    fn test_arrow_to_trino_type_decimal() {
        assert_eq!(
            arrow_to_trino_type(&DataType::Decimal128(18, 2)),
            "decimal(18,2)"
        );
    }

    #[test]
    fn test_arrow_value_to_json_int() {
        let arr = arrow_array::Int64Array::from(vec![42]);
        let val = arrow_value_to_json(&arr, 0);
        assert_eq!(val, serde_json::json!(42));
    }

    #[test]
    fn test_arrow_value_to_json_string() {
        let arr = arrow_array::StringArray::from(vec!["hello"]);
        let val = arrow_value_to_json(&arr, 0);
        assert_eq!(val, serde_json::Value::String("hello".to_string()));
    }

    #[test]
    fn test_arrow_value_to_json_null() {
        let arr = arrow_array::Int64Array::from(vec![Some(1), None]);
        let val = arrow_value_to_json(&arr, 1);
        assert_eq!(val, serde_json::Value::Null);
    }
}
