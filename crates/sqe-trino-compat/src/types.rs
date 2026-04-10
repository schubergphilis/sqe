use arrow_schema::DataType;

pub fn arrow_to_trino_type(dt: &DataType) -> String {
    match dt {
        DataType::Null => "unknown".to_string(),
        DataType::Boolean => "boolean".to_string(),
        DataType::Int8 | DataType::UInt8 => "tinyint".to_string(),
        DataType::Int16 | DataType::UInt16 => "smallint".to_string(),
        DataType::Int32 => "integer".to_string(),
        DataType::UInt32 | DataType::Int64 | DataType::UInt64 => "bigint".to_string(),
        DataType::Float16 | DataType::Float32 => "real".to_string(),
        DataType::Float64 => "double".to_string(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "varchar".to_string(),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView | DataType::FixedSizeBinary(_) => "varbinary".to_string(),
        DataType::Date32 | DataType::Date64 => "date".to_string(),
        DataType::Time32(_) | DataType::Time64(_) => "time".to_string(),
        DataType::Timestamp(_, None) => "timestamp".to_string(),
        DataType::Timestamp(_, Some(_)) => "timestamp with time zone".to_string(),
        DataType::Duration(_) => "interval day to second".to_string(),
        DataType::Interval(arrow_schema::IntervalUnit::YearMonth) => "interval year to month".to_string(),
        DataType::Interval(_) => "interval day to second".to_string(),
        DataType::Decimal128(p, s) | DataType::Decimal256(p, s) => format!("decimal({p},{s})"),
        DataType::List(f) | DataType::LargeList(f) | DataType::FixedSizeList(f, _) => {
            format!("array({})", arrow_to_trino_type(f.data_type()))
        }
        DataType::Map(entries_field, _) => {
            if let DataType::Struct(fields) = entries_field.data_type() {
                if fields.len() == 2 {
                    let key_type = arrow_to_trino_type(fields[0].data_type());
                    let val_type = arrow_to_trino_type(fields[1].data_type());
                    return format!("map({key_type},{val_type})");
                }
            }
            "map(varchar,varchar)".to_string()
        }
        DataType::Struct(fields) => {
            let cols: Vec<String> = fields
                .iter()
                .map(|f| format!("{} {}", f.name(), arrow_to_trino_type(f.data_type())))
                .collect();
            format!("row({})", cols.join(","))
        }
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
        DataType::UInt8 => {
            let arr = array.as_any().downcast_ref::<UInt8Array>().unwrap();
            serde_json::json!(arr.value(row))
        }
        DataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
            serde_json::json!(arr.value(row))
        }
        DataType::UInt16 => {
            let arr = array.as_any().downcast_ref::<UInt16Array>().unwrap();
            serde_json::json!(arr.value(row))
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            serde_json::json!(arr.value(row))
        }
        DataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            serde_json::json!(arr.value(row))
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            serde_json::json!(arr.value(row))
        }
        DataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            serde_json::json!(arr.value(row))
        }
        DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
            serde_json::json!(arr.value(row))
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            let v = arr.value(row);
            // Match Trino formatting: DOUBLE always has decimal point (300.0, 4.0)
            // serde_json preserves this for whole numbers
            serde_json::json!(v)
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            serde_json::Value::String(arr.value(row).to_string())
        }
        DataType::LargeUtf8 => {
            let arr = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            serde_json::Value::String(arr.value(row).to_string())
        }
        // Timestamps must use Trino format: "2024-03-20 08:00:00.000" (space separator, millis)
        // The JDBC driver rejects ISO 8601 "T" separator.
        DataType::Timestamp(unit, tz) => {
            let s = arrow::util::display::array_value_to_string(array, row).unwrap_or_default();
            // Replace 'T' with space and ensure fractional seconds
            let s = s.replace('T', " ");
            // Strip timezone suffix if present (Trino handles tz separately)
            let s = if tz.is_none() {
                // Remove any trailing Z or +00:00 for naive timestamps
                s.trim_end_matches('Z')
                    .trim_end_matches("+00:00")
                    .trim_end_matches("+00")
                    .to_string()
            } else {
                s
            };
            // Ensure fractional seconds exist (Trino expects at least .000)
            let s = if !s.contains('.') {
                let precision = match unit {
                    arrow_schema::TimeUnit::Second => 0,
                    arrow_schema::TimeUnit::Millisecond => 3,
                    arrow_schema::TimeUnit::Microsecond => 6,
                    arrow_schema::TimeUnit::Nanosecond => 9,
                };
                if precision > 0 {
                    format!("{s}.{:0>width$}", 0, width = precision)
                } else {
                    format!("{s}.000")
                }
            } else {
                s
            };
            serde_json::Value::String(s)
        }
        DataType::Date32 | DataType::Date64 => {
            serde_json::Value::String(
                arrow::util::display::array_value_to_string(array, row).unwrap_or_default(),
            )
        }
        DataType::Utf8View => {
            let arr = array.as_any().downcast_ref::<StringViewArray>().unwrap();
            serde_json::Value::String(arr.value(row).to_string())
        }
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => {
            serde_json::Value::String(
                arrow::util::display::array_value_to_string(array, row).unwrap_or_default(),
            )
        }
        DataType::Time32(_) | DataType::Time64(_) => {
            serde_json::Value::String(
                arrow::util::display::array_value_to_string(array, row).unwrap_or_default(),
            )
        }
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => {
            serde_json::Value::String(
                arrow::util::display::array_value_to_string(array, row).unwrap_or_default(),
            )
        }
        _ => {
            serde_json::Value::String(
                arrow::util::display::array_value_to_string(array, row).unwrap_or_default(),
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

    #[test]
    fn test_arrow_value_to_json_utf8view() {
        let arr = arrow_array::StringViewArray::from(vec!["hello"]);
        let val = arrow_value_to_json(&arr, 0);
        assert_eq!(val, serde_json::Value::String("hello".to_string()));
    }

    #[test]
    fn test_arrow_value_to_json_decimal128() {
        let arr = arrow_array::Decimal128Array::from(vec![12345])
            .with_precision_and_scale(10, 2)
            .unwrap();
        let val = arrow_value_to_json(&arr, 0);
        assert_eq!(val, serde_json::Value::String("123.45".to_string()));
    }

    #[test]
    fn test_arrow_value_to_json_time64() {
        use arrow_array::types::Time64MicrosecondType;
        let micros = 10 * 3_600_000_000i64 + 30 * 60_000_000;
        let arr = arrow_array::PrimitiveArray::<Time64MicrosecondType>::from(vec![micros]);
        let val = arrow_value_to_json(&arr, 0);
        assert!(val.as_str().unwrap().starts_with("10:30:00"));
    }

    #[test]
    fn test_arrow_to_trino_type_extended() {
        use std::sync::Arc;
        assert_eq!(arrow_to_trino_type(&DataType::Utf8View), "varchar");
        assert_eq!(arrow_to_trino_type(&DataType::BinaryView), "varbinary");
        assert_eq!(arrow_to_trino_type(&DataType::Time32(arrow_schema::TimeUnit::Millisecond)), "time");
        assert_eq!(arrow_to_trino_type(&DataType::Time64(arrow_schema::TimeUnit::Microsecond)), "time");
        assert_eq!(arrow_to_trino_type(&DataType::Duration(arrow_schema::TimeUnit::Microsecond)), "interval day to second");
        assert_eq!(arrow_to_trino_type(&DataType::Interval(arrow_schema::IntervalUnit::YearMonth)), "interval year to month");
        assert_eq!(arrow_to_trino_type(&DataType::Interval(arrow_schema::IntervalUnit::DayTime)), "interval day to second");
        assert_eq!(arrow_to_trino_type(&DataType::FixedSizeBinary(16)), "varbinary");
        assert_eq!(arrow_to_trino_type(&DataType::List(Arc::new(arrow_schema::Field::new("item", DataType::Int32, true)))), "array(integer)");
        assert_eq!(arrow_to_trino_type(&DataType::Map(Arc::new(arrow_schema::Field::new("entries", DataType::Struct(vec![arrow_schema::Field::new("key", DataType::Utf8, false), arrow_schema::Field::new("value", DataType::Int64, true)].into()), false)), false)), "map(varchar,bigint)");
        assert_eq!(arrow_to_trino_type(&DataType::Null), "unknown");
    }
}
