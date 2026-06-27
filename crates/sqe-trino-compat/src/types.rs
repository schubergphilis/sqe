use arrow_schema::DataType;
use base64::Engine;

pub fn arrow_to_trino_type(dt: &DataType) -> String {
    match dt {
        DataType::Null => "unknown".to_string(),
        DataType::Boolean => "boolean".to_string(),
        DataType::Int8 | DataType::UInt8 => "tinyint".to_string(),
        DataType::Int16 | DataType::UInt16 => "smallint".to_string(),
        DataType::Int32 => "integer".to_string(),
        // u64 nominally exceeds i64::MAX, but the only u64 columns SQE produces
        // are computed aggregates -- count(*), approx_distinct(), row_number()
        // OVER(...) -- whose values fit i64 in practice, and Trino itself types
        // those as bigint. Mapping to bigint (not decimal(20,0)) matches Trino
        // so BI clients see integer columns, not decimals. (#4)
        DataType::UInt32 | DataType::Int64 | DataType::UInt64 => "bigint".to_string(),
        DataType::Float16 | DataType::Float32 => "real".to_string(),
        DataType::Float64 => "double".to_string(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "varchar".to_string(),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView | DataType::FixedSizeBinary(_) => "varbinary".to_string(),
        DataType::Date32 | DataType::Date64 => "date".to_string(),
        DataType::Time32(_) | DataType::Time64(_) => "time".to_string(),
        // Report an explicit precision. SQE timestamps are microsecond-backed
        // and rendered to 6 fractional digits, so timestamp(6) / timestamp(6)
        // with time zone is the consistent, Trino-accurate type. (#5)
        DataType::Timestamp(_, None) => "timestamp(6)".to_string(),
        DataType::Timestamp(_, Some(_)) => "timestamp(6) with time zone".to_string(),
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

/// Normalize a rendered timestamp string to exactly 6 fractional-second digits
/// (truncating extra precision, padding shorter), preserving any trailing
/// timezone offset (`+00:00`, `Z`). Trino reports these columns as
/// `timestamp(6)`, so a nanosecond-backed value rendered with 9 digits (e.g.
/// from `date_trunc`) and a microsecond CAST rendered with 6 must agree.
fn normalize_timestamp_fraction(s: &str) -> String {
    // Find where any timezone suffix begins: the first '+', '-', or 'Z' after
    // the date (skip the first 11 chars: "YYYY-MM-DD " ) so the date's '-'
    // separators are not mistaken for an offset.
    let tz_start = s
        .char_indices()
        .skip(11)
        .find(|(_, c)| *c == '+' || *c == '-' || *c == 'Z')
        .map(|(i, _)| i)
        .unwrap_or(s.len());
    let (body, suffix) = s.split_at(tz_start);

    let normalized_body = if let Some(dot) = body.find('.') {
        let mut frac: String = body[dot + 1..].chars().take(6).collect();
        while frac.len() < 6 {
            frac.push('0');
        }
        format!("{}.{}", &body[..dot], frac)
    } else {
        format!("{}.000000", body.trim_end())
    };
    format!("{normalized_body}{suffix}")
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
            // Mapped to bigint; render as a JSON number, not a decimal string. (#4)
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
            // Match Trino's DOUBLE formatting: Java's Double.toString() uses
            // scientific notation for abs(v) >= 1e7 or abs(v) < 1e-3 (except 0).
            // JSON allows 1.23E8 as a valid number literal.
            // serde_json uses decimal notation; we override for Trino compat.
            let abs_v = v.abs();
            if !v.is_finite() {
                serde_json::json!(v)
            } else if v == 0.0 || (1e-3..1e7).contains(&abs_v) {
                // Normal range: decimal notation (3.14, 1000.0)
                serde_json::json!(v)
            } else {
                // Large/small values: scientific notation to match Trino
                // serde_json::Number doesn't support E notation directly,
                // so we use a raw JSON value via from_str
                let formatted = format!("{:e}", v);
                serde_json::from_str::<serde_json::Value>(&formatted)
                    .unwrap_or_else(|_| serde_json::json!(v))
            }
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
        DataType::Timestamp(_, tz) => {
            let s = arrow::util::display::array_value_to_string(array, row).unwrap_or_default();
            // Replace 'T' with space (the JDBC driver rejects ISO 8601 'T').
            let s = s.replace('T', " ");
            // Strip timezone suffix for naive timestamps (Trino handles tz separately).
            let s = if tz.is_none() {
                s.trim_end_matches('Z')
                    .trim_end_matches("+00:00")
                    .trim_end_matches("+00")
                    .to_string()
            } else {
                s
            };
            // Normalize to exactly 6 fractional digits so timestamp(6) renders
            // consistently regardless of the backing unit -- date_trunc emits 9
            // (nanosecond) while CAST emits 6, and the JDBC type is timestamp(6).
            // (#5)
            serde_json::Value::String(normalize_timestamp_fraction(&s))
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
        DataType::Binary => {
            let arr = array.as_any().downcast_ref::<BinaryArray>().unwrap();
            serde_json::Value::String(
                base64::engine::general_purpose::STANDARD.encode(arr.value(row)),
            )
        }
        DataType::LargeBinary => {
            let arr = array.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
            serde_json::Value::String(
                base64::engine::general_purpose::STANDARD.encode(arr.value(row)),
            )
        }
        DataType::BinaryView => {
            let arr = array.as_any().downcast_ref::<BinaryViewArray>().unwrap();
            serde_json::Value::String(
                base64::engine::general_purpose::STANDARD.encode(arr.value(row)),
            )
        }
        DataType::FixedSizeBinary(_) => {
            let arr = array.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
            serde_json::Value::String(
                base64::engine::general_purpose::STANDARD.encode(arr.value(row)),
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
            "timestamp(6)"
        );
        assert_eq!(
            arrow_to_trino_type(&DataType::Timestamp(
                arrow_schema::TimeUnit::Microsecond,
                Some("UTC".into())
            )),
            "timestamp(6) with time zone"
        );
    }

    #[test]
    fn test_normalize_timestamp_fraction() {
        // Truncate 9 (nanosecond) -> 6.
        assert_eq!(
            normalize_timestamp_fraction("2024-03-20 08:00:00.123456789"),
            "2024-03-20 08:00:00.123456"
        );
        // Pad missing fractional -> .000000.
        assert_eq!(
            normalize_timestamp_fraction("2024-03-20 08:00:00"),
            "2024-03-20 08:00:00.000000"
        );
        // Already 6 -> unchanged.
        assert_eq!(
            normalize_timestamp_fraction("2024-03-20 08:00:00.500000"),
            "2024-03-20 08:00:00.500000"
        );
        // Preserve a timezone offset suffix while normalizing the fraction.
        assert_eq!(
            normalize_timestamp_fraction("2024-03-20 08:00:00.123456789+00:00"),
            "2024-03-20 08:00:00.123456+00:00"
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
    fn test_uint64_maps_to_bigint() {
        // count(*) / row_number() produce UInt64; Trino types those as bigint. (#4)
        assert_eq!(arrow_to_trino_type(&DataType::UInt64), "bigint");
    }

    #[test]
    fn test_uint64_value_rendered_as_number() {
        let arr = arrow_array::UInt64Array::from(vec![42u64]);
        let val = arrow_value_to_json(&arr, 0);
        assert_eq!(val, serde_json::json!(42u64));
        assert!(val.is_number(), "u64 must render as a JSON number, not a string");
    }

    #[test]
    fn test_binary_rendered_as_base64() {
        let arr = arrow_array::BinaryArray::from_iter_values([b"Hello".as_slice()]);
        let val = arrow_value_to_json(&arr, 0);
        assert_eq!(val, serde_json::Value::String("SGVsbG8=".to_string()));
    }

    #[test]
    fn test_fixed_size_binary_rendered_as_base64() {
        let vec_data: Vec<&[u8]> = vec![&[0u8, 1, 2, 3]];
        let arr = arrow_array::FixedSizeBinaryArray::try_from_iter(vec_data.into_iter()).unwrap();
        let val = arrow_value_to_json(&arr, 0);
        assert_eq!(val, serde_json::Value::String("AAECAw==".to_string()));
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
