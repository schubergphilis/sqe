use arrow_schema::DataType;

/// The one Arrow-to-Trino-JSON value encoding lives in the wire crate so the
/// MCP endpoint can share it without linking this server (review 803).
pub use sqe_trino_protocol::encode::arrow_value_to_json;

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
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => "varbinary".to_string(),
        DataType::Date32 | DataType::Date64 => "date".to_string(),
        DataType::Time32(_) | DataType::Time64(_) => "time".to_string(),
        // Report an explicit precision. SQE timestamps are microsecond-backed
        // and rendered to 6 fractional digits, so timestamp(6) / timestamp(6)
        // with time zone is the consistent, Trino-accurate type. (#5)
        DataType::Timestamp(_, None) => "timestamp(6)".to_string(),
        DataType::Timestamp(_, Some(_)) => "timestamp(6) with time zone".to_string(),
        DataType::Duration(_) => "interval day to second".to_string(),
        DataType::Interval(arrow_schema::IntervalUnit::YearMonth) => {
            "interval year to month".to_string()
        }
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
            arrow_to_trino_type(&DataType::Timestamp(
                arrow_schema::TimeUnit::Microsecond,
                None
            )),
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
    fn test_arrow_to_trino_type_decimal() {
        assert_eq!(
            arrow_to_trino_type(&DataType::Decimal128(18, 2)),
            "decimal(18,2)"
        );
    }

    #[test]
    fn test_uint64_maps_to_bigint() {
        // count(*) / row_number() produce UInt64; Trino types those as bigint. (#4)
        assert_eq!(arrow_to_trino_type(&DataType::UInt64), "bigint");
    }

    /// The re-export is the same function the protocol crate tests in depth;
    /// one value here keeps the server-side path honest.
    #[test]
    fn test_arrow_value_to_json_reexport() {
        let arr = arrow_array::Int64Array::from(vec![42]);
        assert_eq!(arrow_value_to_json(&arr, 0), serde_json::json!(42));
    }

    #[test]
    fn test_arrow_to_trino_type_extended() {
        use std::sync::Arc;
        assert_eq!(arrow_to_trino_type(&DataType::Utf8View), "varchar");
        assert_eq!(arrow_to_trino_type(&DataType::BinaryView), "varbinary");
        assert_eq!(
            arrow_to_trino_type(&DataType::Time32(arrow_schema::TimeUnit::Millisecond)),
            "time"
        );
        assert_eq!(
            arrow_to_trino_type(&DataType::Time64(arrow_schema::TimeUnit::Microsecond)),
            "time"
        );
        assert_eq!(
            arrow_to_trino_type(&DataType::Duration(arrow_schema::TimeUnit::Microsecond)),
            "interval day to second"
        );
        assert_eq!(
            arrow_to_trino_type(&DataType::Interval(arrow_schema::IntervalUnit::YearMonth)),
            "interval year to month"
        );
        assert_eq!(
            arrow_to_trino_type(&DataType::Interval(arrow_schema::IntervalUnit::DayTime)),
            "interval day to second"
        );
        assert_eq!(
            arrow_to_trino_type(&DataType::FixedSizeBinary(16)),
            "varbinary"
        );
        assert_eq!(
            arrow_to_trino_type(&DataType::List(Arc::new(arrow_schema::Field::new(
                "item",
                DataType::Int32,
                true
            )))),
            "array(integer)"
        );
        assert_eq!(
            arrow_to_trino_type(&DataType::Map(
                Arc::new(arrow_schema::Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            arrow_schema::Field::new("key", DataType::Utf8, false),
                            arrow_schema::Field::new("value", DataType::Int64, true)
                        ]
                        .into()
                    ),
                    false
                )),
                false
            )),
            "map(varchar,bigint)"
        );
        assert_eq!(arrow_to_trino_type(&DataType::Null), "unknown");
    }
}
