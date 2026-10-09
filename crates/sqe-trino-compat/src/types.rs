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
                .map(|f| {
                    format!(
                        "{} {}",
                        row_field_name(f.name()),
                        arrow_to_trino_type(f.data_type())
                    )
                })
                .collect();
            format!("row({})", cols.join(","))
        }
        other => format!("{other:?}"),
    }
}

/// A row field name as it appears in a `row(...)` type string: bare when it
/// is a plain lowercase identifier, otherwise double-quoted with `"` doubled,
/// so a name holding a space, comma, parenthesis or uppercase letter cannot
/// be misread as part of the field's type.
fn row_field_name(name: &str) -> String {
    let plain = name
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_lowercase() || c == '_')
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
    if plain {
        name.to_string()
    } else {
        format!("\"{}\"", name.replace('"', "\"\""))
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

    /// Every scalar (non-nested) Arrow type SQE can put in a result.
    fn scalar_type() -> impl proptest::strategy::Strategy<Value = DataType> {
        use arrow_schema::{IntervalUnit, TimeUnit};
        use proptest::prelude::*;
        let unit = prop_oneof![
            Just(TimeUnit::Second),
            Just(TimeUnit::Millisecond),
            Just(TimeUnit::Microsecond),
            Just(TimeUnit::Nanosecond),
        ];
        prop_oneof![
            proptest::sample::select(vec![
                DataType::Boolean,
                DataType::Int8,
                DataType::UInt8,
                DataType::Int16,
                DataType::UInt16,
                DataType::Int32,
                DataType::UInt32,
                DataType::Int64,
                DataType::UInt64,
                DataType::Float16,
                DataType::Float32,
                DataType::Float64,
                DataType::Utf8,
                DataType::LargeUtf8,
                DataType::Utf8View,
                DataType::Binary,
                DataType::LargeBinary,
                DataType::BinaryView,
                DataType::FixedSizeBinary(16),
                DataType::Date32,
                DataType::Date64,
                DataType::Time32(TimeUnit::Millisecond),
                DataType::Time64(TimeUnit::Microsecond),
                DataType::Duration(TimeUnit::Microsecond),
                DataType::Interval(IntervalUnit::YearMonth),
                DataType::Interval(IntervalUnit::DayTime),
                DataType::Interval(IntervalUnit::MonthDayNano),
            ]),
            (1u8..=38)
                .prop_flat_map(|p| (Just(p), -10i8..=p as i8))
                .prop_map(|(p, s)| DataType::Decimal128(p, s)),
            (39u8..=76, 0i8..=38).prop_map(|(p, s)| DataType::Decimal256(p, s)),
            (unit, proptest::option::of(Just("UTC")))
                .prop_map(|(u, tz)| DataType::Timestamp(u, tz.map(Into::into))),
        ]
    }

    proptest::proptest! {
        /// The client side (`parse_trino_type`, which the CLI and MCP decoders
        /// use) understands every scalar type name the server emits, so no
        /// scalar column decodes as an opaque string.
        #[test]
        fn scalar_type_names_parse_back(dt in scalar_type()) {
            use sqe_trino_protocol::types::{parse_trino_type, TrinoType};
            let name = arrow_to_trino_type(&dt);
            let parsed = parse_trino_type(&name);
            proptest::prop_assert!(
                !matches!(parsed, TrinoType::Other(_)),
                "{dt:?} -> {name:?} parses as {parsed:?}"
            );
            match dt {
                DataType::Decimal128(p, s) | DataType::Decimal256(p, s) => {
                    proptest::prop_assert_eq!(
                        parsed,
                        TrinoType::Decimal { precision: p, scale: s }
                    );
                }
                DataType::Timestamp(_, tz) => {
                    proptest::prop_assert_eq!(
                        parsed,
                        TrinoType::Timestamp { precision: 6, with_time_zone: tz.is_some() }
                    );
                }
                _ => {}
            }
        }

        /// The JDBC driver's ClientTypeSignature rejects a parameterized
        /// `rawType` (#345): parameters belong in `arguments`.
        #[test]
        fn scalar_type_signatures_keep_parameters_out_of_raw_type(dt in scalar_type()) {
            let name = arrow_to_trino_type(&dt);
            let sig = sqe_trino_protocol::types::type_signature_for(&name);
            proptest::prop_assert!(!sig.raw_type.contains('('), "{name:?} -> {:?}", sig.raw_type);
            if let DataType::Decimal128(p, s) | DataType::Decimal256(p, s) = dt {
                let args: Vec<_> = sig.arguments.iter().map(|a| a.value.clone()).collect();
                proptest::prop_assert_eq!(
                    args,
                    vec![serde_json::json!(p), serde_json::json!(s)]
                );
            }
        }
    }

    /// Nested types of any depth: a client parses the type string the
    /// coordinator reports, so every one must be a Trino type, never the
    /// `Debug` fallback, and a container must name exactly its children.
    mod nested {
        use std::sync::Arc;

        use arrow_schema::{Field, Fields, IntervalUnit, TimeUnit};
        use proptest::prelude::*;

        use super::*;

        fn leaf() -> impl Strategy<Value = DataType> {
            prop_oneof![
                Just(DataType::Null),
                Just(DataType::Boolean),
                Just(DataType::Int8),
                Just(DataType::UInt8),
                Just(DataType::Int16),
                Just(DataType::UInt16),
                Just(DataType::Int32),
                Just(DataType::UInt32),
                Just(DataType::Int64),
                Just(DataType::UInt64),
                Just(DataType::Float16),
                Just(DataType::Float32),
                Just(DataType::Float64),
                Just(DataType::Utf8),
                Just(DataType::LargeUtf8),
                Just(DataType::Utf8View),
                Just(DataType::Binary),
                Just(DataType::LargeBinary),
                Just(DataType::BinaryView),
                (1i32..64).prop_map(DataType::FixedSizeBinary),
                Just(DataType::Date32),
                Just(DataType::Date64),
                Just(DataType::Time32(TimeUnit::Millisecond)),
                Just(DataType::Time64(TimeUnit::Microsecond)),
                Just(DataType::Timestamp(TimeUnit::Microsecond, None)),
                Just(DataType::Timestamp(
                    TimeUnit::Nanosecond,
                    Some("UTC".into())
                )),
                Just(DataType::Duration(TimeUnit::Microsecond)),
                Just(DataType::Interval(IntervalUnit::YearMonth)),
                Just(DataType::Interval(IntervalUnit::MonthDayNano)),
                (1u8..=38, 0i8..=10).prop_map(|(p, s)| DataType::Decimal128(p, s)),
                (1u8..=76, 0i8..=10).prop_map(|(p, s)| DataType::Decimal256(p, s)),
            ]
        }

        fn map_of(k: DataType, v: DataType) -> DataType {
            let entries = Fields::from(vec![
                Field::new("key", k, false),
                Field::new("value", v, true),
            ]);
            DataType::Map(
                Arc::new(Field::new("entries", DataType::Struct(entries), false)),
                false,
            )
        }

        fn nested() -> impl Strategy<Value = DataType> {
            leaf().prop_recursive(4, 32, 4, |inner| {
                prop_oneof![
                    inner
                        .clone()
                        .prop_map(|t| DataType::List(Arc::new(Field::new("item", t, true)))),
                    inner
                        .clone()
                        .prop_map(|t| DataType::LargeList(Arc::new(Field::new("item", t, true)))),
                    (inner.clone(), inner.clone()).prop_map(|(k, v)| map_of(k, v)),
                    prop::collection::vec(inner, 1..4).prop_map(|ts| {
                        DataType::Struct(
                            ts.into_iter()
                                .enumerate()
                                .map(|(i, t)| Field::new(format!("f{i}"), t, true))
                                .collect(),
                        )
                    }),
                ]
            })
        }

        proptest! {
            /// No `Debug` fallback leaks out: Trino type names are lowercase
            /// words, digits and `(),_ `, with balanced parentheses.
            #[test]
            fn every_type_renders_as_a_trino_type(t in nested()) {
                let s = arrow_to_trino_type(&t);
                prop_assert!(
                    s.chars().all(|c| c.is_ascii_lowercase()
                        || c.is_ascii_digit()
                        || "(),_ ".contains(c)),
                    "{t:?} -> {s}"
                );
                let mut depth = 0i32;
                for c in s.chars() {
                    depth += match c { '(' => 1, ')' => -1, _ => 0 };
                    prop_assert!(depth >= 0, "{s}");
                }
                prop_assert_eq!(depth, 0, "{}", s);
            }

            /// A container's type is built from its children's, so nesting
            /// never changes how an element type is reported.
            #[test]
            fn containers_compose_their_children(k in nested(), v in nested()) {
                let (ks, vs) = (arrow_to_trino_type(&k), arrow_to_trino_type(&v));
                let list = DataType::List(Arc::new(Field::new("item", v.clone(), true)));
                prop_assert_eq!(arrow_to_trino_type(&list), format!("array({vs})"));
                prop_assert_eq!(
                    arrow_to_trino_type(&map_of(k.clone(), v.clone())),
                    format!("map({ks},{vs})")
                );
                let row = DataType::Struct(Fields::from(vec![
                    Field::new("a", k, true),
                    Field::new("b", v, true),
                ]));
                prop_assert_eq!(arrow_to_trino_type(&row), format!("row(a {ks},b {vs})"));
            }

            /// The JDBC driver's ClientTypeSignature rejects a `rawType`
            /// holding `<`, `>` or `,`: at every depth the base name goes
            /// in `rawType` and the children in `arguments`.
            #[test]
            fn nested_type_signatures_keep_parameters_out_of_raw_type(t in nested()) {
                fn raw_types(v: &serde_json::Value, out: &mut Vec<String>) {
                    match v {
                        serde_json::Value::Object(m) => {
                            if let Some(serde_json::Value::String(raw)) = m.get("rawType") {
                                out.push(raw.clone());
                            }
                            m.values().for_each(|c| raw_types(c, out));
                        }
                        serde_json::Value::Array(a) => a.iter().for_each(|c| raw_types(c, out)),
                        _ => {}
                    }
                }
                let name = arrow_to_trino_type(&t);
                let sig = sqe_trino_protocol::types::type_signature_for(&name);
                let expected_base = match &t {
                    DataType::List(_) | DataType::LargeList(_) => Some("array"),
                    DataType::Map(..) => Some("map"),
                    DataType::Struct(_) => Some("row"),
                    _ => None,
                };
                if let Some(base) = expected_base {
                    prop_assert_eq!(sig.raw_type.as_str(), base, "{}", name);
                }
                let mut raws = Vec::new();
                raw_types(&serde_json::to_value(&sig).unwrap(), &mut raws);
                for raw in raws {
                    prop_assert!(
                        !raw.contains(['(', ')', ',', '<', '>']),
                        "{name} -> rawType {raw:?}"
                    );
                }
            }

            /// Any field name survives the trip through the `row(...)` type
            /// string into the signature's `fieldName`, however it is
            /// spelled, and the field's type is signed as if it stood alone.
            #[test]
            fn row_field_names_round_trip_through_the_signature(
                field in "[a-zA-Z0-9 ,\"()_]{1,8}",
                t in nested(),
            ) {
                let row = DataType::Struct(Fields::from(vec![
                    Field::new(field.clone(), t.clone(), true),
                    Field::new("z", DataType::Int64, true),
                ]));
                let name = arrow_to_trino_type(&row);
                let sig = serde_json::to_value(
                    sqe_trino_protocol::types::type_signature_for(&name),
                ).unwrap();
                let args = sig["arguments"].as_array().unwrap();
                prop_assert_eq!(args.len(), 2, "{}", name);
                prop_assert_eq!(&args[0]["value"]["fieldName"]["name"], &serde_json::json!(field), "{}", name);
                let alone = serde_json::to_value(
                    sqe_trino_protocol::types::type_signature_for(&arrow_to_trino_type(&t)),
                ).unwrap();
                prop_assert_eq!(&args[0]["value"]["typeSignature"], &alone, "{}", name);
                prop_assert_eq!(&args[1]["value"]["fieldName"]["name"], &serde_json::json!("z"));
            }
        }
    }
}
