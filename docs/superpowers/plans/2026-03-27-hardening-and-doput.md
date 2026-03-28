# SQE Hardening & DoPut Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix all clippy/CI issues, complete data type formatting across Trino and benchmark formatters, add missing Flight SQL metadata methods, implement DoPut for Arrow data ingestion, fix TPC-H decimal precision, and verify security posture.

**Architecture:** Nine independent work streams that can be parallelised. Tasks 1-4 improve type coverage across three formatters (`arrow_to_trino_type`, `arrow_value_to_json`, `cell_to_string`). Tasks 5-6 add missing Flight SQL metadata. Task 7 implements `do_put_statement_ingest` to accept streamed Arrow batches and write them to Iceberg via the existing `WriteHandler` + `write_data_files` infrastructure. Task 8 fixes benchmark comparison. Task 9 is a security audit.

**Tech Stack:** Rust, arrow 57, arrow-flight 57 (with `flight-sql-experimental`), iceberg 0.9, DataFusion 52, tonic 0.14

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `crates/sqe-bench/src/load.rs` | Modify | Extract struct to fix too-many-args |
| `crates/sqe-bench/src/test.rs:355` | Modify | Use `.is_multiple_of(2)` |
| `crates/sqe-coordinator/src/flight_sql.rs:477` | Modify | Fix unused `schema` variable |
| `crates/sqe-trino-compat/src/types.rs` | Modify | Add missing types to both functions + tests |
| `crates/sqe-bench/src/compare.rs` | Modify | Add Timestamp/Time/Utf8View/Decimal256 to `cell_to_string` + decimal normalization |
| `crates/sqe-coordinator/src/flight_sql.rs` | Modify | Implement GetTableTypes, GetXdbcTypeInfo, DoPut |
| `crates/sqe-coordinator/src/write_handler.rs` | Modify | Add `handle_ingest` for DoPut |
| `crates/sqe-coordinator/src/query_handler.rs` | Modify | Expose `write_handler()` accessor |

---

### Task 1: Fix clippy errors

**Files:**
- Modify: `crates/sqe-bench/src/load.rs:11-20`
- Modify: `crates/sqe-bench/src/test.rs:355`
- Modify: `crates/sqe-coordinator/src/flight_sql.rs:477`

- [ ] **Step 1: Fix too-many-arguments in load.rs**

Add a `LoadArgs` struct and refactor the function signature:

```rust
// crates/sqe-bench/src/load.rs — replace lines 11-20

pub struct LoadArgs<'a> {
    pub benchmark: &'a str,
    pub scale: f64,
    pub data_path: &'a str,
    pub s3_args: &'a S3Args,
    pub clean: bool,
    pub catalog: Option<&'a str>,
    pub namespace_override: Option<&'a str>,
}

pub async fn load_benchmark(
    client: &dyn BenchClient,
    args: &LoadArgs<'_>,
) -> anyhow::Result<()> {
```

Update the function body to use `args.benchmark`, `args.scale`, etc. Then update all call sites (search for `load_benchmark(` across the crate).

- [ ] **Step 2: Fix is_multiple_of in test.rs**

Replace line 355:

```rust
// Before:
if quotes_before % 2 == 0 {
// After:
if quotes_before.is_multiple_of(2) {
```

- [ ] **Step 3: Fix unused variable in flight_sql.rs:477**

Prefix with underscore:

```rust
// Before:
let schema = builder.schema();
// After:
let _schema = builder.schema();
```

- [ ] **Step 4: Verify clippy passes**

Run: `cargo clippy --all-targets --all-features -- -D warnings`
Expected: 0 errors

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-bench/src/load.rs crates/sqe-bench/src/test.rs crates/sqe-coordinator/src/flight_sql.rs
# Also add any files that call load_benchmark (e.g. main.rs or cli handler)
git commit -m "fix: resolve all clippy errors (too-many-args, is_multiple_of, unused var)"
```

---

### Task 2: Complete Trino type mapping (`arrow_to_trino_type`)

**Files:**
- Modify: `crates/sqe-trino-compat/src/types.rs:3-21`

- [ ] **Step 1: Write failing tests for missing types**

Add to the existing `#[cfg(test)] mod tests` block (add `use std::sync::Arc;` at top of test module):

```rust
#[test]
fn test_arrow_to_trino_type_extended() {
    use std::sync::Arc;
    // View types
    assert_eq!(arrow_to_trino_type(&DataType::Utf8View), "varchar");
    assert_eq!(arrow_to_trino_type(&DataType::BinaryView), "varbinary");
    // Time types
    assert_eq!(
        arrow_to_trino_type(&DataType::Time32(arrow_schema::TimeUnit::Millisecond)),
        "time"
    );
    assert_eq!(
        arrow_to_trino_type(&DataType::Time64(arrow_schema::TimeUnit::Microsecond)),
        "time"
    );
    // Duration → Trino interval
    assert_eq!(
        arrow_to_trino_type(&DataType::Duration(arrow_schema::TimeUnit::Microsecond)),
        "interval day to second"
    );
    // Interval
    assert_eq!(
        arrow_to_trino_type(&DataType::Interval(arrow_schema::IntervalUnit::YearMonth)),
        "interval year to month"
    );
    assert_eq!(
        arrow_to_trino_type(&DataType::Interval(arrow_schema::IntervalUnit::DayTime)),
        "interval day to second"
    );
    // Fixed-size binary
    assert_eq!(
        arrow_to_trino_type(&DataType::FixedSizeBinary(16)),
        "varbinary"
    );
    // List / Map / Struct
    assert_eq!(
        arrow_to_trino_type(&DataType::List(Arc::new(
            arrow_schema::Field::new("item", DataType::Int32, true)
        ))),
        "array(integer)"
    );
    assert_eq!(
        arrow_to_trino_type(&DataType::Map(
            Arc::new(arrow_schema::Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        arrow_schema::Field::new("key", DataType::Utf8, false),
                        arrow_schema::Field::new("value", DataType::Int64, true),
                    ].into()
                ),
                false,
            )),
            false,
        )),
        "map(varchar,bigint)"
    );
    // Null
    assert_eq!(arrow_to_trino_type(&DataType::Null), "unknown");
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p sqe-trino-compat -- test_arrow_to_trino_type_extended`
Expected: FAIL (types fall through to Debug format)

- [ ] **Step 3: Implement the missing type arms**

Replace the `arrow_to_trino_type` function body (lines 3-21 of types.rs):

```rust
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
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => "varbinary".to_string(),
        DataType::Date32 | DataType::Date64 => "date".to_string(),
        DataType::Time32(_) | DataType::Time64(_) => "time".to_string(),
        DataType::Timestamp(_, None) => "timestamp".to_string(),
        DataType::Timestamp(_, Some(_)) => "timestamp with time zone".to_string(),
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
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p sqe-trino-compat`
Expected: All pass

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-trino-compat/src/types.rs
git commit -m "feat: complete Trino type mapping for all Arrow data types"
```

---

### Task 3: Complete Trino value serialization (`arrow_value_to_json`)

**Files:**
- Modify: `crates/sqe-trino-compat/src/types.rs:23-131`

- [ ] **Step 1: Write failing tests for missing value types**

Add to the existing test module:

```rust
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
    // 10:30:00.000000 = 10*3600*1_000_000 + 30*60*1_000_000
    let micros = 10 * 3_600_000_000i64 + 30 * 60_000_000;
    let arr = arrow_array::PrimitiveArray::<Time64MicrosecondType>::from(vec![micros]);
    let val = arrow_value_to_json(&arr, 0);
    assert!(val.as_str().unwrap().starts_with("10:30:00"));
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p sqe-trino-compat -- test_arrow_value_to_json_utf8view test_arrow_value_to_json_decimal128 test_arrow_value_to_json_time64`
Expected: FAIL (Utf8View has no explicit arm; Decimal128 may pass via fallback but test verifies format)

- [ ] **Step 3: Add explicit arms before the fallback wildcard**

Insert these arms before the `_ =>` wildcard at line 125 in `arrow_value_to_json`:

```rust
        DataType::Utf8View => {
            let arr = array.as_any().downcast_ref::<StringViewArray>().unwrap();
            serde_json::Value::String(arr.value(row).to_string())
        }
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => {
            // Arrow's display formats decimals with correct scale (e.g. "123.45")
            serde_json::Value::String(
                arrow::util::display::array_value_to_string(array, row).unwrap_or_default()
            )
        }
        DataType::Time32(_) | DataType::Time64(_) => {
            // Trino expects "HH:MM:SS.ffffff" — Arrow's display produces this format
            serde_json::Value::String(
                arrow::util::display::array_value_to_string(array, row).unwrap_or_default()
            )
        }
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => {
            // Trino expects hex-encoded binary
            serde_json::Value::String(
                arrow::util::display::array_value_to_string(array, row).unwrap_or_default()
            )
        }
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p sqe-trino-compat`
Expected: All pass

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-trino-compat/src/types.rs
git commit -m "feat: add explicit Trino value serialization for Utf8View, Decimal, Time, Binary"
```

---

### Task 4: Complete benchmark comparator (`cell_to_string`)

**Files:**
- Modify: `crates/sqe-bench/src/compare.rs:180-231`

- [ ] **Step 1: Write tests for the new type arms**

Add a test module to `compare.rs` (or extend if one exists):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::*;
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use std::sync::Arc;

    #[test]
    fn test_cell_to_string_utf8view() {
        let arr = StringViewArray::from(vec!["hello"]);
        assert_eq!(cell_to_string(&arr, 0), "hello");
    }

    #[test]
    fn test_cell_to_string_timestamp() {
        let arr = TimestampMicrosecondArray::from(vec![1_710_000_000_000_000i64]); // 2024-03-09 ...
        let s = cell_to_string(&arr, 0);
        assert!(s.contains("2024"), "timestamp should contain year: {s}");
    }

    #[test]
    fn test_cell_to_string_date32_readable() {
        let arr = Date32Array::from(vec![19800]); // ~2024-03-12
        let s = cell_to_string(&arr, 0);
        assert!(s.contains("2024"), "date should be human-readable: {s}");
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p sqe-bench -- tests::test_cell_to_string`
Expected: FAIL (Utf8View and Timestamp fall to `<Type>` format, Date32 returns raw days)

- [ ] **Step 3: Add missing type arms to `cell_to_string`**

Insert before the `other =>` fallback at line 227 and replace the Date32/Date64 arms:

```rust
        DataType::Utf8View => {
            let arr = array.as_any().downcast_ref::<arrow_array::StringViewArray>().unwrap();
            arr.value(row).to_string()
        }
        DataType::Timestamp(_, _) | DataType::Time32(_) | DataType::Time64(_) => {
            arrow::util::display::array_value_to_string(array, row).unwrap_or_default()
        }
        DataType::Decimal256(_, _) => {
            arrow::util::display::array_value_to_string(array, row).unwrap_or_default()
        }
```

Also fix Date32/Date64 to use human-readable format:

```rust
        DataType::Date32 | DataType::Date64 => {
            arrow::util::display::array_value_to_string(array, row).unwrap_or_default()
        }
```

Add `arrow` dependency to `crates/sqe-bench/Cargo.toml` if not already present:

```toml
arrow = { workspace = true }
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p sqe-bench`
Expected: All pass

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-bench/src/compare.rs crates/sqe-bench/Cargo.toml
git commit -m "feat: extend benchmark comparator with Timestamp, Time, Decimal256, Utf8View, human-readable dates"
```

---

### Task 5: Implement GetTableTypes in Flight SQL

**Files:**
- Modify: `crates/sqe-coordinator/src/flight_sql.rs:631-637,756-762`

- [ ] **Step 1: Write a test**

Add to `crates/sqe-coordinator/src/flight_sql.rs` (or a test file):

```rust
#[cfg(test)]
mod table_types_tests {
    use super::*;

    #[test]
    fn test_table_types_batch_has_correct_schema() {
        let mut builder = arrow_array::builder::StringBuilder::new();
        builder.append_value("TABLE");
        builder.append_value("VIEW");
        let arr = builder.finish();
        let schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("table_type", arrow_schema::DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(arr)]).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.schema(), schema);
    }
}
```

- [ ] **Step 2: Implement `get_flight_info_table_types`**

Replace the unimplemented stub at line 631:

```rust
    async fn get_flight_info_table_types(
        &self,
        query: CommandGetTableTypes,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let flight_descriptor = request.into_inner();
        let ticket = Ticket::new(query.as_any().encode_to_vec());
        let endpoint = FlightEndpoint::new().with_ticket(ticket);
        let info = FlightInfo::new()
            .with_endpoint(endpoint)
            .with_descriptor(flight_descriptor);
        Ok(Response::new(info))
    }
```

- [ ] **Step 3: Implement `do_get_table_types`**

Replace the stub at line 756:

```rust
    async fn do_get_table_types(
        &self,
        _query: CommandGetTableTypes,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let mut builder = arrow_array::builder::StringBuilder::new();
        builder.append_value("TABLE");
        builder.append_value("VIEW");
        let arr = builder.finish();
        let schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("table_type", arrow_schema::DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)])
            .map_err(|e| Status::internal(format!("Failed to build table types: {e}")))?;
        Self::batches_to_stream(vec![batch])
    }
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p sqe-coordinator`
Expected: All pass

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-coordinator/src/flight_sql.rs
git commit -m "feat: implement GetTableTypes in Flight SQL (TABLE, VIEW)"
```

---

### Task 6: Implement GetXdbcTypeInfo in Flight SQL

**Files:**
- Modify: `crates/sqe-coordinator/src/flight_sql.rs:721-727,820-826`

Uses the purpose-built `XdbcTypeInfoDataBuilder` and `XdbcTypeInfo` from `arrow_flight::sql::metadata` which produces a spec-compliant 19-column schema automatically.

- [ ] **Step 1: Add imports at top of flight_sql.rs**

```rust
use arrow_flight::sql::metadata::{XdbcTypeInfo, XdbcTypeInfoDataBuilder};
use arrow_flight::sql::{Nullable, Searchable, XdbcDataType};
```

- [ ] **Step 2: Implement `get_flight_info_xdbc_type_info`**

Replace the stub at line 721:

```rust
    async fn get_flight_info_xdbc_type_info(
        &self,
        query: CommandGetXdbcTypeInfo,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let flight_descriptor = request.into_inner();
        let ticket = Ticket::new(query.as_any().encode_to_vec());
        let endpoint = FlightEndpoint::new().with_ticket(ticket);
        let info = FlightInfo::new()
            .with_endpoint(endpoint)
            .with_descriptor(flight_descriptor);
        Ok(Response::new(info))
    }
```

- [ ] **Step 3: Implement `do_get_xdbc_type_info`**

Replace the stub at line 820:

```rust
    async fn do_get_xdbc_type_info(
        &self,
        query: CommandGetXdbcTypeInfo,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let mut builder = XdbcTypeInfoDataBuilder::new();

        // Boolean
        builder.append(XdbcTypeInfo {
            type_name: "boolean".into(),
            data_type: XdbcDataType::XdbcBit,
            column_size: Some(1),
            nullable: Nullable::NullabilityNullable,
            case_sensitive: false,
            searchable: Searchable::Full,
            unsigned_attribute: Some(false),
            fixed_prec_scale: false,
            auto_increment: Some(false),
            sql_data_type: XdbcDataType::XdbcBit,
            num_prec_radix: Some(0),
            ..Default::default()
        });
        // Integer types
        for (name, dt, size, radix) in [
            ("tinyint",  XdbcDataType::XdbcTinyint,  3,  10),
            ("smallint", XdbcDataType::XdbcSmallint, 5,  10),
            ("integer",  XdbcDataType::XdbcInteger,  10, 10),
            ("bigint",   XdbcDataType::XdbcBigint,   19, 10),
        ] {
            builder.append(XdbcTypeInfo {
                type_name: name.into(),
                data_type: dt,
                column_size: Some(size),
                nullable: Nullable::NullabilityNullable,
                case_sensitive: false,
                searchable: Searchable::Full,
                unsigned_attribute: Some(false),
                fixed_prec_scale: false,
                auto_increment: Some(false),
                sql_data_type: dt,
                num_prec_radix: Some(radix),
                ..Default::default()
            });
        }
        // Float types
        for (name, dt, size) in [
            ("real",   XdbcDataType::XdbcReal,   7),
            ("double", XdbcDataType::XdbcDouble, 15),
        ] {
            builder.append(XdbcTypeInfo {
                type_name: name.into(),
                data_type: dt,
                column_size: Some(size),
                nullable: Nullable::NullabilityNullable,
                case_sensitive: false,
                searchable: Searchable::Full,
                unsigned_attribute: Some(false),
                fixed_prec_scale: false,
                auto_increment: Some(false),
                sql_data_type: dt,
                num_prec_radix: Some(10),
                ..Default::default()
            });
        }
        // Decimal
        builder.append(XdbcTypeInfo {
            type_name: "decimal".into(),
            data_type: XdbcDataType::XdbcDecimal,
            column_size: Some(38),
            create_params: Some(vec!["precision".into(), "scale".into()]),
            nullable: Nullable::NullabilityNullable,
            case_sensitive: false,
            searchable: Searchable::Full,
            unsigned_attribute: Some(false),
            fixed_prec_scale: true,
            auto_increment: Some(false),
            sql_data_type: XdbcDataType::XdbcDecimal,
            minimum_scale: Some(0),
            maximum_scale: Some(38),
            num_prec_radix: Some(10),
            ..Default::default()
        });
        // Varchar
        builder.append(XdbcTypeInfo {
            type_name: "varchar".into(),
            data_type: XdbcDataType::XdbcVarchar,
            column_size: Some(2_147_483_647),
            literal_prefix: Some("'".into()),
            literal_suffix: Some("'".into()),
            create_params: Some(vec!["length".into()]),
            nullable: Nullable::NullabilityNullable,
            case_sensitive: true,
            searchable: Searchable::Full,
            fixed_prec_scale: false,
            sql_data_type: XdbcDataType::XdbcVarchar,
            ..Default::default()
        });
        // Varbinary
        builder.append(XdbcTypeInfo {
            type_name: "varbinary".into(),
            data_type: XdbcDataType::XdbcVarbinary,
            column_size: Some(2_147_483_647),
            literal_prefix: Some("X'".into()),
            literal_suffix: Some("'".into()),
            nullable: Nullable::NullabilityNullable,
            case_sensitive: false,
            searchable: Searchable::Full,
            fixed_prec_scale: false,
            sql_data_type: XdbcDataType::XdbcVarbinary,
            ..Default::default()
        });
        // Date
        builder.append(XdbcTypeInfo {
            type_name: "date".into(),
            data_type: XdbcDataType::XdbcDate,
            column_size: Some(10),
            literal_prefix: Some("DATE '".into()),
            literal_suffix: Some("'".into()),
            nullable: Nullable::NullabilityNullable,
            case_sensitive: false,
            searchable: Searchable::Full,
            fixed_prec_scale: false,
            sql_data_type: XdbcDataType::XdbcDate,
            ..Default::default()
        });
        // Time
        builder.append(XdbcTypeInfo {
            type_name: "time".into(),
            data_type: XdbcDataType::XdbcTime,
            column_size: Some(15),
            literal_prefix: Some("TIME '".into()),
            literal_suffix: Some("'".into()),
            nullable: Nullable::NullabilityNullable,
            case_sensitive: false,
            searchable: Searchable::Full,
            fixed_prec_scale: false,
            sql_data_type: XdbcDataType::XdbcTime,
            ..Default::default()
        });
        // Timestamp
        builder.append(XdbcTypeInfo {
            type_name: "timestamp".into(),
            data_type: XdbcDataType::XdbcTimestamp,
            column_size: Some(29),
            literal_prefix: Some("TIMESTAMP '".into()),
            literal_suffix: Some("'".into()),
            nullable: Nullable::NullabilityNullable,
            case_sensitive: false,
            searchable: Searchable::Full,
            fixed_prec_scale: false,
            sql_data_type: XdbcDataType::XdbcTimestamp,
            ..Default::default()
        });

        let xdbc_data = builder.build().map_err(|e| {
            Status::internal(format!("Failed to build XDBC type info: {e}"))
        })?;

        let batch = xdbc_data.record_batch(query.data_type).map_err(|e| {
            Status::internal(format!("Failed to filter XDBC type info: {e}"))
        })?;

        Self::batches_to_stream(vec![batch])
    }
```

- [ ] **Step 4: Run clippy + tests**

Run: `cargo clippy -p sqe-coordinator -- -D warnings && cargo test -p sqe-coordinator`
Expected: Pass

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-coordinator/src/flight_sql.rs
git commit -m "feat: implement GetXdbcTypeInfo using XdbcTypeInfoDataBuilder for JDBC/BI compatibility"
```

---

### Task 7: Implement DoPut for Flight SQL (Arrow data ingestion)

**Files:**
- Modify: `crates/sqe-coordinator/src/query_handler.rs` (add accessor)
- Modify: `crates/sqe-coordinator/src/write_handler.rs` (add `handle_ingest`)
- Modify: `crates/sqe-coordinator/src/flight_sql.rs:828-870`

This enables clients to stream Arrow RecordBatches via Flight SQL DoPut to insert data into an existing Iceberg table.

- [ ] **Step 1: Expose `write_handler()` on QueryHandler**

In `crates/sqe-coordinator/src/query_handler.rs`, add a public accessor method to `QueryHandler`:

```rust
    pub fn write_handler(&self) -> &WriteHandler {
        &self.write_handler
    }
```

Verify the `write_handler` field exists on the struct. If named differently, grep for `WriteHandler` usage.

- [ ] **Step 2: Add `handle_ingest` to WriteHandler**

Add to `crates/sqe-coordinator/src/write_handler.rs`, after `handle_insert`:

```rust
    /// Handle a Flight SQL DoPut ingest — write streamed Arrow batches to an Iceberg table.
    ///
    /// Unlike `handle_insert` (which executes a SQL SELECT then writes results),
    /// this receives pre-built RecordBatches directly from the client's Arrow stream.
    pub async fn handle_ingest(
        &self,
        session: &Session,
        table_name: &str,
        batches: Vec<RecordBatch>,
    ) -> sqe_core::Result<usize> {
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        if total_rows == 0 {
            return Ok(0);
        }

        // Parse "catalog.schema.table" or "schema.table"
        let parts: Vec<&str> = table_name.split('.').collect();
        let (namespace_str, name) = match parts.as_slice() {
            [ns, tbl] => (*ns, (*tbl).to_string()),
            [_cat, ns, tbl] => (*ns, (*tbl).to_string()),
            _ => {
                return Err(SqeError::Execution(format!(
                    "Invalid table name for ingest: {table_name}"
                )));
            }
        };

        let namespace = iceberg::NamespaceIdent::new(namespace_str.to_string());
        let table_ident = TableIdent::new(namespace, name);

        info!(
            username = %session.user.username,
            table = %table_ident,
            total_rows,
            "Executing DoPut ingest"
        );

        let catalog = self.create_catalog_bridge(session).await?;

        let table = catalog
            .load_table(&table_ident)
            .await
            .map_err(|e| SqeError::Catalog(format!("Failed to load table: {e}")))?;

        let data_files = write_data_files(&table, batches, "ingest").await?;

        if !data_files.is_empty() {
            let tx = Transaction::new(&table);
            let action = tx.fast_append().add_data_files(data_files);
            let tx = action.apply(tx).map_err(|e| {
                SqeError::Execution(format!("Failed to apply fast append: {e}"))
            })?;
            tx.commit(catalog.as_ref()).await.map_err(|e| {
                SqeError::Execution(format!("Failed to commit ingest transaction: {e}"))
            })?;

            info!(table = %table_ident, total_rows, "DoPut ingest committed successfully");
        }

        Ok(total_rows)
    }
```

- [ ] **Step 3: Implement `do_put_statement_ingest` in Flight SQL**

Replace the stub at line 836 of `flight_sql.rs`. The `CommandStatementIngest` struct has `table: String`, `schema: Option<String>`, and `catalog: Option<String>` fields — no `table_definition_path`.

```rust
    async fn do_put_statement_ingest(
        &self,
        ticket: CommandStatementIngest,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        let session = self.get_session_from_request(&request)?;

        // Build qualified table name from catalog + schema + table
        let mut qualified = String::new();
        if let Some(ref cat) = ticket.catalog {
            qualified.push_str(cat);
            qualified.push('.');
        }
        if let Some(ref schema) = ticket.schema {
            qualified.push_str(schema);
            qualified.push('.');
        }
        qualified.push_str(&ticket.table);

        debug!(
            username = %session.user.username,
            table = %qualified,
            "DoPut statement ingest"
        );

        // Decode the Arrow stream into RecordBatches
        let stream = request.into_inner();
        let flight_stream = arrow_flight::decode::FlightRecordBatchStream::new_from_flight_data(
            stream.map_err(|e| arrow_flight::error::FlightError::Tonic(Box::new(e))),
        );

        let batches: Vec<RecordBatch> = flight_stream
            .try_collect()
            .await
            .map_err(|e| Status::internal(format!("Failed to decode Arrow stream: {e}")))?;

        let rows = self
            .query_handler
            .write_handler()
            .handle_ingest(&session, &qualified, batches)
            .await
            .map_err(|e| Status::internal(format!("Ingest failed: {e}")))?;

        Ok(rows as i64)
    }
```

- [ ] **Step 4: Implement `do_put_statement_update` for DML via Flight SQL**

Replace the stub at line 828:

```rust
    async fn do_put_statement_update(
        &self,
        ticket: CommandStatementUpdate,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        let session = self.get_session_from_request(&request)?;

        // Execute the SQL statement (INSERT INTO, CTAS, DDL, etc.)
        let batches = self
            .query_handler
            .execute(&session, &ticket.query)
            .await
            .map_err(|e| Status::internal(format!("Statement execution failed: {e}")))?;

        let rows: i64 = batches.iter().map(|b| b.num_rows() as i64).sum();
        Ok(rows)
    }
```

- [ ] **Step 5: Add required imports to flight_sql.rs**

Ensure these are present at the top:

```rust
use futures::TryStreamExt;
use arrow_flight::decode::FlightRecordBatchStream;
```

- [ ] **Step 6: Run clippy + tests**

Run: `cargo clippy -p sqe-coordinator -- -D warnings && cargo test -p sqe-coordinator`
Expected: Pass

- [ ] **Step 7: Commit**

```bash
git add crates/sqe-coordinator/src/flight_sql.rs crates/sqe-coordinator/src/write_handler.rs crates/sqe-coordinator/src/query_handler.rs
git commit -m "feat: implement Flight SQL DoPut for Arrow data ingestion and statement updates"
```

---

### Task 8: Fix TPC-H decimal precision DIFF

**Files:**
- Modify: `crates/sqe-bench/src/compare.rs:74-78`

The DIFF comes from decimal comparison where Iceberg/DataFusion returns `123.4500` but the expected CSV has `123.45`. Both represent the same value but differ in trailing zeros.

- [ ] **Step 1: Add normalization in the comparison loop**

In `compare_results`, after the exact match check at line 76, add trailing-zero normalization:

```rust
            // Exact match — fast path
            if a == e {
                continue;
            }

            // Normalize: trim trailing zeros from decimal-like strings
            // "123.4500" == "123.45", "100.00" == "100"
            let a_norm = a.trim_end_matches('0').trim_end_matches('.');
            let e_norm = e.trim_end_matches('0').trim_end_matches('.');
            if a_norm == e_norm {
                continue;
            }
```

- [ ] **Step 2: Add a test for this normalization**

```rust
#[test]
fn test_compare_decimal_trailing_zeros() {
    use arrow_array::*;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    let schema = Arc::new(Schema::new(vec![
        Field::new("amount", DataType::Decimal128(10, 4), false),
    ]));
    // 1234500 with scale 4 = 123.4500
    let arr = Decimal128Array::from(vec![1_234_500i128])
        .with_precision_and_scale(10, 4)
        .unwrap();
    let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();

    let csv = "amount\n123.45\n";
    let result = compare_results(&[batch], csv, 1e-4).unwrap();
    assert!(matches!(result, CompareStatus::Pass), "trailing zeros should match: {result:?}");
}
```

- [ ] **Step 3: Run tests**

Run: `cargo test -p sqe-bench`
Expected: All pass

- [ ] **Step 4: Commit**

```bash
git add crates/sqe-bench/src/compare.rs
git commit -m "fix: normalize decimal trailing zeros in benchmark comparison"
```

---

### Task 9: Security audit — token handling

**Files:**
- Read-only audit across all crates

- [ ] **Step 1: Search for any token/credential logging**

```bash
# Check for any access_token being logged (should be ZERO matches)
rg -n 'info!.*access_token|debug!.*access_token|warn!.*access_token|error!.*access_token' crates/

# Check for bearer token in logs
rg -n 'info!.*bearer|debug!.*bearer' crates/ --ignore-case

# Check for password in logs
rg -n 'info!.*password|debug!.*password' crates/ --ignore-case

# Check Display/Debug impls that might leak tokens
rg -n 'impl.*Display.*Session|impl.*Debug.*Session' crates/
rg -n '#\[derive.*Debug.*\]' crates/sqe-core/src/ -A 3 | grep -A 3 Session
```

Expected: No matches that log actual token values. The `token_fingerprint` (a hash) is acceptable.

- [ ] **Step 2: Verify Session struct doesn't derive Debug**

If `Session` derives `Debug`, it could leak `access_token` via `{:?}` formatting. If it does, either remove `#[derive(Debug)]` from Session or implement a manual `Debug` that redacts the token.

- [ ] **Step 3: Verify error messages don't leak tokens**

Check `client_message()` on error types never includes token values.

- [ ] **Step 4: Document findings**

If issues found, fix them. If clean, note it:

```bash
git commit --allow-empty -m "audit: security review — token handling verified clean"
```

---

## Verification

After all tasks complete:

- [ ] `cargo build --all` — clean
- [ ] `cargo test --all` — all pass
- [ ] `cargo clippy --all-targets --all-features -- -D warnings` — 0 errors
- [ ] `cargo audit` — clear

## Post-completion

Update these files:
- `README.md` — mark DoPut, type formatting, XDBC type info as done in roadmap
- `nextsteps.md` — update status line, mark completed items
