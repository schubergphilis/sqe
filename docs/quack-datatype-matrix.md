# Quack RPC datatype matrix

How DuckDB, Arrow/DataFusion, SQE's `LogicalTypeId`, and Iceberg primitive types line up for the Quack RPC path. Status reflects what works through a real `duckdb 1.5.3` CLI session (`SELECT ... FROM quack_query('quack:localhost:9494', ...)`) against `sqe-server` on `feat/quack-wire-fixture-capture` and later.

## Scalar types

| DuckDB | Arrow / DataFusion | `LogicalTypeId` | Iceberg | Quack | Notes |
|---|---|---|---|---|---|
| `BOOLEAN` | `Boolean` | `Boolean` | `boolean` | ✅ | nulls round-trip |
| `TINYINT` | `Int8` | `TinyInt` | (none) | ✅ | |
| `SMALLINT` | `Int16` | `SmallInt` | (none) | ✅ | |
| `INTEGER` | `Int32` | `Integer` | `int` | ✅ | |
| `BIGINT` | `Int64` | `BigInt` | `long` | ✅ | |
| `UTINYINT` / `USMALLINT` / `UINTEGER` / `UBIGINT` | `UInt8` / `UInt16` / `UInt32` / `UInt64` | `UTinyInt` etc. | (none) | ⚠️ | wire encoding works; DataFusion SQL planner rejects unsigned literals in `SELECT` (upstream limitation, not ours) |
| `HUGEINT` / `UHUGEINT` | (no native Arrow) | `HugeInt` / `UHugeInt` | `decimal(38, 0)` | ⚠️ | wire encoding works; DataFusion SQL planner rejects `HUGEINT` |
| `FLOAT` | `Float32` | `Float` | `float` | ✅ | |
| `DOUBLE` | `Float64` | `Double` | `double` | ✅ | |
| `DECIMAL(p, s)` | `Decimal128` / `Decimal256` | `Decimal` | `decimal(p, s)` | ❌ | requires `LogicalType.type_info` (parameterised type); see "Parameterised types" below |
| `VARCHAR` | `Utf8` / `LargeUtf8` / `Utf8View` | `Varchar` | `string` | ✅ | DataFusion 53 emits `Utf8View` by default |
| `BLOB` | `Binary` / `LargeBinary` / `BinaryView` | `Blob` | `binary` | ✅ | nulls round-trip |
| `DATE` | `Date32` | `Date` | `date` | ✅ | both sides use days-since-1970-01-01 |
| `DATE` from `Date64` | `Date64` | `Date` | `date` | ✅ | narrowed to `i32` days |
| `TIMESTAMP_S` / `_MS` / `_US` (default `TIMESTAMP`) / `_NS` | `Timestamp(Second/Millisecond/Microsecond/Nanosecond, None)` | `TimestampSec` / `TimestampMs` / `Timestamp` / `TimestampNs` | `timestamp` | ✅ | timezone discarded; see follow-ups |
| `TIMESTAMP WITH TIME ZONE` | `Timestamp(*, Some(tz))` | `TimestampTz` | `timestamptz` | ❌ | timezone stripped today |
| `TIME` | `Time32(Second/Millisecond)` / `Time64(Microsecond)` | `Time` | `time` | ✅ | Time32 variants widen ×1_000_000 / ×1_000 to i64 microseconds-of-day |
| `TIME_NS` | `Time64(Nanosecond)` | `TimeNs` | (none, project as `time`) | ✅ | i64 nanoseconds-of-day passthrough |
| `UUID` | `FixedSizeBinary(16)` | `Uuid` | `uuid` | ✅ | 16-byte raw passthrough; other widths rejected |
| `INTERVAL` | `Interval(YearMonth/DayTime/MonthDayNano)` | `Interval` | (none) | ✅ | widens into DuckDB's 16-byte `interval_t { months, days, micros }`; ns floored to micros |
| `BIT` | (no native Arrow) | `Bit` | (none) | ❌ | |

## Nested types

| DuckDB | Arrow | `LogicalTypeId` | Iceberg | Quack |
|---|---|---|---|---|
| `LIST<T>` / `ARRAY` | `List` / `LargeList` | not implemented | `list<T>` | ❌ |
| `STRUCT(...)` | `Struct` | not implemented | `struct<...>` | ❌ |
| `MAP<K, V>` | `Map` | not implemented | `map<K, V>` | ❌ |
| `UNION` | `Union` | not implemented | (none) | ❌ |
| `ENUM` | `Dictionary(Int32, Utf8)` | not implemented | (none, project as `string`) | ❌ |

## Parameterised types

DuckDB's `LogicalType` carries optional `ExtraTypeInfo` on the wire (field 101 of the `LogicalType` object). Today our codec rejects any `LogicalType` that ships with `type_info` populated. The types blocked behind that rejection are:

- `DECIMAL(p, s)` — precision + scale live in `DecimalTypeInfo`
- `LIST<T>` / `ARRAY` — element type in `ListTypeInfo`
- `STRUCT(...)` — field name + type pairs in `StructTypeInfo`
- `MAP<K, V>` — key + value types in `MapTypeInfo`
- `ENUM` — dictionary in `EnumTypeInfo`
- User-defined types

Implementing those means:
1. Modeling `ExtraTypeInfo` as a Rust enum in `data_chunk.rs`.
2. Encoding / decoding `ExtraTypeInfo` (each variant uses `WriteValue` with its own field layout).
3. Plumbing the parameters through the `arrow_bridge` (e.g. `Decimal128` carries `(precision, scale)` in its Arrow `DataType`).
4. Choosing what to do for types Iceberg doesn't have a direct mapping for (`ENUM`, nested `UNION`).

That's a substantial follow-up MR on its own, tracked in `openspec/changes/duckdb-quack-protocol-support` as part of the deferred Phase 1.6 work.

## How to reproduce the matrix

```sh
# 1. Start the test stack and bootstrap once.
docker compose -f docker-compose.test.yml up -d
./scripts/bootstrap-test.sh

# 2. Start sqe-server with a Quack listener + BearerPassthrough auth.
cargo build --release --bin sqe-server
target/release/sqe-server --config tests/sqe-quack-test.toml &

# 3. Grab a Polaris bearer.
TOKEN=$(curl -s -X POST http://localhost:18181/api/catalog/v1/oauth/tokens \
  -d "grant_type=client_credentials&client_id=root&client_secret=s3cr3t&scope=PRINCIPAL_ROLE:ALL" \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")

# 4. Query through real DuckDB CLI (1.5.2+):
duckdb -c "
  INSTALL quack FROM core_nightly; LOAD quack;
  CREATE SECRET (TYPE quack, TOKEN '${TOKEN}');
  SELECT * FROM quack_query('quack:localhost:9494',
                            'SELECT 42 AS id, ''alice'' AS name, DATE ''2026-05-25'' AS joined');
"
```

`tests/sqe-quack-test.toml` is a copy of `tests/sqe-test.toml` with `coordinator.quack_port = 9494` and an `[[auth.providers]] type = "bearer_passthrough"` entry.

## Status

Every row marked ✅ has been verified end-to-end with a real `duckdb 1.5.3` CLI session. The verification command for each row is `SELECT <literal>::<type> ...` against `quack_query`, and the assertion is that DuckDB renders the value back without error.
