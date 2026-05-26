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
| `DECIMAL(p, s)` | `Decimal128` | `Decimal` + `ExtraTypeInfo::Decimal { precision, scale }` | `decimal(p, s)` | ✅ | physical width tier-narrowed to i16/i32/i64/i128 per DuckDB; `Decimal256` not supported; negative scale rejected |
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
| `LIST<T>` | `List` / `LargeList` | `List` + `ExtraTypeInfo::List { child }` | `list<T>` | ✅ recursive child type; child element vector reused under `field 106` |
| `STRUCT(...)` | `Struct` | `Struct` + `ExtraTypeInfo::Struct { fields }` | `struct<...>` | ✅ pairs of (name, LogicalType) via `child_list_t` (pair fields 0/1) |
| `MAP<K, V>` | `Map` | `Map` + `ExtraTypeInfo::List { child: STRUCT(key, value) }` | `map<K, V>` | ✅ DuckDB stores MAP as LIST<STRUCT<key,value>>; we reuse the LIST vector layout and stamp the parent type id as Map |
| `ARRAY<T, N>` (fixed) | `FixedSizeList` | `Array` + `ExtraTypeInfo::Array { child, size }` | (none) | ✅ fields 103 (array_size) + 104 (child vector with size*count elements); size=0 honors WritePropertyWithDefault elision |
| `UNION` | `Union` (dense or sparse) | `Union` + `ExtraTypeInfo::Struct { fields }` (tag-prefixed) | (none) | ⚠️ codec verified by unit test: DuckDB's `LogicalType::UNION` factory builds a StructTypeInfo with a UTINYINT "tag" prepended to members, so we reuse the STRUCT wire layout and stamp the parent id as `Union`. DataFusion doesn't emit `UnionArray` in practice; Arrow bridge mapping is deferred. |
| `ENUM` | `Dictionary(Int8/16/32/64 or UInt8/16/32/64, Utf8/LargeUtf8)` | `Enum` + `ExtraTypeInfo::Enum { values }` | (none, project as `string`) | ⚠️ wire codec verified by unit tests (hand-written EnumTypeInfo: fields 200=values_count u64, 201=string list; per-row indices narrow to u8/u16/u32 by dict-size tier). DataFusion's SQL planner rejects the `ENUM(...)` type literal and doesn't dictionary-encode repeated strings, so end-to-end SQL exercising it via DataFusion isn't currently possible — the path is ready for non-DataFusion engines or future DataFusion support |

## Parameterised types

DuckDB's `LogicalType` carries optional `ExtraTypeInfo` on the wire (field 101 of the `LogicalType` object). Wave 2a added the framework plus the `DECIMAL` variant; the remaining variants still surface as `WireError::UnsupportedExtraTypeInfo`:

- `DECIMAL(p, s)` — ✅ encoded via `ExtraTypeInfo::Decimal { precision, scale }`. Storage tier follows DuckDB: precision 1-4 -> i16, 5-9 -> i32, 10-18 -> i64, 19-38 -> i128.
- `LIST<T>` — ✅ encoded via `ExtraTypeInfo::List { child }`. Recursive child type, child element vector under field 106.
- `STRUCT(...)` — ✅ encoded via `ExtraTypeInfo::Struct { fields }`. Pair entries with field 0 (name) + field 1 (LogicalType).
- `MAP<K, V>` — ✅ reuses `ExtraTypeInfo::List { child: STRUCT(key, value) }` per DuckDB's internal `LogicalType::MAP` factory; no separate `MapTypeInfo`.
- `ARRAY<T, N>` — ✅ encoded via `ExtraTypeInfo::Array { child, size }`. Field 200 (child_type, WriteProperty) + field 201 (size, WritePropertyWithDefault default 0).
- `ENUM` — ✅ encoded via `ExtraTypeInfo::Enum { values }`. Custom serializer: field 200 (values_count u64, `WriteProperty`) + field 201 (`WriteList<string>`). Per-row index width follows DuckDB's `EnumTypeInfo::DictType`: <=256 entries -> u8, <=65536 -> u16, otherwise u32.
- `UNION` — ✅ codec routes through STRUCT's wire layout. DuckDB models `UNION(members)` as a StructTypeInfo with a UTINYINT tag prepended to the members, so no new ExtraTypeInfo variant is needed. Arrow bridge mapping is deferred because DataFusion never emits `UnionArray`.
- User-defined types — not implemented.

The full parameterised-type family is wired in the codec. The remaining gaps are upstream (DataFusion's planner rejecting certain SQL syntax) or low-traffic enough to defer (Arrow bridge for ENUM/UNION when DataFusion doesn't emit those array types in practice).

ExtraTypeInfo wire layout (verified against DuckDB v1.5.3 generated serializer):
- Base field 100 (u8): `ExtraTypeInfoType` discriminant — `WriteProperty`, always written.
- Base field 101 (string): `alias` — `WritePropertyWithDefault`, omitted when "".
- Base field 102: deleted; readers tolerate but writers never emit.
- Base field 103 (`unique_ptr<ExtensionTypeInfo>`): `WritePropertyWithDefault`, omitted when null. Unsupported in the codec.
- Subclass fields per variant. For `DECIMAL`: field 200 (width, u8, `WritePropertyWithDefault` default 0) and field 201 (scale, u8, `WritePropertyWithDefault` default 0). Scale 0 is the common case and omits field 201 entirely.

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

## Client mode (Option B)

In addition to serving Quack RPC, SQE can act as a Quack client and pull rows from a remote DuckDB or another sqe-server:

- `sqe-quack-client` crate exposes a synchronous `QuackClient` for programmatic use.
- `QuackTableProvider` adapts a Quack query result to a DataFusion `TableProvider` (eager fetch, in-memory).
- `quack_query(uri, [token,] sql)` is registered as a TVF on every coordinator session, so any SQL client can pull remote tables inline:

  ```sql
  -- 2-arg form (no auth)
  SELECT * FROM quack_query('quack:remote-duckdb:9495', 'SELECT * FROM colors');

  -- 3-arg form (bearer / static token)
  SELECT * FROM quack_query('quack:remote-duckdb:9495', 'remote-secret', 'SELECT * FROM colors');
  ```

This is symmetric to DuckDB's own `quack_query` built-in. Composing the two lets a single DuckDB CLI session route queries through sqe-server, which itself fetches from a remote DuckDB — useful for federated reads or for treating DuckDB as an execution backend for specific workloads.
