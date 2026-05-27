---
title: "The type matrix as a roadmap: seven DuckDB types in two days"
description: "We started with a markdown table tracking which DuckDB types we could round-trip. The table became the roadmap. DECIMAL, LIST, STRUCT, MAP, ARRAY, ENUM, UNION each got their own MR. The surprises came from how DuckDB models the relationships: MAP is LIST<STRUCT>, UNION is STRUCT with a tag field, and DECIMAL packs four widths into one logical type."
pubDate: "2026-05-26"
author: "Jacob Verhoeks"
tags:
  - "duckdb"
  - "quack"
  - "datatypes"
  - "rust"
  - "process"
---



*May 26, 2026*

After the Quack codec port shipped, we had a working server that DuckDB CLI clients could connect to. But it handled exactly seven types: int, bigint, float, double, varchar, blob, boolean. Everything else returned `UnsupportedLogicalType`.

DuckDB has, depending on how you count, around forty types. We needed a plan.

## The matrix

I wrote a markdown file with one row per DuckDB type, four columns: the DuckDB name, the Arrow / DataFusion equivalent, our `LogicalTypeId`, and a verification status (✅ ⚠️ ❌). The file lives at `docs/quack-datatype-matrix.md`. It looked like this after the first pass:

```
| BOOLEAN     | Boolean       | Boolean   | ✅ |
| TINYINT     | Int8          | TinyInt   | ✅ |
| INTEGER     | Int32         | Integer   | ✅ |
| BIGINT      | Int64         | BigInt    | ✅ |
| DECIMAL     | Decimal128    | Decimal   | ❌ |
| VARCHAR     | Utf8          | Varchar   | ✅ |
| BLOB        | Binary        | Blob      | ✅ |
| DATE        | Date32        | Date      | ❌ |
| TIMESTAMP   | Timestamp_us  | Timestamp | ❌ |
| TIME        | Time64_us     | Time      | ❌ |
| UUID        | FixedSizeBin  | Uuid      | ❌ |
| INTERVAL    | Interval_MDN  | Interval  | ❌ |
| LIST<T>     | List          | List      | ❌ |
| STRUCT(...) | Struct        | Struct    | ❌ |
| MAP<K,V>    | Map           | Map       | ❌ |
| ARRAY<T,N>  | FixedSizeList | Array     | ❌ |
| ENUM        | Dictionary    | Enum      | ❌ |
| UNION       | Union         | Union     | ❌ |
```

Plus a "verified" line at the bottom: "Every row marked ✅ has been verified end-to-end with a real duckdb 1.5.3 CLI session."

The doc was not a description of what we had built. It was a description of what we wanted to build, with the cells we did not have yet marked red.

## How the doc directed the work

Each ❌ became a branch. We worked through them in dependency order: Date/Timestamp first (just LogicalTypeId mappings, no `ExtraTypeInfo` needed), then the "leftover types" (Time, UUID, Interval), then DECIMAL (introduces `ExtraTypeInfo`), then the recursive nested types (LIST, STRUCT), then their compositions (MAP, ARRAY), then ENUM, finally UNION.

The MRs landed as a stack. Each one updated the matrix doc to flip its row green and commit the verification command alongside the code. The diff for a typical row looked like:

```diff
- | `DECIMAL(p, s)` | `Decimal128` | `Decimal` | ❌ requires type_info |
+ | `DECIMAL(p, s)` | `Decimal128` | `Decimal` + `ExtraTypeInfo::Decimal { precision, scale }` | ✅ physical width tier-narrowed to i16/i32/i64/i128 per DuckDB; `Decimal256` not supported; negative scale rejected |
```

By the time we got to UNION, the doc was almost entirely green. The remaining red rows were upstream blockers (DataFusion's planner does not accept `ENUM(...)` SQL literals) or low-value (the `BIT` type, which nobody uses).

The doc reaches a kind of saturation point where every change is "okay, also verify the symmetric reverse-direction path through the new client-side codec" instead of "what is this type and how does it serialise". The work converged.

## The three surprises

The cells that turned out interesting were the ones where DuckDB's logical model did not match Arrow's.

**DECIMAL packs four widths into one logical type.** DuckDB's `DECIMAL(p, s)` is a single `LogicalTypeId::Decimal` with `ExtraTypeInfo::Decimal { precision, scale }`. The wire payload is **not** always 16 bytes (the i128 Arrow uses). DuckDB picks a physical integer width based on precision:

```rust
pub fn decimal_physical_width(precision: u8) -> usize {
    match precision {
        0..=4 => 2,    // i16
        5..=9 => 4,    // i32
        10..=18 => 8,  // i64
        _ => 16,       // i128
    }
}
```

Source: `src/common/types/decimal.cpp::DecimalType::GetInternalType`. So a `DECIMAL(10,2)` value `1.23` goes on the wire as 8 bytes (`123` as i64 little-endian), not 16 bytes. Our Arrow bridge had to narrow on encode and re-widen on decode. Negative scales are rejected because DuckDB's `DecimalTypeInfo.scale` is `uint8_t`.

This is invisible from the DuckDB SQL surface. Users write `DECIMAL(10, 2)` and never see the i16/i32/i64/i128 choice. The wire reveals the abstraction.

**MAP is LIST<STRUCT<key, value>>.** DuckDB's `LogicalType::MAP(child)` factory function is:

```cpp
LogicalType LogicalType::MAP(const LogicalType &child) {
    D_ASSERT(child.id() == LogicalTypeId::STRUCT);
    auto &children = StructType::GetChildTypes(child);
    D_ASSERT(children.size() == 2);
    // ... enforces "key" and "value" field names ...
    auto info = make_shared_ptr<ListTypeInfo>(child);
    return LogicalType(LogicalTypeId::MAP, std::move(info));
}
```

The `ListTypeInfo` is the same one used for `LIST<T>`. The only difference between a `MAP` and a `LIST<STRUCT<key, value>>` is the parent `LogicalTypeId`. The wire bytes are identical.

We added MAP support without adding a new `ExtraTypeInfo` variant. The Vector encode/decode just got a second arm:

```rust
LogicalTypeId::List | LogicalTypeId::Map => {
    // identical wire shape for both
    ...
}
```

There is no `MapTypeInfo` class in DuckDB. The matrix doc had been carrying a placeholder row for one. We deleted the placeholder.

**UNION is STRUCT with a tag.** `LogicalType::UNION(members)` is:

```cpp
LogicalType LogicalType::UNION(child_list_t<LogicalType> members) {
    members.insert(members.begin(), {"", LogicalType::UTINYINT});
    auto info = make_shared_ptr<StructTypeInfo>(std::move(members));
    return LogicalType(LogicalTypeId::UNION, std::move(info));
}
```

A union of `(a INT, b VARCHAR)` becomes a struct of `(tag UTINYINT, a INT, b VARCHAR)`. The tag field has an empty string for its name. Each row's tag value identifies which member field holds the active value; the other member columns hold whatever uninitialised bytes happen to be there.

Same dispatch trick: the Vector decode path grew `LogicalTypeId::Struct | LogicalTypeId::Union` and the encode path needed no change because it dispatches on `VectorData::Struct` regardless of the parent id. UNION support was a one-line code change plus a test.

## ENUM is the one that does not auto-generate

DuckDB has a code generator that produces serializers from `src/include/duckdb/storage/serialization/types.json`. Most `ExtraTypeInfo` subclasses are auto-generated: `DecimalTypeInfo`, `ListTypeInfo`, `StructTypeInfo`, `ArrayTypeInfo` all have JSON entries with `members` arrays that map to field IDs.

`EnumTypeInfo` has `"custom_implementation": true`. The generator skips it. The serializer is hand-written in `src/common/extra_type_info/enum_type_info.cpp`:

```cpp
void EnumTypeInfo::Serialize(Serializer &serializer) const {
    ExtraTypeInfo::Serialize(serializer);
    auto strings = FlatVector::GetData<string_t>(values_insert_order);
    serializer.WriteProperty(200, "values_count", dict_size);
    serializer.WriteList(201, "values", dict_size,
        [&](Serializer::List &list, idx_t i) {
            list.WriteElement(strings[i]);
        });
}
```

Two fields, written by hand, neither using the default-elision wrapper. The dictionary index width per row is **not** in the type info; it is implied by the dictionary size:

```rust
pub fn enum_physical_width(dict_size: usize) -> usize {
    if dict_size <= u8::MAX as usize + 1 { 1 }
    else if dict_size <= u16::MAX as usize + 1 { 2 }
    else { 4 }
}
```

The ENUM Vector payload is a tight buffer of `dict_size`-sized integers indexing into the strings. Read 5 bytes, the dictionary has 200 strings: it is 5 `u8` indices. Read 10 bytes, the dictionary has 1000 strings: it is 5 `u16` indices.

That kind of detail is the reason we kept the matrix doc small and the MRs small. ENUM did not slot into the auto-generated subclass machinery, so it got its own MR with its own hand-written encode and decode. Trying to bundle ENUM with the recursive-types arc would have been a 1000-line MR with two different design conversations going at once.

## What the matrix is good for

Type matrices are how scoreboards work for codec ports. Every red cell is a known-unsupported case; every green cell is verified. The doc keeps the work bounded.

We have used the same shape twice before. The Iceberg matrix tracks which Iceberg primitive types map to which DataFusion types and which catalogs accept them. The function-coverage doc tracks which Trino SQL functions we have implemented and which we have not. Each one converges on a green state by attrition.

The trick is making sure the doc updates ship in the same MR as the verification command. If the doc flips a cell to ✅ but the test that proves it is in a separate MR, the doc lies. Treat the matrix row update like a test result: same commit, same diff, same review.

The Quack type arc finished in two days because the matrix told us exactly what was left. Eighteen MRs total in the period, most under 500 lines. None of them needed a design doc; the matrix doc was the design doc.
