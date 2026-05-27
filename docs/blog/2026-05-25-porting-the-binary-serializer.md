---
title: "Porting DuckDB's BinarySerializer to pure Rust"
description: "Ten sub-MRs in a day. The wire format, the fixture-driven debugging loop, and two bugs that the C++ reference encoder ships without telling you: WriteListWithDefault elision, and uninitialised bytes at NULL VARCHAR positions."
pubDate: "2026-05-25"
author: "Jacob Verhoeks"
tags:
  - "duckdb"
  - "quack"
  - "rust"
  - "wire-protocol"
  - "debugging"
---



*May 25, 2026*

The first decision was the easy one. Quack is just a wire protocol over HTTP, and DuckDB serialises everything with its own `BinarySerializer`. We could either link `duckdb-rs` (the official C++ binding) and shell out to its serialiser, or port the serialiser to pure Rust.

Linking would have given us the codec for free. It would also have linked the entire DuckDB binary, about 25 MB, into the SQE coordinator. Worse, the relevant serialiser is not part of DuckDB's stable C API. We checked. It is a C++-only template that the C extension does not expose. Using it would have meant `unsafe extern "C-unwind"` calls into internal symbols, with no guarantee they survive a DuckDB patch release.

A pure-Rust port turned out to be cheap.

## The wire format

DuckDB's `BinarySerializer` is a tagged-object format. Objects open implicitly, write tagged fields, and close with a `0xFFFF` sentinel. Field IDs are 16-bit little-endian. Integers are LEB128 varints, floats are raw IEEE-754 little-endian, strings are varint-length-prefixed bytes. That is the entire format.

```rust
pub fn begin_property(&mut self, field_id: u16) {
    self.out.extend_from_slice(&field_id.to_le_bytes());
}

pub fn end_object(&mut self) {
    self.out.extend_from_slice(&MESSAGE_TERMINATOR_FIELD_ID.to_le_bytes());
}
```

Two methods. The rest of the codec is `write_u32` calling `varint::encode_unsigned`, `write_string` calling `write_data_ptr` after a length prefix, that kind of thing. About 250 lines in `crates/sqe-quack-wire/src/codec.rs`.

The interesting part is what DuckDB's `WritePropertyWithDefault` does. It is a property write that **omits** the field if its value equals the default. The default for `string` is `""`; for `unique_ptr<T>` and `shared_ptr<T>` it is `nullptr`; for `vector<T>` it is the empty vector; for primitives like `bool` and `uint8_t` it is `false` / `0`. This makes the wire significantly more compact for sparse cases, but also makes the codec position-sensitive: a real DuckDB decoder peeks the next field id and matches against the expected one. Miss the elision and you read field N+1's bytes as field N's value.

We hit this twice in production, both times against a real DuckDB sidecar. More on that below.

## Layered MRs

The port landed as ten sub-MRs in a single afternoon. Each one was small enough to read in five minutes:

- `feat/duckdb-quack-wire-codec`: `BinarySerializer` / `Deserializer` primitives plus varint.
- `feat/duckdb-quack-server`: `axum` HTTP handler, `application/vnd.duckdb` content type, message routing.
- `feat/duckdb-quack-datachunk-codec`: `LogicalType`, `Vector`, `DataChunk` encode/decode.
- `feat/duckdb-quack-arrow-bridge`: `RecordBatch` to `DataChunk` (forward direction only at first).
- `feat/duckdb-quack-auth-bridge`: wire the existing `AuthProvider` chain into the server.
- `feat/duckdb-quack-query-executor`: `QueryExecutor` trait, stub executor for tests.
- `feat/duckdb-quack-coordinator-adapter`: feature-gated adapter that runs Quack queries through the real coordinator session.
- `feat/duckdb-quack-binary-wiring`: wire the server into `sqe-server.rs`.
- `feat/duckdb-quack-e2e-test`: first end-to-end test with the real DuckDB CLI.
- `feat/duckdb-quack-default-field-fix`: the first WriteListWithDefault bug, fixed in the heat of the live test.

Each MR had its own focused test suite. The codec primitives crate has 113 unit tests today; most landed in the first three MRs. Each `MessageType` got round-trip tests with byte-level assertions before any executor wiring happened. By the time we plugged the executor in, the codec had been beaten on for hours.

The reason for splitting it this way is not aesthetics. Reviewing a single 5000-line MR that touches a wire codec, an HTTP server, an auth chain, and a DataFusion adapter is not possible. Reviewing ten 500-line MRs is. We caught one real bug in PR review specifically because the diff was small enough to read carefully.

## Capture-driven debugging

The fixture tests use bytes captured from a real DuckDB 1.5.3 instance running `quack_serve()`. The capture tool is a tiny binary that POSTs our own encoded message to a real DuckDB and saves the response bytes verbatim:

```rust
let response_bytes = post_quack(&client, request_bytes)?;
hexdump("DuckDB response to PrepareRequest", &response_bytes);
save("prepare_response_select_1.bin", &response_bytes)?;
```

Then the integration test asserts our decoder reads those bytes back into the expected structure. Whenever we add a feature, we capture a fresh fixture for the relevant query (`SELECT 1`, `SELECT CAST(1.23 AS DECIMAL(10,2))`, `SELECT NULL::VARCHAR`) and pin it as a test asset. The fixtures live in `crates/sqe-quack-wire/tests/fixtures/` and are about 100 bytes each.

The flow is: write the codec from the spec, capture real bytes, find the diff, fix the codec, re-capture, repeat. We did this maybe twenty times during the port. The whole loop is a few seconds: change Rust, `cargo run --example capture_upstream`, hex-dump-compare, edit.

This is what found the first bug.

## Bug 1: WriteListWithDefault

The DuckDB CLI was connecting fine, but our `PrepareResponse` decoder was tripping on every `WHERE 1=0` query: `expected field_id 0x0004, got 0x0005`. The response was missing field 4 (the `results` list of DataChunks) and going straight to field 5 (`result_uuid`).

The DuckDB source code said:
```cpp
serializer.WriteProperty(4, "results", results);
```
But in `PrepareResponse` the call was actually:
```cpp
serializer.WriteListWithDefault(4, "results", results);
```
We had not modelled `WriteListWithDefault`. Adding it was one line on the encode side and three lines on decode:

```rust
let results = if d.read_optional(4)? {
    let count = d.read_list_count()? as usize;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        if !d.read_nullable_present()? {
            return Err(crate::WireError::NullDataChunkWrapper);
        }
        let chunk = decode_data_chunk_wrapper(d)?;
        d.expect_object_end()?;
        out.push(chunk);
    }
    out
} else {
    Vec::new()
};
```

The fixture tests had never exercised an empty results list, because every test query returned at least one row. We added one. Then we discovered the same pattern for `FetchResponse.results`, which manifested as a different symptom: every query above ~100K rows failed on the **final** fetch (the one that signals "no more batches" by sending an empty list). Same fix.

Both bugs are now regression-tested with byte-level assertions that field 4 is never emitted when the list is empty.

## Bug 2: NULL VARCHAR garbage

The second bug is the one I am still thinking about.

A user query that selected a NULL-VARCHAR column from a remote DuckDB returned: `wire codec: string is not valid UTF-8`. Not a length mismatch. Not a field-id mismatch. The wire reader was sitting on a length-prefixed string slot, reading the bytes, and the bytes were not UTF-8.

We captured the raw response. The relevant slice was:

```
0040  66 00 01 01 80 ff ff ff ff ff ff 05 00 c7 8c d9
```

Field 102 (`66 00`), list count 1 (`01`), string length 1 (`01`), single byte (`0x80`). The string DuckDB writes for a NULL position is one byte long, and that byte is `0x80`. Which is not a UTF-8 leading byte.

I went back to the DuckDB source:
```cpp
list.WriteElement<string_view>(
    vdata.validity.RowIsValid(idx)
    ? strings[idx].GetString()
    : "");
```

For NULL positions the code writes `string_view("")`. Empty string. Length zero. But the wire showed length one with a `0x80` byte. So either the source is misleading or the code we read does not run for this case.

The explanation that fits is that `string_view("")` constructs from a C-string literal `""`, and the resulting `string_view` has `.size() == 0` but `.data()` points to the `""` literal's storage which is fine. However when the value comes from a `string_t` that was never validity-checked, `string_t.GetString()` reads its inline payload, which is uninitialised memory at NULL positions. DuckDB's columnar engine does not clear inline payloads at NULL positions because nothing should ever read them.

The fix is to skip the length-prefixed slot **without UTF-8 validation** when the validity mask says the row is null. Whatever bytes are there, leave them. We added:

```rust
pub fn skip_string(&mut self) -> crate::Result<()> {
    let len = self.read_u64()? as usize;
    if self.buf.len() - self.pos < len {
        return Err(crate::WireError::UnexpectedEof);
    }
    self.pos += len;
    Ok(())
}
```

And changed the VARCHAR decode to:

```rust
if valid {
    values.push(Some(d.read_string()?));
} else {
    d.skip_string()?;
    values.push(None);
}
```

The unit test forges the exact wire layout (1 row, NULL validity, 1-byte `0x80` payload) and asserts decode succeeds.

This is the kind of bug a unit-test-only codec port would never catch. The source code is correct under the assumption that nothing reads the inline payload at NULL positions. The wire format leaks the assumption. Live testing made the leak visible.

## What I would do differently

Capture earlier. We had unit tests for every type before we ran a single end-to-end query against real DuckDB. They passed. The first live test surfaced two byte-level discrepancies in fifteen minutes that no unit test would have caught, because we did not know to write them.

The next time I port a wire format, the first MR is the capture tool, not the codec. Code against the bytes, not the spec.
