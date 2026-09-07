# sqe-quack-wire

Pure-Rust port of DuckDB's `BinarySerializer` for the Quack RPC protocol
(`quack_version = 1`, DuckDB extension `v1.5-variegata`). Three layers:

- `codec` / `varint`: field-tagged binary (de)serialisation with every wire
  count bounded against the bytes remaining (`read_bounded_count`).
- `message` / `data_chunk`: the Quack message set and DuckDB's `DataChunk`,
  `Vector` and `LogicalType` layouts (FLAT vectors only).
- `arrow_bridge`: `DataChunk` <-> Arrow `RecordBatch`.

## Trust boundary

The decoder runs on the *client* side of a Quack connection. The coordinator
is a client whenever a user calls `quack_query()`, so every byte a remote
Quack server returns is untrusted input into this crate. The invariant the
tests and fuzz targets enforce (issue #612):

> For any byte string, `decode_message` / `DataChunk::decode` followed by
> `data_chunk_to_record_batch` returns `Ok` or `Err`. It never panics and it
> never sizes an allocation from a wire header that the payload does not back.

Concretely: `row_count`, `child_count`, LIST `(offset, length)` entries and
ARRAY sizes are validated against the decoded payload in `Vector::decode`
and again against Arrow's limits (`i32` offsets, `i32` fixed-list size) in
`arrow_bridge`; every Arrow array is built through `try_new`, so a shape
arrow rejects surfaces as `WireError`, not an assert. `sqe-quack-client`
additionally wraps the decode boundary in `catch_unwind` as a last line of
defence.

## Tests

```bash
cargo test -p sqe-quack-wire
```

`tests/hostile_input.rs` carries the property tests (10k random byte
strings up to 64 KiB, 10k byte-mutated valid chunks) and the explicit
regressions (`row_count = u32::MAX` over an empty column, LIST entries past
their child, offsets summing past `i32::MAX`). `tests/upstream_fixtures.rs`
pins the codec against bytes captured from a real DuckDB `quack_serve`.

## Fuzzing

`fuzz/` is a standalone cargo-fuzz workspace (not a member of the root
workspace, so it never affects `cargo test --workspace`). Three targets:

| target | what it exercises |
| --- | --- |
| `decode_message` | full message decode, then the Arrow bridge for every chunk a response carries |
| `decode_data_chunk` | bare `DataChunk::decode` (no message framing) |
| `data_chunk_to_record_batch` | `DataChunk::decode` then `data_chunk_to_record_batch`: the exact client path |

Run locally (needs `cargo install cargo-fuzz` and a nightly toolchain):

```bash
cd crates/sqe-quack-wire/fuzz
cargo +nightly fuzz run decode_message -- -max_total_time=600
cargo +nightly fuzz run decode_data_chunk -- -max_total_time=600
cargo +nightly fuzz run data_chunk_to_record_batch -- -max_total_time=600
```

Seed corpora live in `fuzz/corpus/<target>/`: the upstream fixtures from
`tests/fixtures/` for `decode_message`, and one valid encoding per supported
vector shape (plus the chunks extracted from the upstream fixtures) for the
two chunk targets. Regenerate them after adding codec support for a new
vector shape:

```bash
cargo run -p sqe-quack-wire --example emit_fuzz_seeds
```

Crashes land in `fuzz/artifacts/<target>/`; reproduce one with
`cargo +nightly fuzz run <target> fuzz/artifacts/<target>/<file>`, then turn
it into a regression in `tests/hostile_input.rs`.

The first fuzz build is slow: `sqe-quack-wire` depends on the repository's
`workspace-hack` crate, which pulls the full DataFusion tree into the fuzz
workspace's build.
