# 605: sqe-quack-wire's arrow bridge panics on hostile DataChunks and the codec has no fuzz harness

- Scope: sqe-quack-wire (arrow_bridge.rs, data_chunk.rs), sqe-quack-client
- Severity: medium
- Category: reliability
- Effort: S (hours) for bounds, M (days) for fuzzing
- Status: open
- Issue: #612

## Problem

`codec.rs` and `data_chunk.rs` are careful: counts are bounded against the
remaining buffer (`read_bounded_count`), recursion depth is capped, and
narrow integer reads reject overflow. The Arrow conversion layer that consumes
the decoded structures does not carry the same discipline.

- `DataChunk::decode` accepts `row_count` as a raw `u32`
  (`crates/sqe-quack-wire/src/data_chunk.rs:976-977`) with no relation to the
  bytes actually present. `data_chunk_to_record_batch` then passes it to every
  column (`arrow_bridge.rs:210-214`).
- `vector_to_array` allocates from that number:
  `StringBuilder::with_capacity(row_count, 0)` (`arrow_bridge.rs:1141`),
  `BinaryBuilder::with_capacity(row_count, 0)` (`:1151`),
  `Vec::with_capacity(row_count + 1)` (`:1220`). A 4-byte chunk claiming
  `row_count = 0xFFFF_FFFF` requests a 16 GiB offsets buffer and aborts the
  process on allocation failure.
- List and map compaction slices the child array with wire-supplied offsets:
  `child_array.slice(*off as usize, *len as usize)` (`arrow_bridge.rs:1249`).
  `arrow::Array::slice` asserts `offset + length <= len` and panics.
- Offsets are accumulated in `i32` from `u64` entries
  (`arrow_bridge.rs:1222-1224`, `1246-1248`); overflow wraps in release,
  producing non-monotonic offsets that `OffsetBuffer::new` rejects with a
  panic.
- There is no `fuzz/` directory and no `cargo-fuzz` target in the workspace
  (checked); the only adversarial coverage is hand-written unit tests and
  `tests/upstream_fixtures.rs`.

Trust boundary: the Quack server decodes only the header, `ConnectionRequest`,
`PrepareRequest`, and `DisconnectMessage` from clients
(`crates/sqe-quack-server/src/app.rs:150-167`), so the server does not decode
client-supplied DataChunks today. The decoder runs on the client side, which
includes the coordinator itself via `quack_query()` (issue 604). A malicious or
buggy remote Quack endpoint can therefore crash the coordinator.

## Why it matters

A wire codec is exactly the component that should never panic. The one place
the decoder is reachable from untrusted input is the coordinator process, where
a panic on a runtime thread aborts every in-flight query.

## Proposed change

1. In `data_chunk.rs::DataChunk::decode`, after decoding the columns, verify
   that every vector's decoded length is at least `row_count` (or clamp
   `row_count` to the minimum decoded length and return
   `WireError::CountExceedsRemaining` when they disagree). Apply the same to
   `child_count` for List/Map/Array.
2. In `arrow_bridge.rs::list_or_map_to_array`, validate every `(off, len)`
   against `child_array.len()` and against `i32::MAX` before slicing or
   pushing offsets; return `WireError` instead of relying on arrow asserts.
3. Wrap `vector_to_array` allocations in `try_reserve` or bound capacities by
   the decoded data length, never by the wire header alone.
4. Add `fuzz/fuzz_targets/{decode_message,decode_data_chunk,data_chunk_to_record_batch}.rs`
   with `cargo-fuzz`, seeded from `tests/upstream_fixtures.rs` and the
   `examples/capture_*.rs` corpora. Run for a fixed budget in a nightly job.
5. Until 1 to 3 land, wrap the decode in `std::panic::catch_unwind` at the
   `QuackClient::execute` boundary so a bad peer yields `ClientError`, not an
   abort.

## Acceptance criteria

- Property test: for any byte string up to 64 KiB, `decode_message` followed by
  `data_chunk_to_record_batch` returns `Ok` or `Err`, never panics
  (proptest, 10k cases, in the crate's test suite).
- Explicit regression tests for: `row_count = u32::MAX` with one empty column;
  a List vector whose entries point past the child; offsets summing past
  `i32::MAX`.
- A fuzz target exists and runs clean for 10 minutes on CI (or a documented
  local command in the crate README).

## Related

- Issue 604 (reachability from the coordinator).
- June 2026 codebase analysis flagged the Quack crates as "early stages" and
  absent from README; this is the concrete robustness gap.
