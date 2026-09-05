# 906: Buffered JSON cap checks compressed size but leaves decompression and decoding unbounded

- Area: Memory safety and large-data execution
- Priority: P1
- Order within area: 1
- Scope: security and large-data execution
- Severity: high
- Category: security
- Effort: M
- Status: fixed (branch `fix/security-scale-review-2026-09-05`, 2026-09-05)
- Issue: #651
- Confidence: high in source-level defect; deployment impact depends on stated prerequisites

## Problem

`read_json` routes array framing or ZIP to `build_buffer_table` during planning (`crates/sqe-catalog/src/read_json.rs:275`). Its 512 MiB limit is compared only against HEAD object size (`crates/sqe-catalog/src/read_json_buffer.rs:55`). The GET is then collected without an actual received-byte cap (`:64`). Gzip and ZIP entries use unbounded `read_to_end` (`:140`, `:179`); ZIP output is concatenated (`:198`). JSON arrays are parsed into `serde_json::Value` and reserialized (`:101`), then all Arrow batches are collected into a MemTable (`:211`).

The supplied runtime is used for store registration, not a memory reservation. Neither decompressed bytes nor decoded Arrow memory is charged here.

## Why it matters

A permitted compressed input can be far below 512 MiB while expanding beyond node memory. Legitimate large JSON inputs also incur multiple representations before any rows are returned; `LIMIT 1` cannot stop this planning-time materialization. Concurrent calls amplify the risk. A remote object changing between HEAD and GET also defeats the only raw-size check.

An attacker needs access to an allowed input location (or permitted inline storage credentials). This is a resource-exhaustion path, not an arbitrary file-read claim.

## Proposed change

Enforce byte limits while downloading and while decompressing, including cumulative ZIP expansion. Reserve decode buffers and Arrow output against a shared pool. Prefer streaming array conversion/decoding or spillable staging. Enforce cancellation during CPU decoding and reject oversized results predictably.

## Validation and acceptance criteria

Test gzip/ZIP expansion over a deliberately small decoded-byte limit, cumulative ZIP entries, HEAD/GET size changes and concurrent calls under a small pool. Require controlled errors and reclaimed memory. The reproduction uses only about 8 MiB of decoded data: 8,188 gzip bytes expand to 8,388,617 bytes. It demonstrates expansion, not an SQE OOM.

## Related

Distinct from 504 (MoR writes), 604 (quack HTTP) and 701 (Trino result materialization).

## Resolution (2026-09-05)

`crates/sqe-catalog/src/read_json_buffer.rs` now routes every representation through a `BufferBudget`: the download is capped on bytes actually received (not only HEAD), gzip and each ZIP entry are decompressed in 64 KiB chunks against the cap with the ZIP cap applied cumulatively, the array reshape visits one element at a time and caps the growing NDJSON output, and every representation is charged to the session memory pool (shared via the scoped inference runtime) with intermediate copies released as they are dropped. Decoded Arrow batches are charged too and stay reserved for the lifetime of the returned table (`ReservedMemTable`). A denied pool grow is a typed `ResourcesExhausted` error. Tests cover gzip expansion over a small cap, pool denial with release on drop, cumulative ZIP caps, streamed reshape with pretty-printed input, pool visibility of decoded batches, and the end-to-end capped path. Cancellation during CPU decoding is not added.
