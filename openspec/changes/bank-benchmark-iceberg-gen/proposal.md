# Proposal: Bank benchmark with direct-to-Iceberg generation

## Why

sqe-bench generates Parquet to a staging directory, then loads it through the engine with a CTAS. Every byte is written twice, and the coordinator's write path becomes the bottleneck. At demo scale that is fatal: a 4 TB/day, 10-12 day financial dataset (40-48 TB) cannot flow through a single coordinator in useful time. The generator itself is not the wall; the double write and the single-node CTAS are.

The demo target is a bank. The story is volume plus time-windowed queries: scan the last 14 days out of 48 TB fast because daily partitions prune the rest. TPC-E fidelity buys nothing here. A purpose-built customer/KYC/accounts/transactions schema resonates more and generates simpler.

## What Changes

1. New benchmark schema `bank` in sqe-bench: `customer`, `account`, `kyc_profile` dimensions plus day-partitioned `transaction` and `account_balance` facts.
2. New Iceberg sink: sqe-bench writes compressed, partition-aligned Parquet straight to the table's S3 location and commits it to Polaris via the vendored iceberg-rust REST client. One `fast_append` per day. The engine is not in the loop.
3. Generation is streaming and bounded: fixed-size Arrow batches, deterministic seed per `(table, day, shard)`, worker memory independent of total scale.
4. Sizing by target bytes: `--days 12 --bytes-per-day 4t` calibrates compressed bytes/row from a pilot shard and derives rows/day. `--dry-run` prints the plan; `--resume` skips days already committed.
5. Demo queries in `benchmarks/queries/bank/`, all shaped as "last 14 days out of N".

The existing `generate` (local Parquet) and `load` (CTAS) paths stay unchanged for the other benchmarks. The sink layer is generic so they can adopt it later.

## Success Criteria

- `sqe-bench generate bank --sink iceberg` against the quickstart stack (Polaris + RustFS) creates the namespace and five tables, writes data, and commits one snapshot per day; SQE queries the tables and row counts match the plan.
- Same seed produces byte-identical Parquet for any `(table, day, shard)`.
- Worker memory stays bounded (batch + row-group + upload buffers) regardless of `--bytes-per-day`.
- A killed run resumes: committed days are skipped, incomplete days regenerate cleanly.
- Feasibility at target: on a 32-64 core box with 10-25 Gbit to in-region S3-compatible storage, projected wall time for 48 TB is 5-13 hours, validated by the calibration report before the long run.

## Rollback

The change is additive: a new generator module, a new sink module, and new CLI flags with defaults that preserve current behavior. Rollback is `git revert`; no data-format, config, or engine change is involved.
