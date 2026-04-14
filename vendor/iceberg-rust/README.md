# iceberg-rust (SQE vendored fork)

This is a vendored copy of the [RisingWave Labs iceberg-rust fork](https://github.com/risingwavelabs/iceberg-rust),
branch `dev_rebase_main_20260303` at commit `645f02a4b533`, with DataFusion upgraded to 53.0 / Arrow 58 / Parquet 58.

## Why a fork?

Apache upstream iceberg-rust (v0.9.0) lacks:
- `RewriteFilesAction` / `OverwriteFilesAction` (Copy-on-Write DELETE/UPDATE)
- `PositionDeleteFileWriter` (Merge-on-Read position deletes)
- `DeletionVectorWriter` (Iceberg V3)

The RisingWave fork provides all of these. SQE applied the DF 53 migration
on top (same changes as upstream PR #2206).

## Upstream tracking

- RisingWave fork: `dev_rebase_main_20260303` @ `645f02a4b533`
- Apache upstream: tracking PRs #2185 (OverwriteAction) and #2203 (RowDeltaAction)
- When upstream merges these, SQE will migrate to official apache/iceberg-rust
