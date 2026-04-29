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

## Alignment opportunity (deferred)

Risingwavelabs's main branch landed its own DataFusion 53 + Arrow 58
rebase on 2026-04-15 (commit `fb290e4c9`, PR #148). SQE's downstream
DF 53 patches now overlap with upstream main; we are no longer the
only fork carrying that work.

Aligning the vendor pin with risingwavelabs main would let us drop
the DF 53 patch family. The remaining SQE-only patches that would
need to ride on top of the new base are:

1. `iceberg::expr::dynamic` (DynamicPredicate API) for runtime filter
   pushdown into IcebergTableScan. Filed upstream as
   apache/iceberg-rust#2376; not yet landed.
2. `iceberg-catalog-rest::sigv4` (AWS SigV4 signer) gated behind a
   new `aws-sigv4` cargo feature for AWS S3 Tables / Glue REST
   federation. Not filed upstream yet.
3. `CatalogBuilder::with_storage_factory` trait default in
   `iceberg::catalog`, added so the upstream HMS / Glue / SQL
   catalog crates compile against the fork's trait unmodified.
4. `FileIOBuilder` scheme-string compatibility shims in the vendored
   apache/iceberg-rust v0.9.0 catalog crates (`hms`, `glue`, `sql`)
   so they speak the fork's FileIO API.

Costs of doing the alignment now: roughly a day to redo the rebase
and re-apply the four patch families above, plus regression risk on
a working production codebase. Benefit: smaller patch surface vs
upstream, easier next vendor refresh.

The natural moment to align is when one of these happens:

- We upstream the SigV4 signer (item 2 above) into either
  risingwavelabs or apache/iceberg-rust. That removes one patch
  family from the rebase.
- apache/iceberg-rust#2376 lands (item 1 above). That removes
  another.
- We need a feature from risingwavelabs main that we don't
  currently have. Then we get the rebase as a side effect.
- Next major version bump (DataFusion 54 / Arrow 59) ships and we
  rebase anyway.

Until one of those, the vendor stays pinned to `645f02a4b533` with
SQE's DF 53 patches.
