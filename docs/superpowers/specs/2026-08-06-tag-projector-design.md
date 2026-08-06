# Tag Projector Design (phase 2b)

**Date:** 2026-08-06
**Status:** design, decisions recorded
**Follows:** `docs/superpowers/specs/2026-08-06-spark-ranger-access-control-parity-design.md`

## Goal

Close the last fail-open on the Spark path. Tag associations live in the Iceberg
property `sqe.column-tags`, which only SQE reads, so a tag-masked column is
protected in SQE and returned RAW by Spark. `ALTER TABLE ... SET TAG` must also
project the association into Ranger's tag store, where Kyuubi and any other
Ranger-plugin engine already look.

The Iceberg property stays the source of truth. Ranger's tag store becomes a
projection of it, which preserves the Phase 3 storage decision (tags travel with
the table) rather than reversing it.

## What was measured first

All on the live `quickstart/polaris-ranger-keycloak` stack, 2026-08-06.

**1. One call does the whole write, and it merges.**
`PUT /service/tags/importservicetags` with `op: add_or_update` writes the tag
definition, the service resource, and the association together, returning 204. A
second import naming a different table LEAVES THE FIRST IN PLACE, so SQE can
project per-table without owning the whole document or reading it first.

```
serviceResources after two imports: [(['orders'], ['ssn']), (['other'], ['x'])]
VERDICT: MERGE (first survived)
```

`op: delete` removes the resources and associations cleanly (bundle drops to zero
resources), which is what `UNSET TAG` needs.

**2. Kyuubi honors a projected association end to end.** With the association
imported and a tag mask on the `tag` service, Spark returned:

```
1  TAGGED-1111
2  TAGGED-2222
3  TAGGED-3333
```

and the raw ssn does not appear. Nothing but the projection was needed: no Atlas,
no tagsync.

**3. Mask types on the tag service must be component-qualified.** `CUSTOM` is
refused with `CUSTOM: is not a valid datamask-type ... service='tag'`; `hive:CUSTOM`
is accepted. Ranger's tag servicedef aggregates each component's mask vocabulary
rather than defining bare names, which the existing docs already describe.

**4. The incremental API is the wrong one.** `POST /service/tags/tagresourcemaps`
wants a tag INSTANCE, not a tag definition (`No tag found for guid=...`), so the
incremental path is four calls with more failure modes. The bulk import is one.

## Decisions

**On projection failure, roll back the Iceberg property.** The two writes cannot be
atomic. Commit the property, project, and if the projection fails revert the
property and fail the statement, so neither engine ends up masking while the other
does not. If the revert ALSO fails, the statement reports drift and names the repair
procedure. That narrow case is the only one that can leave the stores inconsistent.

Rejected: keeping the property and erroring. It ships exactly the fail-open this
phase exists to close. Rejected: projecting first, which flips the exposure to SQE
and can leave a live Ranger tag for a statement that then failed.

**Repair is a CALL procedure**, not a read-path self-heal and not a startup sweep.
A read-path reprojection puts a Ranger write on a read and can retry a broken
projection forever unnoticed. The procedure doubles as the migration path for tables
tagged before the projector existed.

**Projection is opt-in.** A deployment with no second engine reading Ranger gains
nothing from projecting and would acquire a hard Ranger-tag-API dependency on its
DDL path. Default off; the quickstart enables it. The doc note states plainly that
leaving it off means tag masks are SQE-only.

## Design

### Config

```toml
[policy.ranger]
service-name = "query"
project-tags = true   # default false
```

`project_tags` on the existing `RangerPolicyConfig`. Off by default, and inert
unless `[policy] engine = "ranger"`.

### The projector

A trait so the coordinator does not depend on a concrete Ranger type and the
rollback path is testable without a live Ranger:

```rust
#[async_trait]
pub trait TagProjector: Send + Sync {
    /// Make Ranger's tag store match `tags` for this table, adding what is
    /// present and removing what is not.
    async fn project(&self, table: &TagTableKey, tags: &ColumnTags) -> Result<()>;
    fn enabled(&self) -> bool { false }
}
```

- `NoopTagProjector`: the default, `enabled() == false`, `project` returns `Ok`.
- `RangerTagProjector`: built from the same `RangerPolicyConfig` that
  `RangerPolicyStore` uses, reusing its admin-credentialed client shape.

`TagTableKey` carries the `database` and `table` values in the SAME convention SQE
uses for the frontend service, so both engines resolve one key. For the
single-level namespaces in play, that is the namespace name, which is what Kyuubi
sends (Kyuubi has no catalog level).

### Statement path

In `CatalogOps::set_column_tags` (`catalog_ops.rs`), after the existing
`commit_schema_update` and cache invalidation, still holding the per-table lock:

1. Project the new map.
2. On failure, revert by committing the PREVIOUS property value and invalidating
   again, then return a typed error naming the column and the Ranger error.
3. If the revert fails, return an error stating both stores are now inconsistent and
   naming the repair procedure.

Holding the existing per-table lock across all of it matters: the lock is what makes
the read-modify-write safe today, and the rollback is part of that sequence.

### Repair

```sql
CALL sqe.system.reproject_column_tags('cat.ns.tbl');
```

Reads the Iceberg property and projects it wholesale, so it is idempotent and fixes
both directions of drift. Admin-gated like the other maintenance procedures.

## Testing

- Unit: a failing `TagProjector` must cause `SET TAG` to revert the property. Mockable
  because the trait is injected, so no live Ranger needed. This is the assertion that
  matters most and the one a live test would exercise least reliably.
- Unit: the import document shape, asserted against the payload the live probes
  accepted.
- e2e: `tag_column_mask_is_byte_identical_across_engines` in
  `spark_mask_parity_e2e`, the case Phase 2a had to leave out. `SET TAG` through SQE,
  a tag mask on the tag service, then the same value asserted from both engines.
- e2e: `unprojected_tag_is_not_masked_in_spark` is deliberately NOT added. With the
  projector on, that state is unreachable; it is the gap being closed.

## Success criteria

- With `project-tags = true`, a `SET TAG` through SQE makes Spark mask the column,
  asserted byte-identically against SQE.
- A projection failure leaves the Iceberg property unchanged and fails the statement.
- The repair procedure makes a drifted table consistent.
- With `project-tags` off, behaviour is exactly as today.

## Rollback

Turn `project-tags` off. The projector is additive to the DDL path; the Iceberg
property remains the only writer, and existing tag masks keep working in SQE.
