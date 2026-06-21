# SQE and Apache Spark: Ranger policy parity

This is a reference for how SQE compares to Apache Spark when both read the SAME
Apache Ranger policies over the SAME Polaris catalog. The short version: a column
mask authored once in Ranger produces byte-exact identical output in SQE and in
standard Spark.

For how SQE enforces fine-grained policies internally, see
`docs/ranger-fine-grained-enforcement.md`. For the coarse catalog access-control
path, see `docs/ranger-access-control.md`. This document covers only the
cross-engine parity result and its scope.

## The validated result

With the same Ranger `hive`-service policy on the same Polaris catalog, SQE and
standard Spark produce identical masked output. This was validated live (MR !386).

The test runs the same query as the same user against both engines:

```sql
SELECT id, ssn FROM sales_wh.sales.orders
```

Run as `bob`, both engines return the same masked SSNs:

```
xxx-xx-1111
xxx-xx-2222
xxx-xx-3333
```

3 of 3 rows byte-exact across SQE and Spark.

## Why the results agree

Both engines apply the mask through their OWN plan-rewrite layer, not a shared
runtime.

- **SQE** rewrites the `LogicalPlan` in its `PolicyEnforcer` /
  `PolicyPlanRewriter` before DataFusion optimization. See
  `docs/ranger-fine-grained-enforcement.md`.
- **Spark** rewrites its logical plan through Kyuubi's Spark Authz plugin
  (`RangerSparkExtension`).

Both read the same `hive` Ranger service-def: the same policy items, the same
`dataMaskType` strings, and the same transformer templates. SQE reimplements the
Hive char-class transformer faithfully (uppercase to `X`, lowercase to `x`, digit
to `n` for full `MASK`; `x` for every replaced character in the partial masks;
Unicode-scalar counting). Because both engines start from the same policy and
apply the same transformer semantics, the masked values match.

So you get the SAME catalog-level access control (Polaris enforcing the `polaris`
Ranger service) AND the same query-level masking (each engine enforcing the
`hive` Ranger service) across SQE and Spark. One policy set, two engines, the same
answer.

## Scope of the parity

Parity is validated for RESOURCE policies: named-column masks and named-table /
named-column row filters on the `hive` service. These are the policies both
engines resolve by table and column name.

**Tag-based masking is NOT cross-compared.** The two engines source tag-to-column
associations from different places:

- Spark Authz reads tag associations from the Ranger / Atlas tag store.
- SQE reads them from the Iceberg table property `sqe.column-tags` (see
  `docs/ranger-tag-storage-decision.md`).

The mask-per-tag RULE is shared (both read `tagPolicies` from Ranger), but the
ASSOCIATION source differs. Tag parity would require an Iceberg-to-Ranger tag
sync to mirror `sqe.column-tags` into the Ranger tag store. That sync is optional
and not part of this parity result.

## Required Spark configuration

For Spark to resolve the injected Hive mask UDF that the Ranger transformer
template names, Spark must run with the Hive catalog implementation so function
resolution stays in the built-in / Hive function registry:

```
spark.sql.catalogImplementation=hive
```

Without it, Spark cannot resolve the mask function the transformer template
references, and the masked query fails rather than matching SQE.

## Quickstart and version matrix

The reference deployment is the `polaris-ranger-keycloak` quickstart with a
`spark` service added plus a `parity-test.sh` that runs the masked
`SELECT id, ssn FROM sales_wh.sales.orders` as `bob` against both engines and
asserts the output is byte-exact across them.

Validated version matrix:

| Component | Version |
|---|---|
| Apache Spark | 3.5.4 |
| Iceberg Spark runtime | `iceberg-spark-runtime-3.5_2.12-1.8.1` |
| Kyuubi Spark Authz | `kyuubi-spark-authz_2.12-1.11.1` |
| Scala | 2.12 |
| Apache Ranger | 2.8 |
| Apache Polaris | 1.5.0 |
| Keycloak | 26.5 |

Spark 4 is not feasible off the shelf for this parity. The
`kyuubi-spark-authz_2.13` artifact is unpublished, so a Spark 4 (Scala 2.13)
parity stack would need Kyuubi built from source.

## Related docs

- `docs/ranger-fine-grained-enforcement.md` -- how SQE enforces the same policies.
- `docs/ranger-access-control.md` -- the catalog access-control path.
- `docs/ranger-fine-grained-service-type.md` -- why the `hive` service-def is the
  cross-engine sharing point, and the cross-engine matching requirements.
- `docs/ranger-tag-storage-decision.md` -- the tag-association storage decision
  that scopes tag parity out.
