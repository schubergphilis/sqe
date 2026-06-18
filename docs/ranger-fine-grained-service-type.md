# Which Ranger service type for fine-grained policies (row/column/mask/tag) shared with Spark (research + decision)

Question: to do Snowflake-style row filtering, column masking, and tag-based
masking in SQE by reading policies from Apache Ranger - in addition to the coarse
catalog policies Polaris already enforces - and ideally share the SAME Ranger
backend with Apache Spark, which Ranger SERVICE-DEF / type should we use?

## Decision

- **Resource-based row-filter + column-mask: use the `hive` service-def**
  (a.k.a. "Hadoop SQL"). It is the service Apache Spark's Ranger plugin reads, so
  one policy set governs Spark AND SQE.
- **Tag-based masking (the Snowflake tag analog): add a `tag` service** linked to
  that hive service. Engine-agnostic across Spark/Trino/SQE.
- **Keep the `polaris` service** for the coarse catalog/table allow-deny gate
  (Polaris enforces it). Fine-grained is a SEPARATE service that SQE (and Spark)
  read and enforce themselves.
- **Do NOT** put row/mask policies on the `polaris` service (it has no
  `dataMaskDef`/`rowFilterDef`), and do NOT invent a custom SQE service-def
  (it would not be shared with Spark).

| Concern | Ranger service | Enforced by |
|---|---|---|
| catalog/table allow-deny | `polaris` | Polaris embedded authorizer (shipped) |
| row-filter + column-mask (per table/column) | `hive` (shared with Spark) | SQE + Spark, each in its own engine |
| tag-based masking/row-filter (PII everywhere) | `tag` linked to the hive service | SQE + Spark + Trino |

## Why `hive`

- A service-def can host row-filter / data-mask policies only if it declares
  `rowFilterDef` + `dataMaskDef`. Built-in defs that have them: **`hive`,
  `trino`, `presto`, `nestedstructure`**. `hdfs`, `hbase`, and `polaris` do NOT.
- The de-facto OSS Spark FGAC path is **Apache Kyuubi's Spark AuthZ plugin**,
  which binds to a **`hive`-type service** (`ranger.plugin.spark.service.name` =
  a Ranger hive service; "reuses the hive service def"). There is no separate
  `spark` service-def. So sharing with Spark == SQE reads the same hive service.
- `trino` has a richer resource model (catalog/schema/table/column, WITH a
  catalog level) that maps Iceberg more naturally, but Spark does not read the
  `trino` def, so choosing it breaks the share-with-Spark goal. Trade-off:
  `hive` = Spark parity (with namespace flattening, below); `trino` = clean
  Iceberg model, no Spark sharing.

## Sharp edges (decide whether sharing actually works)

1. **Resource-name flattening.** The `hive` def is `database -> table -> column`
   with NO catalog level. SQE must flatten Iceberg catalog + (multi-level)
   namespace into the `database` string using the SAME convention Kyuubi/Spark
   uses (two-part `db.table`; dotted namespace). If SQE emits `db=ns` and Spark
   emits `db=catalog.ns`, the same policy silently fails to match. Validate
   against Kyuubi's actual resolved identifiers. This is the make-or-break detail.
2. **policyType integers**: `0 = access`, `1 = DATAMASK`, `2 = ROWFILTER`.
   (Note: data-mask is 1, row-filter is 2.)
3. **Mask transformers are Hive UDFs.** The 8 built-in mask types and how SQE
   should realize them:

   | Ranger mask type | Effect | SQE realization |
   |---|---|---|
   | `MASK_NULL` | NULL | emit NULL literal (-> existing `MaskType::Nullify`) |
   | `MASK_NONE` | no mask (exemption) | pass-through (place first to carve exceptions) |
   | `CUSTOM` | `valueExpr` with `{col}` | parse the expr -> `MaskType::Custom(Expr)` |
   | `MASK_DATE_SHOW_YEAR` | keep year, zero month/day | `make_date(year(col),1,1)` / `date_trunc('year',col)` |
   | `MASK` | redact letters->x digits->n | needs a `mask()` UDF or char-class rewrite |
   | `MASK_SHOW_LAST_4` | show last 4 | needs `mask_show_last_n()` (new partial-mask type) |
   | `MASK_SHOW_FIRST_4` | show first 4 | needs `mask_show_first_n()` (new partial-mask type) |
   | `MASK_HASH` | hash the value | `MaskType::Hash` (document MD5/SHA choice; Hive uses one) |

   The transformer templates live in the service-def (in the download bundle),
   reference Hive function names, and use a `{col}` placeholder. SQE either
   implements equivalent UDFs or rewrites them into DataFusion expressions.
4. **Row-filter `filterExpr`** is a SQL boolean string in Hive/Spark dialect
   (e.g. `region in (select ... where userid = current_user())`). SQE must
   translate it to its dialect and inject it as a Filter above the scan. It can
   reference `current_user()` and subqueries.
5. **Tagging Iceberg is manual today.** No native OSS Atlas hook for Iceberg, so
   tag->resource associations come from Atlas+tagsync or manual Ranger tag REST.
   Tag-based is the most powerful but the heaviest operational lift.

## How SQE consumes it (pull + evaluate locally, no JVM plugin)

- **One REST call**: `GET /service/plugins/policies/download/{serviceName}`
  returns the full `ServicePolicies` bundle:
  - `policies[]` (resource: access=0, datamask=1, rowfilter=2),
  - `serviceDef` (resource hierarchy, accessTypes, `dataMaskDef.maskTypes` with
    transformer templates, `rowFilterDef`),
  - `tagPolicies` (`.policies[]` + `.serviceDef`) when a tag service is linked,
  - `policyVersion` (returns 304 when `lastKnownVersion` matches -> cheap polling).
  This is exactly what the JVM plugin downloads. The public-v2
  `/api/policy?serviceName=` endpoint returns only a flat resource-policy array
  (no serviceDef, no tags) -> insufficient; use the download endpoint.
- **Tag-to-resource associations** (which tags a table/column has) come from the
  tag store the `RangerTagEnricher` consumes (a separate download / tag REST),
  not the policy bundle. SQE needs them only for tag-based policies.
- **Identity** (the caller's groups/roles) is supplied by SQE at evaluation time.
  IMPORTANT distinction from the Polaris gate: SQE's `SessionUser.roles` come from
  the user's token (`realm_access.roles`), so SQE matches policy items on the
  token roles DIRECTLY - it does NOT depend on Ranger role membership the way
  Polaris does. (Polaris drops token roles; SQE does not.)
- **Evaluation order** to replicate Ranger: tag policies first, deny-overrides,
  then resource access -> data-mask -> row-filter. Feed the result
  (`ResolvedPolicy { row_filters, column_masks, restricted_columns }`) to the
  existing `PlanRewriter`.

## Cross-engine sharing requirements (author once, enforce everywhere)

For a single policy to apply in Spark AND SQE (AND Trino), all must line up:
1. same service-def TYPE (all point at a `hive`-type service) OR a shared `tag`
   service;
2. same service NAME (`ranger.plugin.<engine>.service.name` resolves to the same
   Ranger service);
3. identical resource NAMING (the `database`/`table`/`column` strings each engine
   produces must match exactly - the flattening convention from sharp edge #1).

Trino is the exception: it reads its own `trino` service-def (catalog/schema/
table/column), so it does not auto-share `hive` policies; it shares via the `tag`
service or a duplicated policy set.

## Net

Bind SQE's fine-grained `RangerStore` to the **same `hive` service Spark uses**,
flatten Iceberg names to `database/table/column` with Kyuubi's convention, pull
the **download bundle**, and evaluate row-filter + data-mask (+ tag policies) in
the existing `PlanRewriter`. Layer a **`tag`** service for org-wide PII rules.
This shares one Ranger backend across SQE and Spark with no policy duplication.

## Sources

- Kyuubi Spark AuthZ (uses hive service-def): https://kyuubi.readthedocs.io/en/master/security/authorization/spark/install.html
- Ranger hive service-def (mask types, transformers, rowFilterDef): https://github.com/apache/ranger/blob/master/agents-common/src/main/resources/service-defs/ranger-servicedef-hive.json
- Ranger trino service-def (catalog level): https://github.com/apache/ranger/blob/master/agents-common/src/main/resources/service-defs/ranger-servicedef-trino.json
- Ranger polaris service-def (no mask/rowfilter): https://github.com/apache/ranger/blob/master/agents-common/src/main/resources/service-defs/ranger-servicedef-polaris.json
- Ranger tag service-def + RANGER-1494 (tag masking): https://github.com/apache/ranger/blob/master/agents-common/src/main/resources/service-defs/ranger-servicedef-tag.json , https://issues.apache.org/jira/browse/RANGER-1494
- ServicePolicies bundle + download endpoint: https://github.com/apache/ranger/blob/master/agents-common/src/main/java/org/apache/ranger/plugin/util/ServicePolicies.java
- RangerPolicy policyType constants + row-filter/data-mask structs: https://github.com/apache/ranger/blob/master/agents-common/src/main/java/org/apache/ranger/plugin/model/RangerPolicy.java
- Tag-based policies (Atlas + tagsync, enricher, linking): https://cwiki.apache.org/confluence/display/RANGER/Tag+Based+Policies
- Hive row-filter/column-mask design: https://cwiki.apache.org/confluence/display/RANGER/Row-level+filtering+and+column-masking+using+Apache+Ranger+policies+in+Apache+Hive
