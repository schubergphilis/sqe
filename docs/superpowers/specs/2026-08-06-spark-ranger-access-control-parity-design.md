# Spark/Ranger Access-Control Parity Design

**Date:** 2026-08-06
**Status:** design, approved decisions recorded below
**Related:** `docs/site/book/src/design-notes/ranger-access-control.md`,
`docs/superpowers/specs/2026-07-31-ranger-access-control-e2e-design.md`

## Goal

Spark, running against the same Polaris catalog and the same Ranger instance as SQE,
must be subject to the same access control: object level first (catalog, namespace,
table, view), then row filters, column masks, and tags. The end state is two Ranger
backends, one for Polaris and one shared frontend-query backend that both SQE and
Spark read.

## What was measured first

Every claim below rests on a probe run against the live
`quickstart/polaris-ranger-keycloak` stack on 2026-08-06, not on reading code.

**1. Object-level enforcement for Spark is a credential swap, not an engine change.**
Polaris runs `polaris.authorization.type: ranger` with
`polaris.authorization.ranger.service-name: polaris`, and federates identity from
Keycloak (`preferred_username`, roles from `realm_access/roles`). Give Spark's Iceberg
REST catalog a per-user Keycloak JWT and Polaris authorizes as that user:

```
spark.sql.catalog.probe.token=<alice|bob JWT>
spark.sql.catalog.probe.token-refresh-enabled=false

SELECT count(*) FROM probe.acdemo.orders   -- bob: engineer holds table-data-read -> 1 row
SELECT count(*) FROM probe.ac.orders       -- bob: nobody holds table-data-read
  org.apache.iceberg.exceptions.ForbiddenException:
  Forbidden: Principal 'bob' is not authorized for op 'LOAD_TABLE'
```

Today Spark connects as `root` with `PRINCIPAL_ROLE:ALL`, so the whole tier is
bypassed. `token-refresh-enabled=false` is load-bearing: left on, Iceberg exchanges
the external JWT against Polaris's own token endpoint and the identity reverts.

**2. Renaming the shared service instance works.** A service named `query` of
servicedef type `hive` was created, given one access policy and one mask policy on
`acdemo.orders`, and Kyuubi honored it: `SELECT id, ssn` returned `xxx-xx-1111`,
`xxx-xx-2222`, `xxx-xx-3333`. That mask exists only in `query`, never in `hive`, so
the plugin demonstrably resolved the renamed instance. The servicedef *type* must stay
`hive`, because Kyuubi is hardwired to the `database`/`table`/`column` resource shape.
SQE needs no code change either: its own suite already reads a non-default instance
named `sqe_ac_hive`.

**3. Both engines already agree on resource naming.** SQE writes and reads hive-service
policies with a bare namespace as `database` (`sales`, `ac`), no catalog prefix, which
is exactly what Kyuubi sends. One shared service is achievable with no naming
convention to invent.

**4. Denials from Polaris name the principal and the operation.** `403
ForbiddenException: Principal 'dave' is not authorized for op 'LIST_TABLES'`. SQE
follows a no-information-leakage model instead. The divergence is recorded below, not
fixed here.

A fifth observation shaped the hygiene requirements: the running stack carried 20
policies on the `polaris` service, several of them residue from earlier suite runs,
including a user grant to `dave` that made an unauthorized-looking read succeed. The
first read of that result looked like a fail-open. It was not. Any new suite needs the
same pre-state assertions the SQE suite has.

## Target architecture

| Ranger service | Type | Enforced by | Covers |
|---|---|---|---|
| `polaris` | `polaris` (custom, 69 access types) | Polaris itself | catalog, namespace, table, view, principal, policy |
| `query` (renamed from `hive`) | `hive` (built-in) | each engine: SQE PlanRewriter, Kyuubi RangerSparkExtension | row filters, column masks |
| `tag` | `tag` | each engine, via the attached `tagService` on `query` | mask and filter rules per tag |

The `tag` service is attached to the frontend service through Ranger's `tagService`
field, so it is not a third backend to configure. It is the tag half of the frontend
backend.

Identity reaches the two tiers by different routes, which is the single most important
property of the design:

```
Keycloak ROPC token (verified JWT)  ->  Polaris  ->  `polaris` service   [object level]
HADOOP_USER_NAME (asserted string)  ->  Kyuubi   ->  `query`/`tag`       [fine grained]
```

## Decisions taken

**Rename `hive` to `query` now.** Servicedef type stays `hive`; only the instance name
changes. Doing it before the suite exists avoids a mechanical sweep of the suite later.

**The Rust default stays `hive`.** `default_ranger_policy_service_name()` in
`crates/sqe-core/src/config.rs` keeps returning `"hive"`. Changing a default silently
repoints every existing deployment at a service that may not exist, and the failure
mode is a wholesale enforcement change. The quickstart and docs set `query` explicitly.

**Phase 1 covers read and write.** Write coverage is where a fail-open would actually
hurt, because Polaris vends storage credentials on the write path.

**Tags are projected into Ranger's tag store.** The Iceberg property
`sqe.column-tags` stays the source of truth. `ALTER TABLE ... SET TAG` additionally
writes the tagged resource into Ranger's tag store, so Kyuubi, and any other
Ranger-plugin engine including Trino, sees the same association natively.

## Phase 1: object level for Spark

### Configuration changes

`quickstart/polaris-ranger-keycloak/spark/spark-defaults.conf` drops the identity
settings and keeps everything else:

- remove `credential`, `oauth2-server-uri`, `scope`
- set `token-refresh-enabled false`
- the per-user `token` is injected per invocation, not baked into the file, because one
  static token cannot serve several test users

The rename touches exactly these files, enumerated so the plan has no discovery step:

| File | Change |
|---|---|
| `quickstart/polaris-ranger-keycloak/ranger/bootstrap-ranger.sh` | create `query`, attach `tagService: tag`, rewrite the `"service"` field of seeded policies |
| `quickstart/polaris-ranger-keycloak/spark/ranger-spark-security.xml` | `ranger.plugin.spark.service.name` becomes `query` |
| `quickstart/polaris-ranger-keycloak/sqe.toml` | `[policy.ranger] service-name = "query"` |
| `quickstart/polaris-ranger-keycloak/test.sh`, `parity-test.sh`, `OVERVIEW.md` | 5 references in `parity-test.sh` alone |
| `quickstart/polaris-ranger-service-principal/ranger/bootstrap-ranger.sh` | same service creation |
| `scripts/access-control-demo.sh` | policy posts |
| `docs/site/book/src/design-notes/ranger-access-control.md`, `features/access-control-tutorial.md`, `features/fine-grained-access-control.md` | prose and examples |

The SQE access-control suite is *not* in that list. It creates its own `sqe_ac_hive`
and `sqe_ac_tag` (`tests/it/common/ranger_fixture.rs:26-28`), so the rename cannot
affect it. That is what makes "31 cases still pass unchanged" a meaningful check rather
than a tautology: if the suite breaks, the rename leaked somewhere it should not have.

### Test cases

Named to mirror their SQE counterparts so the pair is greppable.

| Case | Asserts |
|---|---|
| `spark_denied_before_any_grant` | `LOAD_TABLE` 403 with no grant present |
| `spark_grant_select_to_role_enables_exact_rows` | role grant admits exactly the seeded rows |
| `spark_role_grant_and_user_grant_both_apply` | user-level and role-level grants both take effect |
| `spark_write_privileges_are_separate_from_read` | a read-only grant cannot INSERT; `table-data-write` admits it |
| `spark_ranger_deny_overrides_allow` | a deny item beats an allow grant |
| `spark_revoke_disables_access` | revoke closes a previously working read |
| `spark_all_tables_in_schema_grant_covers_the_namespace` | a namespace-wide grant covers a table not named individually |
| `spark_namespace_listing_requires_namespace_list` | `SHOW NAMESPACES` gated by `namespace-list` |
| `spark_view_read_requires_view_access_types` | view reads gated by `view-*`, separately from `table-*` |
| `spark_catalog_level_denial` | a catalog with no `catalog-list` grant is not reachable |
| `mismatched_identity_reveals_the_two_tier_trust_split` | token=alice with `HADOOP_USER_NAME=bob` yields alice's object rights and bob's masks; documents the split rather than claiming it is closed |

## Phase 2: fine-grained parity

Both engines read the same `query` service, so each policy is written once and
asserted twice. Parity assertions compare the two engines' output directly rather than
checking each engine did something.

| Case | Asserts |
|---|---|
| `column_mask_is_byte_identical_across_engines` | same CUSTOM mask, byte-equal output from SQE and Spark |
| `row_filter_returns_identical_rows_across_engines` | same row-filter policy, identical row sets |
| `resource_mask_beats_tag_mask_across_engines` | precedence matches SQE's existing `resource_mask_beats_tag_mask_live` |
| `tag_column_mask_is_byte_identical_across_engines` | gated on the tag projector below |

Two known Kyuubi constraints shape these:

- Row filters over an unprojected column throw `MISSING_ATTRIBUTES` on Spark 3.5
  (Kyuubi #6889). The filter column must be projected. Recorded as a divergence, not
  worked around silently.
- Named mask types are not portable. `MASK_SHOW_LAST_4` renders `xxx-xx-1111` in SQE
  and `nnnUnnU1111` in Kyuubi. Only a CUSTOM transformer with portable standard SQL
  (`concat('xxx-xx-', substr({col},8,4))`) is byte-equal. The existing bootstrap
  already documents this; the suite must not regress to a named type.

### Phase 2b: the tag projector

`ALTER TABLE ... SET TAG` writes the Iceberg property as it does today, then projects
the association into Ranger's tag store as a tagged resource keyed on
`database`/`table`/`column`. Requirements:

- projection failure must not silently succeed the DDL; a tag that exists in Iceberg
  but not in Ranger is precisely the fail-open this phase exists to close
- removing a tag removes the projection
- a repair path for drift, since the Iceberg property can be edited out of band

Until the projector lands, a tag-masked column is protected in SQE and returned raw by
Spark. That direction is stated in the access-control matrix as an open gap.

## Test harness

`crates/sqe-coordinator/tests/it/spark_access_control_e2e.rs`, reusing
`tests/it/common/` (Ranger fixture, role setup, serial lock, pre-state assertions) and
the test-owned-service pattern that gives the SQE suite `sqe_ac_hive` and `sqe_ac_tag`.
A Spark suite can own its services too: the Ranger plugin config is a file the test
writes into the container, with `--driver-class-path` pointing at it, which is the
mechanism validated in probe 2.

Living in `sqe-coordinator` is a compromise. The tests are cross-engine, not
coordinator tests. The alternative, a separate crate, would duplicate the fixture,
token minting, and policy builders. Reuse wins; the file name carries the distinction.

A thin runner `spark_sql(user, query)` shells out to
`docker compose exec -T -e HADOOP_USER_NAME=<u> spark /opt/spark/bin/spark-sql`, injects
the user's JWT as `--conf spark.sql.catalog.<c>.token=`, parses tab-separated rows, and
classifies `ForbiddenException`, `not authorized`, and `MISSING_ATTRIBUTES` distinctly
so a Kyuubi bug never reads as a denial.

Runtime control: each `spark-sql` is a fresh JVM, roughly 4 to 10 seconds warm, and a
policy change needs one Ranger poll interval. Assertions that share a policy state run
as several statements inside one invocation rather than one invocation each.

Driven by `scripts/spark-access-control-test.sh`, gated the same way as the SQE suite
(`#[ignore]` plus an environment gate), and exposed as `make test-access-control-spark`.
The existing `make test-access-control` stays as it is: it deliberately excludes
`spark` and `data-seed` from its dependency chain, which is what keeps it fast.

## Deliberately not mirrored

Most of the SQE suite's 31 cases have no Spark counterpart, and saying which is part of
the deliverable. Spark has no GRANT path into the `polaris` service; grants are written
by SQE or by the platform.

- `a_non_admin_cannot_grant_under_the_default_gate`, `a_delegated_owner_grants_...`,
  `deny_still_requires_an_admin_role_under_ranger_delegate`, `show_grants_lists_both_roles`,
  `check_access_reflects_user_grants`, `one_table_grant_writes_the_namespace_it_needs`:
  SQE's grant-authoring path.
- `cache_ttl_bounds_policy_staleness`, `ranger_outage_fails_closed`: SQE's policy cache
  and failure mode. Kyuubi has its own cache with its own semantics; asserting SQE's
  behavior against it would test nothing.
- `hash_mask_is_keyed_hmac`: SQE's keyed HMAC has no Kyuubi equivalent.
- `unmappable_tag_mask_fails_closed`: cannot hold on Spark until the projector lands,
  and after it lands the failure mode is the projector's, not the engine's.

## Documented divergences

Recorded in `docs/site/book/src/features/access-control-matrix.md`, not fixed here.

1. **Identity assurance differs by tier.** The object tier verifies a JWT signature.
   The fine-grained tier trusts an asserted `HADOOP_USER_NAME`. Closing it means
   running Spark behind a Kyuubi server with real authentication, which is a separate
   piece of work.
2. **Polaris denial messages name the principal and the operation.** SQE hides denied
   objects instead.
3. **Kyuubi row filters need the filter column projected** (Kyuubi #6889).
4. **Named Ranger mask types render differently per engine.** Only portable CUSTOM
   expressions are byte-equal.

## Success criteria

- Spark authenticates to Polaris as the end user, and `make test-access-control-spark`
  proves object-level allow and deny for read and write across catalog, namespace,
  table, and view.
- One policy written once in the `query` service produces byte-identical output from
  SQE and Spark for column masks and identical row sets for row filters.
- `make test-access-control` still passes unchanged at 31 cases, with the service
  rename applied.
- The access-control matrix lists every divergence above, and the tag gap is either
  closed by the projector or listed as open with its fail-open direction stated.

## Rollback

The rename is a config change in three files plus the bootstrap; reverting them
restores `hive`. The Spark credential swap is confined to
`spark-defaults.conf` and the harness; restoring the `root` `credential` line returns
Spark to its current bypassed state. The tag projector is additive to the DDL path and
can be feature-gated off, leaving the Iceberg property as the only writer.

## Coordination

The rename touches data-platform's in-flight #509 asset rename. The shared
`servicedef-polaris.json` is unaffected, since the renamed instance is a `hive`-type
service. Their rename and this one should land in a known order, and the notes already
passed to them about `$SRC_DIR` in `scripts/check-vendored-profile.sh` still apply.
