# grant-profile v4 adoption: foundation landed, rewiring next

Status: the profile is vendored, parsed and proven against its own fixtures. The
grant path still uses SQE's hand-written access-type map, so nothing has changed
in what a GRANT writes yet. This records what will change when it is rewired,
because the deltas are not all in the same direction.

## What landed

`crates/sqe-policy/assets/{grant-profile.json,servicedef-polaris.json}`, vendored
byte-identical from `data-platform/quickstart/assets/ranger/`, plus
`crates/sqe-policy/src/grants/profile.rs`:

- `canonical_privilege` -- alias resolution, whitespace collapsed, upper-cased
- `expand_access_types` -- transitive `impliedGrants` closure, sorted and deduped,
  with a seen-set because nothing in a service definition prevents a cycle
- `plan_grant` -- the multi-level plan, truncated at the level the statement names
- scope rejection, returning `Err` rather than a widened grant

Two files rather than one on purpose. The profile ships SEEDS; the closure lives
in the servicedef and is applied at write time. Pre-expanding in the platform's
generator would make the fixtures self-satisfying -- Rust echoing back what it
read -- instead of proving SQE's closure matches the platform's, and the closure
is precisely what drifted before.

`golden_fixtures_match_the_platform` checks all 26 fixtures, `rejects_are_refused`
all 9. Both assert their counts, so a profile whose fixtures vanished cannot make
them vacuously green.

`scripts/check-vendored-profile.sh` closes the other half: the fixtures prove SQE
agrees with the profile it HOLDS, not that the profile is current. Exit 0
identical, 1 drifted, 2 cannot-compare -- 2 distinct from 1 deliberately, because a
CI job that silently skips a drift gate is indistinguishable from one that passes.
All three paths were exercised.

One correction worth recording: a first pass at `plan_grant` emitted every level of
a privilege's plan and got 22/26 fixtures. The four failures were all statements
naming something SHALLOWER than the privilege's deepest level. A plan truncates at
the named level, because a table-level policy has no name to bind to when no table
was given. Found by implementing the algorithm in Python against the fixtures
before writing any Rust, which cost minutes and would have cost much longer to
find through a Rust rewrite.

## What rewiring will change, per privilege

Deepest-level access types, SQE's current hand-written map vs v4's expansion:

| Privilege | Change |
|---|---|
| `SELECT` | identical (3 types) |
| `DROP` | identical (1 type) |
| `INSERT` | **NARROWS.** SQE currently also grants `table-location-set`, `table-uuid-assign`, `table-format-version-upgrade`, `table-properties-write` |
| `MODIFY` | **NARROWS.** SQE also grants `table-properties-write` |
| `CREATE TABLE` | widens: gains `namespace-list`, `namespace-properties-read`, `table-list` |
| `USAGE` | widens: gains `table-list` |
| `CREATE SCHEMA` | widens: gains `namespace-list` |
| `ALL PRIVILEGES` | widens from 1 access type to 55 |

The two narrowings are security fixes and settle an item that had been tracked
separately. SQE's `INSERT` confers `table-location-set` today, which lets an
append-only grantee REPOINT a table's storage location. v4 excludes it, after
expansion rather than by withholding the seed, because `table-data-write`'s closure
is required to commit an Iceberg snapshot and also drags that in. So the fix is not
a bespoke patch to `WRITE_ACCESS`; it falls out of adopting the profile.

The widenings are all "make it actually work" rather than scope creep. Three are
the catalog and namespace traversal, which the multi-level plan already writes.

`ALL PRIVILEGES` is the interesting one. SQE writes the single access type
`catalog-content-manage` and relies on nothing else, but the Polaris embedded
Ranger authorizer does NOT honour service-def implied grants -- which is why every
other privilege in SQE's map is spelled out explicitly. So `GRANT ALL PRIVILEGES`
today confers one access type that Polaris will not expand, and is largely inert
for the operations an operator would expect it to allow. Expanding the closure at
write time makes it real. Anyone reasoning about what `GRANT ALL` currently does
should assume it does much less than its name.

## Remaining, in order

1. Rewire `build_grant_plan` / `build_grant_revoke_for` onto `plan_grant`, and
   delete `map_sql_to_ranger_access{,_for}`, `READ_ACCESS`, `WRITE_ACCESS`,
   `VIEW_READ_ACCESS`, `MAPPED_PRIVILEGES` and `ResourceLevel`. That last one has
   more callers than it looks: `build_resource_map`, `deny`, `remove_deny_items`,
   and `parse_grant_label` (which validates against `MAPPED_PRIVILEGES`, so it
   needs `known_privileges()` instead). The e2e access-type assertions move with
   the narrowings above.
2. Grant compensation on partial-plan failure: if the deepest level fails, release
   the levels already written. Half a plan is worse than none -- traversal alone is
   inert, a table policy alone is unreachable.
3. GROUP grantees, a two-line change. `grantee_to_fields` refuses them citing
   Ranger usersync, which this deployment does not use: the platform materialises
   every Keycloak group as a Ranger role of the identical name, verified with no
   name transform on either call site. Map `Group(n)` to the roles field. Do NOT
   auto-create the role the way the platform's `ensure_role_exists` does -- its
   grantee comes from a validated API, ours from free-text SQL, where auto-creating
   turns a typo into an empty Ranger role and a grant that silently confers
   nothing.
4. Wire `check-vendored-profile.sh` into CI, gated explicitly, saying so in the
   output when it cannot find the sibling checkout.

`ON ALL TABLES IN SCHEMA` (§9 of the handoff) is already done: `AllTablesInSchema`
and `FutureTablesInSchema` share one match arm producing table `"*"`.
