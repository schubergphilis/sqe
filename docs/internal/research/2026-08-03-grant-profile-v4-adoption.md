# grant-profile v4 adoption: foundation landed, rewiring next

Status: DONE. The profile is vendored and the grant path plans from it. This
records the deltas, which are not all in the same direction, and one migration
gap that adoption does NOT close.

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


---

# Rewiring done, and the migration gap it exposed

`plan_for` / `deepest_policy` in `ranger.rs` now delegate to `profile().plan_grant`,
and the hand-written vocabulary is gone: `map_sql_to_ranger_access{,_for}`,
`READ_ACCESS`, `WRITE_ACCESS`, `VIEW_READ_ACCESS`, `MAPPED_PRIVILEGES`,
`ResourceLevel`, `build_resource_map` and `reject_scope_deeper_than_level`. Eleven
unit tests went with them: their subject is now the golden fixtures, and keeping
hand-written duplicates of the profile's own expectations is how the two drifted in
the first place.

Where the deleted pieces were still needed, they were replaced rather than
reimplemented:

- `REVOKE` and `DENY` take `deepest_policy`, the last entry of the plan. Both act on
  the level the statement NAMES; revoking traversal would strip discovery from
  unrelated grants, and denying it would hide every object under the namespace
  rather than the one named. The scope guard now lives inside `plan_grant`, so all
  three statements agree on what a statement's scope means by construction.
- `retained_access_types` plans the OTHER privilege at the same resource instead of
  looking it up in a table, so held-back sets cannot drift from what the grant
  actually wrote. It recovers the identifiers from the resource map rather than
  having them threaded in: they are exactly what the plan put there.
- `parse_grant_label` validates against `profile().deepest_level`, so a label can
  only name a privilege the profile plans.
- `check_access` uses the deepest level's SEED, not the first element of the
  expanded set. The expansion is sorted, and `INSERT`'s alphabetically-first entry
  is `table-data-read` -- reporting a write privilege as a read.

## GROUP grantees now work

`grantee_to_fields` mapped `Grantee::Group` to an error citing Ranger usersync. That
is wrong for this deployment: the control plane materialises every Keycloak group as
a Ranger ROLE of the identical name, with no name transform on either call site, and
under Ranger no Polaris principal-roles are created at all. A group now goes in the
`roles` field, so a group grant and the same-named role grant are the same write.

Deliberately NOT auto-creating the role, unlike the platform's `ensure_role_exists`:
its grantee arrives from a validated API, ours from free-text SQL, where
auto-creating would turn a typo into an empty Ranger role and a grant that silently
confers nothing.

## Compensation on partial-plan failure

A three-level plan can land partially, because Ranger has no transaction across the
calls. If a later level fails, the levels already written are now rolled back,
innermost first. Half a plan is worse than none: the traversal levels alone confer
discovery the operator never asked for, and they are invisible in a `SHOW GRANTS`
that reports no privilege on the object.

The rollback is best-effort and the outcome is always stated. On success the message
says no partial grant remains; on failure it names the grantee and says discovery
may be retained. Implying a clean failure when compensation itself failed would be
the worse error.

## THE MIGRATION GAP: adoption does not narrow EXISTING grants

This is the part to know before deploying.

Ranger's grant endpoint MERGES access types into whatever policy already covers a
resource, and revoke removes only the types it names. So a policy written by the old
code keeps the four over-broad `INSERT` types, and a `REVOKE INSERT` issued by the
NEW code cannot clear them -- it names the narrower v4 set, and the residue is
outside it.

Concretely, on a fixture table that had been granted by the old code, `INSERT` still
showed all 23 access types including `table-location-set` after this change. Nothing
is wrong with the new planner; the policy predates it.

Found the honest way: the first version of the assertion was placed on a shared
fixture table and failed on residue. Moving it to a table with no policy history
made it pass, which is the correct scope for the claim -- **adoption narrows NEW
grants and leaves existing ones as they were.**

So `table-location-set` is closed for anything granted from here on, and any
deployment that ran the old code needs a one-off cleanup to actually lose it.
Options, none implemented here: re-issue affected grants against freshly created
policies, delete the `polaris` policies and re-grant, or a migration that strips the
four types from existing items. Whichever is chosen wants its own change, because
rewriting live access-control policies in bulk is not something to bury in a
refactor.

## Cleanup for grants already written: `audit-grants`

`cargo run -p sqe-policy --bin audit-grants` recomputes, for every `polaris` policy
carrying provenance, what the current profile says each labelled grantee should hold
at that resource, and reports anything beyond it. `--apply` writes; dry run is the
default, because rewriting live access-control policy in bulk is not a side effect
to have.

A Rust binary rather than a shell script over the JSON, deliberately: it plans
through `profile()`, the same code path a live GRANT uses, so it cannot drift from
what the engine would write today. Reimplementing the closure in Python for a
one-off tool would recreate exactly the duplication that adoption removed.

It will not touch: policies with no provenance label (no basis for deciding what
SHOULD be there, and a hand-written operator policy is not its business); items
naming more than one grantee (an access type may be owed to one of them); or deny
items (narrowing a deny GRANTS access).

Verified live, and both problems it exposed were found by not trusting its output:

**It first reported "0 over-broad items" on a stack that definitely had them.** Every
over-broad item carried an `sqe:`-prefixed label, from before the prefix was aligned
with the platform's `chm`, and the tool only read `chm`. So it would have reported
nothing to do on precisely the deployments it exists to clean. It now reads both on
READ (and writes neither).

**Its second finding would have destroyed a working grant.** `sales_wh.acdemo.orders_eu`
is a VIEW granted with `GRANT SELECT ON VIEW`, but the label recorded only `SELECT`.
A view `SELECT` and a table `SELECT` are different privileges (`SELECT VIEW` vs
`SELECT`) conferring disjoint access types, so planning the label produced table
types and the grantee's legitimate `view-*` types looked like residue. With `--apply`
that grant would have been emptied.

Two fixes came out of it. The label now records the PROFILE privilege name, so a view
grant is labelled `SELECT VIEW` and round-trips: `retained_access_types` had the same
latent bug, computing table access types for a view grant's label. And the tool
refuses any item whose held types are DISJOINT from what the label plans, reporting
it as probable pre-fix provenance rather than guessing -- removal is not recoverable.

Applied live to the legacy over-grant: `sales_wh.acdemo.orders` went from 23 access
types to 19, losing `table-location-set`, `table-uuid-assign`,
`table-format-version-upgrade` and `table-properties-write` while keeping
`table-data-write` and `table-data-read`. The second grantee's item was untouched, and
a re-run reports nothing, so it is idempotent.

## Left open

Nothing from the handoff. `scripts/check-vendored-profile.sh` is now wired into CI as
the `vendored-profile-drift` job, scoped by `changes` to the vendored assets and the
checker, and NOT `allow_failure`: the script exits 2 when it cannot find the platform
checkout, so a job that cannot verify fails rather than reporting success.
