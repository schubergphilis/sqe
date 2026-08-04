# Owner on create

**Status:** specced, not built. Deferred deliberately: `grant_authority` (MR for
`feat/delegated-grant-authority`) makes delegated grants work, and this is the
follow-on that makes them self-sustaining.

## The gap

`delegateAdmin` is SQE's ownership primitive. `GRANT ... WITH GRANT OPTION` confers
it, `[access_control] grant_authority = "ranger-delegate"` makes it usable without
an engine-wide admin role, and Ranger enforces it per resource and per access type.

What is missing is the first grant. A user who runs `CREATE TABLE` owns nothing:
Polaris lets them create it, and Ranger has no policy naming them on it. So every
new table needs an admin to hand its creator authority over it, which puts an admin
in the loop for ordinary work and is the reason delegated grants currently need
seeding.

## What it would do

On a successful `CREATE TABLE` / `CREATE TABLE AS SELECT` / `CREATE VIEW` against a
catalog whose access-control backend enforces grantor authority, write one Ranger
policy naming the creator on the new object, with the object's full privilege set
and `delegateAdmin: true`. The creator can then read, write and hand on access to
their own table without an admin.

The write goes through the same `GrantBackend::grant` path a SQL `GRANT` uses, so it
plans from the vendored profile and cannot drift from what `GRANT` writes.

## Decisions to make before building

**1. Which grantor.** The creator does not yet hold `delegateAdmin` on the table, so
they cannot grant it to themselves: Ranger would 403. The write has to be performed
with SQE's configured admin identity. That is a deliberate exception to "always act
as the caller" and needs to be narrow and stated: only on create, only naming the
creator, only on the object just created. Anything broader reintroduces the
escalation the grantor field exists to prevent.

**2. Traversal.** The creator already reached the namespace to create the table, so
they hold discovery. Writing only the table level is correct; the plan's ancestor
levels are already satisfied and skipped by the mechanism `grant_authority` added.

**3. Failure.** A create that succeeds while the ownership grant fails leaves a
table nobody owns. Rolling the table back would be worse (data loss on CTAS), so
the create reports success with a warning naming what was not written, and the
statement to run by hand. Silent failure is not an option: the operator would
believe they own a table they cannot grant on.

**4. DROP.** Ranger keeps the policy after the table is gone, and a table later
recreated by someone else would inherit the previous owner's grant. Either DROP
removes the owner policy (needs the same admin-identity exception, and must not
remove policies SQE did not write) or create must overwrite rather than merge. The
provenance label (`chm:USER:<name>:OWNER`) is what makes the difference decidable.

**5. Who is the owner when an admin creates a table for someone else.** No answer
from the statement itself. Simplest defensible rule: the authenticated caller, with
`GRANT ... WITH GRANT OPTION` as the way to hand ownership on. An `AUTHORIZATION`
clause is a later addition, not part of this.

**6. Opt-in or default.** Writing a policy per table changes the shape of the Ranger
policy list on every deployment, and on a benchmark load (thousands of tables) that
is a lot of policies. Should be a config switch, off by default, in the same
`[access_control]` block.

## Testing

- e2e: dave creates a table on a `ranger-delegate` handler, then grants on it with
  no admin involved. Mutation: disable the owner write and the grant must 403.
- e2e: the owner policy carries `delegateAdmin: true` and the profile's full
  privilege set for the object, asserted as equality (writing more than the profile
  specifies is as much a drift as writing less).
- e2e: a create whose ownership write fails still reports the table created, and the
  message names the missing grant.
- e2e: drop then recreate as a different user, and the first user holds nothing.
- Unit: the admin-identity exception is reachable ONLY from the create path.

## Not in scope

Namespace and catalog ownership, `ALTER ... OWNER TO`, and ownership as a Polaris
concept (Polaris models it separately from Ranger policy; mirroring both would need
its own design).
