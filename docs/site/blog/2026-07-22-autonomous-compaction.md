---
title: "Autonomous compaction, without a service account"
description: "SQE's founding rule is no service account: every query runs as the authenticated user, tokens passed through to Polaris and S3. Background compaction has no user to be. Here is how we gave Iceberg tables a self-healing maintenance path while keeping that rule intact: a structurally isolated principal, deny-by-default grants, and a commit authority that never leaves the coordinator, even when the rewrite itself is distributed across the worker fleet."
pubDate: "2026-07-22"
author: "Jacob Verhoeks"
tags:
  - "iceberg"
  - "compaction"
  - "security"
  - "distributed-systems"
---

*July 22, 2026*

SQE's founding rule is one sentence: no service account. Every query runs as
the authenticated user. The coordinator never holds a credential of its own;
it passes the caller's bearer token through to Polaris and S3 and lets those
systems decide what the caller can see. It is the cleanest sovereignty story
we have, and we have spent a year and a half not breaking it.

Autonomous compaction breaks it on paper. A background job that rewrites
small files at 2 a.m. has no session, no bearer token, and no human sitting
at a terminal. It has to run as something. The question this post answers is
what that something is, and why it does not reopen the door the founding rule
closes.

## Why the door needed opening at all

Merge-on-Read Iceberg tables accumulate two kinds of debt. Every streaming
insert and every `MERGE INTO` adds a new data file, so file counts climb
independent of table growth. Every `DELETE` and every `UPDATE` adds a
position or equality delete file that every future read has to apply before
it can trust a row. Neither kind of debt is visible in the table's row count.
Both make every scan slower, file by file, delete by delete, until a query
that used to take two seconds takes twenty.

SQE already had the cure. `CALL system.rewrite_data_files` bin-packs small
files into larger ones, applies deletes at rewrite time so they stop costing
anything at read time, and (as of a `strategy => 'sort'` or `zorder(...)`
option) can cluster rows for pruning while it's in there anyway. `CALL
system.expire_snapshots` and `CALL system.remove_orphan_files` clean up
after it. All three are correct, delete-aware, and audited.

They are also entirely manual. Someone has to notice a table is unhealthy,
remember the procedure name, and type the `CALL`. At one table that is a
minor chore. At the scale a production lakehouse runs, it is a queue of
chores nobody clears, and the tables that need compaction most are the ones
too busy for anyone to schedule downtime against. The debt that hurts most is
the debt nobody has time to look at.

## The shape of the identity problem

The obvious fix is a cron job that runs the same `CALL` on a schedule. The
obvious fix needs a credential to run as, and every credential SQE has ever
issued belongs to a human who authenticated through OIDC. Handing a
scheduler one of those tokens would mean minting a session for nobody, or
worse, reusing somebody's. Either way, the query path stops being 100%
user-identity, and the whole reason SQE exists starts to look negotiable.

We did not solve this by relaxing the rule. We solved it by keeping the rule
absolute for the query path and building the maintenance path as a second,
structurally separate thing that happens to share the same binary.

The load-bearing decision is where the new credential type lives.
`OidcM2mProvider`, the client-credentials grant that authenticates the
scheduler, is wired through a dedicated `[maintenance.principal]` config
block owned solely by the maintenance subsystem. We deliberately did not add
an `M2m` variant to the interactive `AuthProviderConfig` and did not touch
`build_auth_chain`. That inversion is the whole compliance argument in one
sentence: the interactive path cannot accidentally pick up the maintenance
token, because the code that constructs that token is never reachable from
any listener. It isn't gated behind a flag that could be flipped by mistake.
It is a different object graph.

Three more layers reinforce the same boundary. The scheduler mints an
ephemeral `Session` from the principal's identity per job and drops it when
the job ends; that session is never inserted into the `SessionManager` that
tracks real user sessions. The scheduler's only capability is calling the
maintenance dispatcher, never the SQL query path, so even a compromised
scheduler cannot run an arbitrary `SELECT` as the maintenance identity.
And the config validates itself: it refuses to start in `active` mode
without a principal configured, and it warns if the principal's client ID
collides with any interactive auth provider's.

## Deny by default, opt in by table

A structurally isolated identity is necessary but not sufficient. An
identity that can silently rewrite every table in the warehouse is still a
service account in spirit, just a well-fenced one. So autonomous compaction
gates on three independent switches, and all three have to agree before a
single byte moves:

A global `mode`, defaulting to `"off"`, then `"advisory"` (report table
health, touch nothing), then `"active"` (actually compact opted-in tables).
A per-table property, `sqe.maintenance.enabled = 'true'`, set by the table's
owner through an ordinary `ALTER TABLE` that shows up in the table's own
history like any other schema change. And a least-privilege Polaris grant on
the maintenance principal covering exactly `TABLE_READ_DATA` and
`TABLE_WRITE_DATA` on the opted-in namespaces. No `CREATE`, no `DROP`, no
admin surface at all. Polaris enforces this independent of SQE, so a bug in
our own gating logic still hits a wall at the catalog.

The failure modes are asymmetric on purpose. A table with the property but
no grant fails loudly, an audit event and a metric, never a silent skip,
because a table owner who flipped the switch and got nothing deserves to
know why. A table with a grant but missing the property is never touched at
all. Defaulting toward "do nothing and say why" rather than "do something
and stay quiet" is the same instinct that put row filters above the table
scan in the security layer: deny by default, and make denial visible.

Before any of this writes anything, it can just tell you what it would do:

```sql
CALL system.table_health(table => 'sales.orders');
```

```
 table          | small_files | avg_file_mb | delete_ratio | eligible_groups | opted_in
-----------------+-------------+-------------+---------------+------------------+----------
 sales.orders    |        1842 |        11.4 |          0.31 |               14 | false
```

Fourteen eligible groups and a table that is not opted in is exactly the
signal an operator needs to make the opt-in decision themselves. Advisory
mode ships this for free, with zero credentials configured, because
`table_health` reads snapshot summaries the coordinator already caches. It
costs nothing and commits to nothing.

Turning the engine on is a config block, not a code change:

```toml
[maintenance]
mode = "active"

[maintenance.principal]
token_endpoint = "https://idp.example.com/realms/sqe/protocol/openid-connect/token"
client_id      = "sqe-maintenance"
client_secret  = "${SQE_MAINTENANCE_CLIENT_SECRET}"
scope          = "PRINCIPAL_ROLE:sqe_maintenance"

[maintenance.scheduler]
schedule = "0 2 * * *"
lease    = "catalog"

[maintenance.compaction]
target_file_size_bytes = 268435456
strategy               = "sort"
```

Nothing here reads like a query engine growing an admin backdoor, because it
isn't one. It is a second, narrower identity, gated three separate ways,
doing one job.

## The rewrite itself is a fleet job, the commit is not

Once a table is opted in and due, the actual compaction work, reading data
files, applying deletes, sorting, encoding new Parquet, is the expensive
part. Running all of it on the coordinator caps compaction at one node's CPU
and NIC while that same node is serving interactive queries. So we
distributed it, and the split we chose says something about what "the
coordinator" means in this system.

Workers do the reading and writing. The coordinator plans groups of files,
hands each group to a worker over Arrow Flight, and the worker builds a
`StaticTable` straight from the metadata location, S3 credentials only, no
catalog token, no ability to list a namespace or look at any other table.
It re-derives its own delete-application plan rather than trusting a
serialized plan from the coordinator, because iceberg-rust marks some of
those fields skip-serialize, and a plan that silently drops partition
identity is worse than no plan at all. The worker reads, applies deletes,
optionally sorts, writes new Parquet directly to the table's data location,
and reports back an Avro-encoded description of what it wrote. Bytes flow
between the worker and S3. What crosses the coordinator-worker channel is
kilobytes of metadata, the same shape as the existing distributed scan path,
just applied to the write side this time.

The coordinator does the one thing no worker is allowed to do: commit. It
collects every group's response, re-runs the `added_rows <= removed_rows`
invariant across the whole job (each worker already checked its own group,
this is the check that can't be delegated), and issues exactly one
`RewriteFilesAction`. One snapshot, every old file swapped for every new
file at once. A worker that returns a bad signature, a resurrection-guard
failure, or a delete-accounting mismatch fails that group; it never gets
anywhere near table state. Commit authority is a property of the
coordinator's code path, not a permission a worker chose not to use.

That split earns its keep on the retry path too. If a concurrent writer
commits between the coordinator's read and its `RewriteFilesAction`, the
commit hits Iceberg's optimistic-concurrency check and fails as a conflict.
The coordinator re-plans from the new snapshot and re-dispatches every group
from scratch, rather than trying to patch a stale attempt. The previous
attempt's worker output becomes an orphan, unreferenced by any commit, and
`remove_orphan_files` reclaims it on its normal age-thresholded sweep. That
is real transient storage cost at terabyte scale on every conflict, and we
accept it rather than build a smarter merge, because the alternative is
trusting a partially-stale plan against a table someone else just changed.

## A lease that is honest about not being load-bearing

Production deployments run more than one coordinator for availability. Two
coordinators both deciding it's 2 a.m. and both compacting `sales.orders`
is not a correctness problem, Iceberg's optimistic commit already guarantees
exactly one of them wins and the other retries into a no-op, but it is a
waste problem: two coordinators each scan, decode, sort, and re-encode the
same terabytes, and one of those efforts gets thrown away.

So multi-coordinator deployments claim a lease, a row appended to
`sqe_system.maintenance_log`, before dispatching a rewrite. We were careful
about what we let ourselves say about that lease. It is an efficiency layer,
not a correctness mechanism, and every failure mode was worked out under the
assumption that it might not be there at all: `lease = "none"` is a
supported, documented config for single-coordinator deployments. If the
lease claim itself fails, or two coordinators race the very first claim on
a brand-new table (the one gap in the lease's own exclusivity, since a
fresh append has no prior row to compare against), the outcome does not
change. Exactly one `RewriteFilesAction` commits. The loser's commit fails
a file-existence check against files the winner already removed, and that
coordinator's job ends having modified nothing. A holder that crashes
mid-job leaves the table exactly as it found it, because nothing commits
until the rewrite is fully planned, dispatched, and validated; the lease
just times out and the next coordinator picks the table back up on its next
tick.

Writing "this is not a correctness mechanism" directly into the design was
a deliberate choice. A lease that people assume is load-bearing tends to
grow load-bearing assumptions nobody wrote down, and one of the ways
distributed systems fail quietly is by discovering years later that a
liveness optimization was secretly the only thing preventing corruption.
Ours never was. We'd rather say that once, in one place, than have someone
rediscover it during an incident.

## What we are not claiming yet

The distributed rewrite path is real code with real tests: worker-side
group rewrite, HMAC-signed requests, the largest-first placement across
healthy workers, the retry-on-a-different-worker logic for transport
failures versus the fail-the-whole-job logic for application-level ones.
What it does not yet have is a validated benchmark run at the scale that
would prove the distribution pays for itself, real multi-worker rewrites
against tens of millions of rows, measured wall-clock against the
coordinator-local path. Our target is a distributed rewrite beating
coordinator-local by more than 2.5x at that scale, and until that number is
measured on real data rather than a wiring smoke test, we are treating it
as a pre-production gate, not a shipped result. We'd rather say that plainly
than let a design document's target number quietly become a marketing
number.

## The debt gets cleaned without anyone typing a CALL

The payoff, when the gate above clears, is small. A table owner sets one
property. From then on, small files stop accumulating past the target size,
delete files stop stacking up past the point where every scan pays for
them, and the table stays fast without a human remembering to schedule
downtime for it. None of that required trusting a service account with the
warehouse. It required building a second identity that structurally cannot
reach the query path, gating it three separate ways, and keeping the one
decision that matters, whether a snapshot commits, on the coordinator no
matter how many workers did the work underneath it.

The rule survives. No service account still means no service account. What
changed is that a background job can now do useful work without needing to
be a person.
