# The catalog traversal is load-bearing for SQE. Do not narrow it.

Answers §7.1 of data-platform's `2026-08-02-sqlengine-grant-profile-handoff-prompt.md`,
which blocks vendoring `grant-profile.json` on this result.

**Verdict: vendor v4. Keep `_TRAVERSE_CATALOG`. The reason it exists has not expired.**

Run 2026-08-02 against the live `polaris-ranger-keycloak` stack (Polaris 1.6
in-memory, Ranger 2.8, Keycloak), user `dave`, table `sales_wh.ac.orders`.

## The question

The platform spiked this on 2026-08-01 and found that catalog-level
`namespace-list` is NOT required to reach a table: a grant at the table
coordinate alone made `LOAD_TABLE` return 200. If that held through the engine,
the traversal could be narrowed, `USE` would become the explicit discovery
grant, and the namespace-enumeration leak would close.

Their spike could not run a query. The stack had SQE's HTTP API disabled, so the
finding was REST-only. This closes that half.

## Result

The platform's REST finding reproduces exactly, and does not survive the engine.

| Step | Polaris REST (direct) | SQE (Flight SQL) |
|---|---|---|
| No grant | `LOAD_TABLE` 404 | denied |
| Table-coordinate grant only | `LOAD_TABLE` **200** | **denied**, 4 min |
| + catalog `namespace-list` | `LIST_NAMESPACES` 200 | **denied**, 3.5 min |
| + namespace `namespace-properties-read` | 200 | **4 rows** |
| Remove catalog `namespace-list` again | `LIST_NAMESPACES` 403 | denied |

Polaris will hand SQE the table. SQE never asks for it.

## Why

`SqeCatalogProvider::schema(name)` answers only for a namespace present in the
cached namespace list (`catalog_provider.rs:474-515`). That list is built by
`list_visible_namespace_names`, which is two calls, and both must succeed:

1. `list_namespaces` needs `LIST_NAMESPACES`, which Polaris authorizes at the
   CATALOG level. A namespace-scoped `namespace-list` does not satisfy it:
   Polaris does not use Ranger's `SELF_OR_DESCENDANTS` matching, so listing is
   denied outright rather than filtered. Confirmed by removing the catalog-level
   item while the namespace-level one was still in place.
2. The visibility filter then probes each namespace with `get_namespace`
   (`rest_catalog.rs:1051`), which is `LOAD_NAMESPACE_METADATA` and needs
   namespace-level `namespace-properties-read`. On 403 the namespace is hidden,
   by design, so ungranted namespace names do not leak through `SHOW SCHEMAS`.

Either failure produces an empty schema list, `schema("ac")` returns `None`, and
DataFusion planning ends at `table 'sales_wh.ac.orders' not found`. `LOAD_TABLE`
is never attempted. Nothing in the SQE log shows a 403, because there is no
denial to report: the table was never asked for.

The visibility probe is not incidental. It is what stops a table-level grant
from leaking every namespace name in the catalog, and removing it to save a
round trip would reopen the leak the v5 narrowing is trying to close.

## The profile's v4 SELECT plan is already right

Derived empirically here, before reading the profile:

```
catalog   -> namespace-list
namespace -> namespace-properties-read
table     -> table-data-read (+ closure)
```

`grant-profile.json` v4 `privileges.SELECT` is those three levels, in that
order. The contract matches the engine. What does not match is SQE's
hand-written map, which returns ONE `ResourceLevel` per privilege and so cannot
express any of this. That is divergence 1 of the handoff, and this run is the
evidence for it: a SQL `GRANT SELECT` writes the table level only and leaves the
grantee unable to read the table it named.

## A second finding: SHOW TABLES does not degrade

§7 of the handoff argues the traversal may be unnecessary because SQE's
`list_namespaces` call sites are all discovery, and `SHOW SCHEMAS` already
degrades to an empty result rather than erroring. Half true.

| Statement, as an ungranted user | Behaviour |
|---|---|
| `SHOW SCHEMAS` | `(0 rows)`, as documented |
| `SHOW TABLES` | hard error: `Failed to list namespaces: 403 Principal 'dave' is not authorized for op 'LIST_NAMESPACES'` |

`SHOW TABLES` leaks the raw Polaris error, including the operation name and the
principal, where `SHOW SCHEMAS` returns nothing. Worth fixing on its own terms:
the two should agree, and the empty result is the right answer for both.

## Re-validated on Polaris 1.7, clean database

Repeated the whole sequence after upgrading the stack to `apache/polaris:1.7.0`
and destroying every volume, so Ranger's Postgres and the S3 bucket both started
empty. dave held no role and no policy item at all: a true zero baseline rather
than the leftover-strewn one above.

| Step | Polaris 1.7 REST | SQE |
|---|---|---|
| No grant | `LOAD_TABLE` **403** | denied |
| `GRANT SELECT ON sales_wh.ac.orders TO USER dave` | `LOAD_TABLE` **200** in ~10s | **denied** |
| Same moment | `LIST_NAMESPACES` **403** | |
| + namespace and catalog traversal | 200 | **4 rows** in ~8s |

Same verdict. 1.7 does not change the conclusion and the traversal stays
load-bearing.

One difference worth noting, though it changes nothing here: an ungranted
`LOAD_TABLE` answers **403** on 1.7 rather than hiding behind a 404. SQE still
reports "table not found" to the caller, because that message comes from SQE's
own planning path once the namespace fails to resolve, not from the Polaris
status code. The information-hiding contract SQE presents is its own.

Suites on the clean 1.7 stack: `scripts/access-control-demo.sh` 32/32,
`make test-access-control` 23/23.

## Method

dave holds no Ranger role, no group, and no wildcard or public policy item, so
the three grants added were his only access. Verified by enumerating every
`policyItems` entry naming him across the `polaris` service before and after.
Every grant was applied by editing a saved copy of the policy and restored from
it afterward; the closing footprint is byte-identical to the opening one, and
dave is denied again while carol still reads 4 rows.

The fixture matters more than it looks. The first attempt ran against
`sales_wh.acdemo.orders` and produced a clean-looking denial that meant nothing:
Polaris runs `in-memory` here and had restarted, so the table did not exist and
carol got the same 404. Ranger policies live in Postgres and survive; Polaris
state does not. Any experiment on this stack needs an admin control proving the
object exists before a denial can be read as a denial.

---

# Follow-up, 2026-08-03: which half of the traversal SQE can write for you

The question this raised: if the traversal is load-bearing and mechanical, why is
the operator typing it? Answered by splitting the two levels and testing them
separately rather than treating "the traversal" as one thing.

## The namespace level is mechanical. The catalog level is a decision.

Isolated live, one variable at a time, on the 1.7 stack:

| dave holds | `SELECT count(*) FROM sales_wh.acdemo.orders` |
|---|---|
| catalog `namespace-list` + table `SELECT` | `table 'sales_wh.acdemo.orders' not found` |
| the same, plus `{catalog, namespace}` `namespace-properties-read` | 3 rows |

carol (admin) read 3 rows throughout, so the table existed for every reading. The
second row differs from the first by ONE access type, written by hand onto the
existing `{namespace: acdemo, catalog: sales_wh}` policy and reverted afterward.

That makes the namespace level a completion of the statement: it is an ancestor on
the path to the table the operator named, required to reach it, conferring nothing
about anything outside that path. `build_grant_plan` now writes it.

The catalog level is categorically different and stays explicit. Catalog-level
`namespace-list` lets its holder enumerate every namespace name in the catalog,
including ones unrelated to the granted table, so auto-adding it would widen the
blast radius of every `GRANT SELECT` while reporting success. It is also granted
once per role rather than once per table, so the ergonomic argument for automating
it is weak. Three statements became two, and the one left is the one that costs
something.

## Two bugs found while establishing this, neither caused by the change

**`SHOW SCHEMAS FROM <catalog>` can answer about a different catalog.** FIXED 2026-08-03, see the closing section. With
`[catalog] warehouse = "sales_wh"`, `SHOW SCHEMAS FROM sales_wh` and
`SHOW SCHEMAS FROM ops_wh` both returned `ops`, `ac` -- ops_wh's namespaces, while
sales_wh actually holds `sales`, `ac`, `acdemo` (confirmed from Polaris's own
`GET /v1/sales_wh/namespaces`). Cause is in `show_catalog`: the explicit name is
preferred, then the guard `cat != self.config.catalog.warehouse` discards it for
the one case where the named catalog IS the configured default, falling through to
`session_catalog(session)`, which re-resolves from the session default. So the
qualifier silently loses exactly when it names the default warehouse.

This matters beyond ergonomics: `SHOW SCHEMAS` is what an operator uses to confirm
a grant took effect, and here it reports on a catalog they did not ask about. It
also cost real time in this investigation, first by making a fixture table look
absent (it was not) and then by making `USAGE` look insufficient for discovery (it
is sufficient; Polaris logged 200 on both the list and the probe).

**`SqeCatalogProvider::schema()` wedges when every namespace probe denies.** A
principal who can list a catalog's namespaces while all per-namespace probes 403
hangs indefinitely instead of reporting "table not found". Stack captured with
`sample`:

```
QueryHandler::execute_query -> SessionContext::sql
  -> SqeCatalogProvider::schema -> contains_namespace
    -> sqe_catalog::runtime_bridge::block_on_compat
      -> std::thread::JoinHandle::join -> pthread_join -> __ulock_wait
```

Same re-entrant-`block_on` family as #195. It reproduces on a current-thread
runtime (`#[tokio::test]`'s default) and not through the container, whose runtime
is multi-threaded, which is why the CLI answered "table not found" promptly in the
same state. Reachable in production by any principal holding catalog discovery and
no namespace visibility -- a state a `REVOKE USAGE ON SCHEMA` produces. It is why
`one_table_grant_writes_the_namespace_it_needs` asserts its pre-state from the
Ranger policy rather than by having dave attempt a read.

---

# Reversal, 2026-08-03: the catalog level is auto-granted after all

The follow-up above concluded that SQE should write the namespace level and refuse
the catalog level, on the grounds that catalog-wide `namespace-list` exposes
sibling namespace names unrelated to the granted table. The reasoning holds; the
conclusion was wrong, and this records why so the argument is not re-run.

`grant-profile.json` v4 specifies the catalog level for every privilege that
reaches into a catalog:

```
SELECT   catalog:[namespace-list] | namespace:[namespace-properties-read] | table:[table-data-read]
INSERT   catalog:[namespace-list] | namespace:[namespace-properties-read] | table:[table-data-write]
                                                exclude table-location-set, table-uuid-assign, table-format-version-upgrade
MANAGE   catalog:[catalog-content-manage]
```

The data-platform control plane generates its Ranger policies from that file, and
both it and SQE write to the same `polaris` service. Two consequences settle it:

1. A SQL grant producing different policies from the equivalent API call makes
   "who granted this, and does it confer the same thing" unanswerable. Provenance
   labels do not help, because the divergence is in the access types, not the
   attribution.
2. The drift gate specified in §8 of the handoff compares `plan_grant` output to
   the profile's fixtures byte for byte. A deliberately narrower plan fails it by
   construction, so the narrowing could not coexist with the mechanism that keeps
   the two implementations honest.

The widening is therefore accepted and documented as a cost of every table grant,
rather than presented as something SQE refuses. Where namespace names are
themselves sensitive, the boundary that actually works is a separate catalog, not
a narrower grant.

Also settled by reading the same contract: the provenance prefix is `chm`, not
`sqe` (`provenance.py:45`), and labels go on the DEEPEST level only. Catalog and
namespace policies are shared plumbing; stamping them with one grantee's privilege
misrepresents them as privately owned and invites a later revoke to release
traversal another grant still depends on. The `sqe:traversal:` marker introduced
by the earlier version is gone.

`INSERT`'s `exclude` list is worth noting for the record: it is subtracted AFTER
the `impliedGrants` closure, never held out of the seeds, because
`table-data-write`'s closure is required to commit an Iceberg snapshot but also
drags in `table-location-set`, which `INSERT` must not confer. SQE's hand-written
`WRITE_ACCESS` still carries `table-location-set`, so that divergence closes with
profile adoption rather than separately.

---

# The read-path wedge has TWO causes, 2026-08-03

The hang recorded above (`contains_namespace` -> `block_on_compat` ->
`pthread_join` -> `__ulock_wait`) is not one bug. Fixing the obvious one leaves the
hang in place, so both are written down here.

## Cause 1: core contention. FIXED.

`block_on_compat`'s current-thread branch spawned an OS thread and called
`handle.block_on(fut)` on the CALLER'S runtime, then blocked the parent in
`join()`. A current-thread runtime has one core. Every real call site is a
synchronous DataFusion hook (`CatalogProvider::schema`, `SchemaProvider::table`, a
TVF's `call`) reached from inside a task being polled, so the caller already holds
that core and keeps holding it while it waits. The child waits for the core the
parent holds; the parent waits for the child. A pure deadlock, independent of any
I/O.

Fixed by giving the spawned thread a runtime of its own.

**Why no test caught it:** `block_on_compat_works_on_current_thread` calls the
bridge from a thread OUTSIDE the runtime, so the core is free. That is not the
shape of any production call site. The test passed for months while the
production path deadlocked, and it still passes against the broken implementation
today. The replacement,
`does_not_deadlock_when_the_caller_holds_the_current_thread_core`, drives a
current-thread runtime from a scratch thread and calls the bridge INLINE from the
task body, then bounds the wait with `recv_timeout` so a regression fails with a
message instead of hanging the suite. Mutation-verified against the old code.

## Cause 2: the future's I/O belongs to the parked runtime. BOUNDED, not fixed.

With cause 1 fixed the query still hangs, and a `sample` of the child thread shows
why: it is running its own runtime and parked in its own IO driver, waiting for a
connection whose hyper task lives on the PARENT runtime's reactor. The parent is
blocked in `join()`, so that reactor is not being driven and the response can never
arrive.

This is not fixable inside `block_on_compat`. Parking the caller's runtime is the
whole strategy, and on a current-thread runtime the caller IS the only thread, so
any future that awaits I/O owned by that runtime cannot complete while the bridge
is blocking on it. The observable symptom is identical to cause 1, which is why
one looked like the whole story.

The shape of a real fix is a dedicated, always-running IO runtime that owns catalog
and object-store futures and the clients they use, so that parking a session's
runtime never stalls the I/O those futures depend on. That is an architectural
change with its own blast radius (where clients are constructed, connection-pool
ownership, shutdown), so it wants its own spec rather than being bolted onto this.

### Bounded, 2026-08-04. Still not fixed, but no longer a hang.

The stall itself stands. What changed is that it now ENDS. `block_on_compat` drives
the future on its worker thread and waits for the result over a channel with
`recv_timeout` (60s) instead of `JoinHandle::join`, so the caller gets
`BridgeError::TimedOut` and the query fails with a message naming the cause.

The choice of guard is the whole point, and two others were ruled out here first.
`tokio::time::timeout` around the query does NOT bound it: the bridge blocks the
runtime thread synchronously, so the timer is never polled and the timeout never
fires. `JoinHandle::join` has no deadline at all. `Receiver::recv_timeout` is an
OS-level wait, so it fires regardless of what any runtime is or is not driving,
which is the same conclusion the `#195` guard reached from the other direction.

Cost, stated rather than hidden: on the deadline the worker thread and its runtime
are left detached, because nothing can safely cancel a future blocked in another
reactor. Bounded by the number of timeouts, and a timeout already means something
is wrong.

The bridge's return type changed from `Option` to `Result<_, BridgeError>` for
this. The third state existed only to mean "could not run" with no reason attached,
which is how a timed-out call would have reported "no tokio runtime available" on a
runtime that was there. Ten call sites now render the real cause.

A unit regression test is possible now, where before it could only hang: a future
that cannot complete must come back as `TimedOut`, driven from a scratch thread so
that a regression fails instead of stalling the suite
(`a_future_that_cannot_finish_fails_loudly_instead_of_hanging`). Mutation-checked by
restoring the unbounded wait, which makes it fail in 10s with its own message.

## Reachability: NOT the server. Corrected.

An earlier draft of this note called the wedge production-reachable and ranked it
above everything else queued. That was wrong, and the correction matters because it
was used to set priorities.

Both coordinator entry points build `new_multi_thread` runtimes
(`sqe-coordinator/src/main.rs`, `bin/sqe_server.rs`), and `sqe-cli` uses
`#[tokio::main]`, which is also multi-thread. The multi-thread branch of
`block_on_compat` uses `block_in_place` + `handle.block_on`, which neither
contends for a single core nor parks the reactor: other worker threads keep
driving it. So neither cause reaches a deployed SQE.

The only current-thread runtimes in the tree are in tests. Exposure is therefore:

- `#[tokio::test]`, whose default flavor is current-thread. This is what made the
  access-control e2e suite hang, twice, and it will bite any future test that
  reaches a sync catalog hook.
- any future embedding that chooses a current-thread runtime. The bridge exists
  precisely to support that (issue #83), so the trap is latent rather than absent.

Cause 1 is worth fixing on its own terms: it is a real defect in a bridge whose
whole purpose is to make current-thread work, and it was masked by a test that
called it from outside the runtime. Cause 2 keeps the current-thread branch unsafe
for I/O futures regardless, so the bridge should not be treated as
flavor-agnostic until a dedicated IO runtime exists.

Related prior art in this repo, and the same lesson: the `#195` guard on
`persistent_warehouse_survives_client_restart` (`sqe-cli/src/embedded.rs`) records
that `tokio::time::timeout` cannot bound a blocking wedge because the timer never
runs, and that the only workable guard is an OS-level one -- a sacrificial thread
with its own runtime, joined with a hard deadline. The unit test added here uses
that same pattern.


---

# `SHOW SCHEMAS` / `SHOW TABLES` catalog resolution, fixed 2026-08-03

Root cause was one comparison. `show_catalog` asked whether the named catalog
differed from `config.catalog.warehouse` -- the LEGACY single-catalog field -- and
used the session catalog when it matched. But the session resolves through
`resolve_default_catalog()`: `query.default_catalog` if set, otherwise the
alphabetically FIRST entry of `flattened_catalogs()` (which sorts, for
deterministic `information_schema` ordering).

With the quickstart's shape -- `[catalogs.ops_wh]`, `[catalogs.sales_wh]`, no
`query.default_catalog`, and `[catalog] warehouse = "sales_wh"` -- the session
default sorts to `ops_wh` while the legacy field says `sales_wh`. So
`SHOW SCHEMAS FROM sales_wh` matched the guard, fell through to the session
catalog, and listed **ops_wh's** namespaces. `SHOW SCHEMAS FROM ops_wh` returned
the same rows by the discovery route, so the two were indistinguishable and both
reported success.

The question is not "is this the legacy warehouse" but "is this the catalog the
session already resolves to". `show_catalog_target` now answers exactly that, as a
pure function, matching a name against both a catalog's config KEY and its
`warehouse` (a legacy deployment is keyed `iceberg` while operators name it by its
warehouse, so comparing keys alone would send `SHOW SCHEMAS FROM wh1` down the
by-name path and fail).

**A second, latent bug fixed alongside it.** The by-name path used
`discover_session_catalog`, which clones a TEMPLATE catalog's config and overrides
only `warehouse`. For a catalog that is actually declared in `[catalogs.*]` that
silently reads it with another catalog's `polaris_url`, auth, backend and cache
TTL. Invisible in the quickstart because both catalogs share a URL. Named catalogs
now resolve from their own config first (`configured_session_catalog`), with
discovery as the fallback for warehouses Polaris knows about and SQE's config does
not.

**Unknown catalogs now error instead of falling back.** Previously an
unresolvable name quietly produced the session catalog's answer. Since
`SHOW SCHEMAS` is how an operator confirms a grant landed, a confidently wrong
answer is worse than a refusal.

Verified live and mutation-checked. Restoring the old comparison makes
`SHOW SCHEMAS FROM sales_wh` return `["ops", "ac", "only_in_ops"]`, which is the
bug reproduced exactly. The e2e case discriminates on a namespace each catalog has
and the other does not, plus `SHOW TABLES FROM sales_wh.ac` vs `ops_wh.ac` (both
have an `ac` namespace holding different tables), and asserts the two lists are not
equal -- equal lists being the precise symptom.
