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

**`SHOW SCHEMAS FROM <catalog>` can answer about a different catalog.** With
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
