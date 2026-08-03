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
