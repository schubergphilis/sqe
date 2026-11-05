**STATUS 2026-11-03: a view body plans like a statement (`fix/view-planner-parity`, ai-portal-data#357, #359).**

Two bugs reported from ai-portal-data, one cause: the view body planned in a
context that was not the statement's.

- **#357, default namespace.** `SqeSchemaProvider::view_planning_context` used
  schema `default` for every view, and `rest_catalog::sqe_view_version`
  wrote the view's own namespace as `default-namespace`. Now:
  - `catalog_ops::create_view` passes the creating session's schema
    (`session.default_schema`, else `default`) to `create_view` /
    `create_or_replace_view`, which record it;
  - `RestCatalog::load_view_body` returns the SQL plus that schema
    (`ViewBody`), and both read paths (`schema_provider.rs` and
    `writable_iceberg_catalog.rs`) plan the body in it;
  - `qualify_view_scans` gets the same schema, so the policy rewriter
    qualifies the scans the body really makes. Using two different schemas
    here would govern the wrong table.
- **#359, function set.** The view context was a bare `SessionContext`. It
  is now `engine_context(engine_session_config())` plus
  `session_tuning::register_engine_functions`, the one list the coordinator
  session and the Hive view path now also call (Trino, extended Trino, JSON,
  variant, geo). `sqe-catalog` gains `sqe-geo` and
  `datafusion-functions-json` as dependencies for that.

No read-side fallback. A view SQE wrote before this fix recorded its own
namespace as `default-namespace`, so its unqualified names now resolve there
instead of in `default`. A view in `default`, or one whose body qualifies its
tables, is unaffected; a view in `marts` whose body says `orders` and meant
`default.orders` must be recreated or qualified. Views written by Spark or
Trino record the right schema and are now read correctly.

Verified 2026-11-03: `sqe-catalog` lib 752 passed (new
`view_default_schema_is_the_current_versions_default_namespace`); new IT
module `view_planner_parity_e2e` (two tests).

**NEXT:** ai-portal-data#358, one unloadable view breaks
`information_schema` and `DESCRIBE` for the whole schema; branch
`fix/information-schema-broken-view`, listing paths only (a direct `SELECT`
from the broken view must still fail). Follow-ups from this branch, not
done: session UDFs such as `current_user()` do not resolve in a view body,
the version's `default-catalog` is not honoured, and Iceberg view bodies skip
the front-door normalization the Hive view path applies.
