## Why

A Hive external table is read with the engine's own storage credentials, like the file TVFs. Its data location is gated by the same list: `HiveManifestStore` runs `TvfPolicy::check_path` with the session's `TvfCaller` on the table location and on every partition location, on create, `ADD PARTITION` and every load (`sqe-catalog/src/hive/store.rs`). So a Hive table only works when its location is an allowed TVF prefix, and then every caller who can query the table can also `read_parquet` its files raw. That read skips the row filters, column masks and column restrictions the policy layer applies to the table. The Iceberg side of this gap is `tvf-prefix-table-overlap`; that change put Hive out of scope and filed #886.

## What Changes

- **A separate list for Hive data.** `[hive] allowed_data_prefixes` (global) and `data_prefixes` per `[[hive.roots]]` entry say where Hive table data may live. The Hive gate checks table and partition locations against this list instead of the TVF list, with the same caller, placeholders and canonicalisation. Who can read a Hive table does not change; only the list does.
- **File TVFs refuse Hive data.** A file TVF path under a Hive data prefix, or under a Hive table location SQE has loaded, is refused, admin roles and `allow_all_object_stores` included. Inline-credential reads stay exempt (the caller's own keys). This is the same refusal point as the Iceberg overlap guard (`check_object_store`, before allow-all and the allow-list), fed by one registry of protected locations.
- **Startup check.** An allowed TVF prefix that overlaps a Hive data prefix is logged as an error naming both; reads under the overlap are refused at run time.
- **Migration.** When neither Hive list is set, the Hive gate falls back to the TVF list for one release and logs a deprecation warning at startup. The TVF refusal of loaded Hive table locations applies either way, so the fallback no longer opens the bypass for a table that has been queried.
- No change to how Hive tables are read, written or cached, or to Glue roots' credentials.

## Capabilities

### New Capabilities
- `hive-data-prefixes`: where Hive table data may live, and the guard that keeps file TVFs out of it.

### Modified Capabilities
- `tvf-prefix-table-overlap` (in flight): its location registry and TVF refusal take Hive locations as well as Iceberg ones.

## Impact

- `sqe-core/src/config/mod.rs`: `HiveConfig` and `HiveRootConfig` gain the prefix lists; a `HiveDataPolicy` (or a second prefix set on `TvfPolicy`) answers the Hive gate; the protected-location registry is shared.
- `sqe-catalog/src/hive/store.rs`: the five `check_path` calls move to the Hive policy; loads record the table location.
- `sqe-catalog` file TVF path: one refusal check, shared with the Iceberg guard.
- Deployments that list Hive locations as TVF prefixes see raw TVF reads of those locations refused. That is intended: those reads bypass policy today.
- Docs: `features/hive-external-tables.md` (or the Hive page), `deployment/configuration.md`, `features/read-parquet.md`.
