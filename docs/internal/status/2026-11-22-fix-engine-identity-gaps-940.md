**STATUS 2026-11-22: engine credential identity closed on discovered warehouses and warehouse aliases (`fix/engine-identity-gaps-940`, #940, openspec engine-credential-identity 2.1-2.2).**

`discovery_template` now drops `credentials`, so a warehouse found by `polaris-auto` reads with the person's delegated keys and never with the engine's. The Ranger default-deny on `select` now covers each engine catalog's `warehouse` alias as well as its config key (`SqeConfig::engine_read_catalog_names`). Before, both paths could hand out rows read with the engine's keys without a `select` grant.

Limit: extending the engine identity to discovered warehouses is a spec change for the owner and is not built.

**NEXT:** archive openspec trino-table-properties after !1925 merges.
