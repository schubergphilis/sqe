### Added

- Engine reads for discovered warehouses (`[query] discovery_engine_read`, off by default)

    With `"template"`, a warehouse found by `catalog_discovery = "polaris-auto"`
    reads with the template catalog's engine `credentials` once SQE has proved
    it: the name is on `discovery_engine_warehouses` (exact names and `*`
    patterns), it is not a configured catalog, warehouse alias, Hive root,
    `system` or `datafusion`, and the person's and the engine's
    `GET /v1/config?warehouse=` calls at the template URL both succeed with the
    same `overrides.prefix`. Any refusal keeps the person's delegated keys.
    Each proof writes a `discovery_engine_proof` audit event, and key fetches
    on a proved warehouse are marked `discovered_engine_key_fetch`. Startup
    refuses the setting without polaris-auto, template `client_credentials`
    with `require_vended_credentials`, Ranger with `catalog_scoped`, and a
    non-empty allowlist. Ranger then denies `select` by default on every
    catalog except configured ones without engine credentials, Hive roots from
    the config file, `datafusion`, `information_schema` and the reserved
    `system` schemas. Writes, maintenance and DDL keep the person's delegated
    keys in both modes.
