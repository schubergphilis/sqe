**STATUS 2026-11-16: catalog in the projected tag key; `production_mode` refuses Ranger without groups (`feat/tag-key-catalog-groups-claim-899`, #899).**

Tag key (`sqe-policy/src/tag_projector.rs`, `sqe-write/src/catalog_ops.rs`):
- `TagTableKey::for_table` builds the tag-store `database` through `resolve_policy_key_for_ident_scoped`, the resolver the mask key uses, so the two cannot drift. Under `catalog_scoped` it is `<catalog>.<namespace>`; scoped with no catalog is an error.
- `project_table_tags` projects under that key. Under `catalog_scoped` it also clears the unscoped resource (previous ∪ new columns, no tags) left from before the setting was switched on. The cleanup is best effort and only logs on failure.
- All three call sites (schema change, SET/UNSET TAG, reprojection) resolve the catalog the planner would: the statement's qualifier, then the session's default catalog, then the configured default.
- `RangerTagProjector::project` skips the import when there are no tags to add, so the cleanup call writes no empty import.
- Behaviour change, unscoped: a multi-level namespace now projects as `wh.sales` (the mask key form). Before, it was the `\x1F`-joined form, which matched no mask.

`groups_claim` rule (`sqe-core/src/config/production.rs`):
- `ranger-groups-claim` refuses `policy.engine = ranger` when an `oidc_password` / `bearer_token` provider has an empty `groups_claim`, or when `auth.providers` is empty (the legacy `[auth]` path always yields no groups). The error names `auth.providers[i] (<type>)`.
- The opt-out is `security.allow_tokens_without_groups`. The rule has a row in `production-guide.md`, and the field is in `sqe.toml.example` and the Helm values.
- Helm renders `config.auth.providers` as `[[auth.providers]]`, and drops `keycloak_url`/`realm` when providers are set. The chart refuses the same condition at template time, and `values-production.yaml` ships an `oidc_password` provider with `groups_claim: groups`.

Gate:
- `cargo test -p sqe-policy -p sqe-write -p sqe-core --lib -- tag production`: 40 + 44 + 36 passed. New tests: `the_database_key_is_the_mask_key`, `scoped_projection_clears_the_unscoped_resource`, `reproject_loaded_tags_projects_and_reports_error` (scoped case), `validate_production_rejects_ranger_without_groups_claim`, `validate_production_accepts_ranger_groups_claim_or_opt_out`.
- Clippy on the three crates with `-D warnings`, `cargo check-server`, `cargo xtask budgets --check` and the rustfmt ratchet all exit 0.
- `helm template` with the CI production args renders TOML that parses with the provider. Removing the providers, or emptying `groups_claim`, fails templating with the named provider. The opt-out renders. `helm lint` passes.
- `make test-access-control` (the unscoped Ranger suite): exit 0, 55 passed, 0 failed. Its tag tests project `database=ac` (single-level namespace, unscoped), as before.

Left open on #899:
- No end-to-end test under `catalog_scoped`. The access-control stack runs unscoped, and the Spark read of `b.sales.orders` needs the portal's Spark to send the scoped key (S10, out of scope).
- Hive: only `CatalogOps` (Iceberg tables) projects tags. A Hive table's tags live in its sidecar and are not projected to Ranger at all, so there is no Hive tag key to align with the `{root}.{namespace}` mask key yet. The resolver is called with an empty Hive catalog set for that reason.

**NEXT:** #903 (`feat/tag-refresh-on-load-903`).
