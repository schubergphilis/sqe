### Security

- Projected Ranger tags carry the catalog under `catalog_scoped` (#899)

    With `policy.ranger.catalog_scoped = true`, `SET TAG` / `UNSET TAG`,
    schema changes and tag reprojection now write the Ranger tag-store
    resource under the same key as the mask (`<catalog>.<namespace>`), so two
    same-named tables in different catalogs no longer share one resource and
    Spark no longer masks the wrong table. The unscoped resource left from
    before the setting is cleared on the next projection. Without
    `catalog_scoped`, a one-level namespace projects as before; a multi-level
    namespace now projects as the dotted path the mask key uses.

- `production_mode` refuses Ranger when tokens carry no groups (#899)

    New rule `ranger-groups-claim`: with `policy.engine = ranger`, every
    `oidc_password` / `bearer_token` provider must set `groups_claim`, and the
    legacy `[auth]` block (no `[[auth.providers]]`, never reads groups) is
    refused. The error names the provider. Opt out with
    `security.allow_tokens_without_groups = true`. The Helm chart renders
    `config.auth.providers` as `[[auth.providers]]`, `values-production.yaml`
    sets an `oidc_password` provider with `groups_claim: groups`, and the
    chart fails at template time on the same condition.
