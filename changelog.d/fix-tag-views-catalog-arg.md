### Fixed

- `main` builds again after the information_schema tag views (#925) crossed two policy changes

    The tag-view visibility check now passes the table's catalog to `resolve_effective_policy` (#919), and the tag-view function takes the catalog resolver's registered name (!1891), which the views report as `catalog_name` and policy keys on.
