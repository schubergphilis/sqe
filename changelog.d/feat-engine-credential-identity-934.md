### Added

- Engine credential identity for catalogs (#934)

    A catalog with `credentials` (client_credentials) reads storage with keys
    the engine fetches from the catalog's `/credentials` endpoint after the
    policy engine allowed `select` for the person. Denied tables scan as empty
    with no keys fetched. Every fetch is audited with the person as actor.
    Writes keep the person's delegated credentials. Under Ranger, a catalog
    with the identity denies `select` by default until a policy grants it, so
    add `select` policies before turning it on. Startup requires
    `require_vended_credentials`, `policy.engine = "ranger"` and
    `[policy.ranger] catalog_scoped = true`. Off by default.
