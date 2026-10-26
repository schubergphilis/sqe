### Added

- `CREATE SECRET (TYPE oauth2, ...)` for Iceberg REST catalogs whose tokens expire

    A bearer secret holds one token, and an `ATTACH`ed REST catalog stops
    answering once that token expires (a Databricks Unity Catalog token
    lasts an hour). An oauth2 secret holds a client credential instead:
    `CLIENT_SECRET` (required), plus `CLIENT_ID`, `TOKEN_URL` and `SCOPE`.
    The REST client runs the OAuth2 client-credentials grant at
    `TOKEN_URL` (default `<location>/v1/oauth/tokens`), sends the returned
    token as a bearer, and fetches a new one when the catalog answers 401.
    `SHOW SECRETS` lists the kind as `oauth2`; the client secret is
    redacted in debug output and zeroized on drop.
