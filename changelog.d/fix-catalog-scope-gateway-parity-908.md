### Fixed

- Catalog scope claims are checked as the run gateway checks them (#908)

    A scope's `iss` must be one string equal to `[session.catalog_scope] issuer`; an array that contains the issuer, which the JWT library accepted, is refused. A trailing `/` in the configured `issuer` is ignored, and `token_type = "JWT"` refuses startup, since it names no scope format. Scopes without `iat`, `nbf`, `aud` or a `typ` header are read as before.
