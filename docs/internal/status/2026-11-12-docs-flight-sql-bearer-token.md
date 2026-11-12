**STATUS 2026-11-12: Flight SQL bearer tokens documented (`docs/flight-sql-bearer-token`).**

A copy-job probe reported "a Polaris bearer token is refused" on Flight SQL. No code is missing. SQE validates a per-request JWT through the provider chain (`session_for_bearer`), so a `bearer_token` provider with the IdP's JWKS accepts personal tokens and keeps per-user identity. A Polaris `client_credentials` token has no JWKS and needs `bearer_passthrough`, which maps every caller to one configured user. `connecting.md` now shows the ADBC `authorization_header` option and both cases. `configuration.md` lists `bearer_passthrough` in the provider table. The ingest-in-transaction note points at arrow-adbc issue 5027 (fix in PR 5028, draft).

**NEXT:** when arrow-adbc 5028 is released, drop the ingest workaround sentence from `connecting.md`.
