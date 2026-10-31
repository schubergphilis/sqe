### Added

- Embedded Iceberg views on an OAuth2 attach

    `sqe-cli --embedded` refused `CREATE VIEW`, view reads and `SHOW CREATE
    VIEW` on a catalog attached with an oauth2 `SECRET`, because the view
    endpoints are called directly and only the catalog client held the
    exchanged token. They now carry that token, and after a 401 the expired
    token is dropped, a new one exchanged and the request sent once more.

### Fixed

- REST catalogs attached with different OAuth2 credentials no longer share
  cached state

    With no session bearer, every OAuth2 catalog on the same URL and
    warehouse had the same fingerprint, so a second attach with another
    credential could reuse the first one's cached catalog, table entries and
    circuit breaker. The fingerprint now covers the credential, its token
    endpoint, scope, audience and resource.
