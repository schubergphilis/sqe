### Added

- The Flight SQL handshake accepts a bearer token

    A client configured with a token, such as the Arrow Flight SQL JDBC
    driver's `token` property, handshakes with `Authorization: Bearer
    <token>`. The handshake used to accept only Basic and refused the
    bearer, so those clients could not connect. It now resolves the bearer
    as every other call does (an SQE session id, or an IdP token through
    the provider chain) and replies with the session id. An unknown token
    fails with `UNAUTHENTICATED`; other schemes stay refused.
