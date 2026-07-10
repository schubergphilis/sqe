# Connect BI tools (Metabase, Superset, DBeaver)

SQE speaks the Trino wire protocol, so Trino-compatible BI tools connect with no SQE-specific driver. Metabase uses the Trino JDBC driver, Superset uses the Trino SQLAlchemy dialect, DBeaver has a built-in Trino connection, and the official Trino CLI works too. All four were verified live against a Polaris + Keycloak stack: schema sync, column browse, typed queries with date bucketing, and result pagination.

## The one rule that trips everyone up: TLS

The Trino JDBC driver and the Trino CLI refuse to send a username and password over a plain HTTP connection. They fail with `TLS/SSL is required for authentication with username and password`. SQE's Trino HTTP endpoint (`[coordinator] trino_http_port`, default 8080) serves plain HTTP, so you put a TLS-terminating reverse proxy in front of it and point the BI tool at the HTTPS URL.

The reference quickstart terminates TLS at nginx and proxies `/v1/` to the SQE container, so the tools connect to `https://<host>/v1/` rather than the raw `:8080`. Any ingress or load balancer that terminates TLS works the same way.

If you cannot terminate TLS, the alternative is bearer-token auth, which the Trino driver allows over plain HTTP: obtain an OIDC access token yourself and pass it as `Authorization: Bearer <token>`. Most BI tools drive username/password, so the TLS route is the practical one.

Basic auth carries the user; the password is the OIDC secret (for a local root client it may be empty). SQE exchanges the credentials for a token against the configured OIDC provider.

## Metabase

Add a database of type **Trino** (or **Starburst**), then:

- Host: your TLS host (for example `sqe.example.com`)
- Port: `443`
- Catalog: your catalog (for example `main_warehouse`)
- Username / Password: the OIDC user and secret
- Enable SSL. For a self-signed cert in a demo, allow the untrusted certificate.

Metabase runs a metadata handshake on connect (prepare a statement, list catalogs and schemas, list tables, describe columns) before it syncs. SQE matches Trino's response shape at each step, so the sync populates tables and columns and date-bucketed questions (month, quarter, year) work.

## Superset

Add a database with a SQLAlchemy URI using the `trino` dialect over HTTPS:

```
trino://<user>:<password>@<host>:443/<catalog>
```

Set the connection to use HTTPS (the Superset Trino dialect defaults to the protocol in the URI host settings). Superset reflects tables through `SHOW COLUMNS` and `information_schema`, both of which resolve against the session catalog.

## DBeaver

Create a new **Trino** connection:

- Host and Port: your TLS host and `443`
- Enable SSL in the connection's SSL tab
- Authentication: username and password (the OIDC user and secret)

The schema browser walks catalogs, then schemas, then tables, and column metadata renders from `DESCRIBE`.

## Trino CLI

```bash
export TRINO_PASSWORD='your-secret'
trino --server https://<host> --user <user> --password \
      --catalog main_warehouse --schema <schema> \
      --execute "SHOW TABLES"
```

Add `--insecure` for a self-signed certificate. Pointing at plain `http://<host>:8080` with `--password` fails the TLS check described above.

## What works

Verified over the wire protocol these tools share:

- `SHOW CATALOGS`, `SHOW SCHEMAS`, `SHOW TABLES` return Trino's exact single-column shapes.
- `DESCRIBE` and `SHOW COLUMNS` resolve double-quoted identifiers (`"catalog"."schema"."table"`).
- Types map to their Trino equivalents: `timestamp(6)` carries its precision, computed aggregates like `count(*)` are `bigint`.
- Large results paginate: the client follows `nextUri` through 1000-row pages until the query reaches `FINISHED`.

## Catalog context

Set the catalog in the connection (or send the `X-Trino-Catalog` header). SQE resolves unqualified names against the session catalog on both the `SELECT` and `SHOW` paths, so a tool that syncs against one catalog sees the same tables its query editor does. See [Trino Compatibility](../features/trino-compatibility.md) for the endpoint reference and [Connecting clients](../getting-started/connecting.md) for the protocol choice.
