# dbt-sqe

A dbt adapter for SQE (Sovereign Query Engine) over ADBC Flight SQL.

## Install

```bash
pip install dbt-sqe   # or: pip install -e adapters/dbt-sqe
```

`setup.cfg` carries the tested ranges; `requirements-lock.txt` pins the
exact, hash-verified set. For a reproducible install use the lock first:

```bash
pip install --require-hashes -r requirements-lock.txt && pip install --no-deps -e adapters/dbt-sqe
```

Refresh the lock after bumping a range (run from `adapters/dbt-sqe`):

```bash
uv pip compile --python-version 3.11 --generate-hashes -o requirements-lock.txt setup.cfg
```

## Authentication

The adapter supports three auth styles in `profiles.yml`. See
`dbt/include/sqe/sample_profiles.yml` for full examples.

### Basic (human user)

```yaml
type: sqe
host: localhost
port: 50051
tls: false  # local plaintext stack; drop for a TLS coordinator
user: alice
password: "{{ env_var('SQE_PASSWORD') }}"
```

### OAuth service principal (recommended for CI / scheduled runs)

The client presents its own OAuth2 `client_id` / `client_secret`. SQE runs the
`client_credentials` grant per connection and forwards the resulting token to
the catalog. No browser, no token fetch in the adapter. Requires the server to
run the `client_credentials_passthrough` auth provider.

```yaml
type: sqe
host: localhost
port: 50051
tls: false  # local plaintext stack; drop for a TLS coordinator
method: oauth
client_id: "{{ env_var('SQE_CLIENT_ID') }}"
client_secret: "{{ env_var('SQE_CLIENT_SECRET') }}"
```

Under the hood `client_id`/`client_secret` are sent as the Flight SQL Basic-auth
username/password; the SQE server performs the grant.

### Pre-obtained bearer token

The client (or CI) fetched its own OAuth token out of band and passes it
through. SQE validates it (the `bearer_token` provider) and forwards it. Useful
for short-lived CI tokens.

```yaml
type: sqe
host: localhost
port: 50051
tls: false  # local plaintext stack; drop for a TLS coordinator
token: "{{ env_var('SQE_TOKEN') }}"
```

Precedence when several are set: `token` > OAuth (`method: oauth` or `client_id`)
> Basic (`user`). `password`, `client_secret`, `token` and `call_headers` values
are never printed by `dbt debug`.

## Connection options

| Key | Default | Notes |
|---|---|---|
| `host` | `localhost` | SQE coordinator host |
| `port` | `50051` | Flight SQL port |
| `tls` | `true` | connect with `grpc+tls://`; set `false` only for a local plaintext coordinator |
| `tls_root_certs` | unset | path to a PEM CA bundle to trust (private CA); requires `tls: true` |
| `catalog` (alias `database`) | `warehouse` | target catalog; every relation and `information_schema` read names it, so it need not be the server's default catalog |
| `schema` | `default` | target schema |
| `threads` | 1 | dbt parallelism |
| `call_headers` | unset | extra headers sent on every Flight SQL call; see below |

### Extra call headers

`call_headers` maps header names to values. dbt-sqe sends them on every Flight
SQL call next to its credentials. An orchestrator uses them to pass per-run
context to SQE, for example a signed catalog scope
(`[session.catalog_scope] header_name`):

```yaml
type: sqe
host: sqe.example.org
token: "{{ env_var('SQE_TOKEN') }}"
call_headers:
  x-portal-catalog-scope: "{{ env_var('DBT_ENV_SECRET_CATALOG_SCOPE', '') }}"
```

- Names are lower-cased. `authorization`, `content-type`, `host`, `te`,
  `user-agent`, `grpc-*` and binary (`*-bin`) headers are refused, so a profile
  cannot replace the credentials.
- A value must be printable ASCII. An empty value sends no header.
- Values are never printed by `dbt debug` or in dbt-sqe's errors, which name the
  header only. A value that is a credential should come from a
  `DBT_ENV_SECRET_*` variable, which dbt also scrubs from its own logs.

## Materializations and model configs

- `table`, `view`, `incremental` (`append`, `delete+insert`, `merge`; `unique_key`
  may be a list), `seed` and `snapshot` (`timestamp` and `check` strategies).
- `table_properties` (alias `tblproperties`): Iceberg table properties, set on the
  table's CREATE OR REPLACE. An incremental model takes new values on `--full-refresh`.

```sql
{{ config(materialized='table',
          table_properties={'write.merge.mode': 'merge-on-read'}) }}
```

## See also

- Service-principal quickstart: `quickstart/polaris-ranger-service-principal/`
- Server auth providers: `crates/sqe-auth/`
