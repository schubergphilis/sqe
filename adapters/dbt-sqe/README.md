# dbt-sqe

A dbt adapter for SQE (Sovereign Query Engine) over ADBC Flight SQL.

## Install

```bash
pip install dbt-sqe   # or: pip install -e adapters/dbt-sqe
```

## Authentication

The adapter supports three auth styles in `profiles.yml`. See
`dbt/include/sqe/sample_profiles.yml` for full examples.

### Basic (human user)

```yaml
type: sqe
host: localhost
port: 50051
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
token: "{{ env_var('SQE_TOKEN') }}"
```

Precedence when several are set: `token` > OAuth (`method: oauth` or `client_id`)
> Basic (`user`). `password`, `client_secret`, and `token` are never printed by
`dbt debug`.

## Connection options

| Key | Default | Notes |
|---|---|---|
| `host` | `localhost` | SQE coordinator host |
| `port` | `50051` | Flight SQL port |
| `catalog` (alias `database`) | `warehouse` | target catalog |
| `schema` | `default` | target schema |
| `threads` | 1 | dbt parallelism |

## See also

- Service-principal quickstart: `quickstart/polaris-ranger-service-principal/`
- Server auth providers: `crates/sqe-auth/`
