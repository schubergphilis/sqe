# Polaris + Keycloak (client credentials)

## Goal

Run SQE against an Apache Polaris catalog where Keycloak owns the user identities. A client connects to SQE with a username and password; SQE exchanges those credentials for that user's bearer token via its own confidential OIDC client (`sqe-client`), then passes the token through to Polaris. Polaris enforces what each user can see. Every query runs as the authenticated user — no service account, no shared credential.

Use this quickstart when an OIDC provider already manages your users and you want SQE to mint tokens on their behalf (JDBC tools, the CLI, dbt). If your clients already hold a token and need SQE to accept it directly, see the `polaris-keycloak-user-token` quickstart instead.

## Components

| Service | Image | Role |
|---|---|---|
| `keycloak` | `quay.io/keycloak/keycloak:26.5.4` | Identity provider. Issues bearer tokens via the OIDC password grant. |
| `keycloak-config` | `adorsys/keycloak-config-cli` | One-shot: imports the `iceberg` realm (one confidential client, three users), then exits. |
| `rustfs` | `rustfs/rustfs` | S3-compatible object store. The Iceberg warehouse lives here. |
| `bucket-init` | `amazon/aws-cli` | One-shot: creates the `warehouse` bucket (RustFS does not auto-create), then exits. |
| `polaris` | `apache/polaris:1.6.0` | Iceberg REST catalog, federated to Keycloak. Validates the tokens SQE forwards. |
| `polaris-setup` | `curlimages/curl` | One-shot: creates the catalog, RBAC roles, OIDC principals, and the `demo` namespace, then exits. |
| `sqe` | built from this repo | The query engine. Flight SQL on 50051, Trino-compat HTTP on 8080. |

## Configuration

### Backend (sqe.toml)

```toml
[[auth.providers]]
type = "oidc_password"
token_url = "http://keycloak:8080/realms/iceberg/protocol/openid-connect/token"
client_id = "sqe-client"
client_secret = "sqe-secret-change-me"   # must match Keycloak's sqe-client secret
roles_claim = "realm_access.roles"        # where SQE reads the user's roles

[catalogs.quickstart]
polaris_url = "http://polaris:8181/api/catalog"
warehouse = "quickstart"

[storage]
s3_endpoint = "http://rustfs:9000"
s3_access_key = "s3admin"
s3_secret_key = "s3adminpw"
s3_path_style = true
s3_allow_http = true
```

`type = "oidc_password"` selects the Resource Owner Password Credentials (ROPC) grant. SQE posts the user's `username` + `password` plus its own `client_id` + `client_secret` to `token_url` and gets back the user's bearer token, which is forwarded to Polaris. The TOML key `quickstart` becomes the SQL catalog name, so tables are addressed as `quickstart.<namespace>.<table>`.

### SQL (queries.sql)

```sql
SHOW SCHEMAS;

DROP TABLE IF EXISTS quickstart.demo.events;
CREATE TABLE quickstart.demo.events (
    id     BIGINT,
    kind   VARCHAR,
    amount DOUBLE
);

INSERT INTO quickstart.demo.events VALUES
    (1, 'click',    1.50),
    (2, 'purchase', 42.00),
    (3, 'click',    0.75),
    (4, 'purchase', 13.25);

SELECT kind, COUNT(*) AS n, ROUND(SUM(amount), 2) AS total
FROM quickstart.demo.events
GROUP BY kind
ORDER BY total DESC;
```

## The test

`run.sh` brings the full stack up with `docker compose up --wait`, then runs `queries.sql` twice via `sqe-cli` over Flight SQL — once as `adminuser` (catalog_admin + data_writer + table_reader) to exercise the complete create/write/read path, and once as `testuser` (table_reader only) to confirm that a lower-privileged user can read the table written by adminuser. Success is asserted by `--stop-on-error`; the output is captured to `OUTPUT.md`.

An optional `--with-tests` flag additionally runs the `test_keycloak_auth_with_test_users` and `test_keycloak_token_refresh` tests in the `sqe-coordinator` integration suite against the live stack (requires a Rust toolchain). Both tests passed on last validation (2026-06-06, 2 passed; 0 failed). Tear down with `./run.sh --down`.

## Output

```
## adminuser (catalog_admin + data_writer + table_reader)

sqe-cli 0.31.4 connected to http://localhost:50051 (flight)
+-------------+
| schema_name |
+-------------+
| demo        |
+-------------+
+----------+---+-------+
| kind     | n | total |
+----------+---+-------+
| purchase | 2 | 55.25 |
| click    | 2 | 2.25  |
+----------+---+-------+

## testuser (table_reader only): read is allowed

sqe-cli 0.31.4 connected to http://localhost:50051 (flight)
+----------+---+
| kind     | n |
+----------+---+
| click    | 2 |
| purchase | 2 |
+----------+---+
```
