# BI driver probes

Two probes that drive SQE through the real client drivers and print one line
per check (`OK`, `FAIL`, `VAL`, `NOTE`). A failure never stops the rest, so one
run lists every gap. They test the driver surface (metadata calls, typed
reads, prepared statements, auth), not SQL semantics; for SQL results against
a Trino baseline, see `tests/parity/`.

| Probe | Driver | Protocol |
|---|---|---|
| `jdbc/Matrix.java` | Trino JDBC (the jar DBeaver ships) | Trino HTTP(S) |
| `jdbc/Matrix.java` | Arrow Flight SQL JDBC | Flight SQL |
| `odbc/` | Arrow Flight SQL ODBC (built from the Arrow source) | Flight SQL |

Both create `<schema>.t` and `<schema>.v` in the session catalog first.

## Server

Any SQE coordinator with a password provider and a bearer provider, for
example the integration stack's Polaris (`make test-integration` brings it up
on `localhost:18181`) and a coordinator config with:

```toml
[[auth.providers]]
type = "client_credentials_passthrough"
token_url = "http://localhost:18181/api/catalog/v1/oauth/tokens"
scope = "PRINCIPAL_ROLE:ALL"

[[auth.providers]]
type = "bearer_passthrough"
user = "root"
```

The Trino JDBC driver sends a password only over TLS: set
`[coordinator.trino_tls]` (a self-signed pair is fine; the driver takes
`SSLVerification=NONE`) and connect to `trino_https_port`.

A token for the token runs:

```bash
TOK=$(curl -s -X POST http://localhost:18181/api/catalog/v1/oauth/tokens \
  -d grant_type=client_credentials -d client_id=root -d client_secret=s3cr3t \
  -d scope=PRINCIPAL_ROLE:ALL | python3 -c 'import sys,json;print(json.load(sys.stdin)["access_token"])')
```

## JDBC

Java 17+ runs the single source file directly. Download the drivers from Maven
Central: `io.trino:trino-jdbc` and `org.apache.arrow:flight-sql-jdbc-driver`.

```bash
J="java --add-opens=java.base/java.nio=ALL-UNNAMED"
# Trino JDBC: password, then token
$J -cp trino-jdbc-<v>.jar testing/drivers/jdbc/Matrix.java \
  "jdbc:trino://127.0.0.1:18443/iceberg" jdbc_matrix SSL=true SSLVerification=NONE user=root password=s3cr3t
$J -cp trino-jdbc-<v>.jar testing/drivers/jdbc/Matrix.java \
  "jdbc:trino://127.0.0.1:18443/iceberg" jdbc_matrix SSL=true SSLVerification=NONE accessToken=$TOK
# Flight SQL JDBC: password, then token
$J -cp flight-sql-jdbc-driver-<v>.jar testing/drivers/jdbc/Matrix.java \
  "jdbc:arrow-flight-sql://127.0.0.1:60051" jdbc_matrix user=root password=s3cr3t useEncryption=false
$J -cp flight-sql-jdbc-driver-<v>.jar testing/drivers/jdbc/Matrix.java \
  "jdbc:arrow-flight-sql://127.0.0.1:60051" jdbc_matrix token=$TOK useEncryption=false
```

## ODBC

The Arrow project publishes no Linux ODBC binary; the Dockerfile builds the
driver from the `apache-arrow-26.0.1` tag (about 20 minutes) and installs it
with pyodbc.

```bash
docker build -t sqe-odbc-probe:26.0.1 testing/drivers/odbc
docker run --rm sqe-odbc-probe:26.0.1 python3 /probe/odbc_probe.py \
  host.docker.internal 60051 --user root --password s3cr3t
docker run --rm sqe-odbc-probe:26.0.1 python3 /probe/odbc_probe.py \
  host.docker.internal 60051 --token "$TOK"
```

`PROBE_EXTRA_CONN` appends to the connection string (for example
`x-sqe-flight-compression=lz4`; the driver forwards unknown keys as headers).

## Last results (2026-11-17)

Identical for password and token.

| Driver | Passed | What still fails |
|---|---|---|
| Trino JDBC 480 | 65 of 74 | in the driver: exported keys, cross reference, index info, best row identifier, version columns, table and column privileges (`SQLFeatureNotSupportedException`); `getFunctions`, `getFunctionColumns` (not implemented) |
| Flight SQL JDBC 19.0.0 | 71 of 72 | `setSchema` (the driver ignores it; set `catalog=` on the URL) |
| Flight SQL ODBC 26.0.1 | 105 of 114 | in the driver (IM001): primary and foreign keys, statistics, special columns, procedures, `SQLNumParams` |
