# Trino HTTP compatibility

SQE speaks enough of the Trino HTTP protocol that Trino clients, JDBC drivers,
and BI tools can point at it unchanged. The coordinator exposes the
`/v1/statement` endpoint with `nextUri` pagination, `/v1/info`, and
`/v1/info/state`. This is a compatibility surface, not a re-implementation of
Trino; it covers the query-submission path that clients actually use.

## Single server

### Prerequisites

```bash
docker compose -f docker-compose.test.yml up -d
./scripts/bootstrap-test.sh
```

### Configuration

The Trino HTTP listener runs alongside Flight SQL. The default port is `8080`
(the test/distributed compose files map it to `28080`):

```toml
[coordinator]
trino_http_port = 8080    # 0 to disable
```

### Run

```bash
# Submit a query. Basic auth carries the user; the password is the OIDC secret
# (empty for the local root client).
curl -s -u root: \
  -H "X-Trino-User: root" \
  -d "SELECT 1 AS one" \
  http://localhost:28080/v1/statement
```

Trino clients follow the `nextUri` field until results are exhausted. A JDBC
client connects with the Trino driver against `http://localhost:28080`.

### Expected output

The first response carries a `nextUri`; following it returns the data:

```json
{
  "columns": [{"name": "one", "type": "bigint"}],
  "data": [[1]],
  "stats": {"state": "FINISHED"}
}
```

### How it is tested

- `crates/sqe-coordinator/tests/integration_test.rs::test_trino_http_query`:
  server startup, Basic auth, `/v1/statement` POST, pagination.
- `test_trino_type_mapping`, `test_trino_batches_to_json`: Arrow to Trino JSON.
- `scripts/trino-parity-test.sh` and `scripts/trino-compat-test.sh`: run the
  same SQL against SQE and a real Trino and diff the results.

## Distributed

The Trino HTTP endpoint lives on the coordinator. Distribution across workers
is identical to the Flight path: the coordinator plans, workers execute. The
client sees a single Trino-compatible endpoint regardless of worker count.

### Prerequisites

```bash
docker compose -f docker-compose.test.yml -f docker-compose.distributed.yml up --build -d
./scripts/bootstrap-test.sh
```

### Run

```bash
scripts/test.sh scenario distributed
```

The scenario exercises the Trino HTTP endpoint on `28080` alongside the
Flight path and confirms worker dispatch.

### Expected output

The distributed scenario's Trino check submits a query to the HTTP endpoint on the
coordinator (`28080`) and follows `nextUri` to completion, alongside the Flight
path on the same cluster. This is covered by the suite; the docker-dependent
re-run was not repeated this round (see the validation matrix note on local
Docker capacity).

## Trino SQL parity

SQE adds a Trino-compatibility function layer (date/time helpers like `year()`,
`month()`, `day_of_week()`, JSON casts, and more) so dbt models and Trino SQL
run with fewer rewrites. The current parity surface is tracked in
[Trino Compatibility](./trino-compatibility.md). The parity scripts
above are the regression guard.

## Notes

- Authentication needs Basic auth (`-u user:password`) to populate the session,
  not just the `X-Trino-User` header. For the local root client the password is
  empty (`-u root:`).
- The Trino layer is optional; enable it with `[trino_compat] enabled = true`.
  Flight SQL is the recommended protocol for SQE-native clients.
