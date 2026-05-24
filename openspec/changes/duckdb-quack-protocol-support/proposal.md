## Why

DuckDB is the default analytics tool for a large engineering audience. The new `quack:` protocol (https://duckdb.org/quack/) turns DuckDB into a client/server engine: any DuckDB instance can connect to a Quack endpoint with `ATTACH 'quack:host'` and query the remote database as if it were local. Two distinct opportunities for SQE:

- **Reach**: DuckDB users have muscle memory in the DuckDB CLI, dbt-duckdb adapter, `marimo`, `Evidence`, and SQL clients that already know how to attach a Quack URI. Exposing SQE as a Quack server gives them policy-enforced Iceberg access without changing tooling.
- **Execution diversity**: DuckDB's Iceberg reader, columnar engine, and vectorised hash joins outperform DataFusion on a few workload shapes (small-data interactive, list/struct-heavy payloads, certain semi-join patterns). Routing plan fragments to a DuckDB worker through Quack gives SQE a second execution backend without re-implementing those features.

Both modes use the same protocol, so the work splits cleanly into a server crate and a client crate. The server addresses reach, the client addresses execution diversity. They share a wire codec and authentication path.

The cost: Quack is brand-new. The protocol surface is small but unstable. We accept churn risk in exchange for being first.

## What Changes

Two new crates plus dialect work in `sqe-sql`:

**Server side (Option A) -- `sqe-quack-server`:**
- Quack RPC listener: TCP socket, length-prefixed message framing, version handshake
- Token authentication: map Quack `TOKEN '...'` to OIDC bearer in `sqe-auth`
- Session adapter: translate `ATTACH`, catalog/schema introspection, prepared statements, query execution to existing `sqe-coordinator` session APIs
- DuckDB SQL dialect parsing in `sqe-sql` (sqlparser-rs `DuckDbDialect`), with translation rules for DuckDB-flavoured AST nodes to DataFusion-compatible LogicalPlan

**Client side (Option B) -- `sqe-quack-client` + `sqe-worker-duckdb`:**
- Quack RPC client (mirror of the server's codec)
- New worker variant `sqe-worker-duckdb` that runs DuckDB embedded behind a thin process boundary, accepts plan fragments over the existing worker control plane, executes via DuckDB, returns Arrow record batches
- Policy enforcement at SQL text level (DuckDB owns its optimizer, so `sqe-policy` cannot rewrite `LogicalPlan` post-translation): row filters injected as WHERE predicates, masked columns rewritten as projections with `CASE WHEN` masks, before SQL leaves the coordinator
- Catalog: DuckDB uses iceberg-extension; we point it at the same REST catalog SQE already uses, with the same bearer token

**Shared:**
- `sqe-quack-wire`: protocol codec (framing, message types, serde) reused by server and client
- DuckDB dialect support in `sqe-sql`: new feature flag `duckdb-dialect`

## Capabilities

### New Capabilities
- `quack-server-protocol`: SQE accepts incoming Quack RPC connections from DuckDB clients
- `quack-server-auth`: Quack `TOKEN '...'` maps to OIDC bearer for `sqe-auth`
- `quack-server-dialect`: DuckDB-dialect SQL parses and translates to SQE LogicalPlan where semantically equivalent
- `duckdb-execution-backend`: SQE coordinator routes plan fragments to DuckDB workers over Quack
- `duckdb-worker-runtime`: embedded DuckDB process running plan fragments as worker
- `policy-sql-text-rewrite`: row filters and column masks applied at SQL text level for non-DataFusion backends

### Modified Capabilities
- `sql-parsing`: now multi-dialect (DataFusion-native, DuckDB, Trino-compat); dialect resolved per session at parse time
- `worker-runtime`: now pluggable (DataFusion or DuckDB), selected per query or per session
- `policy-enforcement`: now has two enforcement points (LogicalPlan rewrite for DataFusion workers, SQL text rewrite for DuckDB workers)

## Impact

- New crate `sqe-quack-wire` (shared codec, no_std-clean)
- New crate `sqe-quack-server` (depends on `sqe-quack-wire`, `sqe-coordinator`, `sqe-auth`, `sqe-sql`)
- New crate `sqe-quack-client` (depends on `sqe-quack-wire`)
- New crate `sqe-worker-duckdb` (depends on `sqe-quack-client`, embeds DuckDB via `duckdb-rs`)
- `sqe-sql`: DuckDB dialect parser behind feature flag `duckdb-dialect`; AST-to-LogicalPlan translation layer
- `sqe-policy`: new `SqlTextRewriter` alongside the existing `PlanRewriter` for backends that own their optimizer
- `sqe-coordinator`: worker selection becomes pluggable (`WorkerKind::Datafusion | WorkerKind::Duckdb`)
- New optional cargo features: `quack-server`, `quack-client`, `worker-duckdb`, `duckdb-dialect`
- Default feature set unchanged (Flight SQL + DataFusion worker)
- Dependency additions: `duckdb-rs` (embedded DuckDB, ~25 MB built artifact, behind feature flag)

## Rollback

All new functionality lives behind cargo features. The default build is unchanged. Removing `--features quack-server,worker-duckdb` removes the new code paths entirely. The existing Flight SQL + DataFusion worker stack is untouched.

If the Quack protocol breaks compatibility, the server crate can pin to a specific Quack protocol version and refuse newer clients with a clear error. The DuckDB worker can be disabled at the coordinator level via config (`worker.kinds = ["datafusion"]`) without removing the crate.
