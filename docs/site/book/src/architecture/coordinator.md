# Coordinator

The coordinator is the brain of SQE. It handles SQL parsing, query planning, security enforcement, and result delivery. In single-node mode, it also executes queries directly.

## Responsibilities

```mermaid
graph TB
    subgraph Coordinator
        FLS["Flight SQL Server"] --> SM["Session Manager"]
        FLS --> QH["Query Handler"]

        QH --> PARSE["SQL Parser<br/>(sqlparser-rs)"]
        QH --> CLASS["Statement Classifier"]
        QH --> PLAN["Query Planner<br/>(DataFusion)"]
        QH --> PE["Policy Enforcer"]
        QH --> CAT["Catalog Ops<br/>(DDL)"]
        QH --> WH["Write Handler<br/>(CTAS, INSERT,<br/>DELETE, UPDATE,<br/>MERGE — CoW)"]

        SM --> AUTH["Authenticator<br/>(Keycloak OIDC)"]
        SM --> TC["Token Cache"]

        PLAN --> DF["DataFusion<br/>SessionContext"]
        DF --> SC["SessionCatalog<br/>(per-user token)"]
    end
```

## Statement Routing

The coordinator classifies every SQL statement and routes it to the appropriate handler:

| Statement | Handler | Description |
|---|---|---|
| `SELECT` | `execute_query` | Plan → policy enforce → execute → stream results |
| `SHOW CATALOGS` | `handle_show_catalogs` | Returns warehouse name |
| `SHOW SCHEMAS` | `handle_show_schemas` | Lists namespaces from Polaris |
| `SHOW TABLES` | `handle_show_tables` | Lists tables in namespace(s) |
| `CREATE TABLE AS SELECT` | `handle_ctas` | Execute SELECT → write Parquet → commit to Iceberg |
| `INSERT INTO` | `handle_insert` | Execute SELECT → append Parquet → commit |
| `CREATE VIEW` | `handle_create_view` | Plan SELECT for schema validation → store in catalog |
| `DROP TABLE` | `catalog_ops.drop_table` | Forward to Polaris REST |
| `CREATE SCHEMA` | `catalog_ops.create_schema` | Create namespace in Polaris |
| `DROP SCHEMA` | `catalog_ops.drop_schema` | Drop namespace from Polaris |
| `EXPLAIN` | `handle_explain` | Show query plan |
| `DELETE FROM` | `handle_delete` | CoW: scan affected files, filter, rewrite via `rewrite_files()` |
| `UPDATE` | `handle_update` | CoW: scan affected files, apply SET, rewrite via `rewrite_files()` |
| `MERGE INTO` | `handle_merge` | CoW: full outer join, classify rows, rewrite via `rewrite_files()` |
| `GRANT/REVOKE` | Policy (Phase 5) | Not yet implemented |

## Session Context

Each query gets a fresh DataFusion `SessionContext` with the user's catalog:

```mermaid
sequenceDiagram
    participant QH as Query Handler
    participant SC as SessionCatalog
    participant POL as Polaris
    participant DF as DataFusion

    QH->>SC: new(catalog_url, warehouse, user_token)
    SC->>POL: list_namespaces() [user_token]
    POL-->>SC: [ns1, ns2, ns3]
    QH->>DF: register_catalog(warehouse, CatalogProvider)
    QH->>DF: sql("SELECT * FROM ns1.table1")
    DF->>SC: get_table("ns1", "table1")
    SC->>POL: load_table("ns1.table1") [user_token]
    POL-->>SC: table metadata + S3 location
    SC-->>DF: TableProvider (Iceberg scan)
```

This means two users running the same query may see different tables, schemas, or data — depending on what Polaris grants them.
