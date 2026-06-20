# System Overview

## Components

```mermaid
graph TB
    subgraph Clients
        JDBC["JDBC / ODBC<br/>(Flight SQL driver)"]
        CLI["sqe-cli"]
        DBT["dbt-sqe adapter"]
        DASH["Dashboards<br/>(Trino compat)"]
    end

    subgraph "SQE Cluster"
        subgraph "Coordinator (sqe-server --mode coordinator)"
            FLS["Flight SQL Server<br/>:50051"]
            TH["Trino HTTP<br/>:8080"]
            SM["Session Manager"]
            QH["Query Handler"]
            PE["Policy Enforcer"]
            SCHED["Scheduler"]
        end

        subgraph "Workers (sqe-server --mode worker)"
            W1["Worker 1<br/>DataFusion executor"]
            W2["Worker 2<br/>DataFusion executor"]
            WN["Worker N<br/>DataFusion executor"]
        end
    end

    subgraph "External Services"
        KC["OIDC provider<br/>(Keycloak / Auth0 / Entra)"]
        CAT["Catalog backend<br/>(Polaris / Nessie / Glue REST /<br/>S3 Tables / Unity / HMS / JDBC)"]
        S3["S3-compatible storage<br/>(AWS / Ceph / R2 / rustfs)"]
    end

    JDBC --> FLS
    CLI --> FLS
    DBT --> FLS
    DASH --> TH

    SM --> KC
    QH --> PE
    QH --> SCHED
    SCHED --> W1
    SCHED --> W2
    SCHED --> WN

    QH --> CAT
    W1 --> S3
    W2 --> S3
    WN --> S3
```

The catalog backend is selectable at runtime. Polaris is the primary target and the only one verified end-to-end for production write paths today. Nessie, AWS Glue, AWS S3 Tables, Unity Catalog OSS, Hive Metastore, JDBC (Postgres), and Hadoop storage-only are all reachable through the same `iceberg::Catalog` trait, with live integration tests in `crates/sqe-catalog/tests/backends_integration.rs`. AWS endpoints share the OSS Iceberg REST code path through the `aws-sigv4` cargo feature on the vendored `iceberg-catalog-rest` crate. See [features/iceberg.md](../features/iceberg.md) for the catalog-by-catalog state.

## Request Flow

A query flows through SQE in these stages:

```mermaid
sequenceDiagram
    participant C as Client
    participant F as Flight SQL Server
    participant SM as Session Manager
    participant QH as Query Handler
    participant PE as Policy Enforcer
    participant DF as DataFusion
    participant CAT as Polaris Catalog
    participant S3 as S3 Storage

    C->>F: do_handshake(user, pass)
    F->>SM: authenticate(user, pass)
    SM->>SM: Keycloak OIDC → session
    F-->>C: bearer token

    C->>F: execute(SQL, token)
    F->>SM: get_session(token)
    F->>QH: execute(session, SQL)

    QH->>QH: parse & classify SQL
    QH->>CAT: create SessionCatalog(user_token)
    QH->>DF: plan SQL → LogicalPlan
    QH->>PE: enforce(user, plan)
    PE-->>QH: secured plan (row filters, column masks)
    QH->>DF: optimize & execute
    DF->>S3: read Parquet files
    S3-->>DF: Arrow RecordBatches
    DF-->>QH: results
    QH-->>F: RecordBatches
    F-->>C: Arrow Flight stream
```

## Single-Node vs Distributed

SQE starts in **single-node mode** by default — the coordinator executes queries locally using DataFusion. No workers needed.

For larger deployments, enable workers:

```mermaid
graph LR
    subgraph "Single-Node (default)"
        C1[sqe-server] -->|local DataFusion| S1[S3]
    end

    subgraph "Distributed"
        C2[Coordinator] -->|plan fragments| W1[Worker 1]
        C2 --> W2[Worker 2]
        W1 --> S2[S3]
        W2 --> S2
    end
```

| Mode | When to use | Config |
|---|---|---|
| Single-node | Dev, small datasets, < 100GB | `sqe-server` (default) |
| Distributed | Production, large scans, parallel I/O | `worker.enabled=true` in Helm |

## Ports

| Port | Protocol | Purpose |
|---|---|---|
| 50051 | gRPC (Flight SQL) | Primary query interface |
| 50052 | gRPC (Flight) | Worker data exchange |
| 8080 | HTTP | Trino-compatible endpoint |
| 9090 | HTTP | Prometheus metrics |
| 9091 | HTTP | Health probes (`/healthz`, `/readyz`) |
