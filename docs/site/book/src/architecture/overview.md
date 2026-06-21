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

The coordinator currently runs as a single replica. It is a single point of failure: a restart drops in-flight queries and session state, which is process-local. Workers are stateless and scale horizontally. Coordinator high availability is on the roadmap. See [Kubernetes & Helm](../deployment/kubernetes.md) for the deployment topology.

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

SQE starts in **single-node mode** by default. The coordinator executes queries locally using DataFusion. No workers needed.

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

## Caching

SQE caches at five layers, each falling through to the next on a miss. The first two layers (session and catalog) live in memory and are short-lived; the last three (table metadata, manifest, footer) hold immutable or near-immutable Iceberg data.

```mermaid
graph TD
    SC["Layer 1: SessionContext cache<br/>moka, SHA-256 token fingerprint<br/>TTL 5 min, max 100 entries"]
    RC["Layer 2: RestCatalog cache<br/>moka, per-warehouse<br/>TTL 5 min"]
    TMC["Layer 3: Table metadata cache<br/>moka, ETag validation<br/>TTL configurable (default 30s)"]
    MC["Layer 4: Manifest file cache<br/>moka, content-addressed<br/>no TTL, default 512 MB"]
    FC["Layer 5: Parquet footer cache<br/>object_store, byte-range<br/>default 256 MB"]

    SC -->|miss| RC
    RC -->|miss| TMC
    TMC -->|miss| MC
    MC -->|miss| FC
    FC -->|miss| S3["S3 storage"]

    DDL["DDL (CREATE / DROP / ALTER)"] -.->|invalidates| SC
```

Invalidation follows the Iceberg data model:

- The session cache is invalidated after any DDL statement (CREATE TABLE, DROP TABLE, ALTER TABLE).
- Table metadata uses ETag-based conditional requests. Polaris returns `304 Not Modified` when metadata has not changed, so a cache hit costs one cheap round-trip rather than a full metadata fetch.
- Manifest files are immutable by Iceberg specification, so they need no TTL-based expiry. They evict only under memory pressure.
- The footer cache evicts on an LRU basis within its configured memory budget.
