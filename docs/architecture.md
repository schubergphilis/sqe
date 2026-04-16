# SQE Architecture

This document provides visual architecture diagrams for the Sovereign Query Engine (SQE). All diagrams use Mermaid syntax for rendering in GitLab, GitHub, or any Markdown viewer with Mermaid support.

## 1. High-Level Architecture

```mermaid
graph TB
    subgraph Clients
        JDBC[JDBC / DBeaver]
        FlightClient[Flight SQL / ADBC]
        HTTP[Trino HTTP / dbt]
    end

    subgraph SQE Coordinator
        FlightSQL[Flight SQL Server<br/>gRPC :50051]
        TrinoHTTP[Trino HTTP Server<br/>:8080]
        Parser[SQL Parser]
        Optimizer[DataFusion Optimizer]
        Scheduler[Fragment Scheduler]
    end

    subgraph Workers
        W1[Worker 1<br/>DataFusion Executor]
        W2[Worker 2<br/>DataFusion Executor]
        W3[Worker N<br/>DataFusion Executor]
    end

    subgraph External Services
        OIDC[OIDC Provider<br/>Keycloak / Auth0 / Okta]
        Policy[OPA / Cedar<br/>Policy Engine]
        Polaris[Apache Polaris<br/>REST Catalog]
        S3[S3-Compatible Storage<br/>AWS S3 / Ceph / RustFS]
    end

    subgraph Observability
        Prom[Prometheus / VictoriaMetrics<br/>:9090]
        OTel[OpenTelemetry Collector<br/>OTLP/gRPC :4317]
    end

    JDBC --> FlightSQL
    FlightClient --> FlightSQL
    HTTP --> TrinoHTTP

    FlightSQL --> Parser
    TrinoHTTP --> Parser
    Parser --> Optimizer
    Optimizer --> Scheduler

    Scheduler --> W1
    Scheduler --> W2
    Scheduler --> W3

    FlightSQL -.->|authenticate| OIDC
    TrinoHTTP -.->|authenticate| OIDC
    Optimizer -.->|enforce policy| Policy

    W1 -->|bearer token passthrough| Polaris
    W2 -->|bearer token passthrough| Polaris
    W3 -->|bearer token passthrough| Polaris
    Polaris --> S3

    FlightSQL -.-> Prom
    W1 -.-> Prom
    FlightSQL -.-> OTel
```

## 2. Query Execution Pipeline

```mermaid
graph LR
    SQL["SQL String"] --> Parse["Parser<br/>(sqlparser-rs)"]
    Parse --> Classify["Statement<br/>Classifier"]
    Classify --> LP["Logical Plan"]
    LP --> PolicyRewrite["Policy Plan Rewrite<br/>(row filters,<br/>column masks)"]
    PolicyRewrite --> Optimize["DataFusion<br/>Optimizer"]
    Optimize --> PP["Physical Plan"]
    PP --> AdaptiveSort["Adaptive Sort<br/>Stripping"]
    AdaptiveSort --> DynFilter["Dynamic Filter<br/>Injection"]
    DynFilter --> Exec["Execution"]
    Exec --> Arrow["Arrow Batches<br/>(LZ4 compressed)"]

    style PolicyRewrite fill:#f9e2ae,stroke:#e8a838
    style AdaptiveSort fill:#d4edda,stroke:#28a745
    style DynFilter fill:#d4edda,stroke:#28a745
```

**Key stages:**

- **Policy Plan Rewrite** -- Security filters and column masks are injected into the logical plan *before* DataFusion optimization. This ensures that user predicates can be pushed through row filters but cannot bypass masked columns.
- **Adaptive Sort Stripping** -- Under memory pressure, non-partition sort requirements are stripped to prevent OOM. Falls back to partition-only ordering.
- **Dynamic Filters** -- Hash join build-side min/max ranges are pushed down into Iceberg scan operators at execution time, pruning files and row groups.

## 3. Crate Dependency Graph

```mermaid
graph TD
    coordinator[sqe-coordinator]
    worker[sqe-worker]
    cli[sqe-cli]
    bench[sqe-bench]
    catalog[sqe-catalog]
    auth[sqe-auth]
    sql[sqe-sql]
    policy[sqe-policy]
    planner[sqe-planner]
    metrics[sqe-metrics]
    trino[sqe-trino-compat]
    core[sqe-core]

    coordinator --> catalog
    coordinator --> auth
    coordinator --> sql
    coordinator --> policy
    coordinator --> planner
    coordinator --> metrics
    coordinator --> trino
    coordinator --> worker
    coordinator --> core

    worker --> catalog
    worker --> planner
    worker --> metrics
    worker --> core

    cli --> core

    bench -.->|Flight SQL client| coordinator

    catalog --> policy
    catalog --> metrics
    catalog --> core

    auth --> core
    sql --> core
    policy --> core
    planner --> core
    trino --> core
    trino --> auth

    style core fill:#e8e8e8,stroke:#666
    style coordinator fill:#cce5ff,stroke:#004085
    style worker fill:#cce5ff,stroke:#004085
```

All crates depend on `sqe-core` for shared types, configuration, and error definitions. The coordinator is the heaviest crate, pulling in nearly everything. Workers are lighter -- they only need catalog access, plan execution, and metrics.

## 4. Caching Architecture

```mermaid
graph TD
    subgraph "Layer 1: Session"
        SC["SessionContext Cache<br/>moka, SHA-256 token fingerprint<br/>TTL: 5 min, max: 100 entries"]
    end

    subgraph "Layer 2: Catalog"
        RC["RestCatalog Cache<br/>moka, per-warehouse<br/>TTL: 5 min"]
    end

    subgraph "Layer 3: Table Metadata"
        TMC["Table Metadata Cache<br/>moka, ETag validation<br/>TTL: configurable (default 30s)"]
    end

    subgraph "Layer 4: Manifest"
        MC["Manifest File Cache<br/>moka, content-addressed<br/>No TTL (immutable by Iceberg spec)<br/>Default: 512 MB"]
    end

    subgraph "Layer 5: Footer"
        FC["Parquet Footer Cache<br/>object_store, byte-range<br/>Default: 256 MB"]
    end

    SC -->|miss| RC
    RC -->|miss| TMC
    TMC -->|miss| MC
    MC -->|miss| FC
    FC -->|miss| S3["S3 Storage"]

    DDL["DDL (CREATE/DROP/ALTER)"] -.->|invalidates| SC

    style SC fill:#d4edda,stroke:#28a745
    style RC fill:#d4edda,stroke:#28a745
    style TMC fill:#fff3cd,stroke:#856404
    style MC fill:#fff3cd,stroke:#856404
    style FC fill:#f8d7da,stroke:#721c24
```

**Invalidation rules:**

- Session cache is invalidated after any DDL statement (CREATE TABLE, DROP TABLE, ALTER TABLE, etc.)
- Table metadata uses ETag-based conditional requests -- Polaris returns 304 Not Modified when metadata has not changed
- Manifest files are immutable by Iceberg specification, so no TTL-based expiry is needed
- Footer cache evicts on LRU basis within the configured memory budget

## 5. Distributed Execution

```mermaid
sequenceDiagram
    participant Client
    participant Coordinator
    participant W1 as Worker 1
    participant W2 as Worker 2
    participant S3 as S3 Storage

    Note over W1,W2: Workers send heartbeats every 5s

    Client->>Coordinator: SQL query (Flight SQL / HTTP)
    Coordinator->>Coordinator: Parse + optimize + policy rewrite

    alt Small scan (< 128 MB or < 4 files)
        Coordinator->>S3: Execute locally
        S3-->>Coordinator: Arrow batches
    else Large scan (distributed)
        Coordinator->>Coordinator: Split plan into scan tasks<br/>(bin-pack to ~256 MB each)
        par Dispatch scan tasks
            Coordinator->>W1: DoGet(scan task + bearer token)
            Coordinator->>W2: DoGet(scan task + bearer token)
        end
        par Execute scans
            W1->>S3: Read Parquet files
            W2->>S3: Read Parquet files
        end
        par Stream results
            W1-->>Coordinator: Arrow batches (ZSTD)
            W2-->>Coordinator: Arrow batches (ZSTD)
        end
    end

    alt Distributed join/aggregate
        Coordinator->>Coordinator: Partition data by hash key
        par DoExchange shuffle
            W1->>W2: Repartitioned batches
            W2->>W1: Repartitioned batches
        end
        W1-->>Coordinator: Partial results
        W2-->>Coordinator: Partial results
    end

    Coordinator->>Coordinator: Final aggregation / merge
    Coordinator-->>Client: Arrow batches (LZ4)
```

**Distribution decision:**

- Queries scanning less than 128 MB total or fewer than 4 data files execute locally on the coordinator
- Larger scans are bin-packed into ~256 MB tasks and dispatched to workers
- Workers are stateless -- they receive the physical plan fragment and the user's bearer token, execute against S3, and stream results back
- Shuffle (for distributed joins and aggregates) uses Arrow Flight `DoExchange` with ZSTD compression
- Client-facing responses use LZ4 compression (faster decompression)
