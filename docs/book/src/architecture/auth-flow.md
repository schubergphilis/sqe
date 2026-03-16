# Authentication Flow

SQE uses Keycloak OIDC with the **Resource Owner Password Credentials (ROPC)** grant for initial authentication, then manages token lifecycle transparently.

## Why ROPC?

Flight SQL's handshake sends username and password directly. There's no browser redirect flow possible over gRPC. ROPC is the standard mechanism for non-interactive clients (JDBC drivers, CLI tools, dbt adapters).

## Complete Flow

```mermaid
sequenceDiagram
    participant Client
    participant SQE as SQE Coordinator
    participant KC as Keycloak
    participant POL as Polaris
    participant S3

    Note over Client,KC: Authentication
    Client->>SQE: Flight Handshake<br/>Basic auth (user:pass)
    SQE->>KC: POST /token<br/>grant_type=password<br/>username, password, client_id
    KC-->>SQE: access_token, refresh_token, expires_in
    SQE->>SQE: Create Session<br/>(id, user, roles, tokens)
    SQE-->>Client: Bearer token (session_id)

    Note over Client,S3: Query Execution
    Client->>SQE: execute(SQL)<br/>Authorization: Bearer session_id
    SQE->>SQE: Lookup session → get access_token
    SQE->>POL: GET /namespaces<br/>Authorization: Bearer access_token
    POL-->>SQE: [namespace list]
    SQE->>POL: POST /tables/load<br/>Authorization: Bearer access_token
    POL-->>SQE: table metadata + S3 credentials
    Note over POL: Polaris vends S3<br/>credentials scoped to<br/>this user + table
    SQE->>S3: GetObject (vended credentials)
    S3-->>SQE: Parquet data
    SQE-->>Client: Arrow Flight stream

    Note over SQE,KC: Background Token Refresh
    SQE->>KC: POST /token<br/>grant_type=refresh_token
    KC-->>SQE: new access_token, new refresh_token
    SQE->>SQE: Update session tokens
```

## Token Refresh

A background task runs every 10 seconds, scanning all active sessions:

```rust
// Pseudocode
loop {
    sleep(10 seconds);
    for session in sessions_expiring_within(60 seconds) {
        match keycloak.refresh_token(session.refresh_token) {
            Ok(new_tokens) => session.update(new_tokens),
            Err(_) => session.mark_expired(),
        }
    }
}
```

The 60-second buffer ensures tokens are refreshed well before expiry, avoiding mid-query auth failures.

## Token Fingerprinting

When a token is refreshed, the iceberg-rust catalog client's internal HTTP session cache still holds the old token. SQE uses a **token fingerprint** (last 8 characters of the access token) as part of the catalog session key. When the fingerprint changes, a new catalog session is created with the fresh token.

```mermaid
graph LR
    T1["Token: ...abc12345<br/>fingerprint: abc12345"] -->|refresh| T2["Token: ...xyz98765<br/>fingerprint: xyz98765"]
    T1 --> CS1["CatalogSession 1"]
    T2 --> CS2["CatalogSession 2<br/>(new, fresh token)"]
```

## Role Extraction

SQE extracts user roles from the JWT `realm_access.roles` claim. These roles are stored in the session and used for policy evaluation:

```json
{
  "realm_access": {
    "roles": ["data-analyst", "finance-reader", "admin"]
  }
}
```

Roles flow through to the Policy Enforcer, which uses them to determine row filters and column masks for each query.
