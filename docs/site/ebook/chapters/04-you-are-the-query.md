# You Are the Query {#sec:auth}

> There is no service account. There is only you.

The security team's question -- "who accessed the customer table last Tuesday?" -- wasn't hard because we lacked logging. It was hard because every query ran as the same identity. Trino's service account read every file, for every user, every time. CloudTrail showed one actor doing everything. The audit trail was technically complete and practically useless.

So the first constraint for SQE wasn't performance. It wasn't SQL compatibility. It was this: when Alice runs a query, S3 must see Alice. Not "sqe-service-account". Not "trino-coordinator". Alice.

That sounds simple. It turned out to be the hardest design decision in the entire engine -- and the one that made everything else possible.

The previous chapter showed how DataFusion gives you a query engine as a library -- a `SessionContext` per user, a `CatalogProvider` per catalog, a `TableProvider` per table. But all of that machinery is useless if the credentials flowing through it belong to the engine instead of the user. The `SessionContext` is only as sovereign as the identity it carries. This chapter is about making sure that identity is real.


## The Three Paths We Considered

### Path 1: Service account with user tagging

The standard approach. The engine holds a service account with broad read access. It logs which user initiated each query. Auditing works through application logs, not CloudTrail.

We tried this first. It took about an hour to realise the problem: the engine becomes the security boundary. If the engine is compromised, every table is readable. If the engine has a bug in its access control logic, data leaks silently. The audit trail says "Alice queried table X" but S3 says "sqe-service read table X, Y, Z, and everything else."

Every major query engine works this way. Trino, Spark, Presto, Starburst -- they all hold a service account, and they all enforce access control in the application layer. The reasoning is pragmatic: it's simpler, it's faster (you can cache data across users), and it's how databases have always worked.

The problem is that it conflates two things that should be separate: who can run a query and who can access the data. The engine decides both. If the engine is wrong about either one, the failure is silent. No alarm fires when a service account reads a file it shouldn't, because the service account can read everything.

::: {.deadend}
**Dead end: service account with tagging.** It's the path of least resistance, and every major
query engine uses it. We built a prototype in an afternoon. The security team rejected it
in a meeting that lasted twelve minutes.
:::

### Path 2: Per-user IAM roles assumed by the engine

Better. The engine receives Alice's identity, then calls STS AssumeRole to get temporary credentials scoped to Alice's permissions. S3 sees Alice's role, not the engine.

This works on paper. In practice, it requires pre-provisioning IAM roles for every user, maintaining a mapping between OIDC identities and AWS roles, and handling the assume-role chain when Polaris also needs to assume on behalf of the user. The credential chain gets deep. The failure modes multiply.

We explored this for about two days. The AI generated working STS assume-role code in twenty minutes. The remaining time was spent on the IAM policy matrix -- each user needs a role with trust relationships to both the engine's execution role and the Polaris service role, scoped S3 policies per namespace, and lifecycle management for onboarding, permission changes, and role deletion. The code was fine. The operational model was not. And it was AWS-specific; on GCP or Azure, the entire mechanism is different.

There's also a subtler problem: STS tokens have a maximum duration of 12 hours. For a 30-second query, fine. For a long-running ETL job, you might need to re-assume the role mid-execution. And if the assume-role call fails because the IAM role was modified while the query was running, the query fails in a way that has nothing to do with the data or the SQL.

### Path 3: Bearer token passthrough

The user authenticates via OIDC. The coordinator receives a JWT. That JWT is forwarded -- unchanged -- to every downstream system: Polaris for catalog operations, S3 for storage access (via Polaris credential vending), workers for distributed execution.

The engine never holds ambient credentials. It never assumes a role. It passes the user's proof of identity through, and every downstream system makes its own access decision based on that identity.

This is the model we built.

The elegance is in what the engine doesn't do. It doesn't evaluate permissions. It doesn't maintain role mappings. It doesn't cache credentials across users. It simply passes the token through and lets each downstream system decide. Polaris decides whether Alice can see the table. S3 decides whether Alice can read the files. The engine is a conduit, not a gatekeeper.

The trade-off is operational. The engine depends on the OIDC provider being available for every handshake. If Keycloak is down, no new sessions can be created. Existing sessions continue to work (the JWT is already in memory), but the engine cannot authenticate new users. This is a real dependency. We mitigate it with the token cache and refresh mechanism, but the hard truth is: if your identity provider is down, your query engine is partially down. In a service account model, the engine can continue operating with its cached credentials. In a passthrough model, it can serve existing sessions but not new ones. We decided that this was an acceptable trade-off because if your identity provider is down, you have bigger problems than query latency.


## Why OIDC Password Grant

OIDC has several grant types. The one nobody uses for query engines is the resource owner password credentials grant -- the user sends username and password directly, and gets back a JWT.

It's considered legacy. It's being deprecated in OAuth 2.1. And it's the only grant type that works cleanly for non-interactive SQL clients.

Here's the problem: a JDBC client connecting to a query engine doesn't have a browser. There's no redirect flow. There's no authorization code exchange. The user types a connection string with a username and password, and the engine needs to turn that into a bearer token.

`client_credentials` is wrong -- that authenticates the *application*, not the *user*. The whole point is that every query runs as a specific human.

So we use the password grant for interactive clients (JDBC, DBeaver, CLI) and accept bearer tokens directly for programmatic clients that handle their own OIDC flow. The coordinator's `do_handshake` method handles both paths.

The `Authenticator` struct in `sqe-auth` abstracts this choice behind a runtime backend selection:

```rust
enum AuthBackend {
    /// OIDC Password Grant (ROPC) -- exchanges username/password for a token
    /// via any OIDC-compliant provider (Keycloak, Auth0, Okta, etc.).
    OidcPassword(OidcPasswordClient),
    /// Generic OAuth2 client_credentials -- obtains a service token from any
    /// OAuth2-compliant endpoint (e.g. Polaris). Username/password are ignored.
    ClientCredentials(OAuthClient),
}

pub struct Authenticator {
    backend: AuthBackend,
    cache: TokenCache,
    refresh_buffer_secs: u64,
}
```

The backend selection happens at startup based on configuration. If `keycloak_url` is set, the engine uses OIDC password grant. If only `token_endpoint` is set, it uses client credentials. The rest of the engine doesn't know or care which backend is active -- it calls `authenticator.authenticate(username, password)` and gets back a `Session`.

This matters because different deployments need different auth models. A production cluster behind Keycloak uses the password grant. A local development stack running against Polaris directly uses client credentials. The integration test suite uses client credentials against a Polaris in-memory catalog. Same engine, same code path, different auth backend.

::: {.datafusion}
**DataFusion deep dive:** The `FlightSqlService::do_handshake` method receives a `HandshakeRequest`
stream. SQE extracts the Basic auth header, calls Keycloak's token endpoint, and returns the
JWT as the session token. Every subsequent Flight call carries this token in the `authorization`
metadata header.
:::


## The Handshake: From Password to Session

The Flight SQL protocol defines a handshake mechanism for authentication. The client sends a `HandshakeRequest` with Basic auth credentials. The server authenticates them and returns a bearer token that the client uses for every subsequent call.

In SQE, the `do_handshake` implementation does the OIDC exchange:

```rust
async fn do_handshake(
    &self,
    request: Request<Streaming<HandshakeRequest>>,
) -> Result<Response<Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>>,
    Status,
> {
    let authorization = request.metadata()
        .get("authorization")
        .ok_or_else(|| Status::invalid_argument("Authorization header not present"))?
        .to_str()
        .map_err(|e| Status::internal(format!("Authorization header not parsable: {e}")))?
        .to_string();

    // Decode Basic auth: base64(username:password)
    let base64_encoded = &authorization["Basic ".len()..];
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(base64_encoded)
        .map_err(|e| Status::invalid_argument(format!("Invalid base64 in auth: {e}")))?;
    let decoded_str = std::str::from_utf8(&decoded)
        .map_err(|e| Status::invalid_argument(format!("Invalid UTF-8 in auth: {e}")))?;

    let parts: Vec<&str> = decoded_str.splitn(2, ':').collect();
    let (username, password) = match parts.as_slice() {
        [user, pass] => (*user, *pass),
        _ => return Err(Status::invalid_argument(
            "Invalid authorization: expected username:password",
        )),
    };

    // OIDC exchange: username/password -> JWT
    let session = self.session_manager
        .authenticate(username, password)
        .await
        .map_err(|e| Status::unauthenticated(format!("Authentication failed: {e}")))?;

    // Return session ID as bearer token
    let result = HandshakeResponse {
        protocol_version: 0,
        payload: session.id.as_bytes().to_vec().into(),
    };

    let mut response = Response::new(Box::pin(futures::stream::iter(vec![Ok(result)])));
    response.metadata_mut().append(
        "authorization",
        MetadataValue::from_str(&format!("Bearer {}", session.id))?,
    );

    Ok(response)
}
```

The handshake extracts the Basic auth header, base64-decodes it to get the username and password, and calls `session_manager.authenticate()`. Under the hood, the `SessionManager` delegates to the `Authenticator`, which POSTs a `grant_type=password` form to the OIDC provider's token endpoint (`{keycloak_url}/realms/{realm}/protocol/openid-connect/token`). The provider validates the credentials and returns a JWT (the access token), a refresh token, and an expiry time. The access token is the user's proof of identity for every downstream call.

The `Authenticator` also extracts roles from the JWT payload -- a lightweight base64 decode of the claims, no signature verification, because the OIDC provider already validated the token. The roles feed into policy enforcement (Chapter 8) where they determine what the user can see and do.

There's a second authentication path. For programmatic clients that already have a JWT -- a backend BFF that obtained a token through the authorization code flow, or a service that got one from its own OIDC exchange -- the coordinator accepts raw bearer tokens directly. If the token in the `authorization` header isn't a known session ID but looks like a JWT (contains dots), the coordinator wraps it into an ad-hoc session and proceeds. Same code path, same token passthrough, no handshake required.

This dual-path design means SQE works with both interactive users (DBeaver, CLI) and programmatic callers (dbt, Airflow, custom services) without either side having to adapt.

::: {.antipattern}
**Antipattern: validating JWTs in the engine.** It's tempting to validate JWT signatures in
the coordinator -- check the issuer, verify the audience, validate the expiry. We considered
it. The problem: the coordinator is not the resource server. Polaris is. S3 is. If the
coordinator rejects a token that Polaris would accept, or accepts a token that Polaris would
reject, you've created a gap. Let the downstream systems validate their own tokens. The
coordinator's job is to pass the token through and handle the error if the downstream
rejects it. Don't build a second authorization layer that can disagree with the first.
:::


## The Token Passthrough Architecture

![Authentication flow: OIDC password grant through coordinator, Polaris credential vending, and bearer token passthrough to workers and S3](diagrams/rendered/04-auth-flow.svg)

Once the coordinator has a JWT, the flow is:

1. **Catalog**: coordinator sends the JWT to Polaris in the `Authorization` header. Polaris validates it and returns table metadata plus temporary S3 credentials scoped to that user's permissions.

2. **Storage**: the S3 credentials from Polaris are used directly. No separate STS call. No role assumption. Polaris did the credential vending.

3. **Workers** (distributed mode): the coordinator sends the JWT along with the plan fragment. The worker uses it to get its own S3 credentials from Polaris. The worker never contacts the coordinator for credentials -- it goes directly to Polaris with the user's token.

4. **Token refresh**: for long-running queries, the coordinator monitors token expiry and pushes refreshed tokens to workers via a background task.

Every I/O operation in the entire system traces back to a specific human identity. CloudTrail shows Alice reading the Parquet files that Alice's query needed. Not the engine. Not a service account. Alice.


## Credential Vending: What Polaris Actually Does

The credential vending flow is the mechanism that makes passthrough possible at the storage layer. Understanding it explains why we didn't need STS AssumeRole.

When SQE loads a table from Polaris, the REST catalog response includes more than just metadata. Polaris returns the table's current metadata location, the schema, the partition spec -- and a set of temporary S3 credentials scoped to the files that table contains. These are short-lived STS tokens that Polaris obtained by assuming a storage role on behalf of the authenticated user.

The important part: Polaris does the IAM role assumption, not SQE. Polaris has the trust relationship with the storage role. Polaris evaluates the user's JWT, determines what the user is allowed to access, and returns credentials that are scoped to exactly that access. SQE receives the credentials and passes them to iceberg-rust's `FileIO` layer, which uses them to read Parquet files from S3.

This is why Path 2 (per-user IAM roles) was unnecessary. Polaris already solves the credential vending problem. It already maps OIDC identities to storage permissions. It already returns short-lived credentials. We didn't need to build any of that. We just needed to pass the user's JWT to Polaris and use what came back.

The credential lifetime is controlled by Polaris configuration, typically 15 minutes to 1 hour. For most queries, this is more than enough. For long-running scans, the credentials might expire before the scan completes. That's the problem the credential refresh mechanism solves.

| Component | What it does | What it knows about the user |
|-----------|-------------|------------------------------|
| Coordinator | Routes queries, manages sessions | JWT, username, roles |
| Polaris | Validates JWT, vends S3 credentials | Full OIDC identity, storage permissions |
| S3 | Accepts STS credentials, serves files | The assumed role (traced to the user) |
| Workers | Execute scan fragments | JWT (forwarded), S3 credentials (vended per fragment) |

Each component makes its own access decision. The coordinator doesn't decide what Alice can read from S3. Polaris does. The coordinator doesn't decide whether Alice's credentials are valid. S3 does. The engine is a coordinator of decisions, not the decision maker.

::: {.iceberg}
**Iceberg deep dive:** The Iceberg REST catalog specification (the `loadTable` endpoint) supports
a `config` field in the response that can include `s3.access-key-id`, `s3.secret-access-key`,
and `s3.session-token`. This is the credential vending mechanism. Polaris implements it by
assuming a storage role scoped to the table's S3 prefix. Different catalogs implement this
differently -- Gravitino has its own credential provider SPI, Unity Catalog returns
credentials via a separate endpoint. SQE relies on the Iceberg REST spec's approach because
it's the most portable. If your catalog returns credentials in the table config, SQE uses
them. If it doesn't, SQE falls back to the ambient credentials in its storage configuration.
The fallback exists for development (local MinIO, RustFS) where credential vending isn't needed.
:::


## Session Lifecycle

Each authenticated query creates a `Session`:

```rust
pub struct Session {
    pub id: String,
    pub user: SessionUser,
    pub access_token: String,
    pub refresh_token: Option<String>,
    pub token_expiry: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub last_activity: DateTime<Utc>,
    pub default_catalog: Option<String>,
    pub default_schema: Option<String>,
    pub source: Option<String>,
}
```

The session holds the JWT (`access_token`), the refresh token for obtaining new JWTs when the current one expires, the token expiry time, and the user's identity. The `id` field is a UUID that serves as the bearer token for subsequent Flight SQL calls. The `Debug` implementation redacts the access and refresh tokens -- credentials never appear in logs.

Sessions have two timeout mechanisms. The `last_activity` timestamp tracks idle time -- if no query has been executed for a configurable duration (default: 15 minutes), the session is swept. The `created_at` timestamp enforces an absolute lifetime (default: 8 hours) regardless of activity. A `SessionManager` wraps the `Authenticator` and a concurrent `DashMap` of active sessions. On each Flight SQL request, it checks whether the background refresh task has updated the token and transparently swaps in the fresh one. Expired sessions are cleaned up on access; a periodic sweep catches sessions where the client disconnected without sending another request.

One session per query, not per connection. A JDBC connection might stay open for hours, but each query might execute with a different token state -- the token might have been refreshed between queries. By creating session state per query, we avoid the stale-token problem that plagues connection-pooled systems.

The `Session` also carries optional context fields: `default_catalog`, `default_schema`, and `source`. These come from client headers (e.g., the Trino compatibility layer reads `X-Trino-Catalog` and `X-Trino-Schema`). The session is not just an authentication container -- it carries the user's working context through the entire query lifecycle.


## The Background Refresh Task

The `Authenticator` spawns a background tokio task that polls every 10 seconds for sessions whose tokens are approaching expiry. When it finds one, it calls the OIDC provider's refresh endpoint using the refresh token (not the user's password) and updates a concurrent `DashMap` cache. The `SessionManager` picks up refreshed tokens on the next `get_session` call -- if the cached token differs from the session's stored token, the session is transparently updated. The user sees nothing.

For the `client_credentials` backend (used in development), there's no refresh token -- the task simply re-fetches from the token endpoint. For `OidcPassword` (production), the refresh token was returned alongside the access token during initial authentication. It typically has a longer lifetime than the access token (hours instead of minutes). When the refresh token itself expires, the session is evicted and the user must re-authenticate.

The entire auth configuration is seven fields: OIDC provider URL, realm, client ID, client secret, token endpoint, refresh buffer (seconds before expiry to trigger refresh), and TLS verification toggle. No IAM role ARNs. No STS configuration. No permission matrices. Seven fields.


## Tokens in Distributed Mode

The single-node model is straightforward: the coordinator holds the token, the token flows to Polaris, Polaris vends credentials, the coordinator reads from S3. Everything happens in one process.

Distributed mode changes the picture. When the coordinator splits a query plan and sends scan fragments to workers, each worker needs its own S3 credentials. The coordinator could pre-vend credentials for every worker, but those credentials expire. A query that runs for two minutes might be fine. A query that scans a terabyte might not.

The `DistributedScanExec` carries the credential state along with the plan:

```rust
pub struct DistributedScanExec {
    scan_tasks: Vec<ScanTask>,
    worker_urls: Vec<String>,
    schema: SchemaRef,
    credential_expiry: Option<DateTime<Utc>>,
    credential_tracker: Option<Arc<CredentialRefreshTracker>>,
    worker_registry: Option<Arc<WorkerRegistry>>,
    max_retries: u32,
    local_executor: Option<Arc<dyn LocalExecutor>>,
    // ...
}
```

Each `ScanTask` includes the S3 credentials that the worker needs to read its assigned files. The `credential_expiry` tells the coordinator when those credentials will stop working. The `credential_tracker` is the mechanism for pushing fresh credentials before that happens.

A `CredentialRefreshTracker` maintains a map of active fragments and their credential expiry times. When the `DistributedScanExec` dispatches a fragment to a worker, it registers that fragment with the tracker. A background task checks every 30 seconds for fragments whose credentials are approaching expiry (within a 5-minute buffer). For those fragments, it calls back to Polaris to vend fresh credentials, then pushes them to the worker via Arrow Flight's `do_action("refresh_credentials")` mechanism. The worker receives the action, hot-swaps the credentials, and continues scanning -- no restart, no data loss, no interruption. Chapter 14 shows what happens when this mechanism is tested under load with 50 concurrent clients.

Every type that holds secrets -- `Session`, `RefreshableCredentials`, `CachedToken` -- has a custom `Debug` implementation that replaces sensitive fields with `[REDACTED]`. Credentials never appear in logs, even in debug mode.

This is the full chain: user authenticates with OIDC, coordinator holds the JWT, coordinator asks Polaris for credentials scoped to the user, coordinator sends those credentials to workers with the plan fragment, workers read from S3 using the user's credentials, and if the credentials expire mid-scan, the coordinator pushes fresh ones. At every point, every byte read from storage is attributed to the user who initiated the query.

::: {.fieldreport}
**Field report: the three-day credential push.** We hit this during load testing: 50 concurrent
clients, some queries queued long enough that their vended S3 credentials expired before
execution started. The worker got a `403 Forbidden` that surfaced as a cryptic Arrow error
about "failed to read Parquet footer." The debugging took most of a day -- we traced it through
Arrow, iceberg-rust's FileIO, and the S3 client before finding the expired STS token. The fix
was the credential refresh tracker: three days of design discussion for 200 lines of code.
The code was straightforward. The thinking -- what if the refresh fails? what if two refreshes
race? -- was not. Chapter 14 has the full load test story.
:::


## The Security Properties

The passthrough model gives you properties that are impossible to achieve with a service account:

- **Attribution**: every S3 access is attributed to a user in CloudTrail
- **Least privilege**: users can only read what Polaris grants them -- the engine can't escalate
- **No lateral movement**: compromising the engine gives the attacker nothing without a valid JWT
- **Compliance**: the engine never stores credentials, never caches tokens to disk
- **Portability**: works with any OIDC provider (Keycloak, Auth0, Okta, Azure AD) and any S3-compatible storage

The one property you lose: caching. A service account can cache data across users because it has ambient access. A passthrough model can't -- each user's query must go through their own credential chain. We accepted the performance cost because the security properties are non-negotiable.

The "no lateral movement" property deserves spelling out. In a service account model, compromising the engine means reading everything. In the passthrough model, an attacker sees the JWTs of currently active sessions -- bad, but those tokens expire in minutes to hours and are scoped to each user's permissions. The blast radius is bounded by the intersection of "who has an active session" and "what each of those users can see." Compare that to "everything in the data warehouse."

The compliance property matters for regulated industries. If your security auditor asks "where are the credentials stored?" the answer is "nowhere." The OIDC provider issues them. The engine passes them through. Polaris vends storage credentials on the fly. S3 validates them on every request. No credential file. No secret in a config map. The credential lifecycle is entirely in-memory and ephemeral.

Rust's ownership model reinforces this. When a session is dropped, the `access_token` and `refresh_token` strings are deallocated -- not "eligible for collection," but gone. No garbage collector holding references longer than expected. Deterministic credential cleanup for free.

::: {.sovereignty}
**Sovereignty principle:** A service account is a shared secret. A bearer token is a proof of identity.
The difference is the difference between "the engine read the data" and "Alice read the data."
When security asks who accessed the customer table, the answer should be a name, not an application.
:::


## What This Model Makes Impossible to Retrofit

It would be natural to ask: why not add bearer passthrough to Trino? We tried. For two years. The answer is that the auth model is not a feature. It's a design constraint that shapes every other decision in the engine.

Trino's connector interface assumes ambient credentials. `ConnectorMetadata`, `ConnectorSplitManager`, `ConnectorPageSourceProvider` -- none of them take a user token. Adding one would be a breaking change to every connector in the ecosystem. And even if you did it, the shuffle layer, the result cache, and the spill-to-disk layer all use the coordinator's credentials.

We maintained a fork of the Trino Iceberg connector -- the "DCAF branch" -- that passed bearer tokens through to Polaris. Every upstream release required re-merging our changes, and every merge conflict was in the authentication layer. After two years, we had a connector that worked for our specific deployment and broke whenever Trino touched credential handling.

SQE doesn't have this problem because the auth model was the first thing we built, not the last. The `Session` struct is the first parameter to every method that touches data. The `SessionContext` is created with the user's credentials. The `SessionCatalog` carries the user's token to Polaris. There is no code path that accesses data without a user identity, because there is no code path that exists without one.

This is the deepest lesson of this chapter. The auth model is not a feature on a roadmap. It is an architectural constraint that determines the shape of everything built on top of it. You can add a feature to a system. You cannot add a constraint. Constraints must be there from the beginning, or you will spend years trying to retrofit them into code that was designed without them.

We know, because we spent those years on Trino before we built SQE.

## Ten Ways to Prove You're You

When we built the first version of SQE, there was exactly one way to authenticate: OIDC password grant. You sent a username and password over the Flight SQL handshake, we exchanged them for a JWT with Keycloak, and that JWT became your session. Simple. Sufficient.

Then came the real world.

The data engineering team wanted to run dbt from Airflow. Airflow already had a JWT (obtained through its own OIDC exchange) and had no username or password to offer. The platform team wanted to connect from AWS Lambda functions using IAM execution roles. The security team wanted mutual TLS for service-to-service calls. A consultant needed an API key that could be rotated without touching an identity provider. The testing infrastructure wanted an anonymous provider that just says yes to everything in CI.

A single provider couldn't serve all of these. But we also didn't want to make auth a configuration burden; the engine needed to figure out which mechanism applied to which connection automatically.

The answer was `AuthProvider` and `AuthChain`.

### The Trait and the Chain

Every auth mechanism implements one trait:

```rust
#[async_trait]
pub trait AuthProvider: Send + Sync {
    async fn authenticate(&self, credentials: &FlightCredentials) -> Result<Identity, AuthError>;

    async fn refresh_catalog_token(&self, identity: &Identity) -> Result<Option<String>, AuthError> {
        Ok(None)
    }
}
```

`FlightCredentials` carries whatever the client sent: a username and password, a bearer token, a client certificate CN, or nothing at all. The provider inspects what it knows how to handle and returns one of three things: an `Identity` on success, `AuthError::NotMyCredentials` if the credential type isn't for it, or `AuthError::AuthFailed` if the credential type matches but the credentials are wrong.

That `NotMyCredentials` variant is the key to how `AuthChain` works. The chain tries providers in order. The first one that returns `Ok(Identity)` wins. If a provider says `NotMyCredentials`, the chain moves on. If a provider says `AuthFailed`, the chain stops immediately. There is no point trying the next provider when the credential type matched but was explicitly rejected.

The implication: a connection carrying a JWT won't accidentally fall through to the anonymous provider just because it was listed last. The bearer token provider will recognise the JWT, attempt validation, and either accept it or definitively reject it.

### The Ten Providers

**`OidcPasswordProvider`** is the workhorse. A JDBC user in DBeaver sends `username:password`; the provider POSTs a `grant_type=password` request to the OIDC token endpoint and gets back a JWT. Works with Keycloak, Auth0, Okta, Zitadel, or any OIDC-compliant provider. Appropriate when your users are humans with credentials in an identity directory.

**`BearerTokenProvider`** validates a pre-obtained JWT via JWKS. The client sends a token it already has, from a browser SSO flow, a service that did its own OIDC exchange, or a backend BFF. The provider fetches the JWKS endpoint, verifies the signature, and extracts the identity. No password exchange needed. This is the right provider for Airflow, dbt running in CI, and any programmatic client that manages its own token lifecycle.

**`TokenExchangeProvider`** implements RFC 8693. It takes an incoming credential (a JWT from one issuer) and exchanges it at the OIDC token endpoint for a user-scoped JWT from another issuer. This is the federated identity path: your users authenticate with your corporate IdP, and the engine mints a Polaris-compatible token without them ever needing Polaris credentials.

**`ApiKeyProvider`** accepts opaque keys from a TOML keys file, identified by a configurable prefix (`sqe_` by default). Comparison is constant-time to prevent timing attacks. Appropriate for scripts, automation, and service accounts that can't do browser-based OIDC and don't need per-user identity. The keys file maps each key to a user ID and role set.

**`AwsIamProvider`** uses STS `GetCallerIdentity` with a SigV4-signed request to verify that the connecting client is who it claims to be on AWS. Lambda functions, EC2 instances, and ECS tasks can authenticate using their IAM execution role without any credentials to manage. Role mappings in `[auth.role_mappings]` translate IAM ARN patterns to SQE roles.

**`MtlsProvider`** authenticates via mutual TLS client certificates. The client certificate's Common Name becomes the user identity; the Organizational Unit can be mapped to roles. This is service-to-service auth for environments where certificate infrastructure is already in place: Kubernetes clusters with cert-manager, internal meshes, compliance environments where every service must present a certificate.

**`AnonymousProvider`** assigns a fixed identity and role set to any connection, regardless of credentials. The only provider that ignores `FlightCredentials` entirely. Appropriate for local development stacks, integration test environments, and public read-only query endpoints. Put it last in the chain; it accepts everything, so anything after it will never be reached.

**`DeviceCodeProvider`** implements RFC 8628, the device authorization grant. The client calls the engine to start a flow, receives a short code and a URL, and tells the user "go to `https://auth.example.com/device` and enter `GBTF-ZPQR`." Once the user completes auth in a browser, the engine's polling loop receives the token. This is how `gh auth login` works. For SQL query tools on servers without browser access, or CLI tools used by non-technical users, this is the right flow.

**`AuthCodeService`** implements OAuth2 Authorization Code + PKCE (RFC 7636). This is the browser redirect flow, the Trino `externalAuthentication=true` path. The client initiates a handshake, gets redirected to the IdP's login page, authenticates there, and the IdP sends the code back to the engine's callback URL. For JDBC clients that can open a browser, this is the most secure interactive flow available.

**Legacy `Authenticator`** is the original `OidcPassword` + `ClientCredentials` backend. It exists for backward compatibility. When the `[[auth.providers]]` array is empty, the factory wraps it in a single-provider chain and everything works exactly as it did before we built any of this. No configuration changes needed for existing deployments.

### The Configuration

Providers are configured as a TOML array. The order matters; it's the order the chain tries them:

```toml
[[auth.providers]]
type = "oidc_password"
token_url = "https://keycloak.example.com/realms/data/protocol/openid-connect/token"
client_id = "sqe"
client_secret = "changeme"

[[auth.providers]]
type = "bearer_token"
jwks_url = "https://keycloak.example.com/realms/data/protocol/openid-connect/certs"
issuer = "https://keycloak.example.com/realms/data"

[[auth.providers]]
type = "api_key"
keys_file = "/etc/sqe/api-keys.toml"
key_prefix = "sqe_"

[[auth.providers]]
type = "anonymous"
user = "dev"
roles = ["public"]

[auth.role_mappings]
"arn:aws:iam::123456789012:role/DataEngineering" = ["analyst", "writer"]
"data-team" = ["analyst"]
```

The `[auth.role_mappings]` table is shared across providers that support it: `AwsIamProvider` maps IAM role ARNs, `MtlsProvider` maps certificate OUs, `ApiKeyProvider` reads them from the keys file.

### OIDC Discovery

The `DeviceCodeProvider` and `AuthCodeService` don't take explicit endpoint URLs. Instead, they take an OIDC issuer URL and perform discovery: a GET request to `{issuer}/.well-known/openid-configuration` returns a JSON document with all the endpoints needed: `token_endpoint`, `authorization_endpoint`, `device_authorization_endpoint`, `jwks_uri`. The `OidcDiscovery` module fetches this once at first use and caches the result in a `OnceCell`. Endpoint overrides are available for environments where the discovery document doesn't match the reachable URLs (common in Kubernetes where the internal and external URLs differ).

### The Design Philosophy

We could have picked one auth mechanism and made it the standard. Every other query engine does. Trino uses a plugin system that ships with LDAP and Kerberos. Spark uses the cluster's Hadoop security. Presto has file-based passwords.

The problem is that "pick one" is a constraint on the operator, not the engine. A data platform serving a bank has different auth requirements from a startup's internal analytics cluster. The bank has Kerberos, mutual TLS, and an IAM policy council. The startup has Okta and a Slack channel. Neither should have to fork the engine to support their auth model.

> Sovereign means you pick your auth, not us.

The `AuthProvider` trait is deliberately minimal: one method, one input type, one output type. Implementing a new provider is an afternoon of work. The chain composes them without any of them knowing about each other. And the legacy fallback means existing deployments don't need to change a thing.

The "you are the query" principle still holds for every one of these ten providers. Whether Alice proves her identity via Keycloak password grant, a pre-obtained JWT, an IAM execution role, or an mTLS certificate, what flows downstream is the same thing: an `Identity` with a `catalog_token` that Polaris and S3 will recognise as Alice. The mechanism changes. The property (every S3 access attributed to a specific human or service) does not.

::: {.devto}
**dev.to connection:** "Software Supply Chain Security: Keeping Your (Rust) Dependencies Clean"
(2025) explored the broader question of trust in your dependency chain. The auth model is the
same question applied to runtime: do you trust the engine to act on behalf of all users, or
do you make each user prove their identity to every system they touch? The answer should be
the same for both: trust nothing, verify everything.
:::

::: {.ailog}
**AI Logbook:** The AI generated working STS assume-role code for Path 2 in twenty minutes, code we then discarded when the human decided the IAM operational model was unworkable. The `Authenticator` struct with its `OidcPassword` and `ClientCredentials` backends, the `SessionManager` with `DashMap`, and the background token refresh task were all AI-implemented from a spec that described the three authentication paths. The human chose Path 3 (bearer passthrough) after two days of evaluating the alternatives; the AI implemented whichever path was specified without questioning the security trade-offs.
:::
