# Findings: 04-you-are-the-query.md

## Thesis
SQE has no service account: every query runs as the authenticated user via OIDC bearer token passthrough, so every downstream system (Polaris, S3, workers) makes its own access decision against the user's real identity. The chapter argues this auth model is an architectural constraint that had to exist from day one, not a feature retrofittable onto engines like Trino.

## Opening
> "There is no service account. There is only you."
Verdict: strong hook. Epigraph plus the security-team anecdote ("who accessed the customer table last Tuesday?") opens on a concrete failure, not preamble.

## Closing
> "We know, because we spent those years on Trino before we built SQE."
Verdict: lands it. (Note: the chapter has TWO endings -- see Pacing. This is the end of the original chapter at L342; the appended "Ten Ways" section ends at L458 with the AI Logbook callout, which is a fine callout but not a closing line.)

## Voice & editorial issues
1. L52 -- wall of text + repetition. The Path 3 trade-off paragraph runs ~8 sentences and says "if your identity provider is down" three times, ending "you have bigger problems than query latency." Good point, overstuffed. Rewrite: split into two paragraphs and cut the middle restatement ("In a service account model... In a passthrough model...") since the next paragraph and the L307 table already cover it.
2. L160 -- "This dual-path design means SQE works with both interactive users... without either side having to adapt." Borderline "this enables" pattern; starts with "This" referring back. Rewrite: "The dual path lets interactive users (DBeaver, CLI) and programmatic callers (dbt, Airflow) connect without either side adapting."
3. L211 -- "The engine is a coordinator of decisions, not the decision maker." Third iteration of the same antithesis: see L46 and L50 ("The engine is a conduit, not a gatekeeper"). Keep L50, cut or vary one of the others.
4. L340 "You can add a feature to a system. You cannot add a constraint." Strongest passage in the chapter. No change; flagging as the keeper.
5. L344+ "Ten Ways to Prove You're You" reads as a later bolt-on. It reopens the chapter after a clean thesis-landing close at L342, AND contradicts earlier framing: L158 says JWTs are decoded with "no signature verification, because the OIDC provider already validated the token," and the L164 antipattern callout says "Don't validate JWTs in the engine" -- but L381 `BearerTokenProvider` "fetches the JWKS endpoint, verifies the signature." Real drift, needs reconciling. See Continuity.
6. L457 AI Logbook -- honest, consistent with voice. No change.

## Mechanical violations (PROSE only)
none. (All `--` are double-hyphens, allowed. No emdash/endash/arrows/emoji in prose.)

## Exclamation marks in prose
none. (All `!` hits are in Rust code fences -- `map_err`, macro `!`, `format!` -- and the `![...]` image markdown at L177.)

## Continuity data
### Concepts INTRODUCED / defined here
- Bearer token passthrough -> JWT forwarded unchanged downstream
- Credential vending -> Polaris returns scoped S3 creds
- OIDC password grant (ROPC) -> username/password to JWT
- `do_handshake` -> Flight SQL auth exchange method
- `Authenticator` / `AuthBackend` -> OidcPassword vs ClientCredentials enum
- `Session` / `SessionManager` -> per-query identity container + DashMap
- Background refresh task -> polls 10s, swaps tokens
- `CredentialRefreshTracker` / `DistributedScanExec` -> distributed cred refresh
- `AuthProvider` trait / `AuthChain` -> pluggable multi-provider auth (ten providers)
- `AuthError::NotMyCredentials` -> chain skip signal
- OIDC discovery (`.well-known/openid-configuration`) -> endpoint auto-fetch

### Concepts ASSUMED (used as if already known)
- `SessionContext`, `CatalogProvider`, `TableProvider` (L11, attributed to ch3)
- Flight SQL / Arrow Flight, `do_action` (protocol assumed known)
- iceberg-rust `FileIO` (L198)
- Polaris REST catalog, `loadTable` endpoint (L214)
- Trino connector SPI: `ConnectorMetadata`, `ConnectorSplitManager`, `ConnectorPageSourceProvider` (L334)
- "DCAF branch" (L336) -- named as if known; may need a one-line gloss

### Key factual / numeric claims
- Trino fork maintained "two years" (L332, L336, L342)
- STS token max duration "12 hours" (L40)
- Polaris credential lifetime "typically 15 minutes to 1 hour" (L202)
- Session idle timeout default 15 minutes (L247)
- Session absolute lifetime default 8 hours (L247)
- Background refresh polls "every 10 seconds" (L256)
- Distributed cred tracker checks "every 30 seconds", "5-minute buffer" (L287)
- "200 lines of code", "three days of design discussion" (L299)
- Load test "50 concurrent clients" (L294, forward-ref L287)
- Auth config "seven fields" (L260) -- VS the ten-provider TOML model (L344+) with far more than seven. Possible drift: "seven fields" describes the legacy `Authenticator`; never reconciled with the superseding `AuthProvider` config.
- "Ten" auth providers (L344, L377) -- count: OidcPassword, BearerToken, TokenExchange, ApiKey, AwsIam, Mtls, Anonymous, DeviceCode, AuthCode, Legacy = 10. Checks out.
- RFCs cited: 8693, 8628, 7636 -- verify on cross-check.
- API key prefix `sqe_`, constant-time comparison (L385)

### Cross-references
- L11 back-ref ch3 (DataFusion as library) -- consistent.
- L158 "policy enforcement (Chapter 8)" forward ref -- confirm ch8 is the policy chapter.
- L287 + L301 forward refs to Chapter 14 (load test, 50 clients) -- consistent.
- L448 devto callout references a 2025 "Software Supply Chain Security" article.

## Pacing
Strong arc for the first ~340 lines: anecdote -> three paths (dead-end callout lands) -> handshake code -> passthrough -> credential vending -> session lifecycle -> distributed -> security properties -> retrofit lesson -> clean close at L342. Then it restarts: "Ten Ways to Prove You're You" (L344-458) doubles the chapter and reopens a closed argument. Well-written internally but creates a double-ending and the JWKS contradiction (issue 5). Recommend folding the ten-provider material BEFORE the L330 retrofit lesson, or marking it an explicit addendum. L52 is the only true wall of text.

## Grade
Voice adherence: A-. Prose is squarely Jacob's voice (short-long rhythm, dead-end callout, dry understatement at L29, honest AI logbook, no forbidden words, no mechanical violations). Held back from A by the structural double-ending, the JWKS / "don't validate in the engine" contradiction introduced by the appended section, the L52 wall of text, and the thrice-repeated conduit/gatekeeper antithesis.
