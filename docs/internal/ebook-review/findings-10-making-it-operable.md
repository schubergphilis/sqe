# Findings: 10-making-it-operable.md

## Thesis
Building an engine that "works" is not the same as building one you can operate and others can deploy. The chapter walks through adding observability (metrics, traces, audit logs, health probes), configuration (TOML + env overlay + validation), and the enterprise/security hardening that turned a prototype into something a bank might approve.

## Opening
> The engine worked. Queries went in, Arrow batches came out, the numbers were correct. We had authentication, catalog integration, a write path, policy enforcement.

Verdict: strong hook. Opens on a confident "it works" claim, then the next single-line paragraph ("We also had no idea what was happening inside it.") flips it. Good setup-and-turn.

## Closing
> If your query engine surprises you, your metrics are wrong. If nobody else can deploy it, your config is wrong. If it panics on user input, your error handling is wrong. Fix all three.

Verdict: lands it. Three-beat parallelism that ties back to the epigraph ("a query engine that surprises you"). Imperative ending, no trailing summary.

## Voice & editorial issues
1. L65-67: the paragraph leans heavily on implementation minutiae (`kind_name`, SQL classifier, `Option<Arc<...>>`). Reads more like a code comment than prose. Consider compressing the test-context aside in L67 ("Making them optional avoids the overhead of maintaining a Prometheus registry in test contexts") since the chapter makes the "zero cost when disabled" point more sharply at L146.
2. L189/L193/L376/L596: British "realised" mixes with US "behavior" throughout. Not a voice-guide rule, but pick one convention for the book. Likely intentional house style (author is Dutch, "realise" recurs). Flag for cross-chapter consistency check, not a rewrite.
3. L342: 7-sentence paragraph, the longest in the chapter, crosses the wall-of-text line. Split: end the rationale, start a new paragraph at "We defined the traits before having multiple implementations because the open-source target demands it."
4. L342: "the open-source target demands it" and "is the end of the open-source goal" are near-identical phrasings one sentence apart. Vary one.
5. L418 + L437: "the pattern scales" is repeated as a refrain ("The pattern scales because it's mechanical" L418; "The pattern is established. The pattern scales." L437). Reads as a verbal tic. Cut one. Recommend dropping the L437 instance since L418 already nails it.
6. L588: "would a bank approve this for production?" is a posed-and-answered question, not a transition rhetorical question ("The answer was 43 findings..." follows immediately). Acceptable under the voice guide. No fix.
7. L596: "The lesson: the safest default and the correct default are not always the same." Clean aphorism. Keep.

## Mechanical violations (PROSE only)
none. The only em-dash/endash/arrow hits are L501-502 inside the circuit-breaker ASCII diagram (code fence), excluded. All prose uses `--` and `->` correctly.

## Exclamation marks in prose
none. The two `!` hits (L121 `info_span!`, L380 `assert_eq!`) are Rust macros in code / inline code spans, not prose.

## Continuity data
### Concepts INTRODUCED / defined here
- Three pillars (applied) -> metrics, traces, logs for an engine
- `MetricsRegistry` -> coordinator Prometheus struct
- `HasRegistry` trait -> generic metrics-server abstraction
- W3C TraceContext propagation -> span linking over gRPC metadata
- `OtelGuard` -> RAII flush of providers on drop
- Audit log (JSONL) -> who-ran-what append-only record
- `query_hash` -> SHA-256 of normalized SQL
- Health endpoints -> `/healthz`, `/readyz`, `/api/v1/status`
- Heartbeat model -> 5s send / 15s healthy window
- Norway problem -> YAML `no` -> false coercion (TOML rationale)
- `SqeConfig` struct + `#[serde(default)]` layering
- Env overlay -> `SQE_<SECTION>__<FIELD>` double-underscore
- `validate()` -> fail-fast accumulated config errors
- `SqeErrorCode` -> 27 structured error variants
- Circuit breaker (Polaris) -> Closed/Open/Half-Open states
- PII redaction -> regex stripping in `query_text`
- Per-query resource limits -> 4-limit table
- `SessionStore` trait -> warm-restart session snapshot
- Evolved `/readyz` -> Polaris reachability check + cache
- Adaptive sort -> `partition_only` vs `adaptive` default

### Concepts ASSUMED (used as if already known)
- DataFusion `ExecutionPlan` / `metrics()` / physical operators
- `EXPLAIN ANALYZE` (used as established)
- Arrow Flight / Flight SQL / `do_get` / `Ticket`
- Distributed coordinator-worker scheduling, scan fragments, `DistributedScanExec`
- Polaris REST catalog, Iceberg manifests, partition pruning
- OIDC / Keycloak / bearer tokens, password grant
- `PolicyEnforcer` trait, passthrough/OPA/Cedar (defined in policy chapter)
- moka cache, `try_get_with`, TOCTOU
- DataFusion `GreedyMemoryPool`, `FairSpillPool`, spill-to-disk
- Twelve-Factor App (named, partially explained)
- dbt adapter (referenced as known)

### Key factual / numeric claims
- Coordinator Prometheus port 9090, workers 9091; host ports 29090/29091/29092 (L63)
- Metric prefixes: coordinator `sqe_`, worker `sqe_worker_` (L63)
- Health port = `prometheus_port + 1` (L163)
- Heartbeat every 5s; healthy if heartbeat within 15s (L165)
- Alert thresholds: error rate >5% / 5min; p95 >30s / 10min; workers below expected / 3min; zero queries / 15min (L85)
- Removed alerts: cache hit <50%, fragment duration >10s (L87)
- OTel shutdown order: meter, tracer, logger (L135)
- Idle timeout 900s/15min; absolute 28800s/8h (L271, L356-358)
- Query cache: 256MB, 5min TTL (L271)
- Worker memory default 8GB; heartbeat 5s; spill `/tmp/sqe-spill` (L269)
- Rate limit: per_user 60/min, global 1000/min (L351-353)
- "~45 individual config keys across 12 sections, only 3 required" (L378); also L420 "12 sections and roughly 45 individual keys"; required: `auth.client_id`, `catalog.polaris_url`, one of `auth.keycloak_url`/`auth.token_endpoint` (L378)
- Config section key-count table (L422-435): coordinator 7, worker 6, auth 7, catalog 4, storage 6, policy 1, metrics 3, rate_limit 3, session 2, query 2, query_cache 4, query_history 2 -> SUMS TO 47, prose says 45 (internal inconsistency, reconcile)
- 27 `SqeErrorCode` variants (L453)
- Trino error codes 65535/65536/65537/65540/65541/65550+n (L457-461)
- Circuit breaker: 5 consecutive failures open, 30s to half-open, single probe (L501-507)
- PII regex categories: email, phone, SSN, credit card (L518-523)
- Per-query limits: max_result_rows 1,000,000; max_concurrent_queries 100; max_query_memory 256MB; slow_query_threshold 30s; semaphore wait timeout 5s (L534-543)
- Session snapshot every 5min, JSONL (L554)
- Evolved `/readyz`: 10s cache, K8s polls every 5s -> 12 pings/min -> reduced to 6/min (L581)
- Default Flight SQL port 50051; trino_http_port 8080 (L182, L211-212)
- Security audit: 43 findings, 2 critical / 13 high / 21 medium / 7 low (L590); 6 categories
- "ran 221 out of 222 benchmark queries correctly and beat Trino by 2.5x to 8.8x" (L590)
- Audit fix scope: 33 files changed, +1,272 / -372 lines (L602); 60 integration tests pass (L602)
- Panic safety: 16 `.unwrap()` call sites in date extraction (L594)
- Audit doc at `docs/issues.md` (L604)
- Single binary `sqe-server`, `--mode` flag / `SQE_MODE` (L397, L283)

### Cross-references
- L583: "The HA story is the next chapter we haven't written yet." (forward ref, vague, not a numbered chapter)
- L399-401: `.ailog` placeholder "[To be completed by AI Logbook agent]" -- unfinished section, must be filled or removed before publication.
- No explicit "as we saw in ch X" back-references; chapter assumes auth/catalog/policy/distributed chapters precede it (see Assumed list).

## Pacing
Flows well for the first two-thirds; story beats ("The Query That Scanned Too Much", field reports) break up reference-heavy stretches. The back half ("The Enterprise Checklist" through the 43-finding audit) is a long enumerated catalog of seven subsystems and risks reading like a changelog; saved by each item leading with the question/incident that prompted it. Only egregious wall of text is L342 (7 sentences, flagged). The config-key counts repeat three times (L378, L420, L426-435), which drags slightly.

## Grade
Voice adherence: A-. Clean mechanically (zero prose violations), strong hook and close, consistent direct/opinionated register with earned short sentences. Docked for the "pattern scales" verbal tic (L418/L437), one over-long paragraph (L342), a couple of near-duplicate phrasings, and an unresolved internal number (config-key table sums to 47, prose says 45).
