# Findings: 15-deploying-sovereignty.md

## Thesis
Shipping SQE from "works on my laptop" to a deployable system: a 47MB multi-stage Docker image, a Helm chart for both single-node and distributed topologies, resource defaults, zero-downtime rolling upgrades, and lightweight local test stacks. Deployability is framed as a sovereignty (and product) decision: an engine nobody can run is not sovereign.

## Opening
> "The engine worked. On my laptop, in a terminal, with `cargo run` and a local Polaris instance, SQE parsed SQL, authenticated users, queried Iceberg tables, and returned Arrow batches over Flight SQL."
Verdict: strong hook. Concrete present-tense scene that sets up the "demo vs deployment" tension, paid off two sentences later ("But 'works on my laptop' is not deployment. It is a demo.").

## Closing
> "The best query engine is the one people actually run. And people run engines they can deploy, test, break, fix, and upgrade without calling a meeting first."
Verdict: lands it. Echoes the epigraph without literally repeating it, and the "without calling a meeting first" callback to the voice register is earned. (But the literal epigraph is then repeated verbatim on L618 after the AI Logbook box, diluting the punch. See voice issue 1.)

## Voice & editorial issues
1. L618: the final line `A sovereign engine that is hard to deploy is just a sovereign engine that nobody runs.` repeats the epigraph (L3-4) almost verbatim AND sits after the stronger L612 close and the AI Logbook box. Two endings competing. Drop L618 (L612 is the stronger close) or drop the epigraph bookend.
2. L604: "Every technical choice in this chapter (the multi-stage build, the Helm topology, the test stack, the resource defaults) is a product decision disguised as infrastructure." "in this chapter" is the throat-clearing register the voice guide warns against. Rewrite: "Every technical choice here. the multi-stage build, the Helm topology, the test stack, the resource defaults. is a product decision disguised as infrastructure."
3. L514: "This matters more than it looks." starts a sentence with "This" referring to the previous sentence (reversible adoption). Voice guide: name the subject. Rewrite: "Reversibility matters more than it looks."
4. L199: "Kubernetes does not restart pods when a mounted ConfigMap changes." is a strong standalone fact buried mid-paragraph; consider promoting to a single-sentence paragraph for emphasis. Minor.
5. Forbidden words: none present (checked delve, leverage, utilize, facilitate, holistic, paradigm, synergy, arguably, essentially, robust, comprehensive, "this approach ensures", "this enables", "this allows for", "it's worth noting"). Clean. L126 "This is not security theater" and L540 "This is intentional" are acceptable emphatic uses, not the AI-tell pattern.

## Mechanical violations (PROSE only)
none. No emdash, endash, or unicode arrows in prose. `->` appears only inside config/code fences.

## Exclamation marks in prose
none. L586 `SQE has been deployed!` is inside a NOTES.txt output block. L131 `![...]` is markdown image syntax.

## Continuity data
### Concepts INTRODUCED / defined here
- cargo-chef -> Docker dependency-layer caching
- Five-stage Dockerfile -> chef/planner/deps/builder/runtime
- 47MB runtime image -> debian:bookworm-slim final
- `worker.enabled` toggle -> one chart, two topologies
- config checksum annotation -> ConfigMap-change pod restart
- `SQE_` env var convention -> double-underscore section nesting overrides TOML
- readiness-gate draining -> `/healthz` liveness, `/readyz` readiness
- PodDisruptionBudget `minAvailable: 1` -> worker eviction floor
- Test stack -> Polaris in-memory + RustFS
- Distributed test stack -> coordinator + 2 workers compose
- Production compose overlay -> drop-in Trino replacement
- Workers-first upgrade order -> protobuf backward-compat rule
- `system.runtime.nodes` verify -> post-upgrade node/version check

### Concepts ASSUMED (used as if already known)
- Flight SQL / Arrow batches / gRPC (assumed competence, correct)
- Polaris REST catalog, OIDC bearer passthrough, Keycloak
- spill-to-disk (referenced as existing behavior)
- fragment scheduling / heartbeat registration / fragment retry (attributed to Ch 14)
- coordinator-worker distributed topology (attributed to Ch 12-14)
- Trino wire-protocol compat (sqe-trino-compat)
- result cache, query history buffer, information_schema, system tables
- bearer token used by workers (token replay attack vector, L540)

### Key factual / numeric claims
- Original single-stage image 2.3GB; 45s cold starts (L13, L15, L118)
- Binaries sqe-server, sqe-worker, sqe-cli total ~40MB (L17, L116, L250)
- Final image 47MB; "98% reduction"; cold pull 3s vs 45s (L116, L118)
- 400+ dependencies; naive recompile 15-25 min (L19); incremental 15 min -> under 3 (L57); warm app build 30-90s (L92)
- bookworm-slim adds 25MB over scratch (L120, L123)
- Non-root UID 1000, group sqe (L101, L126)
- SCCACHE_VERSION=0.9.0; SCCACHE_CACHE_SIZE=2G (L33, L52)
- Ports: 50051 Flight SQL, 50052 worker, 8080 Trino HTTP, 9090 Prometheus, 9091 health/worker-metrics (L108, L111, L538)
- POSSIBLE INCONSISTENCY: Dockerfile HEALTHCHECK hits :9091/healthz (L111) while the coordinator probe story uses a port named `health` for /healthz and /readyz (L271-282) and lists coordinator metrics as 9090. Confirm the coordinator health endpoint port (9091 vs the 50051/8080/9090 trio).
- 7 Helm templates (L137-145)
- heartbeat_interval_secs = 5 (L166, L446)
- Coordinator resources: req 512Mi/500m, lim 2Gi/2 (L226-233)
- Worker resources: req 1Gi/1, lim 8Gi/4 (L235-242)
- Field report worker memory: 512Mi OOM'd -> 8Gi -> settled 4Gi with spill (L250)
- request:limit ratio: rejected 1:8, settled 1:4 (L253-255); prod 2Gi/8Gi (L255)
- "sort buffer for a 200-million-row GROUP BY is 6GB" (L250); "32Gi node schedules 60 pods @512Mi, only 4 use 8Gi" (L253)
- terminationGracePeriodSeconds default 30 (L285)
- Test stack Polaris in-memory + RustFS ~400MB; Polaris starts 8s, RustFS <1s (L354, L356); full quickstart 2GB (L356)
- Polaris image apache/polaris:1.5.0; bootstrap creds "POLARIS,root,s3cr3t" (L326, L328)
- Test stack ports Polaris 18181:8181, RustFS 19000:9000 (L335, L351, L358)
- Distributed test 14 assertions (L458); concurrent test default 10 clients, up to 50+ (L460)
- Concurrent failure: 50 clients vs 2 workers @512MB each OOM'd (L466)
- Upgrade window "5-15 seconds" no new queries (L573)
- Worker memory_limit in distributed test config = "512MB" (L447)

### Cross-references
- L6: "Chapters 3 through 14 built all of that." (back ref)
- L287: "(Chapter 14)" fragment scheduler crash recovery (back ref)
- L377: "the full distributed topology from Chapters 12 through 14." (back ref)
- L466: "Chapter 14 covers the debugging story." (back ref)
- No forward refs.

## Pacing
Flows well. Headers form a readable outline. Code blocks are long but each is preceded by its concept and followed by prose, per voice habits. No wall-of-text paragraph; densest are L253 (5 sentences) and L466 (6 short sentences), both deliberate staccato. The Helm/ConfigMap stretch is the most code-heavy, acceptable for a deployment chapter.

## Grade
Voice adherence: A-. Clean mechanics, strong hook and close, signature transparency (protobuf-upgrade failure, OOM field reports, scratch-vs-slim dead end). Minor dings: duplicated epigraph bookend (L618) competing with the stronger L612 close, a "This"-as-subject opener (L514), and "in this chapter" phrasing (L604).
