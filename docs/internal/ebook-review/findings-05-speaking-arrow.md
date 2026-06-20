# Findings: 05-speaking-arrow.md

## Thesis
The wire protocol is the user experience, and Arrow Flight SQL was the only choice that gave SQE zero-serialisation Arrow results plus broad client compatibility (JDBC/ADBC/Rust) without maintaining a custom driver. The chapter walks the FlightSqlService trait, the three-phase Handshake/GetFlightInfo/DoGet pipeline, the metadata surface, and the known streaming gap.

## Opening
> "The wire protocol is the user experience. Everything else is implementation detail." (epigraph, L3-4)
> "The engine can parse SQL. It can authenticate users. It can plan queries, push them through DataFusion, and produce Arrow record batches. None of that matters if clients can't talk to it." (L6)
Verdict: strong hook. The epigraph states the thesis flat, then L6 lands it with a four-beat build ending on "none of that matters" - the short/long/short rhythm the voice guide asks for.

## Closing
> "Good plumbing is invisible. Users don't think about the wire protocol. They open DBeaver, type a connection string, and run queries. The protocol's job is to never be the reason something doesn't work. Flight SQL has held up its end of that bargain." (L658)
Verdict: lands it. The final narrative line closes the thesis cleanly. Two callouts (.fieldreport, .ailog) follow at L660-670 as sidebars, not the prose close. The recap section "The Wire Protocol Is the User Experience" (L648-658) uses heavy anaphora ("It determined... It determined..." L652) - judged earned, not a trailing summary, because it mirrors the epigraph and ends on a fresh point ("Every step that doesn't exist is a step that can't have bugs", L654) rather than re-listing section titles.

## Voice & editorial issues
1. L429 `"some JDBC client somewhere will silently truncate your decimal values and you'll spend a week figuring out why your financial calculations don't round-trip."` - strong on-voice sentence, keep as is. No change.
2. L287 `"We chose not to."` - exactly the directness the voice guide prescribes. No change.
3. L639-643 The TLS code block dangles slightly after "optional TLS is layered on top:" with no prose explaining the `if let Some(tls)` flow. Minor. Optional fix: add one sentence such as "TLS is opt-in: if the config carries a cert and key, the builder wires it; otherwise the server runs plaintext." Low priority.

No hedging, no throat-clearing, no rhetorical-question transitions, no filler transitions found. Voice is strong throughout.

## Mechanical violations (PROSE only)
None. grep for emdash (U+2014), endash (U+2013), and unicode arrows returned zero hits. The chapter uses `--` (double hyphen) as the sanctioned emdash replacement throughout (L8, L19, L21, etc.) - correct, not a violation.

## Exclamation marks in prose
None. Every `!` in the file is inside code fences (`format!`, `warn!`, `vec!`, `Status::...`). No prose exclamations.

## Continuity data
### Concepts INTRODUCED / defined here
- Arrow Flight SQL -> gRPC SQL client protocol over Arrow IPC
- FlightSqlService trait -> Rust trait, 20+ RPC methods
- Three-phase pipeline -> Handshake, GetFlightInfo, DoGet
- FetchResults (custom protobuf) -> ticket carrying SQL handle
- do_get_fallback path -> route JDBC driver actually uses
- get_session_from_request -> dual auth (session-id vs raw JWT)
- Trino-compat escape hatch -> JSON /v1/statement migration bridge
- Invisible denial at wire level -> no PERMISSION_DENIED status

### Concepts ASSUMED (used as if already known)
- OIDC password grant / bearer token / Keycloak (earlier auth chapter)
- DataFusion record batches, LogicalPlan, optimization
- Policy enforcement / plan rewriting (forward-ref to ch8, but used here)
- Polaris credential vending (L664)
- SHOW SCHEMAS / SHOW TABLES internal statements
- WorkerRegistry / distributed coordinator-worker model
- system.runtime.queries virtual table (L80)

### Key factual / numeric claims
- "over twenty methods" (L64); "Of the 20+ trait methods" (L82); "20+ methods in the FlightSqlService trait" (L656)
- Implemented-methods table (L86-L110) has 25 rows (recounted exactly)
- Unimplemented table (L116-L122) has 7 rows (Substrait x3, transactions x2, savepoints x2)
- AI Logbook (L669): "all 24 FlightSqlService trait methods" - INCONSISTENT (see escalation)
- SqeFlightSqlService struct has six fields (L72-77; prose "Six fields" L80)
- total_records is -1 = "unknown" (L277, L287)
- Arrow Flight SQL JDBC driver "version 15.0" (L392)
- XDBC type list enumerated (L409): boolean, tinyint, smallint, integer, bigint, real, double, decimal, varchar, varbinary, date, time, timestamp = 13; prose says "Fifteen type definitions" (L429)
- decimal column_size 38, minimum_scale 0, maximum_scale 38, num_prec_radix 10 (L412-426)
- Default port 50051 (Flight SQL); do_get_table_types returns ["TABLE", "VIEW"] (L96)
- First integration test passed "March 14, the same day the crates were scaffolded" (L662)
- "Four network surfaces" / four ports: Flight SQL, Trino-compat HTTP, Prometheus, worker health-check (L645)
- CLI flags --protocol flight (default) / --protocol http (L555-556)
- Named: crates arrow-flight, sqe-cli, sqe-coordinator; types SessionManager, QueryHandler, WorkerRegistry, QueryTracker

### Cross-references
- "More on that in a moment" (L23) -> Trino-compat layer, delivered L546-557. OK.
- "The security model (covered in Chapter 8)" (L611) -> explicit forward-ref to ch8. Dispatcher should confirm the security chapter is actually ch8.
- No "as we saw in ch X" back-refs; OIDC/Keycloak/policy enforcement assumed from earlier chapters.

## INCONSISTENCIES to escalate
1. Method count is three-way inconsistent. Prose says "over twenty" / "20+" (L64, L82, L656). The implemented table actually has 25 rows (L86-L110). The AI Logbook (L669) says "all 24". So 24 != 25 (table), and "all 24" also contradicts the 7 explicitly-Unimplemented methods at L116-L122 ("all" cannot be true if Substrait/transactions are left out). Fix: reconcile to one true number.
2. Type count mismatch. Prose says "Fifteen type definitions" (L429) but the enumerated list (L409) names 13 types. Either the list is missing two or "Fifteen" should be "Thirteen". Verify against source and align.

## Pacing
Flows well. Progressive disclosure is clean: reject options -> pick Flight SQL -> trait -> three phases -> fallback -> metadata -> clients -> streaming gap -> errors -> mounting -> reflection. The two method tables are dense but appropriate (inventory, not prose). No walls of text; longest paragraphs stay at 4-5 sentences. Callouts break up the code-heavy middle. No section drags.

## Grade
Voice adherence: A. Clean mechanics (zero emdash/emoji/forbidden words), strong hook and close, consistent short/long rhythm, transparent about the streaming gap and the dead end. Held back from A+ only by the two internal numeric inconsistencies (method count, type count), which are factual not voice.
