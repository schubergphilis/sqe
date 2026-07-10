# Findings: 00-preface.md

## Thesis
The preface argues that no existing query engine satisfies true data sovereignty (every query runs as the authenticated human, identity propagates to storage, policy is enforced inside the engine, no ambient credentials), and that this gap is why SQE was built and open-sourced. It also sets the book's frame: written while building, honest about dead ends.

## Opening
> "We had a Trino cluster. It worked. Mostly."
Verdict: strong hook. Three short sentences; the "Mostly." undercut creates immediate tension and a reason to read on.

## Closing
> "And to everyone who looked at this project and asked "why would you build a SQL engine?", this book is the answer."
Verdict: lands it. Callback to the recurring "why build it" thread; closes as a promise, not a summary. (Actual final lines are the byline "Jacob Verhoeks / March 2026", which is appropriate.)

## Voice & editorial issues
1. L31 `When Apache Polaris appeared (a pure Iceberg REST catalog with no opinions about your query engine, your governance layer, or your cloud provider) the last proprietary piece fell away.` -- Long parenthetical splits subject from verb, forcing a re-read. Voice guide: short sentences carry the weight, three clauses max. Rewrite: "Then Apache Polaris appeared: a pure Iceberg REST catalog with no opinions about your query engine, governance layer, or cloud provider. The last proprietary piece fell away."
2. L33 `Spark was a cluster framework, not a query engine.` vs L48 `Spark is too heavy for interactive queries.` -- The two "why not Spark" framings differ (categorical vs performance). Not contradictory, but tighten to one consistent reason or make the second build on the first explicitly.
3. L29 `Then Apache Iceberg changed the game.` -- "changed the game" is adjacent to the forbidden "game-changer" register and reads as a cliche. Rewrite: "Then Apache Iceberg changed what was possible." or name the specific change.
4. L119 Rafael acknowledgement paragraph is one block of 8+ sentences (wall of text, > 5-sentence rule). Content is warranted but should be broken into 2-3 paragraphs (the "what Rafael built" inventory, then the "why the architecture reflects his questions" point).
5. L103 `From zero crates to a benchmark suite running TPC-H, TPC-DS, ClickBench, SSB, TPC-E, TPC-BB, and TPC-C.` -- Seven-item list reads as inventory padding in a narrative paragraph; trim to headline ones to keep rhythm.
6. L44 `A team that builds a query engine from scratch develops an understanding... that a team running a managed service never acquires` -- slightly abstract/generic for Jacob's voice. Make it concrete: name the thing you understand now that you didn't.

## Mechanical violations (PROSE only)
none. (The ranges `0--2`, `3--6`, `7--10`, `11--14`, `15--17` on L88-96 are ASCII double-hyphens in source, not en-dashes; the `--` before "Sun Tzu" L6 and "Jacob Verhoeks" L127 are also ASCII. grep for U+2014/U+2013/arrows returns zero hits.)

## Exclamation marks in prose
none

## Continuity data
### Concepts INTRODUCED / defined here
- Sovereignty (data infra) -> every component under your control
- Service account problem -> one identity hides all users
- Ambient authority/credentials -> engine trusted to self-enforce
- Plan rewriting (named in part overview) -> security as plan edit
- Dead end callout (`.deadend`) -> eliminated path, via Part IV framing
- Seven sovereignty requirements (L66-72) -> the book's spec
- Five Constants mapping -> Contract/Context/Terrain/Model/Protocol

### Concepts ASSUMED (used as if already known)
- Iceberg, S3, dbt, Kubernetes, Helm, Arrow batches, OIDC, Flight SQL, TPC-H/DS/ClickBench/SSB, REST catalog, zero-trust, mTLS, RBAC, OPA, Cedar. (Acceptable for a preface aimed at a senior data-engineering reader.)

### Key factual / numeric claims
- L103 "316 commits (across all branches) in 15 days"
- L103 benchmark suite: "TPC-H, TPC-DS, ClickBench, SSB, TPC-E, TPC-BB, and TPC-C"
- L88-96 Part structure: Part I = ch 0-2, Part II = ch 3-6, Part III = ch 7-10, Part IV = ch 11-14, Part V = ch 15-17 (17 chapters total)
- L79 *The Art of Agents* = "13 principles"; Five Constants = "Contract, Context, Terrain, Model, Protocol"
- L29 "late 2024" started experimenting with Iceberg REST
- L128 date "March 2026"
- L48 engine gaps: "Trino's auth model is incompatible with zero-trust. Spark is too heavy for interactive queries. DuckDB is single-node. DataFusion is a library, not a product."
- L117 "Polaris team at Snowflake open-sourced the catalog"
- L86 "API documentation, that's in `docs/book`"
- L94 Part IV names "Ballista" and "The load test that broke everything"
- L119 Rafael Herrero owns: K8s deployment, Helm operator, security hardening, network policies, mTLS coordinator<->worker, RBAC, pod security standards, rollouts
- L119 "Schuberg Philis" named as org/security standard

### Cross-references
- L77-79 Forward/external ref to companion book *The Art of Agents* (13 principles, Sun Tzu frame)
- L86 ref to `docs/book` for API documentation
- L88-96 forward map of all five Parts / 17 chapters
- L94 forward promise: Ballista + load test in Part IV ("most dead ends are")
- L108-110 ref to tagged commits / `main` branch as running code

## Pacing
Flows well. Short-then-long rhythm is consistent; section headers form a readable skim outline. Only drag is the L119 Rafael acknowledgement (single 8+ sentence block). The L103 seven-benchmark list and L31 long parenthetical are micro-stumbles, not drags.

## Grade
Voice adherence: A-. Strong hook, clean rhythm, honest framing, zero mechanical/exclamation violations, signature self-deprecating humour lands. Held back from A by the "changed the game" cliche (L29), one wall-of-text acknowledgement (L119), and a subject-splitting parenthetical (L31).
