# Findings: 09-what-you-cant-see.md

## Thesis
Security enforcement in a query engine belongs in the logical plan, not in application code, storage, or the network boundary. SQE rewrites the LogicalPlan before optimization to inject row filters, column masks, and column restrictions, relying on DataFusion's own expression boundaries to prevent predicate pushdown through masks.

## Opening
> Alice can log in. Chapter 4 made sure of that.

Verdict: strong hook. Concrete character, immediate callback to ch4, drops straight into the authn-vs-authz tension. The epigraph ("Security is not a feature. It's a rewrite of the query plan.") lands the thesis in one line.

## Closing
> The `PolicyEnforcer` trait is twenty-six lines of Rust. The plan rewriting that implements it will be several hundred. The security properties it guarantees come not from the amount of code, but from where it sits in the pipeline: after parsing, before optimization. That placement is the entire design.

Verdict: lands it. Returns to the 26-line figure from the trait section and the placement thesis without restating the three-property list. The `.ailog` callout sits after it, which is the book convention, so the prose itself ends clean.

## Voice & editorial issues
1. **L539** `This section is worth dwelling on because it's the most subtle security property in the system.` -- self-announcing throat-clearing, close cousin of "it's worth noting" (forbidden). Cut it and let the example carry the weight: `The most subtle security property in the system shows up when a user predicate hits a masked column. Consider a table with columns...`
2. **L515-516 (Implementation Status intro)** `Here's where we are:` is fine, but the section directly contradicts the prose (see Continuity / factual claims below). This is the biggest editorial problem in the chapter: the table claims `PolicyPlanRewriter`, `PolicyStore`, OPA backend, and Collibra are "Implemented," while L216, L404, L483, L526 describe them as designed-but-not-built. Pick one truth. Given the rest of the chapter and the project memory (Phase 5, OPA "designed for"), the table rows should read "Designed / Phase 5" not "Implemented."
3. **L18** `We considered this for about ten minutes.` -- good dry understatement, keep. (Noting it as a positive only because it is the kind of earned humour the voice guide asks for.)
4. **L91** `The interface is deliberately minimal. Twenty-six lines of Rust:` -- the trait shown (L94-103) is 9 lines, not 26. "Twenty-six lines" recurs at L603. Either the count refers to the full file with imports/attrs (then say so) or it is wrong. As written a reader counts 9 and distrusts the number.
5. **L296-298 `.fieldreport`** and **L605-607 `.ailog`** both tell the same story: the predicate-pushdown test passed on first run because DataFusion respects expression boundaries. Two callouts making the identical point in one chapter is repetitive. Consider cutting the claim from one (the `.ailog` can keep the single-pass-projection bug, drop the duplicate pushdown-test sentence).
6. **L487** `Collibra Protect, which I wrote about in an earlier article, does something similar for Snowflake.` -- "I" is correct per voice guide for personal reference, but "an earlier article" is vague. If it is citable, name it; if not, "in earlier writing" reads less like a dangling reference.
7. **L493** `This is an important architectural property.` -- filler opener pointing back with "This". Name it: `Stateless workers are the architectural payoff.` Same weak pattern appears milder at L86 and L356.

## Mechanical violations (PROSE only)
None. No emdash, endash, unicode arrows, or emoji in prose. The `<--` markers at L62, L314 are inside code fences (allowed). Prose uses "becomes"/"leads to" correctly; "->" only appears in code contexts.

## Exclamation marks in prose
None.

## Continuity data
### Concepts INTRODUCED / defined here
- PolicyEnforcer trait -> evaluate(user, plan) contract
- PassthroughEnforcer -> no-op default enforcer
- PolicyPlanRewriter -> three-step plan rewrite
- ResolvedPolicy -> per-table filters/masks/restrictions
- PolicyStore trait -> governance-platform integration point
- Column mask types -> Redact / Hash / Nullify / Custom
- Row filter -> invisible conjunctive predicate
- Column restriction -> deny-by-omission column removal
- Deny by omission -> sovereignty principle
- Predicate pushdown boundary -> masks block pushdown structurally
- StatementKind::Policy -> coordinator routing variant
- SHOW EFFECTIVE POLICY -> resolved-policy inspection statement

### Concepts ASSUMED (used as if already known)
- JWT validation, identity propagation, S3 scoping, SessionUser (ch4 -- explicitly cross-referenced)
- Polaris REST catalog table-level access
- DataFusion LogicalPlan / optimizer / transform_down / PushDownFilter
- Distributed coordinator/worker split, plan fragments, bearer token passthrough (likely distributed chapter)
- TPC-H Query 1 (L423, used as a benchmark unit without intro)
- moka async cache
- CTAS / MERGE / DELETE write path (implied, not central here)

### Key factual / numeric claims
- "Twenty-six lines of Rust" trait (L92, L603) -- shown trait is 9 lines; cross-check against actual sqe-policy source
- "Phase 5 ... 50+ items" task breakdown (L404)
- "14 integration tests planned" (L580)
- "target is less than 5 milliseconds of overhead on the cached path, measured against TPC-H Query 1" (L423)
- "tens of milliseconds per query" OPA HTTP cost (L409)
- cache_ttl_secs = 60, cache_max_entries = 10000 (L415-416)
- PostgreSQL RLS introduced in version 9.5 (2015) (L504)
- Oracle VPD "predates PostgreSQL RLS by over a decade" (L506)
- "policy_enforcer.evaluate() ... since the first commit / first version of the query pipeline" (L404, L532)
- mask types table: Redact/Hash/Nullify/Custom (L270-275)
- Implementation Status table (L519-530) -- claims OPA backend, PolicyPlanRewriter, PolicyStore, Collibra all "Implemented" -- CONTRADICTS L216/L404/L483/L526 prose. Flag for cross-chapter and source verification.
- OPA endpoints: PUT /v1/data/sqe/grants/..., POST /v1/data/sqe/authz (L366, L368)

### Cross-references
- L5 `Chapter 4 made sure of that` (back-ref, authn)
- L107 `the same identity from Chapter 4` (back-ref, SessionUser)
- L68, L86, L338, L512 PostgreSQL RLS model (recurring anchor, internal)
- L487 `an earlier article` (external, vague back-ref to Collibra Protect piece)
- No explicit forward "we'll cover in ch Y" promises; distributed-execution section (L489-497) assumes the distributed chapter without naming it.

## Pacing
Flows well overall; short/long alternation is healthy and headers read as a clean outline. Two soft spots: the chapter relitigates the predicate-pushdown property three times (Property 2 at L79-81, the `.fieldreport` at L297, and the whole "Predicate Pushdown Boundary" section at L537-567 plus the `.ailog`), which starts to feel like padding by the third pass. The OPA/Cedar/PolicyStore/Collibra stretch (L359-487) is the densest run and could lose the "did not emerge from a vacuum" framing (L502) to tighten. No true walls of text; longest paragraphs stay near 5 sentences.

## Grade
Voice adherence: A-. Clean mechanics, strong hook and close, earned dry humour, opinionated and direct in the right places. Held back from A by one throat-clearing line (L539), the triple-repeated pushdown point, and one filler "This is an important..." opener. The Implementation-Status-vs-prose contradiction (L519-530 vs L216/L404/L526) is a factual-drift problem the author must reconcile, but it is consistency, not voice.
