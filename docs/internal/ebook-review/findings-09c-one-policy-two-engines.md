# Findings: 09c-one-policy-two-engines.md

## Thesis
SQE adds a Ranger fine-grained policy backend that reuses the chapter 9 plan-rewrite enforcer, and because it reads the same `hive` service-def Spark reads, the same masking policy produces byte-identical output in both engines.

## Opening
> The mask is written once, in Ranger.
> SQE and Spark read the same rule and return the same byte.
Verdict: strong hook. Epigraph states the whole payoff in two short lines; the body's first real paragraph (L6) does some chapter-recap throat-clearing but the epigraph carries the open.

## Closing
> That is what a shared service-def buys, and it is the strongest argument for putting fine-grained enforcement in the query plan instead of anywhere else.
Verdict: lands it. Ties the byte-exact result back to the chapter 9 thesis without restating; opinionated and earned. (Note: the literal last block is the AI Logbook callout L193, which also closes well.)

## Voice & editorial issues
1. L6: "This chapter closes the loop with a third enforcement backend that does what chapter 9 described, against Apache Ranger, and then proves something..." Mild throat-clearing / chapter-summary preamble after a strong epigraph. The sentence is also long (three+ clauses). Rewrite: split. "Chapter 9 built the plan-rewrite enforcer with two backends: OPA and Cedar. This chapter adds a third, Apache Ranger. It also proves something OPA and Cedar never could: the same policy enforces identically in a different engine."
2. L15: "so it is worth pinning the difference down before anything else." Close cousin of the forbidden "it's worth noting." Rewrite: "...operators confuse them constantly. Pin the difference down first."
3. L70: "Here is the divergence from the catalog path that the previous chapter flagged." Reads slightly like a stage-direction transition. Acceptable, but tighter: "The previous chapter flagged a divergence here." Minor.
4. L130: "One honest limitation." Good, signature transparency. No change. (Listed as a positive, not a fix.)
5. L74 is a 4-sentence wall of dense mechanism with little white space; consider a line break before "The masking expression keeps the column's Arrow type..." to let the type-safety point breathe. Minor pacing nit, not a voice violation.

## Mechanical violations (PROSE only)
none

## Exclamation marks in prose
none. (L171 "MR !386" is an identifier, not an exclamation.)

## Continuity data
### Concepts INTRODUCED / defined here
- `RangerStore` -> Ranger policy backend (`crates/sqe-policy/src/ranger_store.rs`)
- `hive` Ranger service -> shared fine-grained service-def
- `tag` Ranger service -> linked mask-per-tag rules
- `ResolvedPolicy` struct -> row_filters / column_masks / restricted_columns
- `item_matches` -> user/role policy-item matcher
- mask vocabulary -> MASK_NULL, MASK_HASH, MASK, MASK_SHOW_LAST_4, MASK_SHOW_FIRST_4, MASK_DATE_SHOW_YEAR, CUSTOM, MASK_NONE
- `map_mask` -> Ranger dataMaskType -> SQE MaskType translator
- five session-context UDFs -> current_user(), is_role_in_session(), current_available_roles(), current_database(), current_schema() (`crates/sqe-policy/src/session_udf.rs`)
- `sqe.column-tags` -> Iceberg table-property tag association
- precedence contract -> restricted > resource mask > tag mask merge order
- byte-exact parity (SQE vs Spark) -> source of truth for this claim

### Concepts ASSUMED (used as if already known)
- `PolicyPlanRewriter` / plan rewriting before optimizer (ch9)
- PassthroughEnforcer (ch9)
- OPA + Rego backend, Cedar backend (ch9)
- catalog path / `[access_control] backend = "ranger"`, `polaris` service-def, Polaris embedded authorizer (previous chapter, 09b)
- SQL GRANT/REVOKE (earlier)
- LogicalPlan, Filter nodes, projection, predicate pushdown (DataFusion, assumed competence)
- moka TTL cache
- token realm_access.roles / OIDC bearer passthrough (auth chapters)
- circuit breaker
- DataFusion const-folding, Volatility::Immutable
- Kyuubi / Spark Authz / RangerSparkExtension

### Key factual / numeric claims
- policyType codes: 0 = access, 1 = DATAMASK, 2 = ROWFILTER (L58)
- download endpoint: GET /service/plugins/policies/download/hive with HTTP basic auth (L58)
- flat public-v2 /api/policy endpoint insufficient (L58)
- namespace flattens to last dotted component: sales_wh.sales -> database sales (L72)
- orders* glob patterns NOT matched in this version (L72)
- 111-11-1111 with MASK_SHOW_LAST_4 -> xxx-xx-1111 (L109)
- parity SQL: SELECT id, ssn FROM sales_wh.sales.orders; outputs xxx-xx-1111/2222/3333 as bob (L160-169)
- validated in MR !386 (L171)
- current_database()/current_schema() fold to NULL inside Ranger policy expressions (MVP limitation) (L130)
- Spark fix: spark.sql.catalogImplementation=hive (L176)
- validated matrix: Spark 3.5.4, Iceberg Spark runtime 1.8.1, Kyuubi Spark Authz 1.11.1, Ranger 2.8, Polaris 1.5.0 (L188)
- kyuubi-spark-authz_2.13 unpublished; Spark 4 / Scala 2.13 not off-the-shelf feasible (L188)
- config defaults: timeout-secs = 5, cache-ttl-secs = 30 (L42-43)
- cache keyed by username, namespace, table, sorted role list (L87)
- rationale doc: docs/ranger-tag-storage-decision.md (L148)

### Cross-references
- L6: "Chapter 9 built the plan-rewrite enforcer..." (back-ref ch9)
- L6, L8: "The setup is the one from chapter 9" (back-ref ch9)
- L6, L70, L72: "The previous chapter" (back-ref 09b)
- L46: "the chapter 9 thesis" (back-ref)
- L74: "The rewrite is the chapter 9 mechanism" (back-ref)
- No forward refs.

## Pacing
Flows well overall; tight, scannable headers tell the story. L74 is the densest paragraph (4 sentences of stacked mechanism) and could use one break. The byte-exact section and its two callouts (Field report, AI Logbook) build to the payoff cleanly. No walls of text.

## Grade
Voice adherence: A. Strong epigraph hook, signature transparency ("one honest limitation," fail-closed callout, the "byte-exact is a claim you earn" line), no mechanical violations, opinionated close. Only minor preamble at L6 and one near-miss filler at L15 keep it from being flawless.
