# Findings: 09b-grant-that-polaris-enforces.md

## Thesis
SQE offers a second, catalog-based security model where SQE translates `GRANT`/`REVOKE` into Apache Ranger policies but does not enforce them; Polaris (via its embedded Ranger authorizer) decides who may load a table. The chapter argues the grant protocol is trivial and the Polaris identity model is the real work.

## Opening
> "Chapter 9 put the security boundary inside the query plan. Row filters become `Filter` nodes, column masks become projection expressions, denied columns vanish from the schema."
Verdict: strong hook. Opens by contrasting against the prior chapter's model and immediately introduces the second model where "the engine is not the boundary at all." The epigraph ("The engine writes the policy. It does not enforce it.") sets the thesis in two lines.

## Closing
> "It is where Ranger becomes a third policy backend alongside the OPA and Cedar enforcers from chapter 9, and where the same policy you write once enforces identically in SQE and in Apache Spark."
Verdict: lands it. Forward-points to the next chapter with a concrete payoff (write-once, enforce in SQE + Spark) rather than summarizing. The `.ailog` callout follows but reinforces the chapter's theme rather than trailing-summarizing it.

## Voice & editorial issues
1. L139: "The behavior is the same either way (granted users read, ungranted users do not), but the check that enforces it differs" -- the parenthetical does real work and could be its own short sentence for the rhythm the voice guide prizes. Minor/optional.
2. L124: long sentence "A write loads the table then commits a snapshot, which fans out into snapshot, schema, sort-order, partition-spec, and properties commit operations, every one a separate check." Four+ clauses; past the "three clauses is plenty" guideline. Rewrite: split after "commits a snapshot."
3. No forbidden words found. No hedging, no throat-clearing, no rhetorical-question transitions, no filler transitions. Unusually clean on voice.

## Mechanical violations (PROSE only)
none

## Exclamation marks in prose
none. (`admin-password = "rangerR0cks!"` L91 is inside a code fence, excluded.)

## Continuity data
### Concepts INTRODUCED / defined here
- catalog-based security model -> Polaris-enforced GRANT
- `RangerGrantBackend` -> SQE-to-Ranger write backend (`crates/sqe-policy/src/grants/ranger.rs`)
- `access_control.backend = "ranger"` -> config selector for Ranger path
- realm / `root` resource (`[access_control.ranger] realm = "*"`) -> Polaris root-resource match value
- `map_sql_to_ranger_access` -> SQL privilege -> Ranger access-type expansion
- privilege fan-out (SELECT=3, INSERT=23 access types) -> compensates for ignored implied-grants
- LOAD_TABLE / `table-properties-read` as effective read gate -> SQE uses own S3 creds
- baseline traverse set -> default per-user access types granted
- `validate_identifier` -> rejects `/ ? # %` backslash, whitespace, control chars

### Concepts ASSUMED (used as if already known)
- plan rewriting / `Filter` nodes / column masks / deny-by-omission (ch9)
- OPA and Cedar enforcers (ch9)
- OIDC / Keycloak bearer token, `preferred_username`, `realm_access.roles`
- Apache Polaris REST catalog, Iceberg tables, S3/Parquet
- Ranger usersync, LDAP/AD/SCIM, service-def concept

### Key factual / numeric claims
- "Polaris 1.5" + `polaris.authorization.type=ranger`; embedded authorizer "still marked Beta in the 1.5 line"
- "Apache Ranger 2.8 through the new authorizer API"
- `DefaultAuthenticator` is "the only authenticator in Polaris 1.5"
- quickstart bootstrap principals: `alice`, `bob`, `carol`, `dave`
- quickstart roles: analyst -> alice,bob,carol; engineer -> bob,carol; sqe_admin -> carol
- SELECT -> 3 access types; INSERT -> 23 (table cell says "22 types" + table-data-write)
- ALL -> `catalog-content-manage`; USAGE -> namespace-list + namespace-properties-read
- baseline traverse set omits `table-properties-read`
- config: `url = "http://ranger-admin:6080"`, `service-name = "polaris"`, `realm = "*"`
- Keycloak realm = `iceberg-ranger`
- docs referenced: `docs/polaris-principal-provisioning.md`
- crate path: `crates/sqe-policy/src/grants/ranger.rs`
- `GRANT ... TO GROUP` rejected with `NotImplemented`; principal-role ops "unmapped"
- COUNT NIT: table L117 says INSERT "(22 types)"; prose L124 says "twenty-three." 22 commit types + table-data-write = 23. Internally consistent but a reader may stumble.

### Cross-references
- L6/L35/L37/L39/L42/L144/L152: back-refs to "chapter 9" (09-what-you-cant-see.md) -- accurate.
- L152/L154-156: forward-ref "the next chapter" = 09c-one-policy-two-engines.md (Ranger third backend + SQE/Spark parity) -- accurate per ordering and 09c title.

## Pacing
Flows well. Headers form a readable outline. No walls of text; paragraphs stay 3-5 sentences. The `.deadend`, `.fieldreport`, `.sovereignty`, `.ailog` callouts break density. The access-type table earns its place.

## Grade
Voice adherence: A. Clean of forbidden words and AI tells, strong hook and close, transparent about dead ends and limitations as the voice guide prescribes; only nits are two slightly over-long sentences and a 22-vs-23 count that is correct but could trip a reader.
