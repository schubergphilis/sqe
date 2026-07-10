# Trino-wire compatibility for BI tools

Design doc. Source: data-platform handoff (EnergyCo demo, sub-project 4). Turns four
Trino-protocol gaps into two themed PRs so stock BI tools (Superset, Metabase, DBeaver,
Tableau, anything on the Trino JDBC driver) can reflect schema and run parameterized
queries against SQE without client-side shims.

## Verified state on `main` (not the handoff's older build)

Three read-only exploration passes plus git archaeology established the real state. The
handoff was tested against an older image; several "broken" repros already have handling.

| Handoff issue | Reality on `main` | Verdict |
|---|---|---|
| #1 prepared statements | `PREPARE <name> FROM <sql>` is already intercepted in `parse_session_statement` (protocol.rs:371) and round-trips via `x-trino-added-prepare`. But `EXECUTE <name>` is unrecognized, `EXECUTE IMMEDIATE` unsupported, `X-Trino-Prepared-Statement` header unread, and the emit side does not URL-encode. | Real gap |
| #2 unqualified `information_schema` | Resolves to the config-derived default catalog (`resolve_default_catalog()`, session_context.rs:199), never `session.default_catalog` (set from `X-Trino-Catalog` but unused for name resolution). | Real gap |
| #3 type names | Already fixed for the custom provider (#99, commit 4990003: `iceberg_to_sql_type_info`). The handoff's Arrow names appear because the wrong catalog (#2) routed the query to DataFusion's built-in `information_schema`. | Symptom of #2 (pending check) |
| #4 `DESCRIBE` | No `Statement::Describe` arm in the classifier; falls through to "Statement type not supported". | Real gap |

### The empirical question: ANSWERED (discriminator, 2026-06-26)

`tests/it/trino_metadata_test.rs` against the live stack is definitive: **DataFusion's
built-in `information_schema` shadows the custom provider.** Both unqualified AND
catalog-qualified `iceberg.information_schema.columns` return Arrow type names
(`Int32`, `Utf8`, `Decimal128(10, 2)`, `Date32`, `Timestamp(µs)`); `SHOW COLUMNS` too.
`with_information_schema(true)` (session_context.rs:199) wraps every catalog and intercepts
the `information_schema` schema name, so SQE's custom `InformationSchemaProvider` never
serves SQL queries.

Consequences:
- **#3 is NOT fixed in practice.** Commit #99's `iceberg_to_sql_type_info` is dead code.
- **Security regression**: the policy-filtering work on `information_schema.columns`
  (`a8444d4` fail-closed, `a40955b` per-caller namespace visibility, `53a5d5e` last-component
  policy) is also inert. Metadata reflection is not policy-filtered.
- **#2 confirmed**: the built-in is global; it merges all catalogs (incl. `system`'s
  `jdbc`/`metadata`/`runtime`), so BI tools filtering by `table_schema` only see system
  internals.

This collides with the "keep it standard with DataFusion" steer: the standard built-in is
exactly what produces Arrow names + global merge + inert policy. The PR-A direction is now a
maintainer decision (see fork below).

### PR-A decision: compat layer on top of DataFusion (chosen)

Keep DataFusion's built-in `information_schema` (standard; survives without live Iceberg
connectivity) and add a Trino-compatibility layer that normalizes metadata results on the
**Trino HTTP boundary only**. Implemented in `sqe-trino-compat::info_schema_compat`:

- **Type names (#3)**: `arrow_display_to_trino_type` maps the built-in's Arrow display
  strings (ground-truthed from the live stack: `Int64`, `Utf8`, `Decimal128(10, 2)`,
  `Date32`, `Time64(µs)`, `Timestamp(µs)`, `Timestamp(µs, "+00:00")`, `LargeBinary`, ...)
  to Trino names. The `data_type` column of metadata results is rewritten.
- **Catalog scoping (#2)**: scope the listing to the session catalog (`X-Trino-Catalog`)
  when set; otherwise drop the engine-internal `system`/`datafusion` catalogs so BI tools
  don't see them.
- **Gating**: applied only when `is_metadata_query(sql)` (references `information_schema`,
  or is `SHOW COLUMNS` / `DESCRIBE`), at the `batches_to_trino` call site in `submit_query`.
- **DESCRIBE (#4)**: classified as Utility and aliased to the shared `columns_for_table`
  (same as SHOW COLUMNS) in the coordinator, so it flows through the same path and inherits
  the type translation.

**Known limitations (documented, accepted for this pass):**
- Covers the **Trino HTTP path only**. Flight SQL `DoGet` metadata queries (e.g. dbt-sqe's
  reflection over ADBC) still return Arrow names. Fixing those would require reviving the
  custom provider (the rejected "un-shadow" option) -- tracked separately.
- Type translation matches DataFusion's Arrow display strings, so a DataFusion upgrade that
  changes that rendering needs the mapper updated. The live discriminator test guards this.
- Detection is by SQL shape + a `data_type` / `table_catalog` column; a metadata query that
  aliases `data_type` to another name is not translated. BI tools use the standard shapes.
- Catalog scoping filters by exact session-catalog match; an explicitly cross-catalog
  qualified `other_cat.information_schema...` while a different session catalog is set would
  be over-filtered. BI reflection does not do this.

The deeper finding -- that the custom provider's #99 type mapping and `information_schema`
policy filtering are inert behind the built-in -- remains open as a security follow-up.

## Decomposition: two themed PRs

### PR-A: information_schema / metadata read path (#2 + #3 + #4)

One concern: make Trino-protocol schema reflection resolve to the session catalog and
emit Trino SQL type names. Exact mechanism finalized once the discriminator lands:

- **If the custom provider wins** (expected): #3 is already correct. PR-A makes an
  unqualified `information_schema` reference resolve to `session.default_catalog`. The
  DataFusion-standard lever is to set the default catalog **per query**, not on the cached
  `SessionContext` (the context is cached by `username:token_hash`, NOT catalog -- two
  Trino clients sharing a token with different `X-Trino-Catalog` would otherwise collide).
  Candidate: resolve the table reference / set the active default catalog on the query's
  state from `session.default_catalog` before planning.
- **If the built-in shadows it**: PR-A additionally routes Iceberg-catalog
  `information_schema` to the custom provider (or maps types on the built-in path). Larger;
  inventory consumers first (dbt reflection, `audit_e2e_test`, Flight SQL GetTables/GetColumns).
- **#4 DESCRIBE**: add a `Statement::Describe` arm in the coordinator's Utility branch
  (query_handler.rs:818) that aliases to the existing `handle_show_columns()` path, so it
  inherits the same SQL type names. Trivial, rides the #3 fix.
- Sub-gap: SQE emits bare `timestamp`; Trino BI prefers `timestamp(6)`. Trino parses bare
  `timestamp` as `timestamp(3)`, so acceptable; include precision only if the discriminator
  shows a client actually breaks on it.

### PR-B: prepared statements over Trino HTTP (#1)

**Key realization: Trino prepared statements are STATELESS server-side.** The client
carries the prepared SQL in the `X-Trino-Prepared-Statement: name=<urlencoded-sql>` header
on every `EXECUTE` request (it accumulates them from `x-trino-added-prepare` responses).
So SQE needs a *rewrite* step, not a server-side store:

1. **Shared helper**: move `substitute_placeholders` (flight_sql.rs:2338, private) to
   `sqe-core` as `pub fn substitute_placeholders(sql, &[String]) -> Result<String,String>`.
   Both Flight SQL and Trino reuse it. Carry its existing three unit tests.
2. **`sqe-trino-compat` pure functions** (unit-tested, TDD RED first):
   - `parse_prepared_statements(header_values: &[String]) -> HashMap<String,String>`:
     split each header value on top-level commas (URL-encoding escapes embedded commas as
     `%2C`, so this is safe), then `form_urlencoded`-decode each `name=value` (handles both
     `+`->space and `%XX`, matching Java `URLEncoder` used by the Trino client).
   - `rewrite_execute(sql, &prepared) -> Result<Option<String>, String>`:
     - `EXECUTE IMMEDIATE '<sql>' [USING v1, v2]` -> take inner SQL, substitute.
     - `EXECUTE <name> [USING v1, v2]` -> look up `<name>` (Err if absent), substitute USING
       args into the template's `?` placeholders.
     - anything else -> `Ok(None)` (caller proceeds normally).
   - `parse_using_args(after_using) -> Vec<String>`: split on top-level commas respecting
     single quotes and parens; each arg is a SQL literal expression used verbatim.
3. **Emit fix**: `apply_session_headers` (server.rs:498) currently does
   `format!("{name}={sql}")` with no encoding -- SQL with spaces/commas/`=` corrupts the
   header. URL-encode the SQL value to interoperate with real JDBC drivers.
4. **Wiring** in `submit_query` (server.rs:734): read `x-trino-prepared-statement`
   header(s) -> map; `match rewrite_execute(sql, &map)` -> Some(rewritten) executes the
   concrete SQL, None executes `sql` as today, Err returns a Trino USER_ERROR.

`EXECUTE IMMEDIATE` over Flight SQL is out of scope (Trino-focused). `DEALLOCATE`/`PREPARE`
already round-trip; PR-B only adds the encode fix and the EXECUTE resolution.

## Testing

- **PR-A**: `trino_metadata_test.rs` (the discriminator) becomes an asserted integration
  test: unqualified `information_schema.columns` returns SQL type names and the session
  catalog's tables; `DESCRIBE` matches `SHOW COLUMNS`. Unit tests for any Arrow->Trino
  precision mapping added.
- **PR-B**: unit tests for `parse_prepared_statements`, `rewrite_execute`,
  `parse_using_args`, the moved `substitute_placeholders`, and the emit URL-encoding.
  An integration test over the Trino HTTP path: `PREPARE` then `EXECUTE ... USING` with the
  header replayed, asserting the bound query runs.

## Acceptance criteria (from the handoff)

1. `catalog=<x>` + unqualified `information_schema.{tables,columns}` returns that catalog's
   metadata (#2).
2. `data_type` and `SHOW COLUMNS` return Trino SQL type names (#3).
3. A parameterized `... WHERE x = ?` succeeds via the Trino JDBC driver and the SQLAlchemy
   `trino` dialect with no client flags (#1).
4. `DESCRIBE <table>` matches `SHOW COLUMNS` (#4).
5. End-to-end: stock Superset and Metabase add the SQE database, sync `ws_energy_co.gold`
   with correct types, and render a chart over the read-only ROPC viewer.
