# 710: The Trino compat layer has no wire-level test that runs anywhere, and the source-level rewriters are not tested against quoted identifiers, literals or comments

- Scope: sqe-trino-compat, sqe-sql rewriters, scripts/trino-compat-test.sh
- Severity: medium
- Category: testing
- Effort: M (days)
- Status: open
- Issue: #630

## Problem

`crates/sqe-trino-compat` has a `src/` tree only; there is no `tests/` directory (verified by `ls`). All 3700 lines of `server.rs` are covered by in-file unit tests that construct `TrinoState` with mock authenticators and executors (`server.rs:1797-3707`). Nothing exercises a real Trino client (trino-cli, JDBC, trino-python-client) against the server, and nothing exercises the coordinator's real `QueryHandlerAdapter` through the HTTP layer.

The two scripts that would do this both require hand-started stacks and are not wired to `make test-integration`:

- `scripts/trino-compat-test.sh:1-60` needs SQE on 28080, Trino on 38080, trino-cli, curl, python3, and fetches a Polaris root token from a hard-coded `client_secret=s3cr3t`.
- `scripts/trino-parity-test.sh` runs `sqe-bench compare` and only counts rows.

The pre-parse rewriters that run on every Trino statement are gated on "parse fails, then parse succeeds" and implemented as character scanners:

- `crates/sqe-sql/src/nested_row_cast.rs:55-92` scans for `cast(` and `matching_paren` (`:304-330`) tracks single quotes but not double-quoted identifiers or comments. A `)` inside a `-- comment` or inside `"a)b"` mis-balances the scan.
- `crates/sqe-sql/src/bare_table.rs:48-80` tokenizes correctly but has no test with a leading comment or a quoted table named `"table"`.
- `crates/sqe-sql/src/paren_less_values.rs` tests (`:233-317`) cover one literal-with-comma case and no comments.
- `crates/sqe-trino-compat/src/info_schema_compat.rs:46-84` has no comment or literal-followed-by-dot test (see 704).

`sqe-sql` already depends on `proptest` (`crates/sqe-sql/Cargo.toml:17`) but none of these modules uses it. The rewriters' key invariant, "a statement that already parses is returned byte-identical", is a one-line property test.

## Why it matters

The Trino path is the BI and dbt-trino entry point. Every fix in this scope (issues 700 through 709) will be verified only by unit tests with mocks unless a wire test exists. The memory of the tempto sweep ("~85% environmental") shows the team has tried to borrow Trino's own suite and found it too noisy; a small, owned, deterministic wire test is the missing middle layer. Rewriter regressions on quoted identifiers are exactly the class that unit tests with hand-picked inputs miss and that production SQL from BI tools (which quote everything) hits.

## Proposed change

1. Add `crates/sqe-trino-compat/tests/wire.rs` that starts `start_trino_server_with_options` on an ephemeral port with an in-process DataFusion executor (no Polaris), and drives it with `reqwest` following the protocol as a client would: POST, follow `nextUri` including the queued route, DELETE, `X-Trino-Prepared-Statement` replay, `USE` header echo, Basic and Bearer auth. Assert full JSON shapes against fixture files (golden tests). This runs in `cargo test` with no stack.
2. Add one `make test-integration` case that runs `trino-cli --server http://localhost:28080 --execute` against the coordinator for a fixed set of statements and diffs output against goldens. trino-cli is a single jar; the integration stack already has Java for Polaris.
3. Add proptest properties to each rewriter module: (a) if `Parser::parse_sql(sql).is_ok()` then `rewrite(sql) == sql`; (b) for any generated statement wrapped in a string literal or comment, the literal/comment bytes are unchanged in the output.
4. Add explicit unit cases: leading `--` and `/* */` comments, `"table"` as identifier, `)` inside comments and quoted identifiers, `information_schema.` inside a literal.

## Acceptance criteria

- `cargo test -p sqe-trino-compat` runs a wire-level test with a real HTTP round trip and golden JSON.
- `make test-integration FILTER=trino_cli` passes and is listed in `docs/internal/...` local gate notes.
- Each of `nested_row_cast.rs`, `bare_table.rs`, `paren_less_values.rs`, `info_schema_compat.rs` has a proptest asserting the identity property for already-parseable input.
- The hard-coded `s3cr3t` in `scripts/trino-compat-test.sh:49` is replaced by an environment variable.

## Related

- 700, 704, 708 (the code under test).
- `docs/internal/reviews/2026-07-25-production-readiness-review.md` item 15 ("Add integration tests for sqe-trino-compat") still open; item 17 (33 percent coverage) still applies.
- Memory: Trino compat testing (tempto sweep environmental; allow-list plus parity for signal).
