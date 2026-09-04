# 107: `parse_and_classify` is a keyword-prefix dispatcher ahead of the parser and silently drops every statement after the first

- Scope: sqe-sql (classifier)
- Severity: medium
- Category: reliability
- Effort: M (days)
- Status: open

## Problem

`parse_and_classify` (`crates/sqe-sql/src/classifier.rs:383-715`) is 330 lines
of ordered string tests that run before sqlparser sees the text: `upper ==
"SHOW CATALOGS"`, `strip_prefix_ci(trimmed, "SHOW CREATE SCHEMA ")`,
`"EXPLAIN FULL "`, `"SHOW STATS FOR "`, `"SHOW PARTITIONS"`, `"ANALYZE "`,
`"SHOW EFFECTIVE GRANTS FOR USER "`, `"SHOW TAGS"`, the whole `ALTER TABLE`
family (`:572-637`, seven sub-parsers tried in a documented order), `ATTACH`,
`DETACH`, `CREATE SECRET`, `DROP SECRET`, `SET WRITE_BRANCH`, then two text
rewrites that recurse into the function itself (`rewrite_set_properties` at
`:581`, `rewrite_create_table_like` at `:692`). Each branch has its own
tokenizer (`trim_end_matches(';')`, `split_whitespace`, `trim_matches('"')`),
so quoting, comments and whitespace are handled differently per statement
type. Issue 105 is one consequence.

After the pre-scans, the parser result is truncated:

`classifier.rs:697-714`:

```rust
let statements = Parser::parse_sql(&dialect, sql)
    .map_err(|e| sqe_core::SqeError::Execution(format!("Parse error: {e}")))?;
...
let stmt = statements
    .into_iter()
    .next()
    .ok_or_else(|| sqe_core::SqeError::Execution("Empty SQL".to_string()))?;
classify(stmt)
```

Only `statements[0]` is classified and boxed into the `StatementKind`. For DDL
and DML kinds the coordinator executes that boxed statement (for example
`Statement::CreateTable(ref ct)` at `:722`, consumed as `*stmt` in the tests
at `:1990`). For `Query` the original full text goes to `ctx.sql`, where
DataFusion rejects multi-statement input. So `CREATE TABLE t (...); INSERT
INTO t SELECT ...` creates the table, returns success, and never runs the
insert; `SELECT 1; SELECT 2` errors. There is no test for multi-statement
input in `classifier.rs` (grep for `;` in test SQL finds none).

Two smaller instances of the same "take the first, ignore the rest" shape:
`Statement::Truncate` uses `table_names.first()` (`classifier.rs:906-912`),
so `TRUNCATE a, b` truncates `a` only; `Statement::Use` round-trips through
`Display` and `strip_prefix("USE ")` (`:898-901`) instead of reading the AST.

## Why it matters

A client that sends a script (dbt hooks, migration tools, a psql-style
`-f` file over the Trino CLI) gets a success status for work that was not
done. That is a silent write-path correctness problem, not a parse quirk.

The prefix dispatcher means the accepted grammar is defined by string tests in
a particular order rather than by the parser. Adding a statement type means
finding the right position in the chain and hand-writing a tokenizer; every
existing test (`the_tag_parser_does_not_claim_the_masking_policy_forms`
referenced at `:604-606`) exists to pin an ordering accident. Errors from
these branches are `SqeError::Execution("Parse error: ...")` strings, so they
go through the substring classifier in issue 104 rather than being typed
`SyntaxError`.

## Proposed change

1. Reject or support multiple statements explicitly. Simplest: after
   `Parser::parse_sql`, return `SqeError` with code `SyntaxError` when
   `statements.len() > 1`, with a message naming the position of the second
   statement. If script support is wanted later, return
   `Vec<StatementKind>` and let the coordinator run them in order inside one
   session; do not decide that here.
2. Give the SQE-only grammar a real parser entry point instead of prefix
   tests. sqlparser's `Parser` is public and supports peeking tokens:
   `Parser::new(&dialect).try_with_sql(sql)?` then
   `parser.parse_keywords(&[Keyword::SHOW, ...])` on the token stream. Each
   `try_parse_*` becomes a function over `&mut Parser` that consumes tokens,
   which fixes quoting, comments and case handling uniformly and removes the
   byte slicing from issue 105. Statements the parser already handles
   (`SHOW TABLES`, `USE`, `TRUNCATE`) are then classified from the AST only.
3. Return typed errors: `SqeError::sourced(SqeErrorCode::SyntaxError, ..)` from
   the classifier, with the parser error as source.

## Acceptance criteria

- `parse_and_classify("CREATE TABLE t (id INT); INSERT INTO t VALUES (1)")`
  returns `Err` with `error_code() == SyntaxError`, and an integration test
  over Flight SQL asserts the table does not exist afterwards.
- `TRUNCATE a, b` either truncates both or errors; it does not succeed on `a`
  alone.
- `grep -c 'starts_with(' crates/sqe-sql/src/classifier.rs` (non-test) is
  below 5.
- The proptest in `tests/parser_proptest.rs` still passes.

## Related

- `105-unicode-uppercase-byte-slice-panics-in-sql-prescans.md`
- `106-pre-parse-pipeline-fragmented-per-protocol.md`
- `104-error-classification-by-english-substring.md`
