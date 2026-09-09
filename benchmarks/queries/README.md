# Benchmark query text: provenance

`sqe-bench` sends these files as committed. It no longer rewrites table names
in the SQL (review 707, issue #627): the default schema rides on the
connection instead (`X-Trino-Schema` on the Trino protocol; one
`USE [catalog.]schema` on the session behind the bearer token on Flight SQL,
which the coordinator stores so the per-query connections share it). The one
exception is `--qualify-tables always|auto`, a tokenizer-based qualifier
for connections that cannot carry a default schema (a Flight client without a
bearer token has no session to scope); see `crates/sqe-bench/src/query.rs`. Because the text is no longer rewritten, an
alias that happens to share a table's name is fine again, and three files
that had been edited to dodge the old string rewriter were restored (below).

Each file starts with a header the runner reads (`-- name:`, `-- timeout:`,
optional `-- requires:`); everything after it is the statement.

## Sources per suite

| Suite | Files | Text source | Known deliberate deviations |
|---|---|---|---|
| `tpch/` | q01..q22 | TPC-H 3.0.1 reference queries with the qualification substitution parameters, written in DataFusion-compatible SQL (`DATE` literals, `INTERVAL 'n' DAY`), commit `599dd3c4`. | q11: `FRACTION` is computed as `1.0 / COUNT(*) FROM supplier` instead of the SF1-only literal `0.0001`, so the query is correct at every scale (commit `ccbb7d7b`, verified against DuckDB dbgen oracles). |
| `ssb/` | q1.1..q4.3 | Star Schema Benchmark (O'Neil et al.) 13 queries, commit `f8ac622b`. | The date dimension is `ddate`, not `date`, to avoid the reserved word. |
| `tpcds/` | q01..q99 | TPC-DS qualification queries, hand-transcribed to DataFusion-compatible SQL, commit `5440aec1`; 10 files re-aligned to the official qualification parameters in `7068a6cb` (q08, q24, q41, q43, q44, q54, q63, q64, q85, q91) and verified against dsdgen SF1 in DuckDB. | Six ROLLUP queries carry `-- requires: rollup`. Compatibility edits recorded in `3dcbbd73` (q05 explicit UNION aliases, q08 zip join, q49 explicit `JOIN date_dim ON`, q54 CTE column disambiguation, q59 `date_dim d` alias, q64 typo, q90 `NULLIF` guard). q49 and q51 aliases restored to the reference names, see below. |
| `tpcbb/` | q01..q10 | TPC-BB (BigBench) SQL-only subset, written for this harness against the TPC-DS tables plus `web_clickstreams` / `product_reviews`, commit `42580eb3`. | q08 wraps the projection in a derived table so the computed alias can be filtered in `WHERE` (`adc648d6`). |
| `tpcc/` | 17 files | TPC-C transaction profiles rewritten as read queries plus per-statement DML, authored for this harness (`56098bfc`, `028d491b`). | `history` is `hist` (Polaris reserved word). Not a canonical query text; TPC-C has none. |
| `tpce/` | 18 files | TPC-E transaction profiles rewritten as analytical read queries plus DML, authored for this harness (`abb88fee`, `028d491b`). | Not a canonical query text; TPC-E has none. `broker_volume` alias restored, see below. |
| `clickbench/` | q00..q42 | ClickBench official 43 queries, commit `bb189ce7`; CamelCase columns are double-quoted (`3dcbbd73`). | q18 minute extraction uses integer arithmetic on the Int64 `EventTime`; q18/q28 avoid an alias in GROUP BY for Trino (`b89c222c`). |
| `bank/` | q01..q08 | SQE bank demo queries, authored for this harness (`9b07c37d`). | Not a public benchmark. |

## Restored in review 707

Commit `0ce58763` ("rename column aliases that clash with table names in
benchmark queries") changed alias names only so the old `prefix_tables`
string rewriter would stop qualifying them. With the rewriter gone from the
default path, the pre-`0ce58763` text is back:

- `tpcds/q49.sql`: `AS item_sk` -> `AS item` in the three channel CTEs and
  the final `SELECT` / `GROUP BY` / `ORDER BY` (the TPC-DS reference alias).
- `tpcds/q51.sql`: `web_cume` / `store_cume` -> `web_sales` / `store_sales`
  (the TPC-DS reference aliases).
- `tpce/broker_volume.sql`: `AS tt_name` -> `AS trade_type` in the
  projection and `ORDER BY` (the harness author's original alias).

The other edit in that commit (q54 `customer.c_customer_sk`) resolved a real
column ambiguity introduced by the transcription's `AS c_customer_sk` alias,
not a rewriter artifact, and is kept.
