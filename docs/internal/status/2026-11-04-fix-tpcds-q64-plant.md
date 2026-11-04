**STATUS 2026-11-04: TPC-DS q64 returns rows on generated data (`fix/tpcds-q64-plant`, #785). Stacked on `fix/tpcds-spec-dimension-scaling`.**

q64 was the last `VacuousBug` in the SF1 Trino compare: empty on both engines while official SF1 has 2 rows (empty on 10-18, 11-02 and both earlier branches of today). No single filter was wrong. The query needs one item (colour in a six-colour list, price 65 to 74, ~22 items at SF1) that a store sold and had returned in both 1999 and 2000, that is also in `cs_ui` (catalog sales over twice their refunds), and that was sold under a cdemo whose marital status differs from the buyer's current one. Random data gives about five such (item, store) pairs at SF1; our seed gave none.

The coincidence is planted like q17/q24/q25/q29, after every rng draw so the other values keep their draw parity:

- item 6 is purple at 69.00;
- store tickets 7 (1999-sk 500) and 8 (2000-sk 900) sell item 6 at store 1 to customer 7 under cdemo 1 ('S'), with hdemo, address and promotion set so no NULL key drops the row;
- customer 7's current cdemo is 9 ('M');
- store returns rows 4 and 5 return ticket 7's line, row 6 ticket 8's, so `cs2.cnt <= cs1.cnt` holds even if a random return lands on ticket 8;
- catalog order 7 sells 99 of item 6 and catalog returns row 0 returns that line for 10.00, so the pair passes `cs_ui` on its own.

All guarded on `FkDims::q64_active()` (items >= 6, customers >= 7, cdemos >= 9), which holds from SF0.01. The planted catalog return now uses `ReturnHead.line`, like the store one.

Test `q64_coincidence_is_planted` (SF0.01) checks every link: item colour and price, date years, the marital mismatch, the customer's cdemo, both tickets' keys, the return counts, and the catalog pair against the `HAVING`. Generation golden hashes re-recorded: item, customer, store_sales, store_returns, catalog_sales and catalog_returns changed values, no row count changed.

Gate: `cargo test -p sqe-bench` (240 + 41 pass), `cargo clippy -p sqe-bench --all-targets -- -D warnings` clean, and `BENCH_SCALE=1 ./scripts/benchmark-test.sh --compare-trino tpcds tpcbb` exit 0:

- TPC-DS SF1: 99 pass; Trino matched **99/99**, 0 vacuous-bug; q64 returns 1 row on both. Avg speedup 4.1x, SQE 20.3 s vs Trino 73.8 s.
- TPCBB SF1: 10 pass, Trino matched 10/10, avg speedup 4.6x; q01 13.9 s. q05 returns 389 rows (was 390 on the previous data; Trino returns the same 389).

This run had a normal host load; it confirms the 2-4x slower times in the dimension-scaling run were host contention (SQE TPC-DS 65.6 s there, 20.3 s here on the same data shape).

TPC-DS data must be regenerated.

**NEXT:** Trino value comparison (`ValueDiff`), then SSB/TPC-E money columns as Int64 cents / DECIMAL.
