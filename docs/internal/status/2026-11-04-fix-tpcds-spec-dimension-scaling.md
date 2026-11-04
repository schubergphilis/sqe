**STATUS 2026-11-04: TPC-DS dimensions follow the official row counts above SF1, generated keys cover their whole dimension (`fix/tpcds-spec-dimension-scaling`, #785). Stacked on !1638.**

Above SF1 the generator scaled `customer`, `customer_address`, `store`, `web_site`, `web_page`, `promotion`, `reason`, `call_center` and `catalog_page` linearly. dsdgen grows them sublinearly: SF10 has 500,000 customers (we wrote 1M) and 200 web pages (we wrote 600); SF100 has 2M customers (we wrote 10M). `sublinear_above_sf1` now interpolates every dimension between the official counts at SF10, 100, 300, 1,000, 3,000, 10,000, 30,000 and 100,000, read from Trino's `tpcds` connector (a dsdgen port) with `count(*)`. `item` and `warehouse` used to stop at SF1,000 and now follow the same table. Up to SF1 nothing changes in the row counts.

Key draws that used fixed or exclusive ranges now cover the whole dimension:

- `cs_catalog_page_sk` / `cr_catalog_page_sk` came from a fixed 1..11,718; now `catalog_page_rows(scale)`.
- `p_item_sk` came from the first 18,000 items at every scale; now `item_rows(scale)`.
- `c_current_hdemo_sk` and the catalog/web ship modes now use the dimension's own row count.
- The exclusive ranges `1..73049`, `1..1000`, `1..16`, `1..12` and `1..10` never drew their last value; q18's `c_birth_month IN (1, 6, 8, 9, 12, 2)` never matched a December birthday.
- TPCBB click and review users came from their own domain; they are now TPC-DS customer keys (`user_domain(scale)`), since q10 joins them to the sales customers.

Tests: `dimensions_follow_official_counts_above_sf1` (11 tables at SF10, 100 and 1,000 against the official counts), `small_domains_reach_their_last_value` (the last value of each small domain is drawn and no key passes its table), and the TPCBB key-range test now checks users too. Generation golden hashes re-recorded: 9 tables changed values, none changed row count.

Gate: `cargo test -p sqe-bench` (all pass), `cargo clippy -p sqe-bench --all-targets -- -D warnings` clean, and `BENCH_SCALE=1 ./scripts/benchmark-test.sh --compare-trino tpcds tpcbb` exit 0:

- TPC-DS SF1: 99 pass; Trino matched 98/99. The one miss is q64, `VacuousBug` (empty on both engines, 2 rows canonically); it was already empty on 10-18, 11-02 and in !1638, so it is not this change.
- TPCBB SF1: 10 pass, Trino matched 10/10, row counts per query unchanged.

Timings from this run are not comparable: the host had a load average of ~60 from other processes (headless Chrome, node), and every TPCBB query ran 2-4x slower than the !1638 run an hour earlier, including the 0.1 s ones.

Data generated before this change must be regenerated, at every scale.

**NEXT:** plant the q64 coincidence (stacked on this branch: same golden hashes), then the Trino value comparison and SSB/TPC-E decimals from `origin/main`.
