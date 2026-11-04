**STATUS 2026-11-04: TPC-DS money columns are spec `DECIMAL`, TPCBB click keys join their dimensions (`fix/tpcds-spec-decimal-types`, #785).**

The `sqe-bench` TPC-DS generator wrote all 80 decimal columns of `benchmarks/schemas/tpcds.sql` as `Float64`. They now carry the spec types: money `DECIMAL(7,2)`, GMT offsets and tax rates `DECIMAL(5,2)`, `p_cost` `DECIMAL(15,2)`. A unit test parses the spec DDL and checks every generated table against it (no missing decimals, no extra ones, no floats left). Prices are rounded to cents before the extended amounts are derived, so `ext_sales_price = sales_price * quantity` and `net_paid_inc_tax = net_paid + ext_tax` hold exactly; a second test checks this in integer cents for store, catalog and web sales. `ext_tax` is now the tax on the line, not on one unit. The store tax column is renamed `s_tax_precentage` (the spec's spelling).

TPCBB `web_clickstreams` drew `wcs_web_page_sk` from `SF x 2,040` keys while `web_page` has 60 rows at SF1, and `wcs_click_date_sk` from `SF x 73,049` keys, past the end of `date_dim` from SF2. Both domains now come from the TPC-DS generator (`web_page_rows(scale)`, `DS_DATE_RANGE`), with click dates in the same 1998-2003 window as the sales. A test checks every key at SF0.1 lands in its table.

The `status.rs` result comparator trimmed trailing zeros from integers too, so `100` matched `1`. It now trims only after a decimal point.

Gate: `cargo test -p sqe-bench` (all pass), `cargo clippy -p sqe-bench --all-targets -- -D warnings` clean, `tpcbb_preaggregation_test` passes with a decimal `ss_net_paid`, and `BENCH_SCALE=1 ./scripts/benchmark-test.sh --compare-trino tpcds tpcbb` exit 0:

- TPC-DS SF1: 99 pass; Trino compare matched 98/99, avg speedup 4.8x. The remaining one is q64, empty on both engines; it was already `VacuousBug` on 10-18 and 11-02, so it is generator data coverage, not this change.
- TPCBB SF1: 10 pass, Trino matched 10/10, avg speedup 4.4x. Row counts per query unchanged from the pre-fix run of the same day; q01 16.1 s vs 14.4 s, the rest within noise.

Data generated before this change must be regenerated. The SF10 TPCBB numbers recorded earlier on 2026-11-04 are on pre-fix data and are not comparable with later runs.

Findings left for follow-up branches:

- Dimension row counts grow linearly above SF1 for `web_page`, `store`, `customer`, `customer_address`, `promotion`, `reason`, `call_center`, `web_site` and `catalog_page`; the spec scales them sublinearly (as the generator already does for `item` and `warehouse`). Verify the spec table before changing them.
- The Trino comparison classifies on row counts only, so a value difference with equal row counts is reported as a match.
- SSB and TPC-E money columns are still `Float64`.
- TPC-DS q64 returns no rows on the generated data.

**NEXT:** TPC-DS dimension scaling per spec (sublinear above SF1), then Trino value comparison.
