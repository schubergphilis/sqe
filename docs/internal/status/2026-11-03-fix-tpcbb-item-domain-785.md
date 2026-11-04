**STATUS 2026-11-03: TPCBB item keys follow the TPC-DS item table above SF1 (`fix/tpcbb-item-domain-785`, #785).**

Found while preparing #785 criterion (b), the join-row report for q01. That
report has to measure key distributions rather than assume them.
`web_clickstreams` and `product_reviews` drew item keys from
`1..=scale * 18,000`, which is linear. `tpcds::item_rows` is linear only up
to SF1 and then follows the dsdgen counts: 102,000 at SF10, 204,000 at SF100
and 300,000 at SF1000. So at SF10 43% of the keys, and at SF100 89%, had no
item row, and the q01/q03/q04 item joins dropped them. Both generators now
call `item_domain(scale)` = `tpcds::item_rows(scale)`.

`scaled()` truncates exactly like the old `(scale * 18_000.0) as i32`, with
the same floor of 1, so the data at SF1 and below is byte-identical: same
domain, same RNG draws.

Consequence: every SF10+ TPCBB result so far was produced with the old
domain, e.g. `tpcbb-sf10-flight-2026-10-05T07:12:02.json` with q01 at
86,174 ms. At SF10 the share of click keys that match goes from 57% to 100%,
so q01's join intermediate grows about 1.8× (an estimate of 6.4B -> 11.3B
rows; criterion (b) will measure it). The next SF10 q01 number will be slower. That is a data change, not
an engine regression. Regenerate SF10+ TPCBB data on every rig.

Gate: `cargo test -p sqe-bench` -> 233 + 41 passed, including the new
`item_keys_stay_inside_the_item_table`. Clippy is clean and rustfmt added no
hunks.

Other domain mismatches, left as they are: `wcs_web_page_sk` uses
`scale * 2,040` (TPC-DS web_page has 60 rows at SF1), and `wcs_click_date_sk`
uses `scale * 73,049` (date_dim is a fixed 73,049). No TPCBB query joins
either one. Fixing the date domain would change the data at SF1 and below
and q07's session distribution, so that is a separate change.

Left open on #785 (the main work):

- (a) Harden the existing `benchmarks/experiments/tpcbb-preaggregation.py`
  fixture: DECIMAL(7,2) `ss_net_paid`, NULL item keys, items missing on one
  side, an `i_item_id` shared by two `i_item_sk`, an item whose `net_paid`
  is all NULL, and an exact comparison with matching output types.
- (b) Join input/output rows, memory and runtime at SF1/SF10/SF30 on the
  regenerated data. Do it in the same SF10 generation pass as the #787
  compare.
- (c) A note on scope and applicability.

**NEXT:** `test/tpcbb-q01-preagg-785` (criterion a), then the shared SF10
pass for #785 (b) and #787, then the embedded join-fallback move (#786) and
the engine-bench 150M rerun and MR.
