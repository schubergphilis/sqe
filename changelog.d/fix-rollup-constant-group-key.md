### Fixed

- `ROLLUP`, `CUBE` and `GROUPING SETS` no longer return duplicate groups or subtotals out of order when the `WHERE` clause pins a grouped column

    `WHERE s_state = 'TN' GROUP BY ROLLUP (i_item_id, s_state)` let DataFusion treat `s_state` as constant above the rollup, though the rollup emits it as NULL in its subtotal rows. The final aggregate then assumed sorted input and emitted some groups more than once, and `ORDER BY i_item_id, s_state` sorted on `i_item_id` alone. At SF10 the q27 aggregate gave 92,435 to 94,122 rows for 87,989 groups, varying from run to run. SQE now carries apache/datafusion#24422 in `vendor/datafusion-physical-plan` until DataFusion 56 ships it.
