**STATUS 2026-11-05: `avg(decimal(p,s))` returns `decimal(p,s)` as in Trino (`fix/avg-decimal-scale`, #785).**

`RoundedAvg::return_type` now keeps the argument's decimal type instead of DataFusion's `decimal(p+4,s+4)`. The final division still rounds half away from zero. Checked against Trino 481: `avg(decimal(7,2))` is `decimal(7,2)` there too.

Effect on the TPC-DS compare against Trino: SF1 goes from 90/99 to 95/99. SF10 goes from 91/99 to 92/99, because q06 (`1.2 * AVG(i_current_price)`) and q44 now match. Of the queries still differing, q58, q61 and q78 come down to decimal arithmetic result types: Trino scales division the way Spark does, arrow uses `s1 + 4`. q35 and q75 differ on ties at the LIMIT. q27 is a DataFusion grouping-set bug, fixed on a separate branch.

**NEXT:** decide whether decimal `/` and `*` should take Trino's result types (q58/q61/q78).
