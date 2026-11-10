### Fixed

- The TPCBB q01 pre-aggregation candidate matches the original again, and SF1/SF10 measurements of it are recorded (#785)

    Since `avg(decimal(p,s))` returns `decimal(p,s)` and `sum` returns
    `decimal(38,s)`, the candidate's casts to the old types made it differ
    from the original. It now returns the same types and computes the
    average in whole cents with one half-up rounding. Dividing at scale 6
    and then casting would round twice, which is a cent off once a group
    joins about 10^6 rows. The exactness fixture has such a group.
    `benchmarks/experiments/tpcbb-q01-scale.py` measures join cardinality,
    time and peak RSS at any scale. At SF10 the original joins 6.78 billion
    rows in 127 s; the candidate takes 0.54 s with the same top 20.
