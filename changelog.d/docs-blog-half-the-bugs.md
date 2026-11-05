### Added

- Blog post "Half the bugs were in the benchmark" (#785, #787, #788)

    What the Polaris readers and the SF1 Trino comparison found: the embedded memory pool, the orphaned TopK dynamic filter, `EXPLAIN ANALYZE` outside the query's memory cap and the unapplied spill compression in SQE, and in the benchmark generator the TPCx-BB item domain, floats for money, linear dimension growth, the empty q64, the comparator that matched 100 with 1 and the row-count-only Trino check.
