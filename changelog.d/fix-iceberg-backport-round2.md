### Fixed

- Filters on a column promoted from `INT` to `BIGINT` (or `REAL` to `DOUBLE`) no longer drop rows from files written before the promotion, and DELETE on such a table no longer crashes

    Pruning compared the old files' narrow partition values and column bounds against the widened predicate and skipped them: after `ALTER COLUMN p SET DATA TYPE BIGINT`, `WHERE p = 2` returned nothing for rows written before the change. A DELETE that removed one of those files panicked, and once that was fixed its commit failed with "value is not compatible with type". Backports apache/iceberg-rust #3483 and two SQE vendor patches.

- Iceberg backport round 2: NaN row-group bounds no longer prune `<`/`>` filters (#3502); float values in JSON metadata read like iceberg-java (#3476); a scan with an empty projection no longer reads data columns (#3473); partition specs produced by spec evolution load (#3431) and reuse the highest matching field id (#3413); `rewrite_files` keeps the parent snapshot's properties, added files go into a manifest per partition spec, and snapshot summary totals never wrap below zero (RisingWave #261, #262, #246)
