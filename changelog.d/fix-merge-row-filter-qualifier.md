### Fixed

- MERGE into a table with a row filter no longer fails, and MERGE accepts an unaliased target or source table

    The row filter was rendered with its table qualifier (`orders.id > 1`) and evaluated over MERGE's scratch relation, where that name does not resolve, so every MERGE into a row-filtered table failed to plan its cardinality check. The filter is now rendered with bare column names, as masks already were. Separately, MERGE without a target alias (`MERGE INTO ns.t ... ON t.id = s.id` or `ON ns.t.id = s.id`) failed even without a policy, because the target was renamed to a default alias; an unaliased target or source is now named by its table, and qualified references to it are shortened.
