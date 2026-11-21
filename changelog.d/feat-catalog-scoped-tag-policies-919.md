### Changed

- Tag policies can be scoped to one catalog under `[policy.ranger] catalog_scoped` (#919)

    With the setting on, a column of catalog `c` tagged `pii` is governed by tag policies on `pii` and on `c.pii`. A policy on `c.pii` applies to catalog `c` only; a policy on bare `pii` keeps applying everywhere, and the catalog's mask wins when both mask a column. Column tags stay bare. The Ranger tag projector writes both names, so Spark applies the same policies. `table_files` and `table_partitions` now key their policy lookup on the catalog they read. Nothing changes with the setting off.
