### Changed

- OpenLineage event for a CTAS IF NOT EXISTS on an existing table

    A `CREATE TABLE IF NOT EXISTS ... AS` that finds the table already there used to send a COMPLETE event with no datasets. It now names the table as the one output, carrying the custom dataset facet `sqe_skipped: {"reason": "table_exists"}`, with no `lifecycleStateChange`, no inputs and no column lineage. Events for every other statement are unchanged.
