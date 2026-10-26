### Fixed

- A distributed scan reads a `timestamptz` column that Databricks, Spark or pyarrow wrote

    A worker decodes such a column as `Timestamp(us, "UTC")`, where the plan
    says `"+00:00"`. The coordinator refused the worker's batch with `not a
    width change or type promotion`, so the query failed whenever the scan
    was distributed. Both labels hold UTC instants; the coordinator now
    relabels the column, as the local scan does since the Unity Catalog
    interop fix. Another time unit is still refused.
