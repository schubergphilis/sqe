### Fixed

- A metadata table function that names a catalog is access-checked in that catalog under `[policy.ranger] catalog_scoped`

    `table_files('ns', 't', 'cat')` (and the other metadata functions, and `cat.ns."t$snapshots"`) reads its target from `cat`, but the policy check that refuses a denied target keyed it on the session's catalog. A caller denied `cat.ns.t` could read its file paths, partition values and row counts from a session in another catalog. The check now keys the catalog the call names, and the session's only when it names none.
