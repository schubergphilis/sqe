### Changed

- Spark 4.1 with Iceberg 1.12 now reads back what SQE's merge-on-read writes
  leave behind: V2 position deletes, V3 deletion vectors, equality deletes
  from keyed DELETE, UPDATE and MERGE on V2 and V3, and V3 row lineage. Each
  test checks the snapshot summary first, so a copy-on-write fallback cannot
  pass. The Iceberg matrix cites these tests in place of the empty stub
  tests it used to cite, and its cross-engine caveats are gone (#853, #855).
