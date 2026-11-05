### Changed

- The join fallback is tested for every build estimate and for a chain of joins (#784)

    New unit tests drive the fallback with an exact, an inexact (behind a filter) and an absent (a scan without statistics) build-side estimate. Each one keeps the hash join on the first attempt, overflows the query quota, and completes on the sort-merge retry with the right answer and all memory released. A two-join chain retries as two sort-merge joins. The tests also check the retry plan: every sort-merge join input stays hash-partitioned and never passes through a single-partition `CoalescePartitionsExec` or `SortPreservingMergeExec`.
