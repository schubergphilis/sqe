**STATUS 2026-10-18: `generate_series` / `range` answer under Ranger (`fix/tag-gate-generate-series-847`, #847).**

`scan_column_tags` returns `Known(empty)` for DataFusion's `GenerateSeriesTable`, the provider both `generate_series()` and `range()` build, matched by downcast like the #846 metadata-TVF marker. Before, the scan was `NotRecognised`, the cache fallback missed on `generate_series()`, and the statement was refused with "tag state unknown". Other unrecognised providers still fail closed (`any_other_provider_is_not_recognised`).

Unit test `generate_series_and_range_are_known_untagged` builds both providers through DataFusion's own table functions. New AC test `builtin_generator_tvfs_answer`: carol, alice and bob each get 10 rows from `generate_series(0, 9)` and `range(0, 10)`.

**NEXT:** unchanged.
