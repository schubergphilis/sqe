**STATUS 2026-09-09: Hive table-load round trips (#663) on `perf/hive-table-load-recheck`: one metadata round trip per recheck window instead of a manifest HEAD plus up to two data-prefix listings per lookup.**

Tracker housekeeping first: the 2026-09-04 review findings were filed
twice (#433–#541 and #543–#651). Every speed finding I had queued (#463
307, #464 308, #477 407, #494 512, #511 701, #478 408) is the stale copy
of a finding !952 closed under its #5xx number; the #4xx series is 109
open duplicates. The only speed work left with a Hive angle was not
filed: `HiveSchemaProvider::table` runs per table reference, per
`information_schema` row and for the tag source, and each call paid a
HEAD (manifest ETag), for CSV/JSON a LIST to read the codec off a key
(the doc said "per cache miss"; the code ran it every call), and with
neither ANALYZE stats nor an index a full LIST for size statistics.
Fix: `HiveMetadataCache` recheck window (`[hive] metadata_recheck_secs`,
default 10, `0` = always check) mirrors `catalog.authorization_recheck_secs`;
`HiveTableFacts` (codec, listed bytes) live on the manifest's cache entry
and expire with it or on the TTL; a hit marks the entry verified without
resetting `loaded_at`, which fixes a second defect: ETag-less sources
(Glue) had their TTL extended on every hit, so a continuously queried
table never reloaded. All DDL/write paths already `invalidate`, so
same-process writes stay immediately visible; the window only delays
another writer (book page updated). Tests: counting source + counting
store in `hive_provider_test.rs` (1 HEAD / 1 GET / 2 LIST for three
lookups; recheck 0; invalidate; TTL lapse). Stacked on !974.
**NEXT:** close the #4xx duplicates (109 issues, needs a go-ahead), then
the Flight `USE` / default-schema gap (#627), #659 partials, the
docker-capable CI runner (807 item 3).
