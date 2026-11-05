-- Phase F, SQE: metadata and catalog commands. Read the output; the expected refusal is the last statement.
SHOW TABLES IN @S@;
SELECT snapshot_id, operation FROM @S@."sqe_v3$snapshots" ORDER BY committed_at;
SELECT snapshot_id, operation FROM @S@."sqe_basic$snapshots" ORDER BY committed_at;
SELECT snapshot_id, operation FROM @S@."SF_RUN_V3$snapshots" ORDER BY committed_at;
-- The merge-on-read DELETE must show "added-dvs":"1". The small Snowflake tables are copy-on-write.
SELECT operation, summary FROM @S@."sqe_v3_mor$snapshots" ORDER BY committed_at;
-- Snowflake merge-on-read: the current snapshot must show "total-delete-files" above 0 (V2 position
-- deletes, V3 a deletion vector, "added-dvs"). The compare rows mor_* carry the correctness claim.
SELECT operation, summary FROM @S@."SF_RUN_MOR_V2$snapshots" ORDER BY committed_at;
SELECT operation, summary FROM @S@."SF_RUN_MOR_V3$snapshots" ORDER BY committed_at;
-- $files lists every live file with Iceberg's `content` (0 data, 1 position deletes, 2 equality
-- deletes). MoR V2 must show content 1 in parquet, MoR V3 content 1 in puffin (a deletion vector)
-- with its referenced data file; the small tables show content 0 only.
SELECT content, file_format, count(*), sum(record_count) FROM @S@."SF_RUN_V3$files" GROUP BY 1, 2 ORDER BY 1, 2;
SELECT content, file_format, count(*), sum(record_count) FROM @S@."SF_RUN_V2$files" GROUP BY 1, 2 ORDER BY 1, 2;
SELECT content, file_format, count(*), sum(record_count) FROM @S@."sqe_v3$files" GROUP BY 1, 2 ORDER BY 1, 2;
SELECT content, file_format, count(*), sum(record_count) FROM @S@."SF_RUN_MOR_V2$files" GROUP BY 1, 2 ORDER BY 1, 2;
SELECT content, file_format, count(*), sum(record_count), count(referenced_data_file) FROM @S@."SF_RUN_MOR_V3$files" GROUP BY 1, 2 ORDER BY 1, 2;
SELECT content, file_format, count(*), sum(record_count), count(referenced_data_file) FROM @S@."sqe_v3_mor$files" GROUP BY 1, 2 ORDER BY 1, 2;
DESCRIBE @S@."SF_RUN_EVO";
DESCRIBE @S@."SF_RUN_GEO";
DESCRIBE @S@."SF_RUN_NS";
SHOW CREATE TABLE @S@."SF_RUN_PART";
SHOW CREATE TABLE @S@.sqe_v3;
SELECT * FROM @S@.sf_run_basic LIMIT 1;
