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
-- $files lists data files only (no `content` column), so deletion vectors and delete files do not show here.
SELECT file_format, count(*), sum(record_count) FROM @S@."SF_RUN_V3$files" GROUP BY file_format;
SELECT file_format, count(*), sum(record_count) FROM @S@."SF_RUN_V2$files" GROUP BY file_format;
SELECT file_format, count(*), sum(record_count) FROM @S@."sqe_v3$files" GROUP BY file_format;
DESCRIBE @S@."SF_RUN_EVO";
DESCRIBE @S@."SF_RUN_GEO";
DESCRIBE @S@."SF_RUN_NS";
SHOW CREATE TABLE @S@."SF_RUN_PART";
SHOW CREATE TABLE @S@.sqe_v3;
SELECT * FROM @S@.sf_run_basic LIMIT 1;
