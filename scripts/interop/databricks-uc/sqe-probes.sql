-- Phase F, SQE: metadata and catalog commands. Read the output; the expected refusal is the last statement.
SHOW TABLES IN @S@;
SELECT snapshot_id, operation FROM @S@."sqe_v3$snapshots" ORDER BY committed_at;
SELECT snapshot_id, operation FROM @S@."sqe_basic$snapshots" ORDER BY committed_at;
SELECT snapshot_id, operation FROM @S@."dbx_run_v3$snapshots" ORDER BY committed_at;
SELECT count(*), sum(record_count) FROM @S@."dbx_run_v3$files";
SELECT count(*), sum(record_count) FROM @S@."sqe_v3$files";
DESCRIBE @S@.dbx_run_evo;
DESCRIBE @S@.dbx_run_geo;
SHOW CREATE TABLE @S@.dbx_run_part;
SHOW CREATE TABLE @S@.sqe_v3;
SELECT table_name, count(*) FROM information_schema.columns WHERE table_schema = 'sqe_probe' AND table_name LIKE '%run%' GROUP BY table_name ORDER BY table_name;
SELECT * FROM dbx.tpch.nation LIMIT 1;
