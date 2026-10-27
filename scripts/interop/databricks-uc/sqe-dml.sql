-- Phase D, SQE: DML on tables Databricks wrote, and on SQE tables that now carry Databricks deletion vectors.
DELETE FROM @S@.dbx_run_v3 WHERE id = 11;
UPDATE @S@.dbx_run_v3 SET name = 'sqe-upd' WHERE id = 12;
MERGE INTO @S@.dbx_run_v3 t USING (SELECT 13 AS id, 'sqe-merge' AS name UNION ALL SELECT 200, 'sqe-new') s ON t.id = s.id WHEN MATCHED THEN UPDATE SET name = s.name WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name);
INSERT INTO @S@.dbx_run_basic (id, name, ts) VALUES (5, 'sqe-ins', TIMESTAMP '2026-10-27 12:00:00');
UPDATE @S@.dbx_run_basic SET name = 'B' WHERE id = 2;
INSERT INTO @S@.dbx_run_evo VALUES (6, 'sqe-f', 6.00, 'x6', 'sqe-d');
DELETE FROM @S@.sqe_v3 WHERE id = 21;
UPDATE @S@.sqe_v3 SET name = 'sqe-after-dbx' WHERE id = 22;
