-- Phase C, Databricks SQL: DML on tables SQE wrote. On a V3 table Databricks writes deletion vectors.
DELETE FROM @S@.sqe_v3 WHERE id IN (10, 11);
UPDATE @S@.sqe_v3 SET name = 'dbx-upd' WHERE id = 20;
INSERT INTO @S@.sqe_v3 VALUES (51, 'dbx-ins', TIMESTAMP '2026-12-31 23:59:59.999999');
DELETE FROM @S@.sqe_v2 WHERE id = 2;
INSERT INTO @S@.sqe_basic (id, name, note) VALUES (7, 'dbx-seven', 'from-databricks');
UPDATE @S@.sqe_part SET ts = TIMESTAMP '2026-04-01 00:00:00' WHERE id = 3;
