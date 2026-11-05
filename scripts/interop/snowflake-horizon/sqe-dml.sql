-- Phase D, SQE: DML on tables Snowflake wrote, and on SQE tables that now carry Snowflake deletes.
DELETE FROM @S@."SF_RUN_V3" WHERE "ID" = 11;
UPDATE @S@."SF_RUN_V3" SET "NAME" = 'sqe-upd' WHERE "ID" = 12;
MERGE INTO @S@."SF_RUN_V3" t USING (SELECT 13 AS id, 'sqe-merge' AS name UNION ALL SELECT 200, 'sqe-new') s ON t."ID" = s.id WHEN MATCHED THEN UPDATE SET "NAME" = s.name WHEN NOT MATCHED THEN INSERT ("ID", "NAME") VALUES (s.id, s.name);
DELETE FROM @S@."SF_RUN_V2" WHERE "ID" = 11;
UPDATE @S@."SF_RUN_V2" SET "NAME" = 'sqe-upd' WHERE "ID" = 12;
INSERT INTO @S@."SF_RUN_BASIC" ("ID", "NAME", "TS") VALUES (5, 'sqe-ins', TIMESTAMP '2026-10-27 12:00:00+00:00');
UPDATE @S@."SF_RUN_BASIC" SET "NAME" = 'B' WHERE "ID" = 2;
INSERT INTO @S@."SF_RUN_EVO" VALUES (6, 'sqe-f', 6.00, 'x6', 'sqe-d');
DELETE FROM @S@.sqe_v3 WHERE id = 21;
UPDATE @S@.sqe_v3 SET name = 'sqe-after-sf' WHERE id = 22;
