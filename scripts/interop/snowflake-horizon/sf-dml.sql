-- Phase C, Snowflake: DML on tables SQE wrote. On a V3 table Snowflake writes deletion vectors.
DELETE FROM @S@."sqe_v3" WHERE "id" IN (10, 11);
UPDATE @S@."sqe_v3" SET "name" = 'sf-upd' WHERE "id" = 20;
INSERT INTO @S@."sqe_v3" ("id", "name", "ts") SELECT 51, 'sf-ins', '2026-12-31 23:59:59.999999'::TIMESTAMP_NTZ;
DELETE FROM @S@."sqe_v2" WHERE "id" = 2;
INSERT INTO @S@."sqe_basic" ("id", "name", "note") SELECT 7, 'sf-seven', 'from-snowflake';
UPDATE @S@."sqe_part" SET "ts" = '2026-04-01 00:00:00'::TIMESTAMP_NTZ WHERE "id" = 3;
