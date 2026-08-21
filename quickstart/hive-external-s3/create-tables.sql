-- Four Hive external tables over one S3 bucket, one format each.
--
-- Every statement here is Athena's own CREATE EXTERNAL TABLE syntax. SQE adds
-- no grammar of its own; engine-specific settings ride inside TBLPROPERTIES
-- under an `sqe.` prefix, which is where Glue and Athena put theirs anyway.
--
-- The warehouse root (--hive-root lake=s3://lake/wh/) is where the sidecar
-- manifests land: s3://lake/wh/demo/<table>/_sqe/table.json. LOCATION is
-- where the DATA lives, and the two are deliberately kept apart. A cycle-1
-- scan lists a table's LOCATION without filtering by file extension, so a
-- LOCATION containing the manifest directory would have the CSV reader trying
-- to parse table.json.
--
-- Idempotent on purpose: DROP TABLE removes the manifest and never touches
-- data, so re-running this script re-creates definitions over data that is
-- already in the bucket.

DROP TABLE IF EXISTS lake.demo.events_parquet;
DROP TABLE IF EXISTS lake.demo.events_csv;
DROP TABLE IF EXISTS lake.demo.events_json;
DROP TABLE IF EXISTS lake.demo.events_ndjson;

-- 1. Parquet, two partition keys. Partition columns are declared in their own
--    PARTITIONED BY clause, never repeated in the column list, and they land
--    last in the resulting schema.
CREATE EXTERNAL TABLE lake.demo.events_parquet (
  event_id BIGINT,
  user_id  BIGINT,
  kind     STRING,
  amount   DOUBLE
)
PARTITIONED BY (dt STRING, region STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS PARQUET
LOCATION 's3://lake/data/events_parquet/';

-- 2. CSV with a header row. `STORED AS CSV` does not parse (sqlparser's format
--    keywords are TEXTFILE, SEQUENCEFILE, ORC, PARQUET, AVRO, RCFILE,
--    JSONFILE), and bare TEXTFILE means Hive's control-A delimiter, not comma,
--    so the delimiter is declared explicitly. Hive TEXTFILE is headerless by
--    convention, which is why skip.header.line.count has to say otherwise.
CREATE EXTERNAL TABLE lake.demo.events_csv (
  event_id BIGINT,
  user_id  BIGINT,
  kind     STRING,
  amount   DOUBLE
)
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://lake/data/events_csv/'
TBLPROPERTIES ('skip.header.line.count' = '1');

-- 3. JSON, the canonical Athena form: OpenX JsonSerDe over TextInputFormat.
--    One JSON object per line. A `[{...},{...}]` array document is not
--    readable, in Athena or here.
CREATE EXTERNAL TABLE lake.demo.events_json (
  event_id BIGINT,
  user_id  BIGINT,
  kind     STRING,
  amount   DOUBLE
)
PARTITIONED BY (dt STRING)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION 's3://lake/data/events_json/';

-- 4. The same line-delimited JSON, declared the short way and unpartitioned.
--    STORED AS JSONFILE resolves to the same reader as the SerDe above.
CREATE EXTERNAL TABLE lake.demo.events_ndjson (
  event_id BIGINT,
  user_id  BIGINT,
  kind     STRING,
  amount   DOUBLE
)
STORED AS JSONFILE
LOCATION 's3://lake/data/events_ndjson/';
