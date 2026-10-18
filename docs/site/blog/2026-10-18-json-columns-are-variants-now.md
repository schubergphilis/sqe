---
title: "JSON columns are variants now"
description: "SQE stores JSON as the Iceberg V3 variant type. VARIANT and, on a V3 table, JSON columns hold a binary-encoded value instead of a string, invalid JSON fails the write, and the json functions read both shapes. New tables are V3 by default. Spark 4.1 reads SQE's variants, SQE reads Spark's, and since this week that includes files Spark wrote shredded. SQE still writes unshredded variants and has no typed field access on shredded columns yet."
pubDate: "2026-10-18"
author: "Jacob Verhoeks"
tags:
  - "iceberg"
  - "v3"
  - "variant"
  - "json"
  - "spark"
  - "interoperability"
---

For most of SQE's life a JSON column was a string. You wrote text in, you read text out, and every query that wanted a field parsed the whole document again. Nothing checked that the text was JSON at all. A column declared `JSON` happily stored `not json at all`.

Iceberg V3 adds a real type for semi-structured data: `variant`. SQE now writes it, reads it, and treats `JSON` as a variant on every V3 table. Two merge requests got us there in a week, and a third closed the gap other engines kept opening: shredded files.

Every claim below has an integration test behind it, run against the local Polaris stack, a worker, and Spark 4.1 with Iceberg 1.12.

## What a variant is on disk

A variant is not JSON text. It is two binary buffers. The `metadata` buffer holds the dictionary of field names, and the `value` buffer holds the encoded value, with typed numbers, booleans, strings, arrays and objects. In Arrow, Iceberg reads it as a struct of those two binary fields, tagged with the `arrow.parquet.variant` extension.

The difference from a string shows up on write. A string column accepts anything. A variant column has to parse what it gets, so bad input fails the statement instead of turning into a row nobody can query later.

```sql
CREATE TABLE events (id BIGINT, payload VARIANT);

INSERT INTO events VALUES
  (1, '{"a": 1, "b": [true, null, "x"]}'),
  (2, NULL),
  (3, '42'),
  (4, '"text"'),
  (5, 'null');

SELECT id, payload FROM events ORDER BY id;
-- 1 | {"a":1,"b":[true,null,"x"]}
-- 2 | NULL
-- 3 | 42
-- 4 | "text"
-- 5 | null
```

Row 2 is SQL NULL. Row 5 is the JSON value `null`, which is a different thing, and the variant keeps them apart. Row 3 stores a number and row 4 a JSON string, because a string written to a variant is parsed as JSON. To store text, quote it.

Malformed input is an error, never a silent NULL:

```
INSERT INTO events VALUES (6, '{oops');
-- Failed to write column 'payload' as VARIANT from Utf8:
-- Invalid argument error: JSON format error: key must be a string at line 1 column 2
```

## Clients see JSON text

No client driver has a variant type. The Trino protocol has `json`, Flight SQL and JDBC have strings, and that is what SQE sends. A variant leaves the engine as its JSON text, object keys sorted, no whitespace. `DESCRIBE` and `information_schema.columns` still report `variant`, and `SHOW CREATE TABLE` prints `VARIANT`, so the catalog view stays honest about what is stored.

The JSON functions take a variant as its text. `json_extract`, `json_extract_scalar` and the `json_get_*` family work on a V3 variant column exactly as they work on a V2 string column, so a query written against one runs against the other:

```sql
SELECT id, json_get_int(payload, 'a') FROM events WHERE id = 1;
-- 1 | 1
```

Getting there took a wrapper. DataFusion drops field metadata through most expressions, so by the time a variant reached `json_get_int` it was an anonymous struct, and the functions refused it at planning time. SQE now detects a variant by its storage shape when the extension tag is gone, and wraps every `json*` function so a variant argument arrives as JSON text. The embedded CLI carries the same wrapper as the coordinator.

## JSON is a variant on V3, and V3 is the default

The second change matters more than the first. A `JSON` column on a format-version 3 table is stored as `VARIANT`, also when it sits inside a `STRUCT` or `ROW`. On a V1 or V2 table it stays a string.

And new tables are V3 now. `catalog.default_table_format_version` defaults to 3, so a plain `CREATE TABLE t (doc JSON)` stores a variant. Ask for V2 and you get the old behaviour back:

```sql
CREATE TABLE t_v2 (id INT, j JSON) WITH ('format-version' = '2');
INSERT INTO t_v2 VALUES (1, 'not json at all');   -- accepted, it is a string
```

Flipping the default is a compatibility decision, and we made it on purpose. Trino before 481, Spark before 4.0 and Iceberg before 1.10 cannot open a V3 table at all. If those readers share your tables, set `default_table_format_version = 2` or create the shared tables `WITH ('format-version' = '2')`. Existing tables keep their version.

SQE refuses the combinations that would leave metadata and properties disagreeing. `ALTER TABLE ... ADD COLUMN v VARIANT` on a V2 table fails with "variant needs format v3". Asking for format-version 2 on a table that declares a variant column fails too.

Existing string columns are not converted. The Iceberg spec does not allow evolving `string` to `variant`, so the way across is a copy, where the write parses each value:

```sql
CREATE TABLE t2 (id BIGINT, doc VARIANT);
INSERT INTO t2 SELECT id, doc FROM t1;
```

## Spark reads it, and Spark writes it

A storage type is only worth something if the other engines read it. The interop tests run both directions against Spark 4.1.3 with iceberg-spark-runtime 1.12.0.

SQE writes a V3 table with a `JSON` column, then deletes a row with a merge-on-read `DELETE`, which on V3 writes a deletion vector. Spark reads it with its own variant functions:

```sql
-- in Spark
SELECT id, to_json(j), variant_get(j, '$.a.b', 'int'), typeof(j)
FROM rest.default.variant_to_spark ORDER BY id;
-- 1 | {"a":{"b":2},"s":"x"} | 2    | variant
-- 2 | [1,2]                 | NULL | variant
-- 3 | NULL                  | NULL | variant
```

The deleted row is gone, and `typeof` says `variant`, not `string`. In the other direction Spark writes `VARIANT` values with `parse_json`, and SQE reads them back as the same JSON text and runs `json_get_int` over them.

## Shredded files

Then Spark turned on shredding, and the reads broke.

Shredding is a write-side optimisation in the variant spec. When the values in a column share a shape, the writer pulls the consistent fields out into typed Parquet columns, stored in a `typed_value` group beside `value`. A reader that understands it can prune on those columns and read a field without decoding the whole value. Spark does it with one table property:

```sql
CREATE TABLE rest.default.events (id INT, v VARIANT) USING iceberg
  TBLPROPERTIES ('format-version' = '3',
                 'write.parquet.shred-variants' = 'true');
```

A shredded file failed in SQE twice. The data file's variant column is no longer the two-field struct the reader expects, so the schema check refused it. And before that, Iceberg 1.12 writes column bounds for the shredded variant into the manifest as a serialized variant keyed by field path. The manifest reader tried to parse that as a primitive bound and failed, which made the whole manifest unreadable. A query against the table never got as far as a data file.

The fix has two parts. The manifest reader skips a variant bound instead of parsing it, so pruning on that column stays conservative and every other column keeps its bounds. And every read path folds `typed_value` back into the variant with `unshred_variant` from the Arrow Rust variant crates. SQE has three read paths: the coordinator's direct Parquet read, the iceberg-rust reader, and the worker scan. A fix in one of them is a bug waiting in the other two, so all three unshred. The interop test covers the coordinator, and a distributed test scans the same shredded column on a worker.

The interop test checks that the file really is shredded before it checks anything else: it opens the data file Spark wrote and asserts `typed_value` is in the Parquet schema. Then it reads the rows:

```sql
SELECT id, v, json_get_int(v, 'a'), json_get_str(v, 's')
FROM default.variant_shredded_from_spark ORDER BY id;
-- 1 | {"a":1,"s":"x"}              | 1    | x
-- 2 | {"a":2,"s":"y"}              | 2    | y
-- 3 | {"a":3,"extra":true,"s":"z"} | 3    | z
-- 4 | [1,2]                        | NULL | NULL
-- 5 | NULL                         | NULL | NULL
```

Row 3 has a key the shredded schema does not cover, and row 4 is not an object at all. Both live in `value` rather than `typed_value`, and both come back intact. A shredded table now reads the same JSON as an unshredded one.

## What is not done

The shredded read is a read. SQE writes unshredded variants and ignores `write.parquet.shred-variants`, so a table SQE writes gets none of the shredding benefit.

Unshredding on read also throws the benefit away on the read side. SQE turns `typed_value` back into a plain variant before the query sees it, so there is no pruning on shredded fields and no typed field access. `json_get_int(v, 'a')` decodes the whole value even when `a` sits in its own typed Parquet column. Doing better means pushing the field path into the scan, which is the next real piece of work.

SQE has no `variant_get` or typed path access yet. Field access goes through the variant's JSON text. The `json*` functions cover the queries we see, but code ported from Spark will reach for `variant_get` first. A variant nested inside a `ROW` or `ARRAY` arrives on Flight SQL as the raw storage struct rather than JSON text: only top-level columns are converted.

On the Iceberg matrix both cells, variant type and shredded variant, score partial. Partial is the right score. Reading a format is the first half of supporting it, and shredding exists for the second half.
