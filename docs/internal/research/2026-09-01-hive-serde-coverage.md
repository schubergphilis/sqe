# Which Hive SerDes SQE reads, and which are worth adding

Companion to `2026-09-01-glue-hive-catalog-survey.md`. That one asked what a
real Glue catalog contains; this one asks what Glue can express that SQE
cannot yet read, and which of those gaps are worth closing.

## Read today

`serde_map::resolve_format` accepts five SerDe classes, mapping each to a
DataFusion `FileFormat`:

| SerDe class | Format |
|---|---|
| `org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe` | CSV (delimited text) |
| `org.apache.hadoop.hive.serde2.OpenCSVSerde` | CSV |
| `org.openx.data.jsonserde.JsonSerDe` | JSON |
| `org.apache.hive.hcatalog.data.JsonSerDe` | JSON |
| `org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe` | Parquet |

When a table records no SerDe, `kind_from_input_format` falls back to the
input-format class, accepting `TextInputFormat` (CSV, or JSON when
`classification = json`) and `MapredParquetInputFormat`.

Those five cover 100% of the surveyed catalog: 451 Parquet, 181 openx JSON,
3 LazySimple, 1 OpenCSV, and nothing else.

## Named and rejected

`friendly_keyword_for_input_format` already enumerates the formats worth a
readable rejection rather than a raw class name: ORC, Avro, SequenceFile,
RCFile. That list is the honest statement of what Glue commonly holds and
SQE refuses.

## What is worth adding, in order

**Avro is nearly free.** DataFusion 55 ships an `avro` feature backed by
`datafusion-datasource-avro`, so the work is a cargo feature, one
`HiveFileFormat` variant, and one arm in `listing_options`. Everything else,
the SerDe class match and the input-format fallback, is a two-line addition
each. Avro is also the most likely of the four to show up in a Kafka or
Kinesis-fed lake.

**ORC needs a dependency decision.** DataFusion has no ORC reader. Reading it
means taking `orc-rust` or equivalent, which is a third-party crate outside
the Arrow project. Worth doing only when a catalog actually contains ORC;
this one does not.

**RegexSerDe is a plausible third.** `org.apache.hadoop.hive.serde2.RegexSerDe`
is common for log tables and is implementable over the existing text path:
read whole lines, apply `input.regex` from the SerDe parameters, project the
capture groups as columns. No new dependency. Not present here either.

**SequenceFile and RCFile are not worth it.** Both are legacy Hadoop
container formats with no maintained Rust reader. Rejecting them by name, as
the code already does, is the right answer.

Two further Glue-native SerDes deserve a mention because Glue offers them and
nothing here reads them: the Grok SerDe (`com.amazonaws.glue.serde.GrokSerDe`)
and the Amazon Ion SerDe. Both are AWS-specific and neither appears in this
catalog. They are not currently in `friendly_keyword_for_input_format`, so a
table using one is rejected by raw class name rather than a readable label.
Adding them to that match is a small honesty improvement even without
reading support.

## The gap that is not a SerDe

Compression is invisible in Glue's `StorageDescriptor`, and its
`compressionType` parameter cannot be trusted: it is present on 25 tables in
this catalog and reads `none` on every one, while 127 tables are `.json.gz`.
`serde_map::listing_options` builds `JsonFormat::default()` and never calls
`with_file_compression_type`, so those tables feed gzip bytes to the JSON
parser. `read_json` gets this right through its own `compression` argument,
defaulting to detection from the path extension. The Hive provider needs the
same inference from the object key.

That single fix reaches more tables than every unsupported SerDe combined.
