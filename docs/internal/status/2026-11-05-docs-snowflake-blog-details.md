**STATUS 2026-11-05: Snowflake blog, more on what the run tests and the parquet skip bug (`docs/snowflake-blog-details`).**

Blog-only change to `docs/site/blog/2026-11-05-snowflake-and-sqe-read-each-others-tables.md`:

- New section "What the 40 reads cover": a table of the 27 Snowflake-written
  and 13 SQE-written reads by area (counts taken from `compare.tsv`), and what
  a value match proves (epoch-micro timestamps, widened decimal/int values,
  checksum + range + point lookups on the million-row merge-on-read tables).
- Deletion vectors: the parquet bug explained. DataFusion and SQE read through
  arrow-rs `parquet`; a DV becomes a row selection; `DeltaBitPackDecoder::skip`
  in 59.3.1 only accepted 32/64-value miniblocks (source comment "no current
  writers use anything else", arrow-rs #9794); Snowflake writes 512, Photon
  256; 59.3.2 skips in 64-value chunks with a stack buffer (#11021/#11754).
- Closing paragraphs on interoperability: 120 reads against Snowflake and 81
  against Databricks on one build, all value matches.

No code change, no changelog fragment.

**NEXT:** merge after !1663 and !1664 or before; this MR does not touch the
"Names" section or the "What else the run found" list those two edit.
