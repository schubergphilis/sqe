**STATUS 2026-10-18: variant blog post and stale variant lines (`docs/variant-v3-blog`).**

New post `docs/site/blog/2026-10-18-json-columns-are-variants-now.md` covers VARIANT storage, JSON-as-variant on V3 with V3 as the default, Spark 4.1 interop both ways, and shredded reads (#849). Claims checked against the code and IT tests (error texts, Spark 4.1.3 / iceberg-spark-runtime 1.12.0, test table names, the manifest variant-bound skip). The book's `ddl.md` and `iceberg.md` still said shredded files were unsupported; fixed. The dlt post's ADBC `connect` now passes `autocommit=True` (DB-API defaults to a transaction, so its DML never landed).

**NEXT:** unchanged.
