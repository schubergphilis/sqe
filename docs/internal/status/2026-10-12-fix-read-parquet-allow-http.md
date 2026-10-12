**STATUS 2026-10-12: `read_parquet` derives plain HTTP from an `http://` endpoint (`fix/read-parquet-allow-http`).**

`read_parquet` built its own `AmazonS3Builder` and enabled plain HTTP only when `s3_allow_http = true`, so against an `http://` endpoint (RustFS, MinIO) the first request failed with a bare "builder error". `read_csv`, `read_json` and table scans already derive it from the endpoint scheme; `read_parquet` now does too. An inline `endpoint =>` still has to pass `check_endpoint` first. Found while writing the `{catalog}` placeholder IT, whose `s3_allow_http` workaround is removed here; the IT passes without it (`make test-integration FILTER=tvf_catalog_placeholder`). Unit test: `test_s3_builder_allows_http_for_an_http_endpoint`.

**NEXT:** unchanged: native audit log rotation; #835; #803 when it recurs.
