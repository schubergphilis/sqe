**STATUS 2026-11-21: MERGE under a tag row filter is tested, and the tag-inheritance claims name MERGE again (`test/merge-tag-row-filter-924`).**

!1910 left MERGE out of the tag-inheritance IT and docs because every MERGE into a row-filtered table failed; !1911 fixed that and tested it with a resource filter. This branch adds the tag case: `crates/sqe-coordinator/tests/it/merge_row_filter_e2e.rs` runs the same MERGE UPDATE, unaliased MERGE, MERGE DELETE and MERGE INSERT (inside and outside the filter) under a tag policy `id > 1` on the table tag `eu`, inherited by the columns (#924), in copy-on-write and merge-on-read. Without the `unnormalize_col` fix from !1911 the tag cases fail with the original `No field named <table>.id`.

Docs restored: the tag-inheritance spec (requirement and scenario), tasks 1.5, the fine-grained access control page and the #924 changelog fragment name MERGE next to UPDATE and DELETE.

Gate: `make test-integration FILTER=merge_row_filter` -> 4 passed; `FILTER=merge` -> 18 passed (both on this branch).

**NEXT:** schema and catalog tag inheritance (#924 part 2).
