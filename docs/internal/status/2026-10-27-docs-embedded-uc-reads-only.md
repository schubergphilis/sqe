**STATUS 2026-10-27: correct the embedded-CLI claim from the Unity Catalog interop docs (`docs/embedded-uc-reads-only`).**

!1571 said the embedded CLI runs DML on an attached catalog. It does not: UPDATE, DELETE and MERGE are refused ("UPDATE operation on table ..."), ALTER is unsupported, and CREATE TABLE takes plain columns only (no PARTITIONED BY, STRUCT/ARRAY, TBLPROPERTIES). The harness only ever wrote through the coordinator. Blog, book, harness README and matrix caveats now say "reads only".

**NEXT:** embedded DDL/DML through the coordinator's write handlers (`sqe-write`), after the worker delete-file ticket.
