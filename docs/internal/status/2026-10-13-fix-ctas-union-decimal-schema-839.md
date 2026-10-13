**STATUS 2026-10-13: CTAS creates the table from the analyzed schema (`fix/ctas-union-decimal-schema-839`, #839 part 2).**

`integration_test`'s departments fixture (`500000.0 ... UNION ALL ... 1000000.0`) failed with "cast Decimal128(8,1) to Decimal128(7,1)". With `parse_float_as_decimal` a freshly planned UNION takes its first branch's type (Decimal(7,1)); DataFusion's analyzer widens it to Decimal(8,1) at execution, and the streaming sink then cast the batches onto the narrower table. New `file_tvf_prepare::analyzed_schema` runs the analyzer only; Iceberg CTAS (`handle_ctas_streaming`, on the planning stack), Hive CTAS and the write path's IN-subquery materialiser (whose `MemTable` rejected batches typed differently from the planned schema) use it. `test_view_with_aggregation` fails without the change and passes with it; the `ctas` integration tests pass. The Hive CTAS path is covered by the unit test only.

**NEXT:** native audit log rotation; #803 when it recurs.
