### Fixed

- A statement over its own memory limit fails as `EXCEEDED_QUERY_MEMORY_LIMIT`, not `RESOURCE_EXHAUSTED` (#933)

    A statement that alone needs more than `query.max_query_memory` now carries its own error name (Trino `errorType` `INSUFFICIENT_RESOURCES`, code 65538) over the Trino HTTP endpoint and Flight SQL. Flight answers `FAILED_PRECONDITION` instead of `RESOURCE_EXHAUSTED`, which clients retry, and the Trino endpoint sends no `Retry-After`. Load refusals keep `RESOURCE_EXHAUSTED`: a shared pool full of other statements, the pressure-admission quota, the queue, S3 throttling and the `SQE_<REASON>` admission refusals. A result buffer over the cap, a sort-on-write gate refusal and a Hive insert over the cap use the new name too. The `max_result_rows` and spill-cap refusals are unchanged.
