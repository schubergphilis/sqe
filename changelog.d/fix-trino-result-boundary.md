### Fixed

- Trino result boundary: unsigned integers, non-finite doubles and `CAST(json AS varchar)` now match Trino over HTTP and Flight SQL

    Flight SQL sends an unsigned column as the next wider signed type (`UInt64`/`UInt32` as `Int64`, `UInt16` as `Int32`, `UInt8` as `Int16`), as Trino has no unsigned types; a `UInt64` above `i64::MAX` is an error instead of a silent NULL. The Trino HTTP protocol labels unsigned columns the same way and encodes `NaN`, `Infinity` and `-Infinity` as the strings Trino clients expect, where they were `null` before. `CAST(<json> AS varchar)` returns Trino's text: a JSON string unquoted, a number or boolean as its text, JSON `null` as SQL NULL, and an error for an array or object (`TRY_CAST` gives NULL); it was the JSON text with its quotes. A cast the engine builds itself (named target field, such as a partial column mask) keeps the JSON text.
