### Fixed

- A JSON arrow on a bare column (`payload -> 'session' ->> 'type'`) plans again instead of failing with "Unsupported ast node in sqltorel: Lambda" (#913)

    Since lambdas were added, the parser read a bare column followed by `->` as a one-parameter lambda wherever it stood. The planner now parses `->` as an operator and makes it a lambda only in the second and later arguments of a higher-order function (`transform`, `filter`, `reduce`, `any_match`, ...), so both forms work in one statement. A qualified column (`e.payload -> 'k'`) and `->>` were never affected. Text that only parses with lambda syntax (`lambda x: ...`, a typed lambda parameter) is planned as before. Policy expressions (`ROWS WHERE`, masks) are parsed by DataFusion directly and still read `col -> ...` as a lambda.
