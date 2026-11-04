### Fixed

- The benchmark's Trino comparison checks values, not only row counts (#785)

    A query that returned the right number of rows with a wrong value was reported as a match. Both engines' rows are now sorted and compared cell by cell. Integers and decimals compare exactly after rounding both sides half-up to the smaller scale, so DataFusion's `avg(decimal(7,2))` at scale 6 matches Trino's at scale 2. A float on either side compares within a relative tolerance of 1e-6. A mismatch gets the new status `ValueDiff` with the first differing cell and the count of differing rows, in the compare JSON (`value_diff`) and in the summary. It does not fail the run. On TPC-DS SF1 it flags 12 of the 99 queries and on TPCx-BB SF1 one of 10; most differ in the last digit because DataFusion truncates decimal division and casts where Trino rounds.
