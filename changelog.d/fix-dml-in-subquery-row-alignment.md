### Fixed

- DELETE and UPDATE with `IN (subquery)` changed the wrong rows

    Since v0.30.0 a `WHERE ... IN (SELECT ...)` or `NOT IN (SELECT ...)` in DELETE or UPDATE was evaluated through a join that returns the matched rows first, and the verdict was applied to the data file by position. `DELETE FROM t WHERE id IN (SELECT id FROM k)` with `k` holding id 2 deleted id 1, and a copy-on-write UPDATE overwrote the first row with the matched row's new values. Merge-on-read DELETE and UPDATE were affected the same way; literal `IN (...)` lists, scalar subqueries and MERGE were not. The evaluation now carries each row's position and orders by it, and fails the statement if the row count does not line up. Tables changed by such a statement on an affected release should be checked against their previous snapshot.
