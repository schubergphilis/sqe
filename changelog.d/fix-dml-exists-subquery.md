### Fixed

- DELETE and UPDATE accept a correlated `EXISTS` subquery in the WHERE

    `DELETE FROM t WHERE EXISTS (SELECT 1 FROM k WHERE k.id = t.id)`, its `NOT EXISTS` form and the same WHERE on UPDATE failed with "Physical plan does not support logical expression Exists". The WHERE was evaluated as an expression in a SELECT list, where DataFusion cannot decorrelate a subquery. It now runs as a WHERE clause that returns the positions of the matching rows, and UPDATE reads that verdict instead of evaluating the WHERE a second time. Row filters and column masks apply as before.
