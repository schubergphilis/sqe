### Fixed

- Lineage: column lineage through a view names the view body's base columns

    A view's body is optimized before it is inlined, so its scans read a subset of the table's columns. The column tracer named the table's full schema by position and reported the wrong columns, for example `total <- items.id, items.label` instead of `items.amount, orders.qty`.
