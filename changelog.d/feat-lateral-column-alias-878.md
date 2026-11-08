### Added

- Lateral column aliases: `SELECT a + 1 AS b, b * 2 AS c FROM t` (#878)

    A select-list item can use an alias defined earlier in the same select list, with
    Databricks' rule that a column of the `FROM` relation wins over an alias of the same name.
    When planning fails on a bare name that an earlier item aliases, the planner inlines that
    item's expression in parentheses and plans again, so `b * 2` becomes `(a + 1) * 2`.
    Qualified names, subqueries and lambda parameters keep their own scope.
