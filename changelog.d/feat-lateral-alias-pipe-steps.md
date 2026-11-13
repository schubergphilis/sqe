### Added

- Lateral column aliases in pipe `SELECT` and `EXTEND` steps (#878)

    A pipe step can now refer to an alias defined earlier in the same step, the same way a plain `SELECT` list can: `FROM t |> SELECT a + 1 AS b, b * 2 AS c` and `FROM t |> EXTEND a + 1 AS b, b * 2 AS c` both plan. A real column with the same name still wins over the alias.
