### Fixed

- `bigint` arithmetic and unary minus fail on overflow, as in Trino

    `9223372036854775807 + 1` fails with `bigint addition overflow` instead
    of wrapping to the smallest `bigint`; `-` and `*` do the same. `-x` on
    the smallest `integer` or `bigint` fails with `integer negation
    overflow` or `bigint negation overflow`. The literal `-2147483648` is
    `bigint`, as in Trino.
