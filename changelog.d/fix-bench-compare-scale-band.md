### Fixed

- `sqe-bench --compare-trino` no longer rounds a decimal result twice when the two engines return it at different scales (#785)

    The value comparator rounded the wider side half-up to the narrower scale. A value that both engines had already rounded correctly then differed: TPC-DS q12's `revenueratio` is 1.20649747..., SQE returns 1.2064975 at scale 7, Trino 1.206497 at scale 6, and 1.2064975 re-rounds to 1.206498. Two exact numbers now match when one true value could round to both (`|a - b| < ½·10^-sa + ½·10^-sb`). At equal scales that is still exact equality, so a wrong cent is still a value diff, and so is the q58/q78 `ss_dev` mismatch.
