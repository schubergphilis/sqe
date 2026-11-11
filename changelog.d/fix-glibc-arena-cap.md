### Fixed

- Concurrent spilling queries no longer push the server into the OOM killer on glibc (#788)
  `sqe-server` now caps glibc malloc arenas at two (`mallopt(M_ARENA_MAX)`)
  before any thread starts. With the glibc default (8 x cores), heap freed
  by spilling sorts, joins and aggregates stayed fragmented across dozens of
  arenas, invisible to the memory pool: an SF100 run of two sorts, two
  aggregates and two joins reached 25 GB RSS with 4.6-10 GB in the pool and
  was killed. With two arenas the same run completes. Setting
  `MALLOC_ARENA_MAX` in the environment still wins.
