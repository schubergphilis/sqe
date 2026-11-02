### Fixed

- Distributed compaction plans the table once per job per worker, not once per file group

    A worker rewriting a file group loaded the table metadata, planned a
    delete-aware read of the whole table and listed every live delete file,
    then used the part of that plan covering its two or three files. Every
    group repeated it, so a job cost groups x manifests: on a 200-file table,
    planning was 393 ms of a 420 ms group. Workers now keep the pinned table,
    read plan and live delete files per (job, metadata location, snapshot,
    credentials) and share them across the job's groups, with one in-flight
    build per job. A failed build, such as a snapshot mismatch, is not cached.
    The 100-group distributed benchmark drops from 22.8 s to 2.9 s; 98 of 100
    groups reuse the plan. The cache holds four jobs and drops a job's plan
    after five idle minutes, never while a group is using it.
