### Fixed

- `FOR INCREMENTAL BETWEEN SNAPSHOT` refused valid ranges about half the time

    The start and end snapshots were ordered by comparing their ids, but Iceberg snapshot ids are random. When the older snapshot drew the larger id, the read failed with "start must be older than end". The order is now decided only by walking the end snapshot's parent chain: a start that is not an ancestor of the end is still refused, with "is not an ancestor".
