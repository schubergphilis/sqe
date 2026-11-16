### Added

- A column tag committed by another engine reaches Ranger's tag store (#903)

    With `[policy.ranger] project-tags = true`, every table SQE loads for a
    query is compared with the tags SQE last projected for it. When another
    client changed `sqe.column-tags` (Spark, a raw REST commit), SQE projects
    the difference in the background, so Spark masks what SQE masks. A data
    commit that leaves the tags alone sends nothing to Ranger. The first load
    of a tagged table after a restart projects its tags once; a tag removed
    while the coordinator was down stays in Ranger until
    `CALL system.reproject_column_tags` repairs it. The projection uses the
    coordinator's Ranger credentials on behalf of whoever committed the tag.
