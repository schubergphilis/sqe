### Fixed

- Unknown tag state refuses the statement instead of returning no rows (#840)

    When a table's column tags could not be read (metadata cache miss,
    disabled cache, malformed `sqe.column-tags`), the policy rewriter
    injected a deny-all row filter. The query succeeded with the right
    schema and 0 rows, and a CTAS over it created an empty table, with only
    a WARN in the coordinator log. The statement now fails with
    `AccessDenied`: `tag state unknown for table <name>: its column tags
    could not be read, ...`. It still fails closed. File table functions and
    reserved virtual relations (`information_schema`, `system`) are
    unaffected, since they carry no tags. A client that relied on the empty
    result now sees an error.
