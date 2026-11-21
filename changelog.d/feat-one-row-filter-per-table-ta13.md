### Changed

- One row filter per table and user: a tag row filter that meets a different row filter is refused under every mask precedence (#931)

    When a tag row filter and a resource row filter, or two different tag row filters, apply to the same table for the same user, the statement now fails with `row filter conflict on <catalog>.<schema>.<table>` instead of ANDing them. This matches Unity Catalog ABAC. It used to happen only under `mask-precedence = "refuse"`. Reads and DELETE / UPDATE / MERGE targets are refused alike, and `SHOW EFFECTIVE POLICY`, `SHOW STATS` and the metadata functions show a contested table as denied. The same filter from two sources counts once. Several resource row filters with no tag filter still AND. Upgrade note: a user covered by both kinds of filter now gets an error where they used to get the intersection. Write the intersection as one filter to keep the old answer.
