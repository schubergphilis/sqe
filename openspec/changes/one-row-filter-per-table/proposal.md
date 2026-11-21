# One row filter per table and user (TA-13, #931)

## Why

A tag row filter used to be ANDed with the table's resource row filter, and with every other tag filter on the table. Unity Catalog ABAC does not combine row filters: a query that more than one row filter applies to fails. Spark's tag enforcement on the platform refuses the same case. An engine that ANDs answers with fewer rows where the other engines answer with an error, so a steward who tests a policy in one engine learns nothing about the other.

Under `mask-precedence = "refuse"` (#918) SQE already refused a tag row filter that met a different filter. The other precedences only decide which *mask* wins; they have no say over row filters, so the refusal belongs in every mode.

## What Changes

- When a tag row filter and a different row filter (a resource filter, another tag's filter, or the same tag's filter under its catalog-scoped name) resolve for a table and user, the statement is refused with `row filter conflict on <catalog>.<schema>.<table>` (AccessDenied) under every `mask-precedence`.
- The same filter from two sources is one filter. The deny-all sentinel (a failed lookup, an unparseable filter) is never counted.
- Reads, and the target of DELETE / UPDATE / MERGE, are refused alike: both go through the plan rewriter.
- Statements that only read the policy (`SHOW EFFECTIVE POLICY`, `SHOW STATS`, metadata functions) see a contested table as denied, as under `refuse`.
- Unchanged: several resource row-filter policies with no tag filter still AND.

## Impact

- **Behaviour change for tag policies.** A user covered by both a resource and a tag row filter gets an error where they used to get the intersection. Writing the intersection as one filter restores the old answer.
- `tag_conflict_guard` (the write-time check of #918) still runs only under `refuse`, so in the other modes a `SET TAGS` that creates a row-filter conflict is accepted and the conflict shows when the table is read. Follow-up.
