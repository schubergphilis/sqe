### Security

- Under `[policy.ranger] catalog_scoped`, a tag policy written in SQL now governs one catalog (#919)

    `CREATE POLICY ... ON TAG pii` stores the policy on `<session catalog>.pii` instead of a bare `pii` that applied in every catalog; a dotted tag is kept as written. `SET MASKING POLICY` on such a policy tags the column with the bare `pii`. `CREATE OR REPLACE POLICY` refuses to move a policy to another tag, which would change the catalogs it governs. A bare tag policy created before keeps applying in every catalog: it can only add a mask. With `catalog_scoped` off nothing changes.
