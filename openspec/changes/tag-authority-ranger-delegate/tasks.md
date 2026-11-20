## 1. Config

- [x] 1.1 `TagAuthority { AdminRole (default), RangerDelegate }` and `AccessControlConfig.tag_authority` (alias `tag-authority`); parse test like `grant_authority_parses_both_spellings_and_defaults_safe`
- [x] 1.2 `sqe.toml.example` entry

## 2. Policy store

- [x] 2.1 `PolicyStore::grants_column_privilege(user, table, namespace, column, TablePrivilege)`, default `Ok(false)`
- [x] 2.2 Ranger: `access_grants_column_type` (D2) + impl; unit tests: column allow, other-column allow, `*` allow, wildcard, excludes, column deny, `*` deny, conditional allow ignored, no column resource
- [x] 2.3 In-memory: table-level registry answers for its columns

## 3. Gate

- [x] 3.1 `require_tag_authority` (D3, D4) for SET/UNSET TAGS and SET/UNSET MASKING POLICY
- [x] 3.2 Unit tests (grant_introspection_gate_test): default mode unchanged; delegate + in-memory grant passes; delegate + no grant refused 403; delegate + backend without the model refused; admin passes

## 4. Acceptance (live Ranger)

- [x] 4.1 Steward with column `alter` sets a tag; the mask applies on the next query; then unsets it
- [x] 4.2 Steward refused on another column (deny item, because the fixture's `public` holds `alter`), with an audit record
- [x] 4.3 Bare name under `USE` checked against the session namespace
- [x] 4.4 Default `admin-role` unchanged (existing tests)

## 5. Docs

- [x] 5.1 Book access-control page: the setting, `alter` on the column, the warning about broad `alter` grants, the paths that stay admin-only (D5)
