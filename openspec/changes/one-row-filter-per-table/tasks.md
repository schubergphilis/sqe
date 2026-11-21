## 1. Enforcement

- [x] 1.1 `plan_rewriter::merge_tag_row_filters`: count distinct filters once a tag contributes one, refuse more than one, keep the deny-all sentinel uncounted; called from every precedence
- [x] 1.2 Unit tests under `tag`, `resource` and `refuse`: tag + resource, two tags, the same filter twice, tag alone, sentinel, resource-only still ANDed

## 2. Verification

- [x] 2.1 Live Ranger, default precedence: SELECT and DELETE refused, an untargeted user unaffected (`access_control_e2e::tag_and_resource_row_filter_refuse_the_query`)
- [x] 2.2 Full access-control suite green under the new rule

## 3. Docs

- [x] 3.1 Book: fine-grained access control, "One row filter per table"

## 4. Follow-up

- [ ] 4.1 Refuse a `SET TAGS` that creates a row-filter conflict in every precedence (the #918 write guard runs under `refuse` only)
