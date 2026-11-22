## 1. Catalog scope mode (#295, SQE half)

- [x] 1.1 `CatalogScopeMode` on `CatalogScopeConfig`, default `binding`; validation; startup log of the active mode (D1)
- [x] 1.2 Gate per mode: `binding` as today; `union` verifies, audits and does not attach; `off` installs nothing; scoped client set only in `binding` (D2, D3)
- [x] 1.3 `ScopeReason` as the error source; audit `error_type` from it; read-only message names it (D4)
- [x] 1.4 Unit tests: config parse and validation per mode; the D2 table on the gate's decision; the reason wire names
- [ ] 1.5 Integration: `union` reads outside the scope and refuses a missing scope; `off` accepts a missing scope; `binding` audit reasons (all but the live `union` read are covered: union refuses missing/bad scopes and attaches nothing, offline)
- [x] 1.6 Docs: configuration reference and the security page; Deploy section in the MR
