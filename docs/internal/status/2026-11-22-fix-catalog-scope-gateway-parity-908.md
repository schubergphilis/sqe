# fix/catalog-scope-gateway-parity-908

**Date:** 2026-11-22
**Issue:** #908 (portal deltas from the data-run catalog scope, gateway parity)

The `iss`/`iat`/`typ` deltas were already on main (cc361c381). Compared
claim by claim with the run gateway's scope check, three differences
were left and are closed here:

- `iss` must be one string. The JWT library also accepts an array that
  contains the issuer; RFC 7519 makes `iss` a string. Refused now.
- A trailing `/` in the configured `issuer` is dropped, as the gateway does.
- `token_type = "JWT"` (any case, `application/` prefix) refuses startup.

Same in both: required `iss`, `sub`, `jti`, `exp`, `catalogs`; `nbf`
enforced when present; `iat` and `aud` not checked (bound by `sub`/`jti`);
`typ` absent, `JWT` or the configured type, anything else refused before
any key fetch. Defaults differ by design (`header_name`, `token_type`):
the deployment sets them to the portal's values.

**NEXT:** the gateway's array-`iss` acceptance (same library) is reported
to the portal side; no SQE change pending for #908 deltas.
