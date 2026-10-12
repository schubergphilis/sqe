**STATUS 2026-10-08 (ce): property tests against existing oracles (`test/property-oracles`, #639 item 4).**
- **Trino types.** Every scalar Arrow type's Trino name parses back through `parse_trino_type` to a non-opaque type, with decimal precision/scale and timestamp precision/zone intact. Its `typeSignature` keeps parameters out of `rawType` (#345).
- **Error classifiers.** `has_status_token` is checked against a tokenizer oracle. A 401 marker classifies as AuthenticationFailed under any surrounding noise, and a port containing `401` stays a timeout. Expired engine credentials win over `403 Forbidden`. Both classifiers ignore ASCII case.
- **OCSF mapping.** Caller text (user, groups, client label, query text, error message) lands verbatim through a serialize/parse round trip, and a missing value is an absent key.

**Open:** nested types (`array(...)`, `map(...)`, `row(...)`) still emit a parameterized `rawType` with no arguments; tracked in its own issue.
