**STATUS 2026-09-06: identity, credentials and transport trust batch (#652) fixed and rebased on main, which now carries #657 (errors, audit, observability).**

Branch `fix/identity-transport-trust-p1` (work package #652) closes 204,
207, 208, 405, 604, 401, 601, 702, 606, 600, 603 and 210: scan tickets are
Ed25519-signed by a coordinator-only key with audience and expiry, workers
pin the public key from the heartbeat, heartbeats must come from the host
they advertise, ticket credentials are typed (vended / static /
worker_default) with `production_mode` refusing the implicit default,
`quack_query` is off by default and allow-listed, bearer replay reuses one
session, bearer validation requires an issuer and honours nbf and EC keys,
Trino polls carry a token and are rate-limited on every route, Quack
sessions expire with the token and page large results, the unreachable
shuffle intake is behind `worker-shuffle`, and one TLS builder serves the
Flight, MCP and worker listeners with client TLS on every hop. 209 is
partial. GitLab: issues #543-#651, work packages #652-#659.
**NEXT:** work package #654 (data consistency: caches, writes and
transactions: 302, 304, 507, 407, 500, 502, 501, 506, 505, 608, 511, 514,
plus the 902 fingerprint-bound result cache), then #655 memory safety.

**Previous (2026-09-05, merged): work package #657 (errors, audit, observability) on `fix/errors-audit-observability`.**

Closed findings 607, 800, 104, 305, 709, 306, 812, 410, 612 (610 partial).
Audit chain resumes across restarts with a bounded queue and drop counter;
error codes are typed (`with_code`, `From<DataFusionError>`, catalog HTTP
status) instead of English substring; Trino HTTP errors map by status;
listing outages no longer look like empty catalogs; swallowed restore and
rows-affected errors surface; query spans are not entered across await;
telemetry init and metrics bind fail closed. Each finding file has a
Resolution section.
**NEXT:** remaining P1s (202, 400, 503, worker trust) and 610 lineage mask
facet.
