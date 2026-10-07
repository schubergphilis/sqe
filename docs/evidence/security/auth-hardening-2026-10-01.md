# Authentication hardening — 2026-10-01

Reviewed main `0a7915fa5ac92aaa100bd26e0aab0978cbf55a27` for #779 and #781, including surrounding interactive OAuth state transitions and logging. Additional findings are tracked in #793 and #794.

## Bounded rate-limit state (#779)

Auth now checks the source-IP quota before admitting a credential tuple. Auth tuple, IP, query-user and metadata-user maps each admit at most 16,384 SHA-256 fingerprints (length-prefixed inputs, 32-byte retained keys). Admission, quota consumption and reclamation share a lock. At capacity, new identities are rejected; existing identities retain their consumed budget. This may temporarily reject legitimate new users during a cardinality attack; it intentionally avoids active eviction, which would reset brute-force budgets.

Request-driven reclamation runs at most once per minute and removes only identities idle for at least the larger of one minute and the full burst refill duration. Denied activity extends last-seen time. No detached cleanup task is required. This bounds retained state, not the cost of parsing or hashing incoming oversized credentials, and is not a general connection/admission control solution.

Regressions exercise 10,000 username rotations after the IP quota is consumed, concurrent distinct-key admission at a small cap, preservation of exhausted existing buckets, full-refill idle reclamation, denied activity, and fixed retained key width.

## Interactive OAuth state (#781, #793)

Shared per-session entries serialize callback claims and token consumption. Only one callback receives the PKCE verifier. Pending/in-flight polling remains repeatable; only one completed-token reader succeeds. A consumed tombstone protects observers that obtained the shared entry before cache invalidation.

Completion/failure only mutate an existing nonterminal entry. They do not insert or refresh its deadline, recreate a removed/expired session, or overwrite a terminal result. A successful late exchange for a missing session returns HTTP 400 rather than a success page. IdP authorization-code validation remains the authentication boundary; the callback overlap finding demonstrates login disruption, not an authentication bypass.

Regressions use synchronized threads for competing callback claims and token readers, and cover late results after removal, terminal result preservation, and the original TTL across exchange completion. The Trino handler test also verifies that browser-visible state cannot retrieve the token and that the client capability retrieves it once.

## Secret output (#794)

The challenge-start event no longer contains the client-only poll capability or authorization URL. Auth-code rejection warnings record status rather than an untrusted IdP response body. Challenge/session/token Debug implementations redact secrets; private response and Trino token-poll response types no longer derive Debug. Canary tests verify redaction. This does not add secret zeroization or prove that production debug logging was enabled.

## Validation

- `cargo test -p sqe-auth -p sqe-trino-compat --lib`: 284 auth and 163 Trino tests passed.
- `cargo test -p sqe-coordinator --lib rate_limiter`: 17 passed.
- `cargo clippy -p sqe-auth -p sqe-trino-compat -p sqe-coordinator --tests -- -D warnings`: passed.
- `git diff --check`: passed.
- Local HTTP fixture tests require loopback access. The local build used a temporary SDK-aware Clang linker wrapper because this workstation's default Mach-O linker does not resolve its SDK correctly; no repository build configuration changed.
