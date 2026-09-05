# 900: HTTP endpoint allowlist validates URL userinfo as the destination host

- Area: External data access and credential isolation
- Priority: P1
- Order within area: 1
- Scope: security and large-data execution
- Severity: high
- Category: security
- Effort: S
- Status: fixed (branch `fix/security-scale-review-2026-09-05`, 2026-09-05)
- Confidence: high in source-level defect; deployment impact depends on stated prerequisites

## Problem

`TvfPolicy::check_endpoint` in `crates/sqe-core/src/config.rs:2930` splits the authority on `:` rather than parsing a URL. With `allowed_http_hosts = ["allowed.example"]` and `allow_http = false`, this passes:

```text
https://allowed.example:password@blocked.example/
```

The validator sees `allowed.example`; the URL hostname is `blocked.example`. `check_path` repeats the same parsing at `config.rs:2813`. S3 TVFs pass the validated endpoint to the storage builder (`crates/sqe-catalog/src/read_parquet.rs:339`, `crates/sqe-catalog/src/file_tvf_common.rs:592`).

## Why it matters

An authenticated caller permitted to use a file TVF can bypass the intended endpoint host restriction. Complete inline S3 credentials can satisfy the separate object-path gate when inline credentials are enabled. Actual outbound access additionally requires network reachability and acceptance of the URL by the storage client. HTTP path inference is also exposed before the execution-time lazy-registry gate; do not rely on a later check to undo planning-time IO.

This is a validator bypass, not a demonstrated metadata-service credential theft. No outbound requests were made.

## Proposed change

Parse with `url::Url`, require an accepted scheme, reject username/password components, and compare the parsed host against the allowlist. Share the implementation between path and endpoint gates. Apply destination checks before any inference IO.

## Validation and acceptance criteria

`reproduce.py` compiles and executes the current endpoint method extracted verbatim; it confirms acceptance of the crafted URL and rejection of an ordinary blocked hostname. Add production unit tests for userinfo, IPv6, explicit ports, mixed case and malformed URLs. Add a loopback-only integration test showing a rejected destination receives no request.

## Related

Distinct from prior 604 (quack_query has no destination gate): this bypasses the existing read_* TVF gate.

## Resolution (2026-09-05)

`TvfPolicy::check_path` and `check_endpoint` now share `check_http_destination` (`crates/sqe-core/src/config.rs`), which parses with `url::Url`, requires `http`/`https`, rejects any userinfo, rejects backslashes and empty authorities, and compares the parsed host against the allowlist. `allow_http = true` still requires a well-formed URL without userinfo. Unit tests `tvf_http_destinations_reject_userinfo_and_ambiguous_authorities` and `tvf_http_destinations_parse_ipv6_ports_and_host_case` cover userinfo, IPv6, explicit ports, host case and malformed authorities. The loopback integration test is not added; `reproduce.py` now runs the production unit tests instead of an extracted copy.
