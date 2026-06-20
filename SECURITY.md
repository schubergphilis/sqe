# Security Policy

## Supported Versions

SQE is in pre-1.0 active development. Security fixes land on `main` and
are released as patch versions on the latest minor. Older minors do not
receive security backports while we are pre-1.0.

| Version | Supported |
|---------|-----------|
| 0.15.x  | yes       |
| < 0.15  | no        |

Once SQE reaches 1.0 this policy will be revisited and updated to
reflect a longer support window.

## Reporting a Vulnerability

Please report security issues privately. Do not open a public GitHub
issue, do not file a regular bug report, and do not discuss the
vulnerability on the discussion forum until a fix is published.

**Preferred channel:** email [security@schubergphilis.com](mailto:security@schubergphilis.com)
with `[SQE]` in the subject. Include:

- A description of the issue and the impact.
- Steps to reproduce, or a proof-of-concept.
- Affected versions of SQE if you know them.
- Any mitigations or workarounds you have already identified.
- The contact information you want us to use for follow-up.

If email is not workable, you may use GitHub's private vulnerability
reporting on the
[security advisories page](https://github.com/schubergphilis/sqe/security/advisories/new).

## What to Expect

We aim to:

1. Acknowledge receipt within **3 business days**.
2. Confirm the issue or close it as not-a-bug within **10 business
   days**, with a written explanation either way.
3. For confirmed vulnerabilities, share a target fix date based on
   severity (CVSS 9+ within 14 days, CVSS 7-9 within 30 days, lower
   severity in the next regular release).
4. Coordinate disclosure with you. We aim for a 90-day disclosure
   window from confirmation; we will negotiate if you have a different
   timeline.
5. Credit you in the release notes and any CVE filing, unless you
   prefer to remain anonymous.

## Scope

In scope:

- The SQE coordinator, worker, and CLI binaries.
- The `sqe-*` crates published to crates.io (when that happens).
- Documentation that could mislead operators into insecure
  configurations.
- The vendored `iceberg-rust` fork in `vendor/iceberg-rust/`.

Out of scope:

- Third-party services SQE talks to (Apache Polaris, Project Nessie,
  AWS Glue, AWS S3 Tables, Hive Metastore, Postgres, etc.). Report
  issues there to the respective project.
- Configuration mistakes that are not the result of misleading
  documentation. Permissive IAM, public S3 buckets, weak passwords on
  your catalog database, and similar are not SQE vulnerabilities.
- Apache DataFusion and the broader Rust crate ecosystem. Report
  upstream.

## Hardening Guidance

The
security audit summary
documents the hardening work we have already done (43 findings
resolved). New deployments should:

- Run the coordinator and workers behind TLS in any non-development
  environment. The Flight SQL listener supports TLS; the Trino HTTP
  endpoint should be terminated by an upstream proxy.
- Validate JWT issuers and audiences strictly in
  `[auth]`. Anonymous and bearer-passthrough modes are convenient for
  development but are not appropriate for production.
- Run with the least-privileged IAM / service-account profile that can
  reach the configured catalog and storage backends.
- Keep `cargo audit` and `cargo deny check advisories` in CI.

For details on the auth chain, see [docs/site/book/src/deployment/configuration.md](docs/site/book/src/deployment/configuration.md).
