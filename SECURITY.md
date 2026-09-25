# Security Policy

## Status: pre-1.0, recently open-sourced

SQE is pre-1.0 and was published as open source in 2026. Please read
this before you deploy it anywhere that matters:

- There has been **no independent security review** of this codebase.
  [AUDIT.md](AUDIT.md) is our own review, not a third-party audit.
- SQE is written almost entirely by AI agents under human review. The
  review is real, but it is not the same thing as an audit.
- Under `0.x` semantic versioning, breaking changes land between minor
  versions. Pin a version if you need stability.

None of that means we are relaxed about security reports. It means you
should calibrate your trust accordingly, and that we would rather hear
about a problem than not.

## Supported versions

| Version | Supported |
|---------|-----------|
| latest `0.x` minor | yes |
| any earlier `0.x` minor | no |

Security fixes land on `main` and ship in the next release of the current
minor. While SQE is pre-1.0 there are **no security backports** to
older minor versions. This will be revisited at 1.0.

## Reporting a vulnerability

Please report privately. Do not open a public issue, do not file a
regular bug report, and do not discuss the vulnerability publicly until a
fix is available.

**Preferred:** email
[abuse@schubergphilis.com](mailto:abuse@schubergphilis.com) with `[SQE]`
in the subject. This is Schuberg Philis's standing responsible-disclosure
channel. Encrypted mail is preferred; the PGP public key is at
<https://keybase.io/schubergphilis>, and an encrypted chat option is
linked from the page below. The company's full responsible disclosure
policy, including credit, the hall of fame and the bounty, is at
<https://schubergphilis.com/security> and takes precedence over this
document on anything it covers.

**On GitHub:** if you would rather report through GitHub, use private
vulnerability reporting on the mirror: the
["Report a vulnerability" button](https://github.com/schubergphilis/sqe/security/advisories/new)
under the Security tab.

Please do not send sensitive information over unencrypted channels or
social media.

Please include:

- What the issue is and what an attacker gets out of it.
- Steps to reproduce, or a proof of concept.
- The SQE version (`sqe-server --version`), the deployment shape
  (embedded, coordinator plus workers, Helm) and the catalog and policy
  backends in use.
- Any workaround you have already found.
- Whether you want public credit or prefer to stay anonymous.

Do not include real customer data, live credentials or bearer tokens, or
rows from a production table in a report. A redacted reproduction is
more useful to us than a real one.

## Rules of engagement

If you think you have found a vulnerability, we would very much
appreciate it if you:

- Don't exploit your finding.
- Share the information with just us, not other parties.
- Give us time to assess the situation and respond within a reasonable
  timeframe.

## What to expect

SQE is maintained by a small team. We aim to:

1. Acknowledge your report within **3 business days**.
2. Confirm or close it, with a written explanation either way, within
   **10 business days**.
3. For confirmed issues, set a fix target by severity: CVSS 9+ within
   14 days, CVSS 7-9 within 30 days, lower severity in the next regular
   release. CVSS is the starting point, not the whole answer. A
   vulnerability listed in the CISA Known Exploited Vulnerabilities
   catalog, or with a public exploit, gets the 14-day target whatever its
   score. One that is not reachable in SQE's default configuration may
   drop a tier, with the reason written in the advisory. Bypasses of
   authentication, row filters, column masks or column restriction get
   priority. Expect a release rather than a patch to an older version
   (see *Supported versions*).
4. Coordinate disclosure with you. We aim for a 90-day window from
   confirmation and will negotiate if you need a different timeline.
5. Credit you in the release notes and the advisory if you have told us
   you want public credit. By default you remain anonymous.

These are targets, not contractual commitments, and they apply to this
open source project. Reports about Schuberg Philis production systems
fall under <https://schubergphilis.com/security> instead.

## Credit and bounty

By default you remain anonymous. If you would rather be named, say so
and tell us the name or alias to use; we will credit you in the advisory
and the release notes.

Reports sent to `abuse@schubergphilis.com` are handled under Schuberg
Philis's responsible disclosure policy, which offers reporters a token
of appreciation and maintains a public hall of fame. See
<https://schubergphilis.com/security> for the current options and what
it covers. SQE runs no separate bounty of its own.

## Scope

In scope:

- The `sqe-server`, `sqe-worker` and `sqe-cli` binaries, and the container
  image and Helm chart in this repository.
- The `sqe-*` crates, and the vendored `iceberg-rust` fork in
  `vendor/iceberg-rust/`.
- Documentation that would lead a careful operator into an insecure
  configuration.

Out of scope:

- **The services SQE talks to**: Apache Polaris, Project Nessie, AWS
  Glue, AWS S3 Tables, Hive Metastore, Apache Ranger, your OIDC identity
  provider and your object store. Report those to the respective project
  or vendor.
- **Apache DataFusion, Apache Arrow and other third-party Rust crates.**
  Report upstream. Do tell us if the way SQE calls a dependency turns a
  non-issue into an exploitable one, or if we are pinning something
  known-vulnerable. `cargo deny` and `cargo audit` block merges in CI,
  but they are not infallible.
- **Operator misconfiguration**, including anonymous or
  bearer-passthrough auth in production, permissive IAM, public buckets,
  weak catalog database passwords, or a disabled NetworkPolicy. What the
  team running SQE owns is listed in the
  [shared responsibility](docs/site/book/src/operations/shared-responsibility.md)
  page.
- **Breaking changes between `0.x` minors.** Documented above.
- Findings from a scanner with no demonstrated impact on SQE.

## Hardening guidance

New deployments should:

- Run with `production_mode` on. It refuses anonymous and
  bearer-passthrough auth, insecure transport and unauthenticated
  workers at startup.
- Terminate TLS for the Flight SQL listener in SQE or at ingress, and
  always at ingress for the HTTP and Trino endpoints.
- Validate JWT issuers and audiences strictly in `[auth]`.
- Run with the least-privileged IAM or service-account profile that can
  reach the configured catalog and storage backends.

The auth chain is described in
[configuration](docs/site/book/src/deployment/configuration.md).

## Good faith

If you follow this policy in good faith, we will treat your report as a
contribution: we will work through it with you, we will tell you what we
decided and why, and we will not ask you to stay quiet indefinitely. In
return, please don't access, modify, or retain data that isn't yours,
and don't degrade any service while investigating.

_Last reviewed: 2026-09-25._
