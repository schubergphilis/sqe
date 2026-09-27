# Go-live and Retirement Checklists

Three gates for a deployment. Operational acceptance proves the platform is ready to run SQE. Product acceptance proves SQE does what its users and auditors need. Retirement proves the service went away without losing evidence or leaving access behind.

Run each list against the real environment, not a test copy. Record who ran it, when, and the result of every line. A line that fails blocks the gate until it is fixed or accepted as a risk by the owner. File the record with the deployment: it is the evidence that the gate happened.

What SQE ships and what your platform owns is split in [Shared Responsibility](./shared-responsibility.md). The lists below assume that split.

## Operational acceptance (before go-live)

**Configuration**

- [ ] `production_mode = true`, and the coordinator starts. Startup fails on any rule it enforces, see [What production mode enforces](./production-guide.md#what-production-mode-enforces).
- [ ] Secrets come from a Kubernetes Secret (`existingSecret`), not from values files. `coordinator.worker_secret` and `policy.mask_key` are set.
- [ ] `[auth]` validates issuer and audience. A request without a token is rejected.

**Network and transport**

- [ ] Flight SQL has TLS in SQE or at the ingress. The HTTP and Trino endpoints are reachable only through TLS.
- [ ] `networkPolicy.enabled: true`, with egress limited to the catalog, identity provider, object store and policy service.

**Health and monitoring**

- [ ] `/healthz` and `/readyz` answer `200`, and the probes are green.
- [ ] Prometheus scrapes the coordinator. The rules in `deploy/observability/alerts.yml` are loaded and route to your on-call.

**Audit logging**

- [ ] The audit log sits on a persistent volume with encryption at rest, and `format` is `"native"` or `"both"`.
- [ ] Audit export points at an `https://` collector, and a test query shows up in the SIEM.
- [ ] Block the collector while queries run. `SqeAuditExportStalled` fires after about 40 minutes (30 minutes without a successful ship, held for 10). Unblock it, and the backlog ships.

**Recovery**

- [ ] The catalog database and the object store are backed up, deleted object versions outlive the oldest catalog backup, and one restore has passed the checks in [Backup and Restore](./backup-restore.md#what-restored-means).

## Product acceptance (before go-live)

- [ ] For each policy type in use, a test user sees what the policy allows and nothing more: a row filter drops rows, a column mask returns masked values, a restricted column returns NULL.
- [ ] The same test user's queries appear in the audit log with the policies that applied.
- [ ] A sample audit extract is produced and verified with `sqe-server --verify-audit`, following [Extracting records for an investigation](./audit-logging.md#extracting-records-for-an-investigation).
- [ ] Retention in the SIEM is set per log class. Access to the audit logs is restricted, and that access is itself logged.
- [ ] A representative query from the main user group finishes within its agreed time.

## Retirement

- [ ] Stop client traffic first. Wait until `sqe_audit_export_spool_lag_bytes` is 0, so the last audit events have shipped.
- [ ] Run `sqe-server --verify-audit` on the native audit file and record its sha256 and last hash.
- [ ] Decide what happens to the audit log: keep it for its retention period, transfer it, or destroy it with a record of the destruction. The volume goes only after that decision.
- [ ] Silence the SQE alerts and remove the scrape target, so the shutdown does not look like a silent source.
- [ ] Revoke access: the OIDC client, `worker_secret`, deploy tokens, catalog grants, Ranger service policies and object-store credentials.
- [ ] Delete the remaining volumes and secrets.
