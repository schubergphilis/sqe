# Shared Responsibility

SQE is software you run. We ship the engine, the container image and the Helm chart. You run the cluster, the network, the object store, the catalog, the identity provider and the log pipeline around it. This page draws the line, one area at a time.

Anything under "you own" is not done by SQE. If your security or audit framework asks for it, the evidence comes from your platform, not from this project.

## Patching and images

**SQE ships:**

- A tagged release on every merge to `main`.
- A runtime image on a digest-pinned Chainguard base, running as UID 65532. Renovate proposes base-image and dependency bumps. The image build fails on any vulnerability rated high or critical.
- A 7-day minimum release age on every dependency update. A freshly published package version waits a week before Renovate proposes it. We chose this on purpose: worm-style package compromises such as Shai-Hulud get caught and yanked within days, and a week-old version has had that time. A critical advisory that cannot wait gets a manual bump out of cycle.

**You own:**

- Patching the node OS, kubelet, container runtime and every other host component.
- Moving to new SQE tags. We do not backport fixes to older minors while SQE is pre-1.0 (see `SECURITY.md`).
- Scanning the images that actually run in your cluster, and a maximum image age if your policy has one.

## Vulnerability management

**SQE ships:** a private disclosure channel and response targets in `SECURITY.md`. Every merge request runs static analysis, dependency and image scanning, secret scanning and `cargo deny` / `cargo audit`. A finding rated high or above blocks the merge.

**You own:** scanning your deployment and its external attack surface, penetration testing of your environment, and tracking findings in your own register.

## Malware protection

**SQE ships:** pinned dependencies with a lockfile, advisory and licence gates, and a dependency reputation check. The image carries no antivirus agent.

**You own:**

- Endpoint or runtime protection on the nodes.
- Malware scanning of the buckets SQE reads. SQE reads data files as data. It never inspects file content for malicious payloads, and `read_parquet` or `INSERT ... SELECT` from an external location will happily read whatever sits there.

## Logging

**SQE ships:** OCSF audit events for authentication, sessions, grants, DDL, DML and queries, with a tamper-evident hash chain and OTLP export. See [Audit Logging](./audit-logging.md). Export health shows up as metrics:

- `sqe_audit_export_last_success_timestamp`
- `sqe_audit_export_batch_failures_total`
- `sqe_audit_export_spool_lag_bytes`

**You own:**

- Shipping the events to your SIEM and parsing them there.
- Retention per log class, immutable (WORM) storage and backups of the indexed logs.
- Access control on the logs, with access to the logs itself logged.
- Alerting when SQE goes silent. Alert on the export metrics above, not only on process liveness.
- Disk for the local spool during a collector outage. SQE empties the spool once it is shipped, but keeps every unshipped event, so an outage grows it.

## Encryption

**SQE ships:** TLS on the Flight SQL listener and certificate hot reload. [Production mode](./production-guide.md#what-production-mode-enforces) refuses insecure transport. The Helm chart refuses to expose MCP through an ingress without TLS.

**You own:**

- TLS termination for the HTTP and Trino endpoints at your ingress.
- Encryption at rest of the object store (bucket default SSE, ideally SSE-KMS) and ownership of those keys.
- Encryption of the spill and audit volumes through your StorageClass.
- Protecting the session persistence file when `persistence = "file"`. It holds live session tokens. SQE writes it mode 0600 but does not encrypt it.

## Identity and access

**SQE ships:** strict OIDC bearer-token validation and no service account. Every query runs as the user who sent it. Policy enforcement fails closed. See [Auth flow](../architecture/auth-flow.md).

**You own:**

- The identity provider, MFA and the joiner, mover and leaver process.
- Periodic review of Ranger policies and Polaris grants.
- Storing and rotating SQE's own secrets. [Secret Inventory and Rotation](./secret-rotation.md) lists every one, with what breaks when it changes and how to rotate it.

## Network

**SQE ships:** default-deny ingress NetworkPolicies for the coordinator and the workers. Workers accept traffic from the coordinator only. `values-production.yaml` turns them on; the base `values.yaml` leaves them off for local installs. Listing rules under `networkPolicy.egress` turns on default-deny egress too, with DNS and the coordinator-worker traffic added by the chart. Pods run with the `RuntimeDefault` seccomp profile, and CI fails any chart change that drops a Pod Security "restricted" control.

**You own:** narrowing `networkPolicy.from` to your ingress controller and Prometheus, and filling `networkPolicy.egress` with your catalog, identity provider, object store, policy service and OTLP collector.

## Backup and continuity

SQE holds almost no state. Tables live in your object store and catalog. The coordinator is restartable, not highly available (see [Kubernetes](../deployment/kubernetes.md)).

**You own:**

- Backups of the catalog database (for example the Polaris Postgres), object-store versioning or replication, and the audit log volume. Deleted object versions must outlive your oldest catalog backup, or a catalog restore points at files that are gone; see [Backup and Restore](./backup-restore.md).
- Recovery point and recovery time objectives, retention per class, and restore tests against them (at least quarterly).
- Disaster recovery of everything SQE depends on.

## Hosting and physical security

**You own** the data centre, or the choice of cloud provider and its assurance reports. SQE has no part in it.

## Incidents

**SQE ships:** a security disclosure process in `SECURITY.md` for vulnerabilities in SQE itself, and an [On-Call Runbook](./runbook.md) for known failure modes.

**You own:** incident management for your deployment, including priorities, customer notification and root-cause reports.

## Go-live and retirement

**SQE ships:** standard [go-live and retirement checklists](./go-live-checklist.md) that exercise the controls on this page.

**You own:** running them at go-live and at retirement, and keeping the signed-off record.
