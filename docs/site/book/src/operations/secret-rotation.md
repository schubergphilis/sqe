# Secret Inventory and Rotation

Every secret SQE reads, what breaks when it changes, and how to rotate it without an outage. Store all of them in a Kubernetes Secret or your secret manager and pass them as environment variables. Never commit them to a values file.

SQE reads secrets once at startup. Rotating any of them means updating the secret and restarting the pods that use it. The one exception is TLS on the HTTP listeners, which reloads by itself.

**Cadence.** Rotate on the schedule your security policy sets, at least yearly. Rotate at once when a secret may have leaked, or when someone who could read it leaves the team. Record each rotation: which secret, when, who.

## Secrets you rotate

| Secret | Setting (environment variable) | Used for |
|---|---|---|
| Worker secret | `coordinator.worker_secret` (`SQE_COORDINATOR__WORKER_SECRET`, `SQE_WORKER__WORKER_SECRET`) | Workers authenticate their heartbeat to the coordinator |
| OIDC client secret | `auth.client_secret` (`SQE_AUTH__CLIENT_SECRET`) | Password-grant logins against your identity provider |
| Mask key | `policy.mask_key` (`SQE_POLICY__MASK_KEY`) | Keys the HMAC behind every `MASK_HASH` column mask |
| Ranger Admin password | `access_control.ranger.admin_password` (`SQE_ACCESS_CONTROL__RANGER__ADMIN_PASSWORD`) | Fetching Ranger policies |
| Maintenance principal secret | `[maintenance.principal] client_secret` (TOML only) | The maintenance scheduler's own identity |
| Object-store keys | `storage.s3_access_key` / `storage.s3_secret_key` (`SQE_STORAGE__S3_*`), the worker spill S3 keys, `azure_sas_token` | Static storage credentials, when you do not use workload identity |
| MCP static tokens | `[[mcp.auth.principals]] token_env` | Fixed bearer tokens for MCP principals |
| OpenLineage API key | `[metrics.openlineage] api_key` | Posting lineage events |
| TLS certificates | `coordinator.tls.cert_file` / `key_file`, `worker.tls` | Flight SQL and HTTP listeners |

### How each one rotates

**Worker secret.** One value, shared by the coordinator and every worker. A worker on the old value cannot register, so the coordinator loses it until it restarts. Update the Secret, then restart the coordinator and worker deployments together, in a quiet window.

**OIDC client secret.** Most identity providers let a client hold two secrets for a while. Add the new one at the provider, update SQE and restart the coordinator, then remove the old one. If your provider keeps only one, logins fail between the provider change and the coordinator restart, so do both back to back.

**Mask key.** Rotating it changes every `MASK_HASH` output. Anyone who joins or deduplicates on masked values across the change sees two different hashes for the same input. Agree the date with those consumers first. Update the value and restart the coordinator. Never clear it as a shortcut: without a key the hash is unsalted SHA-256, which is brute-forceable on low-entropy columns, and `production_mode` with the Ranger engine refuses to start.

**Ranger Admin password.** Change it in Ranger, update SQE, restart the coordinator. While SQE holds a wrong password, policy fetches fail and queries fail closed: users are denied, not let through.

**Maintenance principal secret.** Rotate at the identity provider like the OIDC client secret, update the TOML, restart the coordinator.

**Object-store keys.** Create a second key, update SQE and restart the pods that use it, then delete the old key. Better still, drop static keys for workload identity (IRSA or an instance role) and there is nothing to rotate.

**MCP static tokens.** Tokens resolve at startup. Set the new value in the environment variable, restart the coordinator, then hand the new token to the client. Each principal holds one token, so the old one stops working at the restart.

**OpenLineage API key.** Issue a new key at the lineage backend, update SQE, restart the coordinator, revoke the old key.

**TLS certificates.** The HTTP listeners (MCP, Quack) pick up a renewed `cert_file` and `key_file` within a minute, with no restart. The Flight listener keeps the certificate it started with, so restart it when the certificate changes. See [Certificate rotation](../deployment/configuration.md#certificate-rotation) for a reloader setup.

## Keys that rotate themselves

These are not in any configuration and need no action.

- **Scan-ticket signing key.** The coordinator generates a fresh Ed25519 key pair at every start and signs worker scan tickets with it.
- **GDPR token salt.** The salt behind `gdpr_identifier_mode = "tokenize"` is generated at every start. Tokens are stable within one run of the coordinator, not across restarts, so do not correlate them over a restart in the SIEM.

## Outside the running service

The build pipeline holds its own credentials: registry push credentials and the tokens the CI platform uses. They belong to whoever owns the pipeline, not to a deployment. The tag pipeline signs release images keyless through the CI provider's identity (cosign), so there is no signing key to rotate.
