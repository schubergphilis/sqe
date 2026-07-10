# S3 credential vending (next-steps notes)

Future phase, not yet built. Captures the research and the local-test decision so
we can pick it up after the Ranger work. Design/brainstorm only at this point.

## Goal

Replace the static shared S3 key with Polaris credential vending: when SQE loads
a table, Polaris returns short-lived, minimally-scoped S3 credentials for that
table, and SQE reads/writes the data with those creds. This makes per-user S3
access control real (the data path is gated, not just metadata) and removes the
single broad key. Production object store is AWS S3 or NetApp StorageGRID.

## How Polaris vends (mechanism)

- Polaris calls AWS STS `AssumeRole` with a generated inline SESSION POLICY that
  scopes the temp creds to the table's S3 prefix (read-only or read-write). The
  loadTable response (with header `X-Iceberg-Access-Delegation: vended-credentials`)
  carries `s3.access-key-id` / `s3.secret-access-key` / `s3.session-token`.
- Catalog `storageConfigInfo` (S3) fields: `roleArn` (required), `region`,
  `externalId`, `userArn`, `endpoint`, `endpointInternal`, `pathStyleAccess`,
  and `stsEndpoint` (per-catalog STS endpoint override -> point at a non-AWS STS).
- `stsUnavailable: true` tells Polaris to skip AssumeRole and pass static creds
  through (what our current Ranger quickstart uses, which is why vending is off).
  Known bug: apache/polaris#3742 (Polaris still tried to vend with NetApp S3
  despite `stsUnavailable: true`).
- `SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION` (server flag, default false) is a
  test-only bypass that hands the server's ambient creds to every client. Not
  for production; use per-catalog `stsUnavailable` for STS-less stores instead.

## SQE state (what needs building)

- WRITES already consume vended creds: INSERT/MERGE/DELETE use `table.file_io()`,
  which carries the loadTable vended credentials. Likely works end-to-end today.
- READS discard them. The coordinator hardcodes `s3_session_token: ""` in every
  `ScanTask` and reads with the static `[storage]` key
  (`crates/sqe-coordinator/src/query_handler.rs` ~2277). Vending for reads is
  explicitly deferred ("Step 5 / Pluggable Catalogs"): the
  `credential_refresh` callback in `sqe_server.rs` returns `None`.
- Worker side is READY: `build_object_store_with_creds` already applies
  `.with_token(session_token)` and there is a credential-refresh channel for
  mid-scan rotation (`crates/sqe-worker/src/executor.rs` ~836, ~549).
- The gap (the work): coordinator extracts `s3.access-key-id`/`-secret`/
  `-session-token` from the loadTable response and puts them into `ScanTask`
  instead of the static config; wire the deferred `vend_credentials` refresh
  callback (reload table near token expiry, push fresh creds to workers).

## Minimal S3 permissions (Polaris inline session policy)

- Read cred: `s3:GetObject`, `s3:GetObjectVersion` on the table prefix;
  `s3:ListBucket` on the bucket with an `s3:prefix` `StringLike` condition scoped
  to the table prefix; `s3:GetBucketLocation`.
- Write cred: the read set plus `s3:PutObject` and `s3:DeleteObject`
  (DeleteObject needed for merge-on-read position deletes / metadata cleanup).

## Local test decision: two tiers

Enforcement is the object store's contract (StorageGRID in prod), not SQE's.
SQE's deliverable is CONSUMING vended creds. So split the test:

- **Flow tier (default, dev loop + CI): rustack** (github.com/tyrchen/rustack).
  Rust LocalStack-compatible emulator, ~8 MB image, <1s start. Its `AssumeRole`
  uses POST form-body so it works with Polaris's AWS Java SDK `StsClient` (does
  NOT hit the SeaweedFS failure mode), returns access-key/secret/session-token,
  and S3 tolerates the session token. Proves SQE extracts and uses vended creds
  end-to-end, provable by giving SQE NO static key so a read can only succeed via
  vended creds. Does NOT enforce session policies (confirmed in its own design
  spec and code: allow-all IAM sim stubs, bucket policy store-only, S3 maps any
  key to an empty secret). So it cannot prove "a read cred cannot write".
- **Enforcement tier (real full end-to-end): Ceph RGW** (or real StorageGRID).
  Ceph RADOS Gateway has real, enforced STS `AssumeRole` + session policies since
  Nautilus (2019), is the closest open-source analog to StorageGRID, and has a
  published Polaris+RGW STS walkthrough. ~2.5 GB RAM (single all-in-one
  `quay.io/ceph/demo` container, `osd_memory_target` tunable to ~1 GB). This tier
  proves a vended read cred genuinely cannot write or cross table prefixes.
- **Production**: AWS S3 (native STS) or NetApp StorageGRID 12.0+ (added
  AssumeRole + session policies). Both enforce.

Rejected local options: SeaweedFS (STS present but broken with SDK clients:
POST-body AssumeRole 500s, SigV4 ignores `X-Amz-Security-Token`); Garage (no STS
/ no IAM at all, only long-lived per-key-per-bucket keys); MinIO (excluded by
request; it does support AssumeRole + session policies).

## Phase shape (when we build it)

1. SQE coordinator: extract vended creds from loadTable -> `ScanTask`; wire the
   refresh callback. Read-path is the work; write-path already vends.
2. Quickstart `quickstart/polaris-ranger-keycloak` variant (or new) with rustack
   + Polaris `stsEndpoint` -> rustack STS, `stsUnavailable: false`, a role ARN,
   and SQE with NO static `[storage]` key. Test: read succeeds only via vended
   creds.
3. Optional enforcement quickstart with Ceph RGW: prove read-cred-cannot-write.
4. Combine with the Ranger backend: Ranger gates the catalog operation
   (LOAD_TABLE), Polaris vends a cred scoped to that table, the store enforces
   the data path. Defense in depth.

## Sources

- Polaris management spec (`AwsStorageConfigInfo`, `stsEndpoint`):
  https://raw.githubusercontent.com/apache/polaris/refs/heads/main/spec/polaris-management-service.yml
- Polaris config reference (`SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION`):
  https://polaris.apache.org/in-dev/unreleased/configuration/configuration-reference/
- `stsUnavailable` + NetApp bug: https://github.com/apache/polaris/issues/3742
- Ceph RGW + Polaris STS walkthrough:
  https://medium.com/@sharas2050/ceph-rgw-and-polaris-integration-using-sts-and-iam-roles-7e6012ed6bdd
- Ceph STS: https://docs.ceph.com/en/latest/radosgw/STS/
- StorageGRID 12.0 AssumeRole: https://docs.netapp.com/us-en/storagegrid/s3/use-access-policies.html
- rustack: https://github.com/tyrchen/rustack (v0.9.x; STS design spec documents
  the no-enforcement posture)
- SeaweedFS STS broken: https://github.com/seaweedfs/seaweedfs/discussions/8312
- Garage (no IAM/STS): https://garagehq.deuxfleurs.fr/documentation/reference-manual/s3-compatibility/
