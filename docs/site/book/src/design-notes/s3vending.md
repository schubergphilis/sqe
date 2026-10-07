# S3 credential vending

SQE consumes table-scoped S3 credentials vended by an Iceberg REST catalog
(Polaris) on the Iceberg data path: single-node scans, writes, and
distributed scan tickets. Issue #395 added the FileIO gate
(`[catalog] require_vended_credentials`); the later work described under
[SQE state](#sqe-state) asks the catalog for credentials, prefers them over
the static key, honours their expiry, and ships them to workers. This note
keeps the research and the local-test decision alongside.

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

## SQE state

### Asking for credentials

The catalog only vends when the client sends
`X-Iceberg-Access-Delegation: vended-credentials`. SQE sends it when either
setting is on:

```toml
[catalog]
access_delegation = "vended-credentials"   # default "none"
# or, stricter: also keeps static keys out of FileIO entirely
require_vended_credentials = true          # implies access_delegation
```

`SQE_CATALOG__ACCESS_DELEGATION` overrides it. Only `none` and
`vended-credentials` parse; `remote-signing` is rejected at startup because
SQE does not implement the signer. The header goes out as a `header.*`
catalog property, so every catalog request carries it, including the
sibling-warehouse catalogs built for multi-warehouse sessions.

### Which keys FileIO gets

The loadTable (and createTable) response can carry credentials in two
places: the flat `config` map and the `storage-credentials` list. SQE
builds a table's FileIO properties in this order:

1. the response `config`;
2. the `storage-credentials` entry whose `prefix` is the longest prefix of
   the table location (`s3a://` and `s3n://` count as `s3://`); with no
   match, `config` alone stands;
3. SQE's own catalog properties (endpoint, region, path style, and in
   static mode the `[storage]` key pair);
4. if the catalog vended any credential key, those keys are put back on
   top, and any local credential key the catalog did not vend (a static
   session token, say) is removed so the two sets never mix.

So vended keys always beat static ones, and a table whose response has no
credentials keeps the static pair (static mode) or nothing at all
(`require_vended_credentials`). With `require_vended_credentials` the
static pair is never placed in FileIO, and `s3.disable-config-load` /
`s3.disable-ec2-metadata` stop iceberg-rust falling back to `AWS_*` env,
`~/.aws/config`, IRSA, or IMDSv2.

### Expiry and refresh

Vended sets carry `s3.session-token-expires-at-ms`. SQE reads the earliest
expiry in a table's FileIO and treats the credentials as usable until five
minutes before it (or half the remaining lifetime, for tokens shorter than
ten minutes):

- **Table metadata cache.** An entry is not served, fresh or stale, past
  that point. A stale hit would otherwise be revalidated with an ETag 304
  that proves the metadata unchanged and hands back the dead keys. The
  next lookup reloads the table and gets a new set.
- **Prepared statements.** A stored plan holds its tables' FileIO. When
  any of those credentials is past its usable point, execution re-plans
  from the SQL text, which reloads the tables.
- **Distributed scans.** The coordinator registers each fragment with its
  expiry and a reloader bound to the querying user's session. The refresh
  task reloads the table as that user, without the cache, and pushes the
  new set to the worker over `refresh_credentials`; the worker applies it
  between files. A reload that yields no complete set (key, secret,
  token, expiry) pushes nothing, and the worker keeps its current keys
  until they expire.

The vendored REST client does not call the catalog's
`client.refresh-credentials-endpoint`; refresh is a full loadTable. That
costs a metadata round trip per refresh, and it re-runs the catalog's
authorisation check, so a revoked grant stops the refresh.

### Workers

`[coordinator] distributed_credentials` picks what a scan ticket carries:

| Mode | Ticket carries |
|------|----------------|
| `auto` (default) | the table's vended set; the static pair only when the table has none (warns once) |
| `vended` | the table's vended set (key, secret, session token, expiry); a table without one is not distributed |
| `static` | the `[storage]` pair |
| `worker_default` | nothing; workers use their own chain |

A key pair equal to the configured static pair is never treated as vended.
For a multi-tenant cluster set `distributed_credentials = "vended"` with
`require_vended_credentials = true`: workers then never see the engine key.

### What still uses the static key

These paths do not go through an Iceberg catalog's loadTable, so there is
nothing to vend. They use the `[storage]` keys or the AWS SDK chain no
matter what the settings above say:

- **File TVFs** (`read_parquet`, `read_csv`, `read_json` and friends) read
  with the engine key, but only under prefixes listed in
  `[storage.tvf] allowed_object_store_prefixes` (empty by default) or a
  catalog's `tvf_prefixes`.
- **Hive external tables** read with the `[storage]` pair (the mount's own
  keys when it was created with them), and distributed Hive scans ship
  that pair to workers.
- **Distributed compaction** ships the static `[storage]` pair to the
  workers that rewrite files.

A deployment with no static key can still run Iceberg reads and writes;
those three features then need their own credentials or stay unused.

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

## Phase shape (step 1 done, 2-4 remaining)

1. Done: SQE asks for, prefers, expires, and refreshes vended credentials,
   and ships them on the `ScanTask`.
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
