---
title: "read_parquet shouldn't read /etc/shadow"
description: "Modern object-store abstractions unify the filesystem and HTTP behind a single URL. That's a feature for ergonomics. It's a security trap when the URL comes from a user. SQE shipped that trap and then closed it. The IMDS pivot is the part worth telling."
pubDate: "2026-05-13"
author: "Jacob Verhoeks"
tags:
  - "security"
  - "ssrf"
  - "datafusion"
  - "rust"
  - "object-store"
---



*May 13, 2026*

```sql
SELECT * FROM read_parquet('/etc/shadow');
```

That should not work.

It did. Until this week, on every single deployment of SQE running before version 0.31.5. Any authenticated SQL user could ask `read_parquet`, `read_csv`, `read_json`, or `read_delta` to open an arbitrary file on the coordinator filesystem and stream the contents back as table rows. The parser would fail somewhere later, sure. The Parquet footer check would reject `/etc/shadow` as malformed. But by that point the file has been opened, the bytes have been read, and the error message will helpfully include the first eight kilobytes of context.

The same TVFs would happily fetch from arbitrary HTTP hosts. Including the one at `http://169.254.169.254`.

That is the AWS Instance Metadata Service. The address is link-local and unrouted, so it only resolves from inside an EC2 instance, an EKS pod, or an ECS task. Once you're inside, IMDS hands out the IAM role credentials attached to the host. Worker pods often run with cloud-side privileges far higher than the user's. The user has Polaris read on one schema; the worker has S3 read on the whole bucket. A path-traversal hole in a SQL function becomes an IAM-role credential lift.

This post is about how that happened, how we closed it, and why "modern object-store abstractions" make the problem easier to ship than the previous generation did.

## How a URL became a system call

DataFusion's TVFs hand the user-supplied path to `ListingTable`. `ListingTable` resolves it through DataFusion's object-store registry. The registry maps scheme to backend: `s3://` to the S3 client, `gs://` to the GCS client, `http(s)://` to a lazy HTTP store, and everything else, by default, to `LocalFileSystem`.

The classifier in SQE's TVF layer recognised the cloud schemes by prefix and routed accordingly. The fall-through was implicit. If the URL did not start with `s3://`, `abfss://`, `gs://`, or `hf://`, and was not `http(s)://`, the registry's default kicked in. `LocalFileSystem` reads from disk. Naively, as the coordinator's UID.

The HTTP path had a different shape but the same outcome. `LazyHttpObjectStoreRegistry::get_store` constructs an HTTP store on demand for any `http(s)://host` that arrives at the registry. There was no host check. The registry treated `data.example.com` and `169.254.169.254` as equivalent inputs.

Both behaviours are intentional in the object-store crate. The library author's contract is "I open URLs." The downstream user's contract should be "I let trusted operators open URLs, not arbitrary tenants." The mismatch sits in the middle. Nobody owned it.

## The two attacks

`SELECT * FROM read_parquet('/etc/shadow')` is the obvious one. The interesting variants:

```sql
-- Coordinator's Kubernetes service-account token.
SELECT * FROM read_parquet('/var/run/secrets/kubernetes.io/serviceaccount/token');

-- Mounted secret volumes.
SELECT * FROM read_csv('/etc/sqe-secrets/polaris.token');

-- Process environment, with all the AWS_* and KUBERNETES_* vars in it.
SELECT * FROM read_parquet('/proc/self/environ');

-- Inline AWS credentials staged for debugging.
SELECT * FROM read_parquet('file:///root/.aws/credentials');
```

Most of these will produce a Parquet parse error, then a `DataFusionError::Plan` that the client sees as "Schema inference failed: ... near byte 0x53." The byte 0x53 is `S`. The first eight bytes of `/etc/shadow` are `root:!:1`. The bytes are in the error.

Even if the parser rejected before any bytes left the box, the side effects are not zero. `LocalFileSystem::head` runs `stat(2)`. That tells you what files exist. That tells you whether you guessed the right path for the secret. Build a wordlist, iterate, and you have a reconnaissance loop.

The HTTP variant is more dangerous because it pivots:

```sql
SELECT * FROM read_parquet('http://169.254.169.254/latest/meta-data/iam/security-credentials/');
```

IMDSv1 (still enabled on plenty of EKS clusters) returns the IAM role name as a plain text body. The follow-up:

```sql
SELECT * FROM read_parquet('http://169.254.169.254/latest/meta-data/iam/security-credentials/<role>');
```

returns JSON containing `AccessKeyId`, `SecretAccessKey`, `Token`, `Expiration`. Two TVF calls and the user has the worker's IAM credentials.

The same pivot exists for GCP at `169.254.169.254` (different path) and Azure at the same address. The "modern" infrastructure decision that all three clouds picked the same magic IP was good for portability. It is also why one SSRF gadget works across all three.

## What "modern" got us

The pre-Iceberg era of analytical SQL had separate primitives. `LOAD DATA INFILE` in MySQL opened files. `COPY FROM` in Postgres opened files. `wget` shelled out. The boundaries were obvious. The DBA disabled the dangerous ones. The auditor knew which ones to ask about.

Object-store libraries unified the boundary. `object_store::ObjectStore` is one trait. The implementations are interchangeable. The URL parser routes. Code that uses `ObjectStore` cannot tell whether it is reading from a local file, an S3 bucket, or a public HTTP endpoint, because the trait deliberately hides the distinction.

That is a good abstraction for the *library*. It is the wrong abstraction for the *security boundary*. A security boundary needs to be visible at the place where a privilege decision is being made. The library decides "do I support this URL?" The application decides "should this user be able to ask for this URL?" Those are different questions. The first one says yes to `file:///` because the library exists. The second one should say no to `file:///` from a user-supplied parameter, regardless of what the library supports.

When we wired DataFusion TVFs into the SQL surface we made the same mistake every other engine has made on this code path. The "where does this read?" question got delegated to the object-store layer. The object-store layer answered "wherever the URL points," which is its job. The "should this user be allowed to point there?" question landed in a gap between the SQL parser (which trusts the planner) and the object store (which trusts the registry).

DuckDB and Trino have both walked this path. DuckDB shipped `httpfs` and `parquet_scan` with very few guards and added them over time. Trino has a `hive.allow-local-files` config knob and a longer list of allowed schemes. Both engines learned, in production, that the right place for the check is the SQL function dispatch, not the object store.

## The fix shape

The change in MR !190 adds a new config section and a single check function:

```toml
[storage.tvf]
allow_local_paths = false       # default: reject /etc/shadow etc.
allow_http = false              # default: no arbitrary HTTP hosts
allowed_http_hosts = []         # default: empty means no HTTP at all
```

`TvfPolicy::check(path)` runs at the entry of every file TVF, before any object store is constructed:

```rust
impl TableFunctionImpl for ReadParquetFunction {
    fn call(&self, exprs: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        let args = parse_args(exprs)?;
        // Reject local-path and arbitrary HTTP-host arguments BEFORE
        // constructing the object store.
        self.storage.tvf.check(&args.path).map_err(|e| {
            DataFusionError::Plan(format!("read_parquet: {e}"))
        })?;
        // ... existing code ...
    }
}
```

The decision tree is short. Cloud object-store schemes (`s3://`, `s3a://`, `abfss://`, `abfs://`, `azure://`, `az://`, `gs://`, `gcs://`, `hf://`) pass unconditionally because they go through SQE's credential-managed paths. `http(s)://...` URLs check the host against `allowed_http_hosts` as an exact-match, case-insensitive comparison; the operator opts in to specific hosts. Everything else is treated as a local path and rejected unless `allow_local_paths = true`.

The defaults matter. Fail-closed. An operator who deploys SQE without reading the docs gets the safe behaviour. The opt-out is two lines of TOML and visible in config diffs. The audit trail is "operator explicitly granted local filesystem read to TVF arguments."

Seven regression tests cover the matrix:

```
tvf_default_allows_object_store_schemes
tvf_default_rejects_local_absolute_paths
tvf_default_rejects_arbitrary_http_hosts        # the IMDS case
tvf_allowed_http_host_is_accepted_exact_match
tvf_allow_http_true_bypasses_allowlist
tvf_allow_local_paths_true_permits_filesystem
tvf_malformed_http_url_returns_error
```

The third test asserts that the literal string `http://169.254.169.254/latest/meta-data/iam/security-credentials/` is rejected by the default policy. We named it after the path that motivated it. Future operators reading the test list see the threat model encoded directly.

## Why the check goes at the TVF, not the object store

The right layering question is "where does the trust boundary live?" Two answers were on the table:

**Answer A.** Patch DataFusion's `LocalFileSystem` to refuse anything outside a configured root. This is the "sandbox the library" approach. It feels right because the library is the thing that touches the filesystem. It is wrong because (a) the library is used by other code paths that legitimately want local access (the spill manager, the temp file path), and (b) the SSRF case (HTTP to IMDS) is not a filesystem problem at all.

**Answer B.** Check the URL at the entry of each TVF dispatch. This is the "gate the user-input boundary" approach. The check runs once, before any backend is selected. It catches both attacks symmetrically because both attacks share an entry point (the TVF's path argument) even though they reach different backends.

We picked B. The rule is straightforward: when user input becomes a URL becomes a system call, check at the user-input boundary. Don't check at the URL layer (the URL is well-formed). Don't check at the system call layer (the syscall is too late). Check at the function call that the user can name, where the privilege decision belongs.

The same rule applies anywhere `object_store::ObjectStore` is configurable from SQL. Right now in SQE that is the four file TVFs and the ATTACH path for raw filesystem catalogs. We hardened the TVFs; ATTACH has its own admin gate, which addresses the same class of issue from a different angle.

## What did not work

The first patch we considered was a regex on the path argument. Reject anything matching `^/(etc|proc|sys|var)`. This is the wrong shape. A regex deny-list is unbounded; every interesting Linux file lives somewhere we forgot to add. It does not catch `file:///root/.aws/credentials`, which uses a different prefix. It does not catch HTTPS to a non-IMDS attacker-controlled host. Allowlists beat denylists for this class.

The second one we considered was setuid-style separation: have the TVF read through a worker process running as nobody. The blast radius limit is real, but the engineering cost is high (a whole new process, IPC, error handling) and it does not address the SSRF case at all (a nobody-uid worker can still HTTP-GET IMDS). Useful future work, wrong first step.

The third was a chroot. Same blast-radius logic. Same flaw: the threat model is SQL input becoming network traffic, not just SQL input becoming local file access. Chroot helps the local case and ignores the cloud case.

The right shape is the one we shipped. One config, one check, two attacks closed. The IMDS test name reminds future readers what they are defending against.

## What to take away

If you build a SQL engine, an analytics tool, or any system that turns user input into a URL into a backend call, you need a policy check at the user-input boundary. The object-store abstraction is genuinely good engineering. The price of the abstraction is that the filesystem and the public internet now look the same to the code that does the read. Whatever decides "should this user be allowed to read this" has to do it before that uniformity kicks in.

Defaults matter. Fail-closed defaults catch the deployments where the operator did not know to look. An audit log entry that says "TVF check denied: local filesystem paths are disabled" is a feature; it tells the operator that someone tried, and that the gate worked.

Test names matter. `tvf_default_rejects_arbitrary_http_hosts` is functional. `// The IMDS scenario from the issue.` in the test body is documentation. Both are there. Future readers maintain them both.

The IMDS pivot is not exotic. It is the second tutorial on every cloud penetration testing course. The fact that a SQL function can reach it is the kind of defect that lives in a codebase quietly until someone runs an audit pass with the right mental model. We ran one. We fixed it. The next time we add a function that takes a URL we will write the check first.
