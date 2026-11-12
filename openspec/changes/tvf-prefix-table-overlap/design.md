## Context

`TvfPolicy::check_object_store` (`sqe-core/src/config/mod.rs`) gates `read_parquet`, `read_csv` and `read_json` on object-store paths. Those reads use the engine's `[storage]` credentials, never the caller's vended credentials. A path passes in five cases:

- the caller is trusted-local;
- the call carries complete inline credentials and `allow_inline_credentials` is set;
- `allow_all_object_stores` is set;
- the caller holds a role in `object_store_admin_roles`;
- the canonical path lies under an allowed prefix after `{user}` and `{catalog}` substitution.

Matching stops at a path segment boundary (`object_prefix_matches`). Placeholder values are restricted to `[A-Za-z0-9._@+-]` (`substitute_placeholders`), so a username cannot inject `/` or `..`.

Per-catalog `tvf_prefixes` and the `{catalog}` expansion apply only when the session catalog answered the caller's own bearer (`tvf_caller_for`, `session_context.rs`). That proof is listing access, not write access.

SQE does not know a catalog's warehouse base location. Polaris holds `default-base-location` and `allowedLocations` in its catalog storage config, and does not hand them to users. Polaris refuses an Iceberg table whose location lies outside `allowedLocations`, for every engine. Only a loaded table's location is known to SQE (`metadata().location()`).

Hive external tables read their files through the same allow-list: each manifest load passes `check_path` (`session_context.rs`, the Hive provider). A Hive root therefore has to be an allowed prefix, and an overlap guard cannot apply to it (see Non-Goals).

The published production guide recommends `allowed_object_store_prefixes = ["s3://your-warehouse/"]`. Followed literally, that opens every Iceberg table's files to raw reads.

## Goals / Non-Goals

**Goals:**
- An allowed TVF prefix cannot be used to read an Iceberg table's files raw through SQE.
- One config line serves every catalog, discovered ones included, with no restart.
- Fail closed: an overlap refuses the read or the create, and never just logs.
- The docs stop recommending the warehouse as a TVF prefix.

**Non-Goals:**
- A per-catalog write-access probe. A per-user prefix limits each caller to their own staging folder, so catalog write access adds nothing there. A prefix shared by all callers is warned about instead.
- Hive tables. Their roots must be allowed prefixes for the tables to work at all, so an overlap guard would break every Hive table. How Hive file access relates to the user TVF allow-list is handled in #886.
- Credential vending for TVFs. Reading staging files with the caller's vended credentials would remove the bypass at the root, but Polaris vends credentials per table, not per prefix.

## Decisions

**1. No new key: every allowed prefix is guarded against Iceberg tables.**
The approved request was a per-catalog staging prefix. A global `s3://<staging-bucket>/{catalog}/{user}/` already gives that, with discovered catalogs included. A second key next to `tvf_prefixes` would duplicate it. Every allowed prefix reads with engine credentials, so every allowed prefix needs the guard. Guarding a staging key alone would leave the bypass open on the existing prefixes.
*Alternative:* a `staging_prefix` key per catalog. Rejected: it needs a config change and a restart per catalog, and leaves the other prefixes unguarded.

**2. Template-aware overlap, on the TVF canonicaliser.**
A prefix with placeholders is matched as a pattern: each segment that contains a placeholder matches any single segment. A location overlaps a prefix when either one is equal to the other or lies under it, compared on segments after `canonicalize_object_url` (scheme aliases, lowercase scheme, dot segments and encoded slashes refused). The guard uses the TVF canonicaliser, not maintenance's `canonicalize_uri`, because it must agree with the read check it protects. The test lives on `TvfPolicy` (`iceberg_location_overlap(&self, location) -> Option<&str>`, returning the offending configured prefix).

**3. Refuse creation before any data is written.**
CREATE TABLE ... LOCATION, CTAS with a location, and table registration check the resolved location before the catalog call. When the catalog chooses the location, the create returns it before SQE writes data. CTAS creates the table first and writes after, so the check runs between those two steps. On overlap, SQE drops the still-empty table and fails the statement, naming the prefix. If the drop fails (the caller holds CREATE without DROP, or the catalog errors), the statement still fails, the error names the table left behind, and the location goes into the overlap registry (decision 4). An empty table under a refused prefix exposes nothing.

**4. Record on load, refuse TVF reads under it.**
The REST and SQLite Iceberg load paths call the overlap test on every load, including cache hits; the test is a string comparison. An overlapping location goes into a process-wide `OverlapRegistry` (a `RwLock<HashSet<String>>` of canonical locations). SQE logs an error once per location, naming the table and the prefix. The table itself stays queryable under its policies.
*Why:* refusing the table instead would leave the TVF bypass open.

**5. Order of the checks in `check_object_store`.**
The overlap refusals (decisions 4 and 6) run after the trusted-local and inline-credential exits and before the allow-all, admin-role and allow-list checks. An inline-credential read uses the caller's own keys, which the bucket's IAM already bounds, so it is not a bypass. A trusted-local caller is the operator on the host. Allow-all and admin roles read with the engine key, so they are refused like everyone else.

**6. Probe for an Iceberg table above the path.**
A table SQE has not loaded is not in the registry. Before the allow-list check, `check_object_store` takes each ancestor directory of the requested path that is at or below the matched prefix root. For each one it lists `<ancestor>/metadata/` with a one-key limit and looks for `*.metadata.json`. A hit refuses the read and names the table directory. A path with no wildcards probes at most its depth below the prefix root. A glob probes the ancestors of its literal leading part, and the listing refuses any listed key that sits inside a `metadata/` directory holding `*.metadata.json`. Results are cached per statement.
*Why:* `read_parquet('s3://stage/a/t/data/*.parquet')` names no metadata path, so a check on the path text alone misses it.
*Cost:* a few LIST calls per TVF statement under a prefix. Paths outside every prefix are denied before any probe.

**7. Startup warning for shared prefixes.**
An allowed prefix with no `{user}` placeholder gets one startup warning, as `allow_all_object_stores` already does.

**8. Docs.**
The production guide's `s3://your-warehouse/` and `s3://prod-warehouse/` examples become a landing bucket, and the guide states that a TVF prefix never covers a warehouse. The configuration and `read_parquet` pages gain the staging recipe: one global `s3://<staging-bucket>/{catalog}/{user}/` line, a staging root outside every Polaris catalog's `allowedLocations`, and the refusals. Polaris `allowedLocations` is the authoritative control for Iceberg tables, and it binds every engine.

## Risks / Trade-offs

- [The ancestor probe adds LIST calls] -> bounded by path depth below the prefix root, cached per statement, and skipped for denied paths.
- [A dataset whose layout contains a `metadata/` directory with `*.metadata.json` files is refused] -> such a directory is an Iceberg table by definition. The error names the rule.
- [The overlap registry grows] -> it holds only misplaced Iceberg locations, so it stays empty in a correct deployment.

## Migration Plan

No config migration. A deployment whose prefix covers an Iceberg table starts refusing TVF reads under that table: at once via the probe, and on load via the registry. The error log names both. The changelog flags the change under Security, and points deployments that followed the old production guide at the corrected example.

## Open Questions

- Does the file TVF glob listing see each object key before it reads, so decision 6 can refuse per key? If not, the probe covers the literal leading part only, and the docs say so.
