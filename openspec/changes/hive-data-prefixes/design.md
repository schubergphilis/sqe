## Context

Hive external tables (`[[hive.roots]]` and `ATTACH ... TYPE hive_external`) keep their definitions in sidecar manifests or Glue and read the data with the engine's storage credentials or a named `SECRET`. Nothing on the storage side scopes a read to the user, so SQE gates locations itself. Today that gate is the file TVF allow-list:

- `HiveManifestStore::{check_location, put, get, ...}` call `TvfPolicy::check_path(location, caller, false)` on the table location and every partition-index location (`sqe-catalog/src/hive/store.rs`).
- A `SELECT` and Hive DDL run the check as the session user (`hive_session_caller`, `sqe-coordinator/src/hive_ddl_handler.rs`). The attached store itself carries the attaching user's caller, or `trusted_startup_config()` for a configured root, which skips the allow-list; manifest reads use it.

So the allow-list says both "a user may `read_parquet` here" and "a Hive table may live here". Every deployment with a Hive table has to grant the first to get the second, and the policy layer only sees the second.

## Goals / Non-Goals

**Goals**
- A Hive table's location never has to be a file TVF prefix.
- A file TVF read under Hive table data is refused, whoever asks, unless it brings its own credentials.
- Who may read or create a Hive table does not change: the same caller, `{user}` and `{catalog}` placeholders and canonicalisation apply, against a different list.

**Non-Goals**
- Per-user storage credentials for Hive reads.
- Changing Glue roots' credential handling or Hive caching.
- The Iceberg overlap guard itself (`tvf-prefix-table-overlap`); this change shares its registry and refusal point.

## Decisions

### D1. Two lists, one matcher
`[hive] allowed_data_prefixes` applies to every root; `data_prefixes` on a `[[hive.roots]]` entry adds prefixes for that root. They use the TVF prefix grammar and matcher (`canonicalize_object_url`, `{user}`, `{catalog}`), so the Hive gate is `check_path` against another prefix set. A `HiveDataPolicy` built from `HiveConfig` holds it; `HiveManifestStore` takes it in place of the `TvfPolicy` it holds now. Admin roles and `allow_all_object_stores` keep their meaning for the Hive gate only if D5 says so.

*Alternative:* derive the allowed area from the root (data must live under the root `location`). Rejected: sidecar roots routinely point at data written by other engines elsewhere, and Glue roots have no location.

### D2. Protected-location registry, shared with Iceberg
The overlap change adds a process-wide registry of locations file TVFs must refuse. Hive adds two kinds of entry: every Hive data prefix (static, from config, with placeholder segments as wildcards) and every Hive table location SQE loads (dynamic, so a table under a broad prefix is covered precisely). The refusal sits in `check_object_store` after the trusted-local and inline-credential exits and before allow-all, admin roles and the allow-list, exactly where the Iceberg refusal goes.

### D3. Startup overlap check
At startup, every allowed TVF prefix is compared with every Hive data prefix. An overlap is logged at error level naming both; startup continues, and D2 refuses the reads. Refusing to start would turn a config upgrade into an outage.

### D4. Migration fallback
When neither `allowed_data_prefixes` nor any root's `data_prefixes` is set, the Hive gate uses the TVF list, as today, and startup logs a deprecation warning that names the new keys. D2's dynamic entries still apply, so a raw read of a table SQE has loaded is refused during the fallback. The fallback is removed one release later.

### D5. ATTACH (open: needs a decision)
An attached root's tables are checked per reading session (unchanged). What is open is who may attach a root whose locations other users will then read with the engine's credentials. Options:

1. **Unchanged:** any user who may `ATTACH` may attach a root; each location is still checked against the Hive list for every reader. Under a `{user}` prefix, other users simply cannot read it.
2. **Admin-only ATTACH of `hive_external`**, as for configured roots.

Recommended default: 1, because the per-reader check already bounds what any user reads to the Hive list expanded for that user, and admin-only ATTACH would break the self-service staging use. Admin roles bypass the Hive gate only if the TVF gate's admin bypass is kept for it (`allow_all_object_stores` too); recommended: keep the bypass for the Hive gate, since it controls where tables may live, not raw reads.

## Risks / Trade-offs

- **Existing TVF reads of Hive data stop working** once D2 lands. Intended; the changelog says so under Security.
- **Broad Hive prefixes block TVF staging below them.** A deployment that wants both keeps them apart; the startup check (D3) names the overlap.
- **Dynamic registry growth** is bounded by the number of Hive tables loaded, as for Iceberg.

## Open Questions

- D5: confirm option 1 and the admin bypass for the Hive gate.
- Should the fallback (D4) last one release or be dropped at once for deployments with no Hive roots? (No roots means nothing to migrate.)
