## 0. Decisions

- [ ] 0.1 Settle D5 (who may ATTACH `hive_external`; admin bypass for the Hive gate) and the fallback length (D4)

## 1. Config and policy

- [ ] 1.1 `[hive] allowed_data_prefixes` and `[[hive.roots]] data_prefixes`, validated with the TVF prefix grammar
- [ ] 1.2 `HiveDataPolicy` from `HiveConfig` with the D4 fallback; unit tests for match, placeholders, fallback

## 2. Hive gate

- [ ] 2.1 `HiveManifestStore` checks locations with `HiveDataPolicy` (the five `check_path` sites in `store.rs`)
- [ ] 2.2 Hive table loads record the table location in the protected-location registry

## 3. TVF refusal (with `tvf-prefix-table-overlap` 4.1/4.3)

- [ ] 3.1 Registry takes static Hive prefixes (placeholder segments as wildcards) and loaded Hive locations
- [ ] 3.2 `check_object_store` refuses paths under them, after the inline-credential exit and before allow-all and admin roles
- [ ] 3.3 Unit tests: admin refused, inline credentials allowed, staging outside the Hive prefixes allowed

## 4. Startup

- [ ] 4.1 Error per overlapping (TVF prefix, Hive prefix) pair; deprecation warning for the fallback

## 5. Integration

- [ ] 5.1 Hive table under a Hive prefix that no TVF prefix covers: create, `SELECT`, `ADD PARTITION`
- [ ] 5.2 Overlapping lists: `read_parquet` of the table's file refused; `SELECT` still masked
- [ ] 5.3 Fallback: loaded table's files refused to `read_parquet`

## 6. Docs and close-out

- [ ] 6.1 Hive page, `deployment/configuration.md`, `features/read-parquet.md`: the two lists and the refusal
- [ ] 6.2 Status fragment, changelog fragment under Security
