## Why

A data service that fronts SQE grants its people metadata access in Polaris (load table, list) and decides who sees which rows and columns in Ranger. Today SQE asks Polaris for vended storage keys with the person's own token (`X-Iceberg-Access-Delegation` on their `loadTable`). A person without `TABLE_READ_DATA` gets a 403 and cannot query at all, so the service has to grant every person data access in Polaris. That grant also lets them read the files directly with their own keys, outside SQE's masks and row filters (#934).

## What changes

- A per-catalog **credential identity**: `[catalogs.<name>.credentials]`, the same `client_credentials` shape as `[catalogs.<name>.auth]`. When unset, nothing changes.
- When set, SQE loads tables **as the person without the delegation header**, then calls `GET .../tables/{t}/credentials` **as the engine** and fills the table's FileIO with the keys for the table location. Polaris scopes those keys to the one table and to the engine's own privileges.
- SQE fetches keys only after the person's own load succeeded, and only when the person passes the policy engine's `select` check for that table. A person whose policy denies the table gets the schema and no rows, and SQE makes no `/credentials` call and touches no object.
- **Writes keep the person's delegation.** INSERT, UPDATE, DELETE, MERGE, TRUNCATE, CTAS and every commit load the target with the person's delegated `loadTable`, as today. The engine identity never supplies write keys, so a person without write-data in Polaris cannot write through SQE. The engine principal should hold read-data only.
- The `TableReloader` (credential refresh in long scans, worker tickets) follows the same rule: reload as the person, keys as the engine for reads.
- Audit: each key fetch writes an `auth` event with the person as actor and the engine client id in `app_name`; failures are audited too. Keys are never logged; the key holder redacts itself in `Debug`.
- Startup refuses the setting when no policy engine enforces `select` (passthrough or no policy store) or when `require_vended_credentials` is off for that catalog.

## Out of scope

- Hive catalogs and file TVFs: they do not use Polaris vending.
- Key caching across statements. Keys are fetched per load and live as long as the loaded table.
- A live check against an external identity provider; done outside this repo.
