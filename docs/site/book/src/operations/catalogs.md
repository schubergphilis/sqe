# Runtime catalog management

SQE supports DuckDB-style `ATTACH` and `DETACH` for mounting Iceberg
catalogs from SQL at runtime. Credentials live in a session-local
secret store managed with `CREATE SECRET` / `DROP SECRET`. The same
six backends documented in [Catalog backends](../getting-started/catalogs.md)
work here, plus a SQLite backend for local prototyping.

Use this when:

- An analyst wants to point at a partner's catalog for a single
  session without redeploying.
- The dev loop needs a quick local catalog without editing TOML.
- Operators want to provision shared bearer tokens or AWS profiles
  centrally and have queries reference them by name.

ATTACH is process-local. Catalogs attached via SQL are wiped on
coordinator restart. Static TOML catalogs (the `[catalog]` and
`[catalogs.*]` blocks) are the right shape for "this is part of the
deployment." ATTACH is the right shape for "this is part of this
session."

## Syntax

```sql
ATTACH '<location>' AS <name> (TYPE <kind>, <key> = <value>, ...);

DETACH <name>;

CREATE SECRET <name> (TYPE <kind>, <key> = <value>, ...);
DROP SECRET <name>;
SHOW SECRETS;
```

`<location>` is the connection target (URL, ARN, file path) and its
meaning depends on `TYPE`. Option keys are case-insensitive. String
values are single-quoted. The one exception is `SECRET <name>` which
takes a bare identifier so it can be looked up in the secret store.

## Catalog kinds

| `TYPE` value     | `<location>` shape                            | Required options          | Optional options                |
|------------------|-----------------------------------------------|---------------------------|---------------------------------|
| `iceberg_rest`   | URL of the Iceberg REST endpoint              | `WAREHOUSE`               | `SECRET` (bearer)               |
| `glue`           | empty string (region drives discovery)        | `WAREHOUSE`               | `SECRET` (aws), `REGION`        |
| `s3tables`       | empty string                                  | `TABLE_BUCKET_ARN`        | `SECRET` (aws), `ENDPOINT_URL`  |
| `hms`            | Thrift URI (`thrift://host:9083`)             | `WAREHOUSE`               |                                 |
| `jdbc`           | JDBC connection string                        | `WAREHOUSE`               | `SECRET` (basic)                |
| `sqlite`         | local directory path                          |                           |                                 |
| `hadoop`         | warehouse path on object store or local FS    |                           |                                 |

## Secret kinds

| `TYPE` value | Required keys                                | Used by                |
|--------------|----------------------------------------------|------------------------|
| `bearer`     | `TOKEN`                                      | `iceberg_rest`         |
| `basic`      | `USERNAME`, `PASSWORD`                       | `jdbc`                 |
| `aws`        | any of `ACCESS_KEY_ID`, `SECRET_ACCESS_KEY`, `SESSION_TOKEN`, `REGION`, `PROFILE` | `glue`, `s3tables` |

A bearer secret stores one token. A basic secret stores a username
and password. An AWS secret can hold any combination of credential
fields; missing fields fall through to the standard AWS credential
chain (env vars, profile, IMDS).

## Example: REST catalog with bearer token

```sql
CREATE SECRET partner_tok (TYPE bearer, TOKEN 'eyJhbGciOiJSUzI1...');

ATTACH 'http://catalog.example.com:9090/api/catalog' AS partner
  (TYPE iceberg_rest, WAREHOUSE 'analytics', SECRET partner_tok);

SELECT * FROM partner.sales.orders LIMIT 10;

DETACH partner;
DROP SECRET partner_tok;
```

The token never appears in plan history or query logs after the
`CREATE SECRET` statement; subsequent statements reference it by
name. The token bytes are zeroized when the secret is dropped or
the coordinator exits cleanly.

## Example: AWS Glue with explicit credentials

```sql
CREATE SECRET aws_dev (TYPE aws,
  ACCESS_KEY_ID = 'AKIA...',
  SECRET_ACCESS_KEY = 'wJalrXUt...',
  REGION = 'eu-central-1');

ATTACH '' AS glue_dev
  (TYPE glue, WAREHOUSE 's3://my-warehouse/', SECRET aws_dev);

SELECT * FROM glue_dev.public.events LIMIT 5;
```

## Example: AWS Glue using the standard credential chain

Skip `SECRET` and the AWS SDK uses its default chain (env vars,
shared profile, IMDS, container credentials).

```sql
ATTACH '' AS glue_prod
  (TYPE glue, WAREHOUSE 's3://prod-warehouse/');
```

This is the same chain `aws-sdk-glue` uses everywhere else. EKS
service accounts, EC2 instance roles, and `~/.aws/credentials`
profiles all work without an explicit `CREATE SECRET`.

## Example: SQLite for local prototyping

```sql
ATTACH '/tmp/sqe-dev' AS local (TYPE sqlite);

CREATE SCHEMA local.tutorial;
CREATE TABLE local.tutorial.events (id BIGINT, ts TIMESTAMP);
INSERT INTO local.tutorial.events VALUES (1, NOW());
```

The location is a directory. SQE creates `<dir>/catalog.db`
(SQLite-backed Iceberg catalog) and a `<dir>/warehouse/` subdirectory
for table data. Useful for dbt model development without a Polaris
deployment.

## Example: a shared PostgreSQL catalog over JDBC

```sql
CREATE SECRET pg_cat (TYPE basic, USERNAME 'iceberg', PASSWORD 's3cr3t');

ATTACH 'jdbc:postgresql://db.internal:5432/iceberg' AS shared (
  TYPE jdbc,
  WAREHOUSE 's3://lake/warehouse',
  SECRET pg_cat
);

SELECT * FROM shared.analytics.events LIMIT 10;
```

The backend is the same SQL catalog `TYPE sqlite` uses, so PostgreSQL, MySQL,
and SQLite all work through one code path. Write the location either as
`jdbc:postgresql://...`, the form Java tools use, or as `postgresql://...`; the
`jdbc:` prefix is stripped when present.

`WAREHOUSE` is required because a SQL catalog stores table metadata pointers
rather than the data itself, so the data root cannot be inferred from the
connection URL.

Credentials belong in a `basic` secret rather than inline in the URL. The secret
wins if the URL also carries a `user:password@` pair, which keeps rotating the
secret meaningful. Characters that would otherwise break a URL, such as `@`,
`:`, and `/`, are percent-encoded on the way in, so a password does not need
pre-escaping.

Requires one of the `sql`, `sql-postgres`, or `sql-sqlite` cargo features. A
binary built without them reports which feature is missing.

## SHOW CATALOGS and SHOW SECRETS

`SHOW CATALOGS` includes every TOML-configured catalog plus the two
coordinator-registered system catalogs (`system`, `datafusion`) plus
every name added via `ATTACH`. The list updates immediately after
each ATTACH or DETACH.

```sql
SHOW SECRETS;
```

returns a two-column result (`name`, `type`). Secret values are not
exposed; the table is for inventory only.

## Authorization

Out of the box, ATTACH and CREATE SECRET are open to any authenticated
session. Lock them down through the same OPA / Cedar policy backend
that gates GRANT and REVOKE: write a rule that denies
`statement_kind == "attach"` for non-admin roles. The plan rewriter
sees the statement before it reaches the registry, so a denied ATTACH
errors at policy enforcement time, not at catalog build time.

## Lifecycle and persistence

ATTACH mounts are process-local and in-memory. A coordinator restart
forgets every catalog attached via SQL. Static TOML catalogs (the
`[catalog]` and `[catalogs.*]` blocks) are the right shape for "this
is part of the deployment." ATTACH is the right shape for "this is
part of this session."

`CREATE SECRET` is memory-only by default. Set
`[session] secrets_path` (or `SQE_SESSION__SECRETS_PATH`) to snapshot
the store as plaintext JSON after every create or drop. The file is
written at mode 0600. Startup logs a warning because the bytes are
credentials. Do not put the file on a shared volume.

The snapshot restores secrets after a restart. It does not restore
ATTACH mounts. Re-issue ATTACH after boot, or keep shared catalogs
in TOML.

Persistent ATTACH is a feature operators ask for but most do not
want once they think it through. A catalog attached at 9 AM on
Monday is in the system at 3 AM on Sunday because someone forgot
to DETACH it. The credentials behind it have rotated. Queries
against it return 401. The on-call engineer wakes up to a query
failure for a catalog they did not know existed.

## Troubleshooting

**`catalog '<name>' is already attached; DETACH it first`.** A catalog
with that name is in the registry. Issue `DETACH <name>` first or
choose a different name. The check is case-sensitive.

**`catalog '<name>' is not attached`.** DETACH was issued for an
unknown name. Check `SHOW CATALOGS` for the spelling.

**`secret '<name>' is referenced by attached catalogs: <list>`.**
DROP SECRET while one or more attached catalogs reference it. DETACH
the listed catalogs first, then retry the drop. The error names every
referencing catalog so you do not have to chase them one at a time.

**`secret '<name>' not found`.** `ATTACH ... SECRET nonexistent` was
issued without a matching `CREATE SECRET`. Names are case-sensitive.

**`Failed to list namespaces: ...`** during ATTACH. The catalog was
built but the initial `list_namespaces` call against the backend
failed. Check that `<location>` and the credentials are correct, and
that the network path between the coordinator and the catalog is
reachable. The error message includes the upstream HTTP status or
SDK error.

**Bearer token is in the request but the catalog returns 401.** Check
that the token is valid against the catalog's expected issuer. Bearer
tokens stored as secrets are forwarded as-is; SQE does not reissue
or refresh them.

## Dynamic Polaris catalog discovery

`ATTACH` and `[catalogs.*]` both name catalogs explicitly. For
dynamically-provisioned Polaris warehouses (IaC, per-tenant, random-suffixed),
enable lazy discovery instead:

```toml
[query]
catalog_discovery = "polaris-auto"   # default is "static"
```

With `polaris-auto`, a query referencing a 3-part identifier whose catalog is
not statically declared triggers a one-time probe against Polaris for a
warehouse of that name, using the caller's own bearer token. If Polaris
resolves it (and the caller is authorized), SQE registers it into the session
exactly like a static catalog -- same policy enforcement, dynamic-filter
pushdown, and credential passthrough -- and the query proceeds. No `sqe.toml`
change, no restart:

```sql
-- main_warehouse_9d679d was created in Polaris at runtime, never declared in TOML
SELECT count(*) FROM "main_warehouse_9d679d".analytics.orders;
```

Properties:

- **Authorization is unchanged.** The probe uses the caller's bearer; Polaris
  rejects warehouses they are not authorized for. A denied or nonexistent
  warehouse returns the same `unknown catalog` error -- existence is not
  leaked.
- **Per-session scoping.** The discovered catalog is registered into the
  caller's session, not shared process-wide, so vended credentials and
  visibility stay per-user. A second reference in the same session reuses it
  without re-probing.
- **Drop-out within the session TTL.** A renamed or dropped warehouse stops
  resolving on the next session refresh.
- **`static` (the default) is unchanged** -- an undeclared catalog errors with
  no Polaris probe.
- **REST/Polaris only.** Glue, S3 Tables, and HMS still require static
  declaration or `ATTACH`. `SHOW CATALOGS` lists statically-configured and
  already-discovered catalogs, not warehouses never yet referenced.

## v1 limitations

- No on-disk persistence. ATTACH does not survive a restart.
- No encryption-at-rest for secrets. The store holds plaintext bytes
  in memory; `Drop` zeroizes on clean shutdown but does not protect
  against process dumps or memory snapshots taken while running.
- No mTLS to attached REST catalogs. Bearer tokens only.
- No KERBEROS for HMS. The HMS path uses the upstream Thrift client's
  default authentication.
- Authorization is enforced through the policy backend (OPA / Cedar).
  There is no built-in role check for ATTACH or CREATE SECRET out of
  the box; operators wire it themselves through policy rules.
- The embedded CLI (`sqe-cli` ad-hoc mode) supports the same syntax
  as the cluster server. Embedded ATTACH targets the same in-memory
  registry but does not share state across CLI invocations.
