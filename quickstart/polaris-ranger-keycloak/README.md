# Polaris + Apache Ranger + Keycloak

A second access-control option for Apache Polaris. SQE translates `GRANT` /
`REVOKE` / `SHOW GRANTS` into Apache Ranger Admin REST calls (the `ranger`
access-control backend). Polaris 1.5's embedded Ranger authorizer enforces those
policies when SQE asks Polaris for table access on behalf of a user.

## Run it

```bash
cp .env.example .env
docker compose up -d --build --wait   # Ranger Admin first-boot takes 2-4 min
./test.sh            # SQE GRANT/REVOKE + fine-grained mask enforcement
./parity-test.sh     # SQE <-> Spark (Kyuubi Authz) Ranger mask cross-compare
```

Or `./run.sh` (does both). Tear down with `./run.sh --down`. First run also
builds an Apache Spark image and downloads roughly 250 MB of Spark/Iceberg/Kyuubi
jars.

## What it proves

`test.sh` runs the full matrix (all green from a clean bring-up):

- Two catalogs (`sales_wh`, `ops_wh`); 3-part names route to the right one via
  `[query] catalog_discovery = "polaris-auto"`.
- A `GRANT SELECT` visibly enabling a read that was denied before it.
- Role grants (`analyst`, `engineer`) and a user grant (`bob`).
- A Ranger DENY added to the same policy, overriding an allow (deny precedence).
- Negative tests: an ungranted user and a read-only role are denied.
- `SHOW GRANTS` round-trip, a `REVOKE` that takes effect, and `CHECK ACCESS`.

## SQE <-> Spark cross-compare

`parity-test.sh` proves SQE and Apache Spark agree on a Ranger column mask. Both
engines share ONE Polaris catalog and ONE Ranger `hive` service. Running
`SELECT id, ssn FROM sales_wh.sales.orders` as `bob` (role `engineer`) returns
the SAME masked output in both:

| Engine | Identity to Ranger | ssn output |
|---|---|---|
| SQE | `bob` (Keycloak ROPC) | `xxx-xx-1111`, `xxx-xx-2222` |
| Spark 3.5 + Kyuubi Authz | `bob` (`HADOOP_USER_NAME`) | `xxx-xx-1111`, `xxx-xx-2222` |

Spark connects to Polaris as the `root` service account; the per-user ssn mask
is the Kyuubi `RangerSparkExtension`'s job, keyed on `bob`. The mask policy uses
a CUSTOM portable-SQL transformer (`concat('xxx-xx-', substr({col},8,4))`) so the
two engines render byte-identical output. See `OVERVIEW.md` for why named Ranger
mask types are NOT byte-portable between SQE and Kyuubi.

Spark 4.0 is not used: `kyuubi-spark-authz_2.13` is not published to Maven
Central, so Spark 3.5 (Scala 2.12) + `kyuubi-spark-authz-shaded_2.12-1.11.1` is
the latest pre-built combo. See `spark/Dockerfile`.

## Endpoints

| Service | URL | Credentials |
|---|---|---|
| Keycloak | http://localhost:38080 | admin / admin |
| Polaris | http://localhost:28181 | OIDC (Keycloak) |
| Ranger Admin | http://localhost:26080 | admin / rangerR0cks! |
| SQE Flight SQL | grpc://localhost:60061 | Keycloak users below |

## Users (realm `iceberg-ranger`, password `<user>123`)

| User | Roles | Purpose |
|---|---|---|
| carol | sqe_admin, engineer, analyst | runs GRANT/REVOKE |
| bob | engineer, analyst | read + write |
| alice | analyst | read-only |
| dave | (none) | negative tests |

See `OVERVIEW.md` for the architecture and the identity model.
