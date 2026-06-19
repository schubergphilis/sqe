# spark-parity: SQE vs Spark/Kyuubi Ranger mask parity

Proves byte-exact parity between SQE and Apache Spark 3.5 (Kyuubi Authz
extension) for a Ranger `hive` resource data-mask policy.

The claim: a Ranger `MASK_SHOW_LAST_4` policy on `sales.orders.ssn` for role
`engineer` produces the same masked output (`xxx-xx-1111`) whether the query
runs through SQE (PlanRewriter) or Spark (Kyuubi RangerSparkExtension). Both
engines share ONE Polaris 1.5 catalog and ONE Ranger `hive` service.

## Run it

```bash
cp .env.example .env
docker compose up -d --build --wait
./parity-test.sh
```

Or `./run.sh` (does both). Tear down with `./run.sh --down`.

First run builds SQE (roughly 5 min on cold Rust cache) and downloads roughly
300 MB of Spark/Iceberg/Kyuubi jars. Ranger Admin first-boot takes 2-4 min.

## Version matrix

| Component | Version | Notes |
|---|---|---|
| Spark | 3.5.4 (scala2.12-java17-ubuntu) | Spark 4.0 not usable; see below |
| iceberg-spark-runtime | 3.5_2.12-1.8.1 | |
| kyuubi-spark-authz | 2.12-1.11.1 | latest stable; Spark 3.5 fully supported |
| Ranger | 2.8.0 | |
| Polaris | 1.5.0 | |
| Keycloak | 26.5.4 | |

### Why not Spark 4.0?

Spark 4.0 is Scala 2.13-only. `kyuubi-spark-authz_2.13` is NOT published to
Maven Central (verified 2026-06-19: 0 results). Kyuubi enables authZ compile
support for Spark 4.0 in source (issue #7256, merged to branch-1.10 and master)
but does not ship a pre-built artifact. Using Spark 4.0 would require building
Kyuubi from source inside the Docker image (roughly 30 min additional build
time). The latest usable pre-built combo is Spark 3.5 + `_2.12-1.11.1`.

## What it proves

`parity-test.sh` runs `SELECT id, ssn FROM sales_wh.sales.orders ORDER BY id`
as `bob` (Ranger role=engineer) in both engines:

- SQE: via sqe-cli ROPC (user=bob, password=bob123).
- Spark: via spark-sql with `HADOOP_USER_NAME=bob` (Kyuubi picks up OS user for
  Ranger role resolution).

The test extracts the `ssn` column from each output, asserts both show
`xxx-xx-NNNN`, and diffs them. Any difference fails.

## Architecture

```
bob --ROPC--> SQE --Ranger hive svc--> mask applied by PlanRewriter
bob --OS user--> Spark/Kyuubi --Ranger hive svc--> mask applied by RangerSparkExtension
           |
           +-- both read Iceberg table in Polaris REST catalog
           +-- both read policies from Ranger `hive` service
```

Polaris uses native auth (no Ranger authorizer on Polaris itself). The masking
is applied by each engine independently before returning results.

## Known limitation: mask_show_last_n UDF

The `MASK_SHOW_LAST_4` Ranger transformer injects a `mask_show_last_n(...)` SQL
expression into the Spark plan. This function is a Hive GenericUDF. Kyuubi
Authz ships it inside its jar (via bundled ranger-plugins-common). If Spark
throws `Undefined function: mask_show_last_n` at analysis time, the Kyuubi jar
version is mismatched for the deployed Ranger transformer. See the troubleshoot
section below.

## Endpoints

| Service | URL | Credentials |
|---|---|---|
| Keycloak | http://localhost:48080 | admin / admin |
| Polaris | http://localhost:48181 | root / polaris-root-secret |
| Ranger Admin | http://localhost:46080 | admin / rangerR0cks! |
| SQE Flight SQL | grpc://localhost:60061 | Keycloak users below |

## Users (realm `iceberg-ranger`, password `<user>123`)

| User | Roles | Purpose |
|---|---|---|
| carol | sqe_admin, engineer, analyst | seeds data (data-seed service) |
| bob | engineer, analyst | parity query user |
| alice | analyst | unmasked baseline (no engineer role) |

## Troubleshoot

**Spark: `Undefined function: mask_show_last_n`**
The Kyuubi jar does not ship the Hive mask UDFs for your Ranger version. Try
adding `hive-exec-core` to the Spark image or update to Kyuubi 1.10.3+.

**Spark: no rows returned / `AnalysisException: MISSING_ATTRIBUTES`**
A row-filter policy slipped in. The stack seeds ssn-mask-only; check that no
row-filter policy exists on the `hive` service in Ranger Admin
(http://localhost:46080).

**Polaris auth 401 from Spark**
Verify `spark.sql.catalog.sales_wh.credential=root:polaris-root-secret` matches
`POLARIS_BOOTSTRAP_SECRET` in your `.env`.

**SQE policy not applying**
After data-seed, the Ranger plugin polls every 10 seconds. Wait 30 seconds and
retry. Check `docker compose logs sqe` for `[policy] ranger mask` log lines.
