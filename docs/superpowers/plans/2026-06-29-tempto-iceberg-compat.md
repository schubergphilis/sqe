# Tempto Iceberg Compatibility Harness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run the official upstream `trino-product-tests` Iceberg suite (via the `trinodb/tempto` framework) against SQE's Trino HTTP endpoint and report compatibility as pass / fail / documented-exclusion.

**Architecture:** A fully Docker-based harness. The published `io.trino:trino-product-tests:465` jar (no Trino source build) is run by a small Gradle project inside a container; it speaks the Trino JDBC protocol to a Caddy TLS terminator that forwards to SQE's plain-HTTP compat endpoint. SQE runs against Polaris + RustFS with its single catalog named `iceberg` (the name the upstream tests hardcode). A curated allow-list selects the pure-Trino runnable tests; everything Spark/Hive/HDFS-coupled is excluded and documented.

**Tech Stack:** Docker Compose, Caddy (TLS), Gradle + JDK 23 (container), `io.trino:trino-product-tests:465`, `io.trino.tempto:tempto-core` (transitive), Trino JDBC driver, Apache Polaris 1.5.0, RustFS, SQE (`sqe-trino-compat`).

## Global Constraints

- **Host requires only Docker** (Docker 29 + Compose v5 present). Gradle/Caddy/JDK run in containers. Do not add host-level JVM/Gradle deps.
- **Pinned versions:** Trino product-tests + JDBC = **465** (matches `docker-compose.compare.yml` `trinodb/trino:465`). Polaris **1.5.0**. RustFS `latest` (matches test stack).
- **Auth:** HTTP **Basic** `root` / `s3cr3t` over TLS. The Trino JDBC driver refuses password/token over plain HTTP, so TLS is mandatory. Do NOT use Bearer (SQE test `[auth]` only exchanges Basic creds with Polaris; it does not validate inbound JWTs).
- **Catalog name MUST be `iceberg`** — upstream tests hardcode `USE iceberg.default`, `iceberg.default.<table>`, and assert `CREATE TABLE iceberg.default...`. SQE presents `[catalog] warehouse` as its SQL catalog name, so the tempto SQE config sets `warehouse = "iceberg"`.
- **Polaris location non-overlap:** each catalog needs a distinct `allowedLocations`. The `iceberg` catalog uses bucket `s3://warehouse-iceberg/` (the existing stack already uses `s3://warehouse/` and `s3://warehouse-discovery/`).
- **Git:** work on branch `test/tempto-iceberg-compat`. Commit per task. Never push to main.
- **No emdash/endash/Unicode arrows** in any committed docs (repo voice rule); use `->` in code, plain hyphens in prose.
- **Reuse, do not duplicate:** layer on top of `docker-compose.test.yml`; mirror `scripts/bootstrap-test.sh` rather than reinventing Polaris bootstrap.

---

## File Structure

- `testing/tempto/build.gradle.kts` — Gradle project; pulls `trino-product-tests:465`, `application` plugin, mainClass = `io.trino.tests.product.TemptoProductTestRunner`.
- `testing/tempto/settings.gradle.kts` — root project name.
- `testing/tempto/tempto-configuration.yaml` — single `trino` database pointing at the TLS proxy with Basic creds.
- `testing/tempto/allowlist.txt` — curated FQ test class/method list (one per line, `#` comments).
- `testing/tempto/exclusions.md` — every excluded class/method + reason.
- `testing/tempto/Caddyfile` — TLS terminator config (`:8443` -> `sqe:8080`).
- `tests/tempto/coordinator-tempto.toml` — SQE coordinator config, `warehouse = "iceberg"`, single-node (no workers).
- `docker-compose.tempto.yml` — overlay adding `tls-proxy` and `tempto-runner` services and the tempto SQE config mount.
- `scripts/tempto/bootstrap-iceberg-catalog.sh` — create `warehouse-iceberg` bucket + `iceberg` Polaris catalog + namespace (mirror of `bootstrap-test.sh`, parameterized).
- `scripts/tempto-test.sh` — top-level orchestrator: up stack, bootstrap, wait, run runner, summarize.
- `docs/internal/process/tempto-iceberg-compat.md` — usage + coverage + how to add/exclude a test.

---

## Task 1: Gradle runner project + runner spike

Proves the published jar resolves and the tempto runner CLI is reachable, with zero dependence on the SQE stack. This is the highest-risk unknown, so it goes first.

**Files:**
- Create: `testing/tempto/settings.gradle.kts`
- Create: `testing/tempto/build.gradle.kts`

**Interfaces:**
- Produces: a Gradle `run` task with mainClass `io.trino.tests.product.TemptoProductTestRunner`, runnable in a `gradle:8.10.2-jdk23` container, that accepts tempto CLI args via `--args`.

- [ ] **Step 1: Write `settings.gradle.kts`**

```kotlin
rootProject.name = "sqe-tempto"
```

- [ ] **Step 2: Write `build.gradle.kts`**

```kotlin
plugins {
    application
}

repositories {
    mavenCentral()
}

dependencies {
    // Brings in tempto-core, trino-jdbc, and the iceberg product-test classes transitively.
    implementation("io.trino:trino-product-tests:465")
}

application {
    mainClass.set("io.trino.tests.product.TemptoProductTestRunner")
}

// Pass tempto args from the command line, e.g.:
//   gradle run --args="--help"
tasks.named<JavaExec>("run") {
    // Tempto writes reports/temp under the working dir; keep it inside the project.
    workingDir = layout.projectDirectory.asFile
}
```

- [ ] **Step 3: Resolve dependencies in a container**

Run:
```bash
docker run --rm -v "$PWD/testing/tempto:/work" -w /work \
  -v sqe-tempto-gradle:/home/gradle/.gradle \
  gradle:8.10.2-jdk23 gradle --no-daemon dependencies --configuration runtimeClasspath
```
Expected: build succeeds; output lists `io.trino:trino-product-tests:465`, `io.trino.tempto:tempto-core`, and `io.trino:trino-jdbc`. No "Could not resolve" errors.

- [ ] **Step 4: Run the tempto runner help (the spike)**

Run:
```bash
docker run --rm -v "$PWD/testing/tempto:/work" -w /work \
  -v sqe-tempto-gradle:/home/gradle/.gradle \
  gradle:8.10.2-jdk23 gradle --no-daemon -q run --args="--help"
```
Expected: tempto prints its CLI usage, listing options including `--config`, `--tests`, `--groups`, `--exclude-groups`, `--report-dir`. Record the EXACT option names and `--tests` matcher syntax shown; later tasks depend on them. If `--help` is not honored by `TemptoProductTestRunner`, capture the usage/error it prints instead and note the accepted flags.

- [ ] **Step 5: Commit**

```bash
git add testing/tempto/settings.gradle.kts testing/tempto/build.gradle.kts
git commit -m "test(tempto): gradle runner project for trino-product-tests 465"
```

---

## Task 2: TLS terminator (Caddy)

Gives the JDBC driver the HTTPS endpoint it requires, forwarding to SQE's plain HTTP.

**Files:**
- Create: `testing/tempto/Caddyfile`

**Interfaces:**
- Produces: an HTTPS listener on container port `8443` (service name `tls-proxy`) that reverse-proxies to `http://sqe:8080`, using Caddy's internal self-signed CA.

- [ ] **Step 1: Write `Caddyfile`**

```
{
    auto_https disable_redirects
    local_certs
}

https://tls-proxy:8443 {
    tls internal
    reverse_proxy http://sqe:8080
}
```

- [ ] **Step 2: Sanity-check Caddy parses the config**

Run:
```bash
docker run --rm -v "$PWD/testing/tempto/Caddyfile:/etc/caddy/Caddyfile:ro" \
  caddy:2 caddy validate --config /etc/caddy/Caddyfile --adapter caddyfile
```
Expected: `Valid configuration`. (The `tls-proxy` hostname resolves inside the compose network in Task 4; validation here only checks syntax.)

- [ ] **Step 3: Commit**

```bash
git add testing/tempto/Caddyfile
git commit -m "test(tempto): caddy TLS terminator for SQE compat endpoint"
```

---

## Task 3: SQE tempto stack config + iceberg-catalog bootstrap

Stands up SQE with catalog `iceberg` on Polaris + RustFS.

**Files:**
- Create: `tests/tempto/coordinator-tempto.toml`
- Create: `scripts/tempto/bootstrap-iceberg-catalog.sh`

**Interfaces:**
- Consumes: existing `docker-compose.test.yml` services (`polaris`, `rustfs`).
- Produces: a Polaris catalog named `iceberg` backed by `s3://warehouse-iceberg/` with namespace `default`; a SQE coordinator config exposing SQL catalog `iceberg`.

- [ ] **Step 1: Write `tests/tempto/coordinator-tempto.toml`** (single-node; copy of the distributed coordinator with no workers and warehouse renamed)

```toml
# SQE coordinator config for the tempto compatibility stack.
# Single-node (no workers); catalog presented to SQL as "iceberg".
[coordinator]
flight_sql_port = 50051
trino_http_port = 8080
worker_urls = []
allow_unauthenticated_workers = true

[metrics]
prometheus_port = 9090

[auth]
token_endpoint = "http://polaris:8181/api/catalog/v1/oauth/tokens"
client_id = "root"
client_secret = "s3cr3t"

[catalog]
catalog_url = "http://polaris:8181/api/catalog"
warehouse = "iceberg"

[storage]
s3_endpoint = "http://rustfs:9000"
s3_access_key = "s3admin"
s3_secret_key = "s3admin"
s3_region = "us-east-1"
s3_path_style = true

[query_cache]
enabled = true
max_memory_mb = 128
max_entry_mb = 5
ttl_secs = 300

[query_history]
max_entries = 10000
ttl_secs = 1800
```

- [ ] **Step 2: Write `scripts/tempto/bootstrap-iceberg-catalog.sh`** (mirror of `bootstrap-test.sh`, dedicated to the `iceberg` catalog + its own bucket)

```bash
#!/usr/bin/env bash
set -euo pipefail
# Create the 'iceberg' Polaris catalog + 'warehouse-iceberg' bucket + 'default'
# namespace for the tempto compatibility stack. Idempotent.

POLARIS_URL="${POLARIS_URL:-http://localhost:18181}"
S3_URL="${S3_URL:-http://localhost:19000}"
S3_ACCESS_KEY="${S3_ACCESS_KEY:-s3admin}"
S3_SECRET_KEY="${S3_SECRET_KEY:-s3admin}"
CLIENT_ID="${CLIENT_ID:-root}"
CLIENT_SECRET="${CLIENT_SECRET:-s3cr3t}"
WAREHOUSE="iceberg"
BUCKET="warehouse-iceberg"
NAMESPACE="default"

echo "=== Tempto iceberg-catalog bootstrap ==="

echo -n "Waiting for Polaris..."
for i in $(seq 1 60); do
  HTTP=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$POLARIS_URL/api/catalog/v1/oauth/tokens" \
    -d "grant_type=client_credentials&client_id=$CLIENT_ID&client_secret=$CLIENT_SECRET&scope=PRINCIPAL_ROLE:ALL" 2>/dev/null || echo "000")
  [ "$HTTP" = "200" ] && { echo " ready"; break; }
  [ "$i" -eq 60 ] && { echo " TIMEOUT (HTTP=$HTTP)"; exit 1; }
  echo -n "."; sleep 1
done

echo -n "Creating bucket '$BUCKET'... "
CODE=$(curl -s -o /dev/null -w "%{http_code}" -X PUT "$S3_URL/$BUCKET" \
  -u "${S3_ACCESS_KEY}:${S3_SECRET_KEY}" 2>/dev/null || echo "000")
case "$CODE" in 200|204) echo "created" ;; 409) echo "exists" ;; *) echo "FAILED ($CODE)"; exit 1 ;; esac

echo -n "Getting Polaris token... "
TOKEN=$(curl -sf -X POST "$POLARIS_URL/api/catalog/v1/oauth/tokens" \
  -d "grant_type=client_credentials&client_id=$CLIENT_ID&client_secret=$CLIENT_SECRET&scope=PRINCIPAL_ROLE:ALL" \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")
[ -z "$TOKEN" ] && { echo "FAILED"; exit 1; }
echo "done"

echo -n "Creating catalog '$WAREHOUSE'... "
CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$POLARIS_URL/api/management/v1/catalogs" \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d "{\"catalog\":{\"name\":\"$WAREHOUSE\",\"type\":\"INTERNAL\",\"storageConfigInfo\":{\"storageType\":\"S3\",\"allowedLocations\":[\"s3://$BUCKET/\"],\"endpoint\":\"$S3_URL\",\"endpointInternal\":\"http://rustfs:9000\",\"pathStyleAccess\":true},\"properties\":{\"default-base-location\":\"s3://$BUCKET/\",\"polaris.config.drop-with-purge.enabled\":\"true\"}}}" 2>/dev/null)
case "$CODE" in 200|201) echo "done" ;; 409) echo "exists" ;; *) echo "FAILED ($CODE)"; exit 1 ;; esac

echo -n "Granting catalog admin... "
curl -s -o /dev/null -X POST "$POLARIS_URL/api/management/v1/catalogs/$WAREHOUSE/catalog-roles" \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"catalogRole":{"name":"catalog_admin"}}' 2>/dev/null || true
curl -s -o /dev/null -X PUT "$POLARIS_URL/api/management/v1/catalogs/$WAREHOUSE/catalog-roles/catalog_admin/grants" \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"grant":{"type":"catalog","privilege":"CATALOG_MANAGE_CONTENT"}}' 2>/dev/null || true
curl -s -o /dev/null -X PUT "$POLARIS_URL/api/management/v1/principal-roles/service_admin/catalog-roles/$WAREHOUSE" \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d "{\"catalogRole\":{\"name\":\"catalog_admin\"}}" 2>/dev/null || true
echo "done"

echo -n "Creating namespace '$NAMESPACE'... "
CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST \
  "$POLARIS_URL/api/catalog/v1/$WAREHOUSE/namespaces" \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d "{\"namespace\":[\"$NAMESPACE\"]}" 2>/dev/null)
case "$CODE" in 200|201) echo "done" ;; 409) echo "exists" ;; *) echo "WARN ($CODE)" ;; esac
echo "Bootstrap complete."
```

- [ ] **Step 3: Make the script executable + verify shell syntax**

Run:
```bash
chmod +x scripts/tempto/bootstrap-iceberg-catalog.sh
bash -n scripts/tempto/bootstrap-iceberg-catalog.sh && echo "syntax OK"
```
Expected: `syntax OK`.

- [ ] **Step 4: Commit**

```bash
git add tests/tempto/coordinator-tempto.toml scripts/tempto/bootstrap-iceberg-catalog.sh
git commit -m "test(tempto): SQE iceberg-catalog config + Polaris bootstrap"
```

---

## Task 4: Compose overlay + live connectivity check

Wires SQE + TLS proxy + runner into one stack and proves Basic-over-TLS reaches SQE end to end.

**Files:**
- Create: `docker-compose.tempto.yml`
- Create: `testing/tempto/tempto-configuration.yaml`

**Interfaces:**
- Consumes: `docker-compose.test.yml` (polaris, rustfs), Task 1 Gradle project, Task 2 Caddyfile, Task 3 SQE config.
- Produces: services `sqe` (compat HTTP 8080, host 28080), `tls-proxy` (8443, host 28443), `tempto-runner` (manual `run` profile); a tempto `trino` database definition.

- [ ] **Step 1: Write `docker-compose.tempto.yml`**

```yaml
# docker-compose.tempto.yml
# Tempto Iceberg-compatibility overlay. Compose with the test stack:
#   docker compose -f docker-compose.test.yml -f docker-compose.tempto.yml up -d sqe tls-proxy
# Then bootstrap + run via scripts/tempto-test.sh.
services:
  sqe:
    build:
      context: .
    ports:
      - "28080:8080"   # Trino HTTP compat (host debugging)
    environment:
      SQE_CONFIG: /etc/sqe/config.toml
    volumes:
      - ./tests/tempto/coordinator-tempto.toml:/etc/sqe/config.toml:ro
    depends_on:
      polaris:
        condition: service_healthy

  tls-proxy:
    image: caddy:2
    ports:
      - "28443:8443"   # HTTPS (host debugging)
    volumes:
      - ./testing/tempto/Caddyfile:/etc/caddy/Caddyfile:ro
    depends_on:
      - sqe

  tempto-runner:
    image: gradle:8.10.2-jdk23
    working_dir: /work
    volumes:
      - ./testing/tempto:/work
      - sqe-tempto-gradle:/home/gradle/.gradle
    # Driven explicitly by scripts/tempto-test.sh; no default command.
    entrypoint: ["gradle", "--no-daemon"]
    command: ["help"]
    depends_on:
      - tls-proxy
    profiles: ["run"]

volumes:
  sqe-tempto-gradle:
```

- [ ] **Step 2: Write `testing/tempto/tempto-configuration.yaml`** (only the `trino` database `onTrino()` needs; in-network hostnames)

```yaml
databases:
  trino:
    host: tls-proxy
    port: 8443
    server_address: https://tls-proxy:8443
    jdbc_driver_class: io.trino.jdbc.TrinoDriver
    jdbc_url: jdbc:trino://tls-proxy:8443/iceberg/default?SSL=true&SSLVerification=NONE
    jdbc_user: root
    jdbc_password: s3cr3t
    jdbc_pooling: false

tests:
  assert:
    float_tolerance: 0.000001
```

- [ ] **Step 3: Validate the merged compose**

Run:
```bash
docker compose -f docker-compose.test.yml -f docker-compose.tempto.yml config >/dev/null && echo "compose OK"
```
Expected: `compose OK` (no merge/key errors).

- [ ] **Step 4: Bring up the stack and bootstrap**

Run:
```bash
docker compose -f docker-compose.test.yml -f docker-compose.tempto.yml up -d --build sqe tls-proxy
POLARIS_URL=http://localhost:18181 S3_URL=http://localhost:19000 \
  scripts/tempto/bootstrap-iceberg-catalog.sh
```
Expected: stack starts; bootstrap prints `Bootstrap complete.`

- [ ] **Step 5: Prove Basic-over-TLS reaches SQE and catalog is `iceberg`**

Run (trino-cli is on the host; talk to the host-published HTTPS proxy port):
```bash
trino --server https://localhost:28443 --insecure \
  --user root --password <<< "" 2>/dev/null; \
TRINO_PASSWORD=s3cr3t trino --server https://localhost:28443 --insecure \
  --user root --password --catalog iceberg --schema default \
  --execute "SHOW CATALOGS"
```
Expected: output includes `iceberg`. If trino-cli password prompting is awkward in non-interactive mode, substitute the proven curl path against the proxy:
```bash
curl -sk -u root:s3cr3t -H "X-Trino-User: root" -H "X-Trino-Catalog: iceberg" \
  -H "Content-Type: text/plain" --data "SHOW CATALOGS" \
  https://localhost:28443/v1/statement | python3 -c "import sys,json;d=json.load(sys.stdin);print(d.get('stats',{}).get('state'),d.get('nextUri'))"
```
Expected: state `QUEUED`/`RUNNING`/`FINISHED` (not an auth error). Follow `nextUri` if needed to confirm `iceberg` appears.

- [ ] **Step 6: Commit**

```bash
git add docker-compose.tempto.yml testing/tempto/tempto-configuration.yaml
git commit -m "test(tempto): compose overlay + tempto trino connection config"
```

---

## Task 5: Curated allow-list + first tempto run

**Files:**
- Create: `testing/tempto/allowlist.txt`

**Interfaces:**
- Consumes: the exact `--tests` matcher syntax recorded in Task 1 Step 4.
- Produces: a newline-delimited curated test list consumed by `scripts/tempto-test.sh` (Task 7).

- [ ] **Step 1: Write `testing/tempto/allowlist.txt`** (start with the pure-`onTrino` classes; file-existence/HDFS methods are handled by exclusions in Task 6)

```
# Curated upstream Iceberg product-tests that run against SQE alone (no Spark/Hive).
# Format: one tempto test matcher per line; '#' comments and blank lines ignored.
# Verified pure-onTrino at trino tag 465. Expand as SQE gains features.
io.trino.tests.product.iceberg.TestIcebergInsert
io.trino.tests.product.iceberg.TestIcebergPartitionEvolution
io.trino.tests.product.iceberg.TestCreateDropSchema
```

- [ ] **Step 2: Run the allow-list against SQE through the runner**

Run (uses the in-network config; `--tests` value built from the allow-list. Adjust flag names to match Task 1 findings):
```bash
TESTS=$(grep -vE '^\s*#|^\s*$' testing/tempto/allowlist.txt | paste -sd, -)
docker compose -f docker-compose.test.yml -f docker-compose.tempto.yml run --rm \
  tempto-runner -q run --args="--config /work/tempto-configuration.yaml --report-dir /work/reports --tests $TESTS"
```
Expected: tempto connects (no JDBC/TLS/auth errors) and executes the listed tests. Some assertions may FAIL on genuine SQE compatibility gaps (for example `SHOW CREATE TABLE` formatting) -- that is real signal, not harness breakage. Record which methods pass vs fail.

- [ ] **Step 3: Triage failures into the allow-list**

For each failing method that fails due to a feature SQE does not support or a known formatting diff (not a harness bug), remove it from `allowlist.txt` (or narrow the matcher to `Class.method` form for the passing methods) so the allow-list is green. Capture every removed item for Task 6.

Expected after triage: re-running Step 2 yields all-pass over the allow-list.

- [ ] **Step 4: Commit**

```bash
git add testing/tempto/allowlist.txt
git commit -m "test(tempto): curated green Iceberg allow-list"
```

---

## Task 6: Document exclusions

**Files:**
- Create: `testing/tempto/exclusions.md`

- [ ] **Step 1: Write `testing/tempto/exclusions.md`** documenting why each upstream Iceberg test class (and any allow-list method removed in Task 5) is not run. Use this structure, filling the SQE-gap rows from Task 5 triage:

```markdown
# Tempto Iceberg test exclusions

Upstream group: `io.trino.tests.product.iceberg` at trino tag 465.
Run model: curated allow-list (`allowlist.txt`). Everything not on the
allow-list is excluded for one of the reasons below.

## Excluded: require Spark/Hive (cannot run against SQE alone)
- `TestIcebergSparkCompatibility` -- sets up tables via onSpark() (308 calls).
- `TestIcebergSparkDropTableCompatibility` -- onSpark() table setup.
- `TestIcebergRedirectionToHive` -- needs a Hive catalog + redirection.
- `TestIcebergHiveViewsCompatibility` -- needs Hive views.
- `TestIcebergHiveMetadataListing` -- needs Hive metastore listing.
- `TestIcebergFormatVersionCompatibility` -- onSpark() cross-version setup.

## Excluded: need HDFS/file-existence assertions (tempto hdfs config)
- `TestCreateDropSchema.testDropSchemaFiles*` -- assertFileExistence via HDFS.

## Excluded: unsupported procedure / feature in SQE (from Task 5 triage)
- <Class.method> -- <reason> -- <tracking pointer if any>

## Partially included
- `TestIcebergProcedureCalls` -- only procedures SQE supports; rest excluded.
- `TestIcebergOptimize` -- included only if SQE supports `ALTER TABLE ... EXECUTE optimize`.
```

- [ ] **Step 2: Commit**

```bash
git add testing/tempto/exclusions.md
git commit -m "docs(tempto): document Iceberg test exclusions and reasons"
```

---

## Task 7: Orchestrator script

**Files:**
- Create: `scripts/tempto-test.sh`

**Interfaces:**
- Consumes: all prior artifacts.
- Produces: `scripts/tempto-test.sh [--no-build]` that brings the stack up, bootstraps, runs the allow-list, and prints a pass/fail/skip summary; exit non-zero on any allow-list failure.

- [ ] **Step 1: Write `scripts/tempto-test.sh`**

```bash
#!/usr/bin/env bash
set -euo pipefail
# Run the upstream Trino Iceberg product-tests against SQE via tempto.
# Usage: scripts/tempto-test.sh [--no-build]
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT_DIR"

COMPOSE=(docker compose -f docker-compose.test.yml -f docker-compose.tempto.yml)
BUILD_FLAG="--build"
[ "${1:-}" = "--no-build" ] && BUILD_FLAG=""

echo "=== Tempto Iceberg compatibility run ==="
"${COMPOSE[@]}" up -d $BUILD_FLAG sqe tls-proxy

echo "Bootstrapping iceberg catalog..."
POLARIS_URL=http://localhost:18181 S3_URL=http://localhost:19000 \
  "$SCRIPT_DIR/tempto/bootstrap-iceberg-catalog.sh"

echo "Waiting for SQE compat endpoint..."
timeout 60 bash -c 'until curl -sf http://localhost:28080/v1/info >/dev/null; do sleep 1; done' \
  || { echo "ERROR: SQE not reachable on 28080"; exit 1; }

TESTS=$(grep -vE '^\s*#|^\s*$' testing/tempto/allowlist.txt | paste -sd, -)
echo "Running tempto allow-list: $TESTS"
set +e
"${COMPOSE[@]}" run --rm tempto-runner -q run \
  --args="--config /work/tempto-configuration.yaml --report-dir /work/reports --tests $TESTS"
RC=$?
set -e

echo ""
if [ $RC -eq 0 ]; then
  echo "RESULT: PASS (allow-list green)"
else
  echo "RESULT: FAIL (rc=$RC) -- see testing/tempto/reports/ for the tempto report"
fi
exit $RC
```

- [ ] **Step 2: Make executable + syntax check**

Run:
```bash
chmod +x scripts/tempto-test.sh && bash -n scripts/tempto-test.sh && echo "syntax OK"
```
Expected: `syntax OK`.

- [ ] **Step 3: End-to-end run**

Run:
```bash
scripts/tempto-test.sh
```
Expected: prints `RESULT: PASS (allow-list green)` and exits 0. (Adjust flag names if Task 1 revealed different tempto CLI options.)

- [ ] **Step 4: Commit**

```bash
git add scripts/tempto-test.sh
git commit -m "test(tempto): one-command orchestrator for the iceberg compat run"
```

---

## Task 8: Docs + repo state

**Files:**
- Create: `docs/internal/process/tempto-iceberg-compat.md`
- Modify: `README.md` (roadmap checklist), `nextsteps.md` (status), `.gitignore` (ignore `testing/tempto/reports/`)

- [ ] **Step 1: Write `docs/internal/process/tempto-iceberg-compat.md`**

Document: what it does, the one command (`scripts/tempto-test.sh`), the stack diagram (SQE -> caddy TLS -> JDBC -> tempto), how the catalog must be `iceberg`, how to add a test to `allowlist.txt`, how to record an exclusion, and the published-jar pin (`io.trino:trino-product-tests:465`). Keep to repo voice (no emdash, `->` not arrows).

- [ ] **Step 2: Ignore generated reports**

Add to `.gitignore`:
```
testing/tempto/reports/
```

- [ ] **Step 3: Update README roadmap + nextsteps** per CLAUDE.md "After Completing Work": mark a Trino-compat testing item done and point NEXT at expanding the allow-list.

- [ ] **Step 4: Commit**

```bash
git add docs/internal/process/tempto-iceberg-compat.md .gitignore README.md nextsteps.md
git commit -m "docs(tempto): usage + coverage notes; update roadmap"
```

---

## Self-Review

**Spec coverage:** upstream subset (Task 5 allow-list) ✓; pinned trino 465 published jar + tempto runner (Tasks 1,4) ✓; test-stack + Basic-over-TLS (Tasks 2,3,4) ✓; curated allow-list + tracked exclusions (Tasks 5,6) ✓; local script + docs, no CI (Tasks 7,8) ✓. Coverage-reality (Spark-heavy upstream) handled by exclusions ✓. Open-risk spike is Task 1 (runner CLI) and Task 4 Step 5 (live auth) ✓.

**Placeholder scan:** the only deliberately deferred content is the per-failure exclusion rows (Task 6 Step 1) and allow-list triage (Task 5 Step 3) -- both are filled from live run output, not guessable ahead of time; every file/command/config is concrete.

**Type/name consistency:** catalog name `iceberg`, warehouse `iceberg`, bucket `warehouse-iceberg`, services `sqe`/`tls-proxy`/`tempto-runner`, ports 8080/8443 (host 28080/28443), runner mainClass `io.trino.tests.product.TemptoProductTestRunner`, gradle volume `sqe-tempto-gradle`, config path `/work/tempto-configuration.yaml` -- consistent across all tasks.
