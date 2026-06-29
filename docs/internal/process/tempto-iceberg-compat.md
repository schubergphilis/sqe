# Tempto Iceberg compatibility testing

Run the official upstream `trino-product-tests` Iceberg suite (via the
`trinodb/tempto` framework) against SQE's Trino HTTP endpoint, to measure how
Trino-compatible SQE is when querying Iceberg tables.

## One command

```bash
scripts/tempto-test.sh              # run the curated allow-list against SQE
scripts/tempto-test.sh --baseline   # run the same allow-list against real Trino
scripts/tempto-test.sh --no-build   # skip the SQE image rebuild
```

Host requirement: Docker only. Gradle, the JDK, Caddy, and the
`trino-product-tests` jar all run in containers. Reports land in
`testing/tempto/reports/` (gitignored).

## How it fits together

```
docker-compose.test.yml + docker-compose.compare.yml   (existing parity stack)
  polaris + rustfs + sqe (:28080) + trino (:38080)
  + docker-compose.tempto.yml:
      tls-proxy     caddy, internal RSA cert, https://tls-proxy:8443 -> sqe:8080
      tempto-runner gradle:8.10.2-jdk23 running io.trino:trino-product-tests:465
                    via io.trino.tests.product.TemptoProductTestRunner
```

Data flow: the tempto-runner uses the Trino JDBC driver -> Caddy terminates TLS
-> SQE on `:8080` -> Polaris REST catalog + S3 (rustfs). SQE and the real Trino
both expose catalog `iceberg`, which the upstream tests hardcode.

## Why each piece exists (the non-obvious bits)

- **No Trino source build.** `io.trino:trino-product-tests:465` is published to
  Maven Central, so a small Gradle project (`testing/tempto/`) pulls it plus
  tempto-core and trino-jdbc. The Confluent Maven repo is added because the jar
  references Confluent-hosted Kafka artifacts (unused by Iceberg, but they must
  resolve for the classpath).
- **Catalog is `iceberg` for free.** SQE registers the single `[catalog]` block
  under `Config::LEGACY_CATALOG_NAME = "iceberg"`, so no warehouse rename or new
  Polaris catalog is needed; the stack reuses `test_warehouse` and
  `scripts/bootstrap-test.sh`.
- **Single-node SQE config.** The compare stack mounts the distributed
  `coordinator.toml` (worker URLs); `tests/tempto/coordinator-tempto.toml`
  remounts a single-node config so SQE serves tempto without workers.
- **TLS proxy.** The Trino JDBC driver refuses Basic password auth over plain
  HTTP, and SQE has no native TLS listener, so Caddy terminates TLS in front.
  Two gotchas, both encoded in `testing/tempto/Caddyfile`:
  - `default_sni tls-proxy` -- the JVM omits the TLS SNI extension for the
    single-label host `tls-proxy`, so Caddy needs a default cert to serve.
  - `key_type rsa2048` + `protocols h1 h2` -- broadly compatible with the JVM
    TLS stack.
- **LDAP stub in the tempto config.** The product-tests jar registers an LDAP
  `SuiteModuleProvider` unconditionally; the Guice injector will not build
  unless `ldap.admin.dn` / `ldap.admin.password` / `ldap.url` exist. The values
  are never used (no LDAP test is on the allow-list).
- **Gradle heap.** `testing/tempto/gradle.properties` raises the heap; the
  dependency graph OOMs the default.

## Current status

The harness is verified end to end. On 2026-06-29 the 3-test curated allow-list
ran **3 SUCCEEDED / 0 FAILED against the real Trino baseline** (Trino 481) and
**0 / 3 against SQE**, every SQE failure the same response-shape bug: SQE emits
`data: []` for column-less DDL/update statements, which the Trino 465 JDBC
client rejects. The harness produces real passes; SQE fails solely on that bug.
See `testing/tempto/exclusions.md` "Headline finding" for the root cause, a
no-tempto reproduction, and the suggested SQE-side fix.

Note: the baseline used the locally-available `trinodb/trino:481` image via a
one-off image override; `docker-compose.compare.yml` pins `465`, which
`scripts/tempto-test.sh --baseline` will pull on first use.

## Adding a test to the allow-list

1. Pick a pure-`onTrino()` Iceberg test (no `onSpark()`/`onHive()`/`HdfsClient`)
   from `io.trino.tests.product.iceberg` at trino tag 465.
2. Add its fully-qualified class (or `Class.method`) to
   `testing/tempto/allowlist.txt`.
3. Run `scripts/tempto-test.sh`. If it fails for a genuine SQE gap (not a
   harness issue), move it to `testing/tempto/exclusions.md` with the reason.

## Version pins

- `io.trino:trino-product-tests` and the Trino server image: **465** (matches
  `docker-compose.compare.yml`).
- Runner main class: `io.trino.tests.product.TemptoProductTestRunner`.
- Runner image: `gradle:8.10.2-jdk23`.
