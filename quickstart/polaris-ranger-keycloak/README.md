# Polaris + Apache Ranger + Keycloak

A second access-control option for Apache Polaris. SQE translates `GRANT` /
`REVOKE` / `SHOW GRANTS` into Apache Ranger Admin REST calls (the `ranger`
access-control backend). Polaris 1.5's embedded Ranger authorizer enforces those
policies when SQE asks Polaris for table access on behalf of a user.

## Run it

```bash
cp .env.example .env
docker compose up -d --build --wait   # Ranger Admin first-boot takes 2-4 min
./test.sh
```

Or `./run.sh` (does both). Tear down with `./run.sh --down`.

## What it proves

`test.sh` runs the full matrix (all green from a clean bring-up):

- Two catalogs (`sales_wh`, `ops_wh`); 3-part names route to the right one via
  `[query] catalog_discovery = "polaris-auto"`.
- A `GRANT SELECT` visibly enabling a read that was denied before it.
- Role grants (`analyst`, `engineer`) and a user grant (`bob`).
- A Ranger DENY added to the same policy, overriding an allow (deny precedence).
- Negative tests: an ungranted user and a read-only role are denied.
- `SHOW GRANTS` round-trip, a `REVOKE` that takes effect, and `CHECK ACCESS`.

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
