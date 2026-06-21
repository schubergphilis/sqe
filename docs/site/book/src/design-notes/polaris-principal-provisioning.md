# Polaris principals + external auth: do you need to pre-create them? (research)

Question investigated: with users managed only in Keycloak and Apache Polaris in
external-auth mode (authorization delegated to Apache Ranger), can Polaris
authenticate a user WITHOUT a Polaris principal entity, i.e. purely federated
from the token? Tested empirically AND confirmed from source.

## Answer (short)

No. Polaris always requires a principal ENTITY in its own metastore to
authenticate, regardless of `authentication.type` (internal / mixed / external).
The token is only a lookup key; the entity must exist. There is no config to
turn this off; only a custom `Authenticator` (code) could.

BUT this is usually invisible: in the quickstarts it "just works" with
Keycloak-only users because the bootstrap script auto-creates a Polaris principal
per Keycloak user. Polaris still needs them; the automation creates them for you.
So the right model is "auto-provision principals transparently," not "no
principals."

## Why it looked like it works without principals

The provisioning is hidden in automation. In this Ranger quickstart,
`quickstart/polaris-ranger-keycloak/polaris/bootstrap-data.sh` does:

```sh
mkprincipal() { api POST "$MGMT/principals" "{\"principal\":{\"name\":\"$1\",\"type\":\"USER\"}}"; }
for u in alice bob carol dave; do mkprincipal "$u"; done
```

and the shared `quickstart/_shared/polaris/bootstrap.sh` creates
`root`/`adminuser`/`testuser` the same way. The names match the Keycloak
`preferred_username`, so when a Keycloak user's token arrives, Polaris resolves
it to the pre-created principal. You only touched Keycloak; the script touched
Polaris. That is why the 13/13 Ranger test passes without anyone creating
principals by hand.

## The empirical test (external mode, no principals)

Config: `polaris.authentication.type=external`, the `mkprincipal` loop removed,
a Keycloak-only user `carol` (federated, no Polaris principal), authorization via
Ranger (carol is a member of the Ranger role `sqe_admin`). Result: 401 on every
call. Raw Polaris log (in-container):

```
DefaultAuthenticator  Resolving principal for credentials:
  PolarisCredential{principalName=carol, principalRoles=[...analyst, engineer, sqe_admin...]}
DefaultAuthenticator  Failed to resolve principal from credentials=...   -> HTTP 401
```

The JWT verified fine and Ranger had carol's roles; the failure is the Polaris
principal LOOKUP (a metastore separate from Ranger). So Keycloak + Ranger alone
is not enough. (Reverted afterward; the committed quickstart stays on `mixed`.)

Also: pure `external` mode disables the internal `root` token, so you cannot
bootstrap the first principal (chicken-and-egg). The quickstart uses `mixed` so
the bootstrap (internal token) can create principals, after which external
Keycloak users authenticate against them.

## Source confirmation (apache-polaris-1.5.0, unchanged on main)

- `runtime/service/.../auth/Authenticator.java` is the interface; the ONLY
  implementation is `DefaultAuthenticator` (`@Identifier("default")`).
- `DefaultAuthenticator.resolvePrincipalEntity()` calls
  `metaStoreManager.findPrincipalById(...)` / `findPrincipalByName(...)`; if null
  it logs "Failed to resolve principal from credentials={}" and throws
  `NotAuthorizedException`. No branch synthesizes or auto-creates a principal.
- Class javadoc: "it does not support federated principals that are not managed
  by Polaris."
- `polaris.authentication.authenticator.type` (`AuthenticationRealmConfiguration`,
  `@WithDefault("default")`) only accepts registered `@Identifier` strings, of
  which only `default` exists. Any other value needs a custom compiled bean.
- The OIDC external path: `OidcPolarisCredentialAugmentor` ->
  `DefaultPrincipalMapper` extracts `principalId`/`principalName` from the claims
  (`polaris.oidc.principal-mapper.{id,name}-claim-path`) and hands a
  `PolarisCredential` to `DefaultAuthenticator`, which still does the metastore
  lookup. OIDC builds the credential; the entity must pre-exist.
- No identity-federation feature flag exists (`polaris.principal.*` /
  `polaris.authentication.federation.*` do not exist; every "federation" key is
  CATALOG federation, unrelated).

## Disposition

- **Provision a Polaris principal per user** (no principal-roles needed: roles
  come via Ranger role membership). It can be fully automated and transparent:
  - quickstart/dev: the bootstrap script (as today).
  - production / data-platform BFF: create the Polaris principal when the BFF
    onboards a user (it already manages users + grants), so the operator manages
    identities only in Keycloak + Ranger.
- **Do not** rely on pure-token federation; it is not supported by config.
- **Only** way to eliminate provisioning entirely: write a custom Polaris
  `Authenticator` bean that auto-creates/synthesizes the principal from the token
  and set `polaris.authentication.authenticator.type` to its identifier. That is
  a code change / custom Polaris build (and an upstream-PR candidate), worth it
  only if removing the provisioning step is a hard requirement.

## Related notes

- Identity model summary: `quickstart/polaris-ranger-keycloak/OVERVIEW.md`.
- Ranger backend design: `docs/superpowers/specs/2026-06-18-ranger-access-control-backend-design.md`.
- BFF OPA->Ranger migration (carries this finding):
  `../data-platform/docs/prompts/opa-to-ranger-migration-prompt.md`.
