**STATUS 2026-11-20: Polaris 1.9.0 in the test stack and the quickstarts (`chore/polaris-1.9`).**

Polaris was held on 1.7.0 because the embedded Ranger authorizer the
access-control stack depends on did not work on 1.8. Polaris 1.9.0 (2026-11-14)
lifts that hold. `docker-compose.test.yml` and the five quickstart compose files
move to `apache/polaris:1.9.0`; the quickstart READMEs and the book's version
table follow. Renovate's !1261 (1.7.0 -> 1.8.0, plus Spark 4.2.0 and a
cargo-chef digest) is superseded for Polaris; Renovate rebases it for the rest.

The quickstarts keep their own `ranger/servicedef-polaris.json`. Polaris 1.9
ships `polaris-ranger-servicedef.json` with the same access types and resources
but with impliedGrants added: every table write type implies
`table-properties-read`, and `table-data-write` implies `table-properties-write`.
The shared grant profile (`crates/sqe-policy/assets/grant-profile.json`)
deliberately does not confer `table-properties-write` with data writes, and
Ranger uses whatever definition was registered, so re-importing upstream's file
would widen grants. That is a separate decision. If 1.9 asks Ranger for
`table-properties-read` on a commit, the access-control suite shows it as a
denied write.

**NEXT:** run `make test-access-control` and `make test-integration` against
1.9.0, then add polaris and polaris-admin-tool 1.9.0 to the docker-tools
`3rdparty/` mirror (keep 1.7.0 until no compose file references it).
