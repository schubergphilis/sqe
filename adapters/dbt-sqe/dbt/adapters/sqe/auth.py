"""Map dbt-sqe credentials to ADBC Flight SQL connection options.

Kept free of dbt imports so the mapping can be unit-tested without the dbt
runtime installed. The three auth styles all resolve to ADBC `db_kwargs`:

- OAuth client_credentials (service principal): `client_id` / `client_secret`
  are sent as the Flight SQL Basic-auth username/password. SQE's
  `client_credentials_passthrough` provider runs the grant per connection and
  forwards the token to the catalog. No token is fetched in the adapter.
- Pre-obtained bearer token: set the Flight SQL authorization header to
  `Bearer <token>`; SQE's `bearer_token` provider validates and forwards it.
- Basic auth (human user): `user` / `password` as before.
"""

from typing import Optional, Tuple


class AuthError(ValueError):
    """Raised when an auth profile is internally inconsistent."""


def flight_db_kwargs(
    *,
    user: Optional[str] = None,
    password: Optional[str] = None,
    method: Optional[str] = None,
    client_id: Optional[str] = None,
    client_secret: Optional[str] = None,
    token: Optional[str] = None,
) -> dict:
    """Return the ADBC Flight SQL `db_kwargs` for the given credentials.

    Precedence: an explicit ``token`` wins, then OAuth (``method: oauth`` or a
    ``client_id``), then plain Basic auth, then anonymous (empty dict).
    """
    if token:
        return {"adbc.flight.sql.authorization_header": f"Bearer {token}"}

    normalized = (method or "").strip().lower()
    if normalized == "oauth" or client_id:
        if not client_id:
            raise AuthError(
                "dbt-sqe: method 'oauth' requires 'client_id' (and 'client_secret')"
            )
        # Service-principal client_id/secret travel as Flight Basic auth; the SQE
        # server turns them into a client_credentials grant.
        return {"username": client_id, "password": client_secret or ""}

    if user:
        return {"username": user, "password": password or ""}

    return {}


def flight_location(
    host: str,
    port: int,
    *,
    tls: bool = True,
    tls_root_certs: Optional[str] = None,
) -> Tuple[str, dict]:
    """Return the Flight SQL URI and any TLS `db_kwargs`.

    TLS is on by default; credentials and bearer tokens travel in every call,
    so plaintext (`tls: false`) is an explicit opt-out for local stacks.
    ``tls_root_certs`` is a path to a PEM bundle; ADBC takes the PEM contents.
    """
    if not tls:
        if tls_root_certs:
            raise AuthError("dbt-sqe: 'tls_root_certs' requires 'tls: true'")
        return f"grpc://{host}:{port}", {}

    db_kwargs = {}
    if tls_root_certs:
        with open(tls_root_certs, encoding="utf-8") as f:
            db_kwargs["adbc.flight.sql.client_option.tls_root_certs"] = f.read()
    return f"grpc+tls://{host}:{port}", db_kwargs
