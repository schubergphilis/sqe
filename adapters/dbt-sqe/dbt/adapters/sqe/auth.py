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

from typing import Optional


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
