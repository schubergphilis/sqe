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

Extra call headers (``call_headers``) become ADBC
``adbc.flight.sql.rpc.call_header.<name>`` options, sent as gRPC metadata on
every Flight SQL call (for example a signed catalog scope, #908).
"""

import re
from typing import Mapping, Optional, Tuple


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


CALL_HEADER_PREFIX = "adbc.flight.sql.rpc.call_header."

# gRPC metadata keys: lowercase letters, digits, '-', '_' and '.'.
_HEADER_NAME = re.compile(r"^[a-z0-9][a-z0-9_.-]*$")
# Headers the adapter or gRPC itself sets: a profile must not replace them.
_RESERVED_HEADERS = {"authorization", "content-type", "te", "user-agent", "host"}
# Printable ASCII, the only values gRPC allows in a non-binary header.
_HEADER_VALUE = re.compile(r"^[\x20-\x7e]*$")


def flight_call_headers(headers: Optional[Mapping[str, str]] = None) -> dict:
    """Return the ADBC `db_kwargs` that send ``headers`` on every Flight SQL call.

    Names are matched case-insensitively and sent lowercase, as gRPC requires.
    A name that is not a valid gRPC metadata key, ends in ``-bin``, starts
    with ``grpc-``, or is one the adapter or gRPC sets (``authorization`` and
    the like) is refused, so a profile cannot replace the credentials. A value
    must be printable ASCII; an empty value sends nothing, so a profile can
    point a header at an environment variable that is not always set. Errors
    name the header, never its value: values are often credentials.
    """
    out: dict = {}
    seen: set = set()
    for raw_name, value in (headers or {}).items():
        name = str(raw_name).strip().lower()
        if (
            not _HEADER_NAME.match(name)
            or name.endswith("-bin")
            or name.startswith("grpc-")
            or name in _RESERVED_HEADERS
        ):
            raise AuthError(f"dbt-sqe: call_headers: '{raw_name}' is not a header the profile may set")
        if name in seen:
            raise AuthError(f"dbt-sqe: call_headers: '{name}' is set twice")
        seen.add(name)
        key = CALL_HEADER_PREFIX + name
        if value is None:
            continue
        if not isinstance(value, str) or not _HEADER_VALUE.match(value):
            raise AuthError(
                f"dbt-sqe: call_headers: the value of '{name}' must be printable ASCII text"
            )
        if value:
            out[key] = value
    return out


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
