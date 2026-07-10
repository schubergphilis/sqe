"""Unit tests for the dbt-sqe auth -> ADBC db_kwargs mapping.

Deliberately imports only `dbt.adapters.sqe.auth` (no dbt runtime), so it runs
with plain `python -m pytest` or `python tests/test_auth.py`.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dbt", "adapters", "sqe"))

import auth  # noqa: E402  (dbt-free module)


def test_basic_auth_maps_user_password():
    assert auth.flight_db_kwargs(user="alice", password="pw") == {
        "username": "alice",
        "password": "pw",
    }


def test_oauth_method_maps_client_id_secret_to_basic():
    out = auth.flight_db_kwargs(
        method="oauth", client_id="sp-reader", client_secret="s3cret"
    )
    assert out == {"username": "sp-reader", "password": "s3cret"}


def test_client_id_without_method_is_treated_as_oauth():
    out = auth.flight_db_kwargs(client_id="sp-reader", client_secret="s3cret")
    assert out == {"username": "sp-reader", "password": "s3cret"}


def test_oauth_missing_client_id_raises():
    try:
        auth.flight_db_kwargs(method="oauth", client_secret="s3cret")
    except auth.AuthError:
        return
    raise AssertionError("expected AuthError when method=oauth but no client_id")


def test_token_takes_precedence_and_sets_bearer_header():
    out = auth.flight_db_kwargs(token="ey.jwt.tok", client_id="ignored", user="ignored")
    assert out == {"adbc.flight.sql.authorization_header": "Bearer ey.jwt.tok"}


def test_anonymous_when_nothing_set():
    assert auth.flight_db_kwargs() == {}


def test_oauth_missing_secret_defaults_empty_password():
    out = auth.flight_db_kwargs(method="oauth", client_id="sp-reader")
    assert out == {"username": "sp-reader", "password": ""}


if __name__ == "__main__":
    import traceback

    failures = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"ok   {name}")
            except Exception:
                failures += 1
                print(f"FAIL {name}")
                traceback.print_exc()
    print(f"\n{('PASS' if failures == 0 else 'FAIL')}: {failures} failure(s)")
    sys.exit(1 if failures else 0)
