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


def test_tls_is_default_and_plaintext_is_opt_out():
    assert auth.flight_location("h", 50051) == ("grpc+tls://h:50051", {})
    assert auth.flight_location("h", 50051, tls=False) == ("grpc://h:50051", {})


def test_tls_root_certs_file_is_passed_as_pem():
    import tempfile

    with tempfile.NamedTemporaryFile("w", suffix=".pem", delete=False) as f:
        f.write("-----BEGIN CERTIFICATE-----\nX\n-----END CERTIFICATE-----\n")
    try:
        uri, kwargs = auth.flight_location("h", 1, tls_root_certs=f.name)
    finally:
        os.unlink(f.name)
    assert uri == "grpc+tls://h:1"
    assert kwargs == {
        "adbc.flight.sql.client_option.tls_root_certs": "-----BEGIN CERTIFICATE-----\nX\n-----END CERTIFICATE-----\n"
    }


def test_tls_root_certs_without_tls_raises():
    try:
        auth.flight_location("h", 1, tls=False, tls_root_certs="/ca.pem")
    except auth.AuthError:
        return
    raise AssertionError("expected AuthError when tls_root_certs is set with tls: false")


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


# ── Extra Flight SQL call headers (#772): e.g. a signed catalog scope (#908) ──

SECRET = "zz-scope-marker-772"  # gitleaks:allow


def _refused(headers):
    try:
        auth.flight_call_headers(headers)
    except auth.AuthError as e:
        return str(e)
    raise AssertionError(f"expected AuthError for {list(headers)}")


def test_call_headers_map_to_adbc_rpc_call_header_options():
    out = auth.flight_call_headers({"X-Portal-Catalog-Scope": SECRET, "traceparent": "00-ab-cd-01"})
    assert out == {
        "adbc.flight.sql.rpc.call_header.x-portal-catalog-scope": SECRET,
        "adbc.flight.sql.rpc.call_header.traceparent": "00-ab-cd-01",
    }


def test_no_call_headers_is_nothing():
    assert auth.flight_call_headers(None) == {}
    assert auth.flight_call_headers({}) == {}


def test_an_empty_value_sends_nothing():
    # A profile may point a header at an env var that is not always set.
    assert auth.flight_call_headers({"x-portal-catalog-scope": "", "x-other": None}) == {}


def test_call_headers_cannot_replace_the_credentials_or_grpc_headers():
    for name in ("authorization", "Authorization", "grpc-timeout", "content-type", "te", "user-agent", "host",
                 "x-scope-bin", "", "x scope", "x:scope", "-x", "x\nscope"):
        assert "may set" in _refused({name: "v"}), name


def test_a_header_set_twice_by_case_is_refused():
    assert "set twice" in _refused({"x-scope": "a", "X-Scope": "b"})


def test_a_value_that_is_not_printable_ascii_is_refused_without_echoing_it():
    for value in (SECRET + "\r\nx-injected: 1", SECRET + "é", 42):
        msg = _refused({"x-portal-catalog-scope": value})
        assert "x-portal-catalog-scope" in msg
        assert SECRET not in msg and "injected" not in msg


def test_call_headers_do_not_touch_auth_kwargs():
    merged = auth.flight_db_kwargs(token="t") | auth.flight_call_headers({"x-portal-catalog-scope": SECRET})
    assert merged["adbc.flight.sql.authorization_header"] == "Bearer t"


def test_a_header_set_twice_is_refused_even_when_the_first_is_empty():
    assert "set twice" in _refused({"x-scope": "", "X-Scope": "b"})
