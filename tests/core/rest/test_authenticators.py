"""Tests for authentication helpers."""

from __future__ import annotations

import jwt
import pytest
import requests_mock
from freezegun import freeze_time
from cryptography.hazmat.primitives.asymmetric.rsa import (
    RSAPrivateKey,
    RSAPublicKey,
    generate_private_key,
)
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
    PublicFormat,
)

from hotglue_singer_sdk.authenticators import OAuthAuthenticator, OAuthJWTAuthenticator
from hotglue_singer_sdk.streams import RESTStream
from hotglue_singer_sdk.tap_base import Tap


@pytest.mark.parametrize(
    "stream_name,other_stream_name,auth_reused",
    [
        (
            "some_stream",
            "some_stream",
            False,
        ),
        (
            "some_stream",
            "other_stream",
            False,
        ),
        (
            "single_auth_stream",
            "single_auth_stream",
            True,
        ),
        (
            "single_auth_stream",
            "reused_single_auth_stream",
            True,
        ),
        (
            "cached_auth_stream",
            "cached_auth_stream",
            True,
        ),
        (
            "cached_auth_stream",
            "other_cached_auth_stream",
            False,
        ),
    ],
    ids=[
        "naive-auth-not-reused-between-requests",
        "naive-auth-not-reused-between-streams",
        "singleton-auth-reused-between-requests",
        "singleton-auth-reused-between-streams",
        "cached-auth-reused-between-requests",
        "cached-auth-not-reused-between-streams",
    ],
)
def test_authenticator_is_reused(
    rest_tap: Tap, stream_name: str, other_stream_name: str, auth_reused: bool
):
    """Validate that the stream's authenticator is a singleton."""
    stream: RESTStream = rest_tap.streams[stream_name]
    other_stream: RESTStream = rest_tap.streams[other_stream_name]

    assert (stream.authenticator is other_stream.authenticator) is auth_reused


class _FakeOAuthAuthenticator(OAuthAuthenticator):
    def oauth_request_body(self) -> dict:
        return {}


@pytest.mark.parametrize(
    "oauth_response_expires_in,default_expiration,expected_expires_in",
    [
        (123, None, 123),
        (123, 234, 123),
        (None, 234, 234),
        (None, None, None),  # implementation does None + timestamp -> TypeError
    ],
    ids=[
        "expires-in-and-no-default-expiration",
        "expires-in-and-default-expiration",
        "no-expires-in-and-default-expiration",
        "no-expires-in-and-no-default-expiration",
    ],
)
def test_oauth_authenticator_token_expiry_handling(
    rest_tap: Tap,
    requests_mock: requests_mock.Mocker,
    oauth_response_expires_in: int | None,
    default_expiration: int | None,
    expected_expires_in: int | None,
):
    """Validate various combinations of expires_in and default_expiration.

    update_access_token() stores expires_in as absolute Unix timestamp:
    request_time.timestamp() + (response expires_in or default_expiration).
    When both are None it raises TypeError (None + int).
    """
    response = {"access_token": "an-access-token"}
    if oauth_response_expires_in is not None:
        response["expires_in"] = oauth_response_expires_in

    requests_mock.post("https://example.com/oauth", json=response)

    authenticator = _FakeOAuthAuthenticator(
        stream=rest_tap.streams["some_stream"],
        auth_endpoint="https://example.com/oauth",
        default_expiration=default_expiration,
    )

    # Freeze time so stored expiry is deterministic: request_time.timestamp() == 1000
    with freeze_time("1970-01-01 00:16:40"):
        if expected_expires_in is None:
            with pytest.raises(TypeError, match="unsupported operand"):
                authenticator.update_access_token()
            return
        authenticator.update_access_token()

    # SDK stores absolute timestamp: 1000 + (response expires_in or default)
    relative = oauth_response_expires_in if oauth_response_expires_in is not None else default_expiration
    assert authenticator.expires_in == 1000 + relative


@pytest.fixture
def private_key() -> RSAPrivateKey:
    return generate_private_key(public_exponent=65537, key_size=4096)


@pytest.fixture
def public_key(private_key: RSAPrivateKey) -> RSAPublicKey:
    return private_key.public_key()


@pytest.fixture
def private_key_string(private_key: RSAPrivateKey) -> str:
    return private_key.private_bytes(
        Encoding.PEM,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=NoEncryption(),
    ).decode("utf-8")


@pytest.fixture
def public_key_string(public_key: RSAPublicKey) -> str:
    return public_key.public_bytes(
        Encoding.PEM,
        format=PublicFormat.PKCS1,
    ).decode("utf-8")


def test_oauth_jwt_authenticator_payload(
    rest_tap: Tap,
    private_key_string: str,
    public_key_string: str,
):
    class _FakeOAuthJWTAuthenticator(OAuthJWTAuthenticator):
        private_key = private_key_string
        oauth_request_body = {"some": "payload"}

    authenticator = _FakeOAuthJWTAuthenticator(stream=rest_tap.streams["some_stream"])

    body = authenticator.oauth_request_body
    payload = authenticator.oauth_request_payload
    token = payload["assertion"]

    assert jwt.decode(token, public_key_string, algorithms=["RS256"]) == body


def test_oauth_authenticator_hg_access_token_refresh(
    rest_tap: Tap,
    requests_mock: requests_mock.Mocker,
    monkeypatch: pytest.MonkeyPatch,
):
    rest_tap._config["_refresh_token_via_hg_api"] = True
    rest_tap._config["refresh_token"] = "keep-me"

    monkeypatch.setenv("API_URL", "https://api.hotglue.com")
    monkeypatch.setenv("ENV_ID", "env-1")
    monkeypatch.setenv("FLOW", "flow-1")
    monkeypatch.setenv("TENANT", "tenant-1")
    monkeypatch.setenv("TAP", "tap-salesforce")
    monkeypatch.setenv("API_KEY", "secret-key")

    requests_mock.get(
        "https://api.hotglue.com/env-1/flow-1/tenant-1/connectors/tap-salesforce/accesstoken",
        json={
            "success": True,
            "access_token": "hg-token",
            "expires_in": 3600,
        },
    )

    authenticator = _FakeOAuthAuthenticator(
        stream=rest_tap.streams["some_stream"],
        auth_endpoint="https://example.com/oauth",
    )

    with freeze_time("1970-01-01 00:16:40"):
        authenticator.update_access_token()

    assert rest_tap.config["access_token"] == "hg-token"
    assert rest_tap.config["expires_in"] == 1000 + 3600
    assert rest_tap.config["refresh_token"] == "keep-me"
    assert requests_mock.last_request.qs == {"include_properties": ["expires_in"]}
    assert requests_mock.last_request.headers["x-api-key"] == "secret-key"


def test_oauth_authenticator_hg_refresh_requires_env(
    rest_tap: Tap,
    monkeypatch: pytest.MonkeyPatch,
):
    rest_tap._config["_refresh_token_via_hg_api"] = True

    for env_var in ["API_URL", "ENV_ID", "FLOW", "TENANT", "TAP", "API_KEY"]:
        monkeypatch.delenv(env_var, raising=False)

    authenticator = _FakeOAuthAuthenticator(
        stream=rest_tap.streams["some_stream"],
        auth_endpoint="https://example.com/oauth",
    )

    with pytest.raises(RuntimeError, match="Missing required env vars"):
        authenticator.update_access_token()


def test_oauth_authenticator_local_refresh_when_hg_flag_not_true(
    rest_tap: Tap,
    requests_mock: requests_mock.Mocker,
    monkeypatch: pytest.MonkeyPatch,
):
    rest_tap._config["_refresh_token_via_hg_api"] = False

    monkeypatch.setenv("API_URL", "https://api.hotglue.com")
    monkeypatch.setenv("ENV_ID", "env-1")
    monkeypatch.setenv("FLOW", "flow-1")
    monkeypatch.setenv("TENANT", "tenant-1")
    monkeypatch.setenv("TAP", "tap-salesforce")
    monkeypatch.setenv("API_KEY", "secret-key")

    requests_mock.post(
        "https://example.com/oauth",
        json={"access_token": "local-token", "expires_in": 123},
    )
    hg_request = requests_mock.get(
        "https://api.hotglue.com/env-1/flow-1/tenant-1/connectors/tap-salesforce/accesstoken",
        json={"success": True, "access_token": "should-not-be-used", "expires_in": 9999999999},
    )
    local_request = requests_mock.post(
        "https://example.com/oauth",
        json={"access_token": "local-token", "expires_in": 123},
    )

    authenticator = _FakeOAuthAuthenticator(
        stream=rest_tap.streams["some_stream"],
        auth_endpoint="https://example.com/oauth",
    )
    authenticator.update_access_token()

    assert rest_tap.config["access_token"] == "local-token"
    assert isinstance(rest_tap.config["expires_in"], int)
    assert hg_request.call_count == 0
    assert local_request.call_count == 1
