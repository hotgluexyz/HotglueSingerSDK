"""Tests for target_sdk OAuth auth and Hotglue API refresh."""

from __future__ import annotations

import json
import logging
import os
import tempfile
from typing import Any, Dict
from unittest.mock import MagicMock, patch

import pytest
from freezegun import freeze_time
from hotglue_etl_exceptions import InvalidCredentialsError
from requests import HTTPError

from hotglue_singer_sdk.target_sdk.auth import OAuthAuthenticator


class _FakeTarget:
    """Minimal target-like object for testing auth."""

    name = "fake_target"
    _config: Dict[str, Any]
    _config_file_path: str | None
    logger: logging.Logger

    def __init__(self, config: Dict[str, Any], config_file_path: str | None = None):
        self._config = dict(config)
        self._config_file_path = config_file_path
        self.logger = logging.getLogger("test")

    def confirm_fetch_access_token_support(self) -> bool:
        return True


@pytest.fixture
def target_config():
    return {
        "client_id": "client-1",
        "client_secret": "secret-1",
        "refresh_token": "ref-1",
        "access_token": "old-token",
        "expires_in": 1,
    }


@pytest.fixture
def target_with_config_file(target_config):
    """Target with a real config file path so we can assert file writes."""
    fd, path = tempfile.mkstemp(suffix=".json")
    os.close(fd)
    try:
        with open(path, "w") as f:
            json.dump(target_config, f, indent=4)
        t = _FakeTarget(config=target_config, config_file_path=path)
        yield t
    finally:
        if os.path.exists(path):
            os.remove(path)


@freeze_time("1970-01-01 00:16:40")
def test_target_oauth_hg_api_refresh_success(
    target_with_config_file,
    monkeypatch,
):
    """When _refresh_token_via_hg_api is True, target fetches from Hotglue API."""
    t = target_with_config_file
    t._config["_refresh_token_via_hg_api"] = True

    monkeypatch.setenv("API_URL", "https://api.hotglue.com")
    monkeypatch.setenv("ENV_ID", "env-1")
    monkeypatch.setenv("FLOW", "flow-1")
    monkeypatch.setenv("TENANT", "tenant-1")
    monkeypatch.setenv("TARGET", "target-snowflake")
    monkeypatch.setenv("API_KEY", "secret-key")

    # API returns relative expires_in (seconds from now); code stores now + expires_in
    expires_in = 222333444
    token_json = {
        "success": True,
        "access_token": "hg-target-token",
        "expires_in": expires_in,
    }

    with patch(
        "hotglue_singer_sdk.target_sdk.auth.fetch_access_token_from_hotglue_api"
    ) as mfetch:
        mfetch.return_value = token_json
        auth = OAuthAuthenticator(t, auth_endpoint="https://oauth.example.com/token")
        auth.update_access_token()

    assert t._config["access_token"] == "hg-target-token"

    assert t._config["expires_in"] == expires_in
    mfetch.assert_called_once_with("target-snowflake")



def test_target_oauth_hg_api_refresh_requires_env(
    target_config,
    monkeypatch,
):
    """With _refresh_token_via_hg_api True and env missing, raises RuntimeError."""
    for env_var in ["API_URL", "ENV_ID", "FLOW", "TENANT", "TARGET", "API_KEY"]:
        monkeypatch.delenv(env_var, raising=False)

    t = _FakeTarget(config={**target_config, "_refresh_token_via_hg_api": True})
    auth = OAuthAuthenticator(t, auth_endpoint="https://oauth.example.com/token")

    with pytest.raises(RuntimeError, match="Missing required env vars"):
        auth._update_access_token_via_hg_api()


def test_target_oauth_does_not_fall_back_on_other_hg_refresh_errors(
    target_config,
    monkeypatch,
):
    """Unknown HG failures must not fall back to local refresh."""
    t = _FakeTarget(config={**target_config, "_refresh_token_via_hg_api": True})
    auth = OAuthAuthenticator(t, auth_endpoint="https://oauth.example.com/token")

    def fail_hg_refresh() -> None:
        raise RuntimeError("NOT_ALLOWLISTED: Hotglue access token refresh was not successful: boom")

    monkeypatch.setattr(auth, "_update_access_token_via_hg_api", fail_hg_refresh)

    with patch("hotglue_singer_sdk.target_sdk.auth.requests.post") as mpost:
        with pytest.raises(RuntimeError, match="not successful"):
            auth.update_access_token()

    assert mpost.call_count == 0


def test_target_oauth_falls_back_to_local_when_hg_unsupported(
    target_config,
    monkeypatch,
    caplog,
):
    """Allowlisted HG failures fall back to local refresh."""
    t = _FakeTarget(config={**target_config, "_refresh_token_via_hg_api": True})
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "access_token": "local-token",
        "expires_in": 3600,
    }
    mock_response.raise_for_status = MagicMock()

    with patch(
        "hotglue_singer_sdk.target_sdk.auth.requests.post", return_value=mock_response
    ) as mpost:
        auth = OAuthAuthenticator(t, auth_endpoint="https://oauth.example.com/token")

        def fail_hg_refresh() -> None:
            raise RuntimeError("ALLOWLISTED: Missing required env vars for Hotglue access token refresh: ENV_ID, FLOW")

        monkeypatch.setattr(auth, "_update_access_token_via_hg_api", fail_hg_refresh)
        with caplog.at_level(logging.WARNING):
            auth.update_access_token()

    assert t._config["access_token"] == "local-token"
    assert mpost.call_count == 1
    assert "Failed to update access token via Hotglue API" in caplog.text
    assert "Falling back to local refresh" in caplog.text


def test_target_oauth_tries_hg_without_capability_check(target_config, monkeypatch):
    """Targets without access_token_support still attempt the Hotglue API."""
    t = _FakeTarget(config={**target_config, "_refresh_token_via_hg_api": True})
    t.confirm_fetch_access_token_support = lambda: False

    monkeypatch.setenv("API_URL", "https://api.hotglue.com")
    monkeypatch.setenv("ENV_ID", "env-1")
    monkeypatch.setenv("FLOW", "flow-1")
    monkeypatch.setenv("TENANT", "tenant-1")
    monkeypatch.setenv("TARGET", "salesforce-v3")
    monkeypatch.setenv("API_KEY", "secret-key")

    with patch(
        "hotglue_singer_sdk.target_sdk.auth.fetch_access_token_from_hotglue_api",
        return_value={"access_token": "hg-token", "expires_in": 3600},
    ) as mfetch:
        auth = OAuthAuthenticator(t, auth_endpoint="https://oauth.example.com/token")
        auth.update_access_token()

    assert t._config["access_token"] == "hg-token"
    mfetch.assert_called_once_with("salesforce-v3")


@freeze_time("1970-01-01 00:16:40")
def test_target_oauth_local_refresh_when_hg_flag_false(
    target_with_config_file,
    monkeypatch,
):
    """When _refresh_token_via_hg_api is False, target uses local OAuth refresh."""
    t = target_with_config_file
    t._config["_refresh_token_via_hg_api"] = False

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "access_token": "local-token",
        "refresh_token": "local-refresh",
        "expires_in": 3600,
    }
    mock_response.raise_for_status = MagicMock()

    with patch("hotglue_singer_sdk.target_sdk.auth.requests.post") as mpost:
        mpost.return_value = mock_response
        with patch(
            "hotglue_singer_sdk.target_sdk.auth.fetch_access_token_from_hotglue_api"
        ) as mfetch:
            auth = OAuthAuthenticator(
                t, auth_endpoint="https://oauth.example.com/token"
            )
            auth.update_access_token()

    assert t._config["access_token"] == "local-token"
    assert t._config["refresh_token"] == "local-refresh"
    assert t._config["expires_in"] == 1000 + 3600
    assert mfetch.call_count == 0
    assert mpost.call_count == 1


@freeze_time("1970-01-01 00:16:40")
@pytest.mark.parametrize(
    "oauth_response_expires_in,default_expiration,expected_relative",
    [
        (123, None, 123),
        (123, 234, 123),
        (None, 234, 234),
        (None, None, None),
    ],
    ids=[
        "expires-in-and-no-default-expiration",
        "expires-in-and-default-expiration",
        "no-expires-in-and-default-expiration",
        "no-expires-in-and-no-default-expiration",
    ],
)
def test_target_oauth_local_refresh_expires_in_handling(
    target_config,
    oauth_response_expires_in,
    default_expiration,
    expected_relative,
):
    t = _FakeTarget(config={**target_config, "_refresh_token_via_hg_api": False})
    response = {"access_token": "local-token"}
    if oauth_response_expires_in is not None:
        response["expires_in"] = oauth_response_expires_in

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = response
    mock_response.raise_for_status = MagicMock()

    with patch("hotglue_singer_sdk.target_sdk.auth.requests.post", return_value=mock_response):
        auth = OAuthAuthenticator(
            t,
            auth_endpoint="https://oauth.example.com/token",
            default_expiration=default_expiration,
        )
        auth._update_access_token_locally()

    if expected_relative is None:
        assert t._config["expires_in"] is None
    else:
        assert t._config["expires_in"] == 1000 + expected_relative
    assert t._config["refresh_token"] == "ref-1"


def test_target_oauth_local_refresh_keeps_refresh_token_when_not_rotated(target_config):
    t = _FakeTarget(config={**target_config, "_refresh_token_via_hg_api": False})
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "access_token": "local-token",
        "expires_in": 3600,
    }
    mock_response.raise_for_status = MagicMock()

    with patch("hotglue_singer_sdk.target_sdk.auth.requests.post", return_value=mock_response):
        auth = OAuthAuthenticator(t, auth_endpoint="https://oauth.example.com/token")
        auth._update_access_token_locally()

    assert t._config["access_token"] == "local-token"
    assert t._config["refresh_token"] == "ref-1"


def test_target_oauth_local_refresh_raises_invalid_credentials(target_config):
    t = _FakeTarget(config={**target_config, "_refresh_token_via_hg_api": False})
    mock_response = MagicMock()
    mock_response.status_code = 400
    mock_response.text = "invalid_grant"
    mock_response.json.return_value = {"error": "invalid_grant"}
    mock_response.raise_for_status.side_effect = HTTPError("400 Client Error")

    with patch("hotglue_singer_sdk.target_sdk.auth.requests.post", return_value=mock_response):
        auth = OAuthAuthenticator(t, auth_endpoint="https://oauth.example.com/token")
        with pytest.raises(InvalidCredentialsError):
            auth._update_access_token_locally()

    assert auth.state["auth_error_response"] == {"error": "invalid_grant"}


def test_target_oauth_local_refresh_skips_config_write_without_path(target_config):
    t = _FakeTarget(config={**target_config, "_refresh_token_via_hg_api": False})
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "access_token": "local-token",
        "expires_in": 3600,
    }
    mock_response.raise_for_status = MagicMock()

    with patch("hotglue_singer_sdk.target_sdk.auth.requests.post", return_value=mock_response):
        auth = OAuthAuthenticator(t, auth_endpoint="https://oauth.example.com/token")
        auth._update_access_token_locally()

    assert t._config["access_token"] == "local-token"


def test_is_token_valid_false_until_refreshed(target_config):
    auth = OAuthAuthenticator(
        _FakeTarget(target_config),
        auth_endpoint="https://oauth.example.com/token",
    )
    assert auth.is_token_valid() is False


@freeze_time("1970-01-01 00:16:40")
def test_is_token_valid_true_without_expires_in_after_refresh(target_config):
    t = _FakeTarget(config={**target_config, "_refresh_token_via_hg_api": False})
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"access_token": "local-token"}
    mock_response.raise_for_status = MagicMock()

    with patch("hotglue_singer_sdk.target_sdk.auth.requests.post", return_value=mock_response):
        auth = OAuthAuthenticator(t, auth_endpoint="https://oauth.example.com/token")
        auth._update_access_token_locally()

    assert auth.last_refreshed is not None
    assert auth.expires_in is None
    assert auth.is_token_valid() is True


@freeze_time("1970-01-01 00:16:40")
def test_is_token_valid_respects_expires_in_buffer(target_config):
    t = _FakeTarget(config={**target_config, "_refresh_token_via_hg_api": False})
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"access_token": "local-token", "expires_in": 60}
    mock_response.raise_for_status = MagicMock()

    with patch("hotglue_singer_sdk.target_sdk.auth.requests.post", return_value=mock_response):
        auth = OAuthAuthenticator(t, auth_endpoint="https://oauth.example.com/token")
        auth._update_access_token_locally()

    # expires at 1060; now is 1000; remaining 60 < 120 buffer
    assert auth.is_token_valid() is False
