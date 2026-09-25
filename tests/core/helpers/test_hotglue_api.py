"""Tests for Hotglue API helper."""

from __future__ import annotations

import json

from unittest.mock import MagicMock, patch
from urllib.parse import urlparse

import pytest
import requests

from hotglue_etl_exceptions import InvalidCredentialsError

from hotglue_singer_sdk.helpers._hotglue_api import fetch_access_token_from_hotglue_api


def _credential_error_env(monkeypatch):
    monkeypatch.setenv("API_URL", "https://api.hotglue.com")
    monkeypatch.setenv("ENV_ID", "e")
    monkeypatch.setenv("FLOW", "f")
    monkeypatch.setenv("TENANT", "t")
    monkeypatch.setenv("API_KEY", "k")


def _error_response(status_code: int, body: dict) -> MagicMock:
    mock_response = MagicMock()
    mock_response.status_code = status_code
    mock_response.text = json.dumps(body)
    mock_response.json.return_value = body
    mock_response.raise_for_status.side_effect = requests.HTTPError(
        str(status_code), response=mock_response
    )
    return mock_response


def test_fetch_access_token_success(monkeypatch):
    """Returns response JSON when API returns success with access_token and expires_in."""
    monkeypatch.setenv("API_URL", "https://api.hotglue.com")
    monkeypatch.setenv("ENV_ID", "env-1")
    monkeypatch.setenv("FLOW", "flow-1")
    monkeypatch.setenv("TENANT", "tenant-1")
    monkeypatch.setenv("API_KEY", "secret-key")

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "success": True,
        "access_token": "tok-123",
        "expires_in": 999888777,
        "refresh_token": "ref-456",
    }
    mock_response.raise_for_status = MagicMock()

    with patch("hotglue_singer_sdk.helpers._hotglue_api.requests.get") as mget:
        mget.return_value = mock_response
        result = fetch_access_token_from_hotglue_api("my-connector")

    assert result["access_token"] == "tok-123"
    assert result["expires_in"] == 999888777
    assert result["refresh_token"] == "ref-456"
    assert result["success"] is True
    mget.assert_called_once()
    call_kw = mget.call_args[1]
    assert call_kw["params"] == {"include_properties": "expires_in"}
    assert call_kw["headers"]["x-api-key"] == "secret-key"
    url = mget.call_args[0][0]
    assert "env-1/flow-1/tenant-1/connectors/my-connector/accesstoken" in url
    assert url.endswith("/accesstoken"), "endpoint path must be /accesstoken not /accesstokens"


def test_fetch_access_token_default_api_url(monkeypatch):
    """Uses https://api.hotglue.com when API_URL is not set."""
    monkeypatch.delenv("API_URL", raising=False)
    monkeypatch.setenv("ENV_ID", "e")
    monkeypatch.setenv("FLOW", "f")
    monkeypatch.setenv("TENANT", "t")
    monkeypatch.setenv("API_KEY", "k")

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"success": True, "access_token": "x", "expires_in": 1}
    mock_response.raise_for_status = MagicMock()

    with patch("hotglue_singer_sdk.helpers._hotglue_api.requests.get") as mget:
        mget.return_value = mock_response
        result = fetch_access_token_from_hotglue_api("conn")
    assert result["access_token"] == "x"
    parsed_url = urlparse(mget.call_args[0][0])
    assert parsed_url.hostname == "api.hotglue.com"


def test_fetch_access_token_missing_connector_id(monkeypatch):
    """Raises RuntimeError when connector_id is empty or None."""
    monkeypatch.setenv("ENV_ID", "e")
    monkeypatch.setenv("FLOW", "f")
    monkeypatch.setenv("TENANT", "t")
    monkeypatch.setenv("API_KEY", "k")

    with pytest.raises(RuntimeError, match="Missing required env vars"):
        fetch_access_token_from_hotglue_api("")

    with pytest.raises(RuntimeError, match="Missing required env vars"):
        fetch_access_token_from_hotglue_api(None)  # type: ignore[arg-type]


def test_fetch_access_token_missing_env_vars(monkeypatch):
    """Raises RuntimeError listing missing env vars when any required env is missing."""
    monkeypatch.delenv("ENV_ID", raising=False)
    monkeypatch.delenv("FLOW", raising=False)
    monkeypatch.delenv("TENANT", raising=False)
    monkeypatch.delenv("API_KEY", raising=False)

    with pytest.raises(RuntimeError, match="Missing required env vars"):
        fetch_access_token_from_hotglue_api("tap-1")

    err = None
    try:
        fetch_access_token_from_hotglue_api("tap-1")
    except RuntimeError as e:
        err = e
    assert err is not None
    assert "ENV_ID" in str(err)
    assert "FLOW" in str(err)
    assert "TENANT" in str(err)
    assert "API_KEY" in str(err)


def test_fetch_access_token_http_error(monkeypatch):
    """Retries transient API errors before raising RuntimeError."""
    monkeypatch.setenv("API_URL", "https://api.hotglue.com")
    monkeypatch.setenv("ENV_ID", "e")
    monkeypatch.setenv("FLOW", "f")
    monkeypatch.setenv("TENANT", "t")
    monkeypatch.setenv("API_KEY", "k")

    mock_response = MagicMock()
    mock_response.status_code = 500
    mock_response.text = "Internal Server Error"

    with patch("hotglue_singer_sdk.helpers._hotglue_api.requests.get") as mget, patch(
        "backoff._sync.time.sleep"
    ):
        mget.return_value = mock_response
        with pytest.raises(RuntimeError, match="Failed Hotglue access token refresh"):
            fetch_access_token_from_hotglue_api("c1")
    assert mget.call_count == 8


def test_fetch_access_token_success_after_retry(monkeypatch):
    """Returns response JSON when transient API error recovers."""
    monkeypatch.setenv("API_URL", "https://api.hotglue.com")
    monkeypatch.setenv("ENV_ID", "e")
    monkeypatch.setenv("FLOW", "f")
    monkeypatch.setenv("TENANT", "t")
    monkeypatch.setenv("API_KEY", "k")

    retry_response = MagicMock()
    retry_response.status_code = 423
    retry_response.text = "Locked"
    success_response = MagicMock()
    success_response.status_code = 200
    success_response.json.return_value = {"success": True, "access_token": "x", "expires_in": 1}
    success_response.raise_for_status = MagicMock()

    with patch("hotglue_singer_sdk.helpers._hotglue_api.requests.get") as mget, patch(
        "backoff._sync.time.sleep"
    ):
        mget.side_effect = [retry_response, success_response]
        result = fetch_access_token_from_hotglue_api("c1")
    assert result["access_token"] == "x"
    assert mget.call_count == 2


def test_fetch_access_token_non_retryable_http_error(monkeypatch):
    """Raises RuntimeError without retrying non-transient API errors."""
    monkeypatch.setenv("API_URL", "https://api.hotglue.com")
    monkeypatch.setenv("ENV_ID", "e")
    monkeypatch.setenv("FLOW", "f")
    monkeypatch.setenv("TENANT", "t")
    monkeypatch.setenv("API_KEY", "k")

    mock_response = MagicMock()
    mock_response.status_code = 401
    mock_response.text = "Unauthorized"
    mock_response.raise_for_status.side_effect = requests.HTTPError(
        "401", response=mock_response
    )

    with patch("hotglue_singer_sdk.helpers._hotglue_api.requests.get") as mget:
        mget.return_value = mock_response
        with pytest.raises(RuntimeError, match="Failed Hotglue access token refresh"):
            fetch_access_token_from_hotglue_api("c1")
    assert mget.call_count == 1


def test_fetch_access_token_success_false(monkeypatch):
    """Raises RuntimeError when response has success is not True."""
    monkeypatch.setenv("API_URL", "https://api.hotglue.com")
    monkeypatch.setenv("ENV_ID", "e")
    monkeypatch.setenv("FLOW", "f")
    monkeypatch.setenv("TENANT", "t")
    monkeypatch.setenv("API_KEY", "k")

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"success": False, "error": "something failed"}
    mock_response.raise_for_status = MagicMock()

    with patch("hotglue_singer_sdk.helpers._hotglue_api.requests.get") as mget:
        mget.return_value = mock_response
        with pytest.raises(RuntimeError, match="not successful"):
            fetch_access_token_from_hotglue_api("c1")


def test_fetch_access_token_missing_access_token(monkeypatch):
    """Raises RuntimeError when response has no access_token."""
    monkeypatch.setenv("API_URL", "https://api.hotglue.com")
    monkeypatch.setenv("ENV_ID", "e")
    monkeypatch.setenv("FLOW", "f")
    monkeypatch.setenv("TENANT", "t")
    monkeypatch.setenv("API_KEY", "k")

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"success": True, "expires_in": 123}
    mock_response.raise_for_status = MagicMock()

    with patch("hotglue_singer_sdk.helpers._hotglue_api.requests.get") as mget:
        mget.return_value = mock_response
        with pytest.raises(RuntimeError, match="did not include access_token"):
            fetch_access_token_from_hotglue_api("c1")


def test_fetch_access_token_missing_expires_in(monkeypatch):
    """Raises RuntimeError when response has no expires_in."""
    monkeypatch.setenv("API_URL", "https://api.hotglue.com")
    monkeypatch.setenv("ENV_ID", "e")
    monkeypatch.setenv("FLOW", "f")
    monkeypatch.setenv("TENANT", "t")
    monkeypatch.setenv("API_KEY", "k")

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"success": True, "access_token": "t"}
    mock_response.raise_for_status = MagicMock()

    with patch("hotglue_singer_sdk.helpers._hotglue_api.requests.get") as mget:
        mget.return_value = mock_response
        with pytest.raises(RuntimeError, match="did not include expires_in"):
            fetch_access_token_from_hotglue_api("c1")


def test_fetch_access_token_invalid_credentials_message(monkeypatch):
    """Raises InvalidCredentialsError with the upstream message, unwrapped."""
    _credential_error_env(monkeypatch)
    upstream = (
        "Failed OAuth login, response was '{\"error\":\"invalid_grant\","
        "\"error_description\":\"expired access/refresh token\"}'. "
        "400 Client Error: Bad Request for url: "
        "https://login.salesforce.com/services/oauth2/token"
    )
    mock_response = _error_response(400, {"Code": "BadRequestError", "Message": upstream})

    with patch("hotglue_singer_sdk.helpers._hotglue_api.requests.get") as mget:
        mget.return_value = mock_response
        with pytest.raises(InvalidCredentialsError) as excinfo:
            fetch_access_token_from_hotglue_api("c1")

    assert str(excinfo.value) == upstream
    assert "Failed Hotglue access token refresh" not in str(excinfo.value)
    assert "api.hotglue.com" not in str(excinfo.value)
    assert mget.call_count == 1


def test_fetch_access_token_invalid_credentials_code(monkeypatch):
    """Raises InvalidCredentialsError when the API reports the error class directly."""
    _credential_error_env(monkeypatch)
    mock_response = _error_response(
        401, {"Code": "InvalidCredentialsError", "Message": "Credentials are no longer valid"}
    )

    with patch("hotglue_singer_sdk.helpers._hotglue_api.requests.get") as mget:
        mget.return_value = mock_response
        with pytest.raises(InvalidCredentialsError, match="Credentials are no longer valid"):
            fetch_access_token_from_hotglue_api("c1")


def test_fetch_access_token_non_credential_client_error_stays_runtime_error(monkeypatch):
    """Keeps RuntimeError for client errors that are not credential failures."""
    _credential_error_env(monkeypatch)
    mock_response = _error_response(
        400,
        {"Code": "BadRequestError", "Message": "Connector doesn't support get access token"},
    )

    with patch("hotglue_singer_sdk.helpers._hotglue_api.requests.get") as mget:
        mget.return_value = mock_response
        with pytest.raises(RuntimeError, match="Failed Hotglue access token refresh") as excinfo:
            fetch_access_token_from_hotglue_api("c1")

    assert not isinstance(excinfo.value, InvalidCredentialsError)
    # authenticators.update_access_token matches this text to fall back to local refresh
    assert "Connector doesn't support get access token" in str(excinfo.value)


def test_fetch_access_token_non_string_code_still_matches_message(monkeypatch):
    """Classifies on Message even when Code is not a string (e.g. numeric 400)."""
    _credential_error_env(monkeypatch)
    mock_response = _error_response(
        400, {"Code": 400, "Message": "Failed OAuth login, response was 'invalid_grant'"}
    )

    with patch("hotglue_singer_sdk.helpers._hotglue_api.requests.get") as mget:
        mget.return_value = mock_response
        with pytest.raises(InvalidCredentialsError, match="Failed OAuth login"):
            fetch_access_token_from_hotglue_api("c1")


def test_fetch_access_token_non_string_message_still_matches_code(monkeypatch):
    """Classifies on Code even when Message is not a string."""
    _credential_error_env(monkeypatch)
    mock_response = _error_response(
        401, {"Code": "InvalidCredentialsError", "Message": {"nested": "object"}}
    )

    with patch("hotglue_singer_sdk.helpers._hotglue_api.requests.get") as mget:
        mget.return_value = mock_response
        with pytest.raises(InvalidCredentialsError, match="Invalid credentials for this connection"):
            fetch_access_token_from_hotglue_api("c1")
