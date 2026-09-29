"""Tests for Hotglue API helper."""

from __future__ import annotations

import json

from unittest.mock import MagicMock, patch
from urllib.parse import urlparse

import pytest
import requests

from hotglue_etl_exceptions import InvalidCredentialsError

from hotglue_singer_sdk.helpers._hotglue_api import (
    FALLBACK_TO_LOCAL_REFRESH_ERRORS,
    _credential_error_message,
    fetch_access_token_from_hotglue_api,
)


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

def test_fetch_access_token_non_400_stays_runtime_error(monkeypatch):
    """Only a 400 means the token retrieval itself failed; other 4xx still alert."""
    _credential_error_env(monkeypatch)
    mock_response = _error_response(
        401, {"Code": "UnauthorizedError", "Message": "Invalid x-api-key"}
    )

    with patch("hotglue_singer_sdk.helpers._hotglue_api.requests.get") as mget:
        mget.return_value = mock_response
        with pytest.raises(RuntimeError) as excinfo:
            fetch_access_token_from_hotglue_api("c1")

    assert not isinstance(excinfo.value, InvalidCredentialsError)


def test_fetch_access_token_400_without_parseable_body(monkeypatch):
    """A 400 is classified from the status even when the body is not JSON."""
    _credential_error_env(monkeypatch)
    mock_response = MagicMock()
    mock_response.status_code = 400
    mock_response.text = "<html>Bad Request</html>"
    mock_response.json.side_effect = ValueError("no json")
    mock_response.raise_for_status.side_effect = requests.HTTPError(
        "400", response=mock_response
    )

    with patch("hotglue_singer_sdk.helpers._hotglue_api.requests.get") as mget:
        mget.return_value = mock_response
        with pytest.raises(InvalidCredentialsError, match="Invalid credentials for this connection"):
            fetch_access_token_from_hotglue_api("c1")


def test_credential_error_message_falls_back_to_code_when_status_missing():
    """Classifies on the readable status name in Code when no status_code is set."""
    response = MagicMock()
    response.status_code = None
    response.json.return_value = {"Code": "BadRequestError", "Message": "Failed OAuth login"}

    assert _credential_error_message(response) == "Failed OAuth login"


def test_credential_error_message_ignores_other_statuses():
    """A non-400 with no recognised Code is not a credential error."""
    response = MagicMock()
    response.status_code = 404
    response.json.return_value = {"Code": "NotFoundError", "Message": "Resource not found"}

    assert _credential_error_message(response) is None


def test_fetch_access_token_fallback_errors_stay_runtime_error(monkeypatch):
    """Capability failures come back as 400 too, but are not credential errors.

    Callers match these messages to fall back to a local refresh, so they must
    keep raising RuntimeError and keep alerting.
    """
    _credential_error_env(monkeypatch)
    for fallback_error in FALLBACK_TO_LOCAL_REFRESH_ERRORS:
        mock_response = _error_response(
            400, {"Code": "BadRequestError", "Message": f"{fallback_error} for connector"}
        )
        with patch("hotglue_singer_sdk.helpers._hotglue_api.requests.get") as mget:
            mget.return_value = mock_response
            with pytest.raises(RuntimeError) as excinfo:
                fetch_access_token_from_hotglue_api("c1")
        assert not isinstance(excinfo.value, InvalidCredentialsError), fallback_error
        assert fallback_error in str(excinfo.value)


def test_fetch_access_token_non_string_code_still_classified_by_status(monkeypatch):
    """A non-string Code does not stop the 400 status from classifying."""
    _credential_error_env(monkeypatch)
    mock_response = _error_response(400, {"Code": 400, "Message": "Failed OAuth login"})

    with patch("hotglue_singer_sdk.helpers._hotglue_api.requests.get") as mget:
        mget.return_value = mock_response
        with pytest.raises(InvalidCredentialsError, match="Failed OAuth login"):
            fetch_access_token_from_hotglue_api("c1")


@pytest.mark.parametrize("parsed", [["a", "list"], "a string", None, 42])
def test_credential_error_message_handles_non_dict_json_body(parsed):
    """Valid JSON that is not an object must not blow up on body.get()."""
    response = MagicMock()
    response.status_code = 400
    response.json.return_value = parsed

    assert _credential_error_message(response) == "Invalid credentials for this connection."


def test_credential_error_message_non_dict_json_body_non_400():
    """Same, but a non-400 is still not a credential error."""
    response = MagicMock()
    response.status_code = 500
    response.json.return_value = ["a", "list"]

    assert _credential_error_message(response) is None
