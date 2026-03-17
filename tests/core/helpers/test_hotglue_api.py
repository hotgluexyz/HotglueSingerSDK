"""Tests for Hotglue API helper."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from urllib.parse import urlparse

from hotglue_singer_sdk.helpers._hotglue_api import fetch_access_token_from_hotglue_api


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
    """Raises RuntimeError when API returns non-2xx."""
    monkeypatch.setenv("API_URL", "https://api.hotglue.com")
    monkeypatch.setenv("ENV_ID", "e")
    monkeypatch.setenv("FLOW", "f")
    monkeypatch.setenv("TENANT", "t")
    monkeypatch.setenv("API_KEY", "k")

    mock_response = MagicMock()
    mock_response.status_code = 500
    mock_response.text = "Internal Server Error"
    mock_response.raise_for_status.side_effect = Exception("500")

    with patch("hotglue_singer_sdk.helpers._hotglue_api.requests.get") as mget:
        mget.return_value = mock_response
        with pytest.raises(RuntimeError, match="Failed Hotglue access token refresh"):
            fetch_access_token_from_hotglue_api("c1")


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
