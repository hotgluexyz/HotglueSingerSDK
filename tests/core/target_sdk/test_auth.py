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
    relative_expires = 222333444
    token_json = {
        "success": True,
        "access_token": "hg-target-token",
        "expires_in": relative_expires,
    }

    with patch(
        "hotglue_singer_sdk.target_sdk.auth.fetch_access_token_from_hotglue_api"
    ) as mfetch:
        mfetch.return_value = token_json
        auth = OAuthAuthenticator(t, auth_endpoint="https://oauth.example.com/token")
        auth.update_access_token()

    assert t._config["access_token"] == "hg-target-token"
    # now=1000 (frozen), so expires_in = 1000 + relative_expires
    expected_expires_in = 1000 + relative_expires
    assert t._config["expires_in"] == expected_expires_in
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
        auth.update_access_token()


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
    assert mfetch.call_count == 0
    assert mpost.call_count == 1
