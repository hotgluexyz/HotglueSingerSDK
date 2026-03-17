"""Shared Hotglue API helpers (e.g. access token fetch)."""

from __future__ import annotations

import os
from typing import Any

import requests



def fetch_access_token_from_hotglue_api(connector_id: str | None) -> dict[str, Any]:
    """Fetch access token from the Hotglue access token API endpoint.

    Reads ENV_ID, FLOW, TENANT, API_KEY, API_URL from environment.
    Caller supplies connector_id (e.g. os.environ.get("TAP") or "TARGET").

    Args:
        connector_id: Connector identifier (e.g. tap or target id).

    Returns:
        The validated response JSON dict (at least access_token, expires_in;
        may include refresh_token etc.).

    Raises:
        RuntimeError: If required env vars or connector_id are missing, or
            the API request fails or response is invalid.
    """
    env_id = os.environ.get("ENV_ID")
    flow_id = os.environ.get("FLOW")
    tenant = os.environ.get("TENANT")
    api_key = os.environ.get("API_KEY")
    api_url = os.environ.get("API_URL", "https://api.hotglue.com").rstrip("/")

    required = {
        "ENV_ID": env_id,
        "FLOW": flow_id,
        "TENANT": tenant,
        "API_KEY": api_key,
        "connector_id": connector_id,
    }
    missing = sorted([key for key, value in required.items() if not value])
    if missing:
        raise RuntimeError(
            "Missing required env vars for Hotglue access token refresh: "
            + ", ".join(missing)
        )
    endpoint = (
        f"{api_url}/{env_id}/{flow_id}/{tenant}/connectors/{connector_id}/accesstoken"
    )
    token_response = requests.get(
        endpoint,
        params={"include_properties": "expires_in"},
        headers={"x-api-key": api_key},
    )
    try:
        token_response.raise_for_status()
    except Exception as ex:
        raise RuntimeError(
            f"Failed Hotglue access token refresh, response was "
            f"'{token_response.text}'. {ex}"
        ) from ex

    token_json = token_response.json()
    if token_json.get("success") is not True:
        raise RuntimeError(
            f"Hotglue access token refresh was not successful: {token_json}"
        )
    if token_json.get("access_token") is None:
        raise RuntimeError(
            "Hotglue access token refresh response did not include access_token."
        )
    if token_json.get("expires_in") is None:
        raise RuntimeError(
            "Hotglue access token refresh response did not include expires_in."
        )

    return token_json
