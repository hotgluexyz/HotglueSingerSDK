"""Shared Hotglue API helpers (e.g. access token fetch)."""

from __future__ import annotations

import logging
import os
from typing import Any

import backoff
import requests
from hotglue_etl_exceptions import InvalidCredentialsError

from hotglue_singer_sdk.exceptions import RetriableAPIError

logger = logging.getLogger(__name__)

# Access token errors Hotglue can't recover from; keep them out of credential handling.
FALLBACK_TO_LOCAL_REFRESH_ERRORS = (
    "Connector doesn't support get access token",  # Tap CLI has no --access-token
    "Fetch access token support is not implemented",  # No access_token_support
    "does not support real time",  # No realtime tap/target lambda
    "Missing required env vars",  # Local dev: ENV_ID/FLOW/TENANT/API_KEY/TAP|TARGET unset
    "No available connector found for target",  # v1 TARGET->tap unable to resolve
    "is not an available connector",  # v1 TARGET->tap unable to resolve
)


def _credential_error_message(response: requests.Response) -> str | None:
    """Return the upstream error message when a failed response is a credential error. """
    try:
        body = response.json()
    except ValueError:
        return None
    if not isinstance(body, dict):
        return None

    # ("BadRequestError" is a 400). A 400 from that endpoint means the token retrieval failed
    code = body.get("Code")
    if response.status_code != 400 and code != "BadRequestError":
        return None

    message = body.get("Message") or ""
    if any(error in message for error in FALLBACK_TO_LOCAL_REFRESH_ERRORS):
        return None

    return message or "Invalid credentials for this connection."


@backoff.on_exception(
    backoff.expo,
    RetriableAPIError,
    factor=2,
    max_tries=8,
    on_backoff=lambda details: logger.warning(
        "Backing off Hotglue access token request for %.1f seconds after %s tries.",
        details["wait"],
        details["tries"],
    ),
)
def _get_access_token_response(endpoint: str, api_key: str) -> requests.Response:
    """Fetch a Hotglue access-token response."""
    token_response = requests.get(
        endpoint,
        params={"include_properties": "expires_in"},
        headers={"x-api-key": api_key},
        timeout=300,
    )
    status_code = token_response.status_code
    if status_code in [429, 423] or 500 <= status_code <= 599:
        raise RetriableAPIError(token_response.text, token_response)
    token_response.raise_for_status()
    return token_response


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
        InvalidCredentialsError: If the access token API reports that the
            connection's credentials are invalid or expired.
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
    logger.info(
        "Starting Hotglue access token request for env_id=%s flow=%s tenant=%s connector_id=%s.",
        env_id,
        flow_id,
        tenant,
        connector_id,
    )
    try:
        token_response = _get_access_token_response(endpoint, api_key)
    except (RetriableAPIError, requests.HTTPError) as ex:
        if ex.response is None:
            raise RuntimeError(f"Failed Hotglue access token refresh. {ex}") from ex

        response_text = ex.response.text
        credential_error = _credential_error_message(ex.response)
        if credential_error:
            logger.warning(
                "Hotglue access token refresh failed with invalid credentials for "
                "env_id=%s flow=%s tenant=%s connector_id=%s. Response was '%s'. %s",
                env_id,
                flow_id,
                tenant,
                connector_id,
                response_text,
                ex,
            )
            raise InvalidCredentialsError(credential_error) from ex
        raise RuntimeError(
            f"Failed Hotglue access token refresh, response was "
            f"'{response_text}'. {ex}"
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
    if "expires_in" not in token_json:
        raise RuntimeError(
            "Hotglue access token refresh response did not include expires_in."
        )

    logger.info(
        "Fetched Hotglue access token for env_id=%s flow=%s tenant=%s connector_id=%s.",
        env_id,
        flow_id,
        tenant,
        connector_id,
    )
    return token_json
