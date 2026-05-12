import json
import os
from abc import abstractmethod
from datetime import datetime
from typing import Any, Dict, Optional

import logging
import requests

from hotglue_singer_sdk.helpers._hotglue_api import fetch_access_token_from_hotglue_api
import base64


class Authenticator:
    def __init__(self, target, state: Dict[str, Any] = dict()):
        self.target_name: str = target.name
        self._config: Dict[str, Any] = target._config
        self._auth_headers: Dict[str, Any] = {}
        self._auth_params: Dict[str, Any] = {}
        self.logger: logging.Logger = target.logger
        self._config_file_path = target._config_file_path
        self._target = target
        self.state = state

    @property
    @abstractmethod
    def auth_headers(self) -> dict:
        raise NotImplementedError()

class BasicAuthenticator(Authenticator):
    """Implements basic authentication.

    This Authenticator implements basic authentication by concatinating a
    username and password then base64 encoding the string. The resulting
    token will be merged with any HTTP headers specified on the target.
    """

    def __init__(
        self,
        target,
        username: str,
        password: str,
    ) -> None:
        """Create a new authenticator.

        Args:
            target: The target instance to use with this authenticator.
            username: API username.
            password: API password.
        """
        credentials = f"{username}:{password}".encode()
        auth_token = base64.b64encode(credentials).decode("ascii")
        auth_credentials = {"Authorization": f"Basic {auth_token}"}

        if self._auth_headers is None:
            self._auth_headers = {}
        self._auth_headers.update(auth_credentials)
    
    @classmethod
    def create_for_stream(
        cls: type[Authenticator],
        target,
        username: str,
        password: str,
    ) -> Authenticator:
        """Create an Authenticator object specific to the target.

        Args:
            stream: The stream instance to use with this authenticator.
            username: API username.
            password: API password.

        Returns:
            Authenticator: A new
                :class:`hotglue_singer_sdk.target_sdk.auth.Authenticator` instance.
        """
        return cls(target, username=username, password=password)

class BearerTokenAuthenticator(Authenticator):
    """Implements bearer token authentication.

    This Authenticator implements Bearer Token authentication. The token
    is a text string, included in the request header and prefixed with
    'Bearer '. The token will be merged with HTTP headers on the target.
    """

    def __init__(
        self,
        target
    ) -> None:
        """Init authenticator.
        """
        super().__init__(target)

    def auth_headers(self) -> dict:
        return {"Authorization": f"Bearer {self._config['access_key']}"}


class ApiAuthenticator(Authenticator):
    def __init__(
        self,
        target,
        state: Dict[str, Any] = {},
        header_name = "x-api-key",
        header_value_prefix = "",
        config_key = "api_key",
    ) -> None:
        """Init authenticator.
        """
        super().__init__(target, state)
        self._header_name = header_name
        self._header_value_prefix = header_value_prefix
        self._config_key = config_key

    @property
    def auth_headers(self) -> dict:
        result = {}

        result[self._header_name] = f"{self._header_value_prefix}{self._config[self._config_key]}"

        return result


class OAuthAuthenticator(Authenticator):
    """API Authenticator for OAuth 2.0 flows."""

    def __init__(
        self,
        target,
        state = {},
        auth_endpoint: Optional[str] = None,
    ) -> None:
        """Init authenticator.
        """
        super().__init__(target, state)
        self._auth_endpoint = auth_endpoint

    @property
    def auth_headers(self) -> dict:
        if not self.is_token_valid():
            self.update_access_token()
        result = {}
        result["Authorization"] = f"Bearer {self._config.get('access_token')}"
        return result

    @property
    def oauth_request_body(self) -> dict:
        """Define the OAuth request body."""
        return {
            "refresh_token": self._config["refresh_token"],
            "grant_type": "refresh_token",
            "client_id": self._config["client_id"],
            "client_secret": self._config["client_secret"],
        }

    def is_token_valid(self) -> bool:
        access_token = self._config.get("access_token")
        now = round(datetime.utcnow().timestamp())
        expires_in = self._config.get("expires_in")
        if  expires_in is not None:
            expires_in = int(expires_in)
        if not access_token:
            return False

        if not expires_in:
            return False

        return not ((expires_in - now) < 120)

    def update_access_token(self) -> None:
        if self._config.get("_refresh_token_via_hg_api", False) is True:
            self._update_access_token_via_hg_api()
            return
        self._update_access_token_locally()

    def _update_access_token_via_hg_api(self) -> None:
        """Update token from the Hotglue access token API endpoint."""
        connector_id = os.environ.get("TARGET")
        token_json = fetch_access_token_from_hotglue_api(connector_id)
        self.access_token = token_json["access_token"]
        self.expires_in = int(token_json["expires_in"])

        self._config["access_token"] = self.access_token
        self._config["expires_in"] = self.expires_in

    def _update_access_token_locally(self) -> None:
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        self.logger.info(f"Oauth request - endpoint: {self._auth_endpoint}, body: {self.oauth_request_body}")
        token_response = requests.post(
            self._auth_endpoint, data=self.oauth_request_body, headers=headers
        )

        try:
            token_response.raise_for_status()
            self.logger.info("OAuth authorization attempt was successful.")
        except Exception as ex:
            self.state.update({"auth_error_response": token_response.json()})
            raise RuntimeError(
                f"Failed OAuth login, response was '{token_response.json()}'. {ex}"
            )

        token_json = token_response.json()
        self.access_token = token_json["access_token"]

        self._config["access_token"] = token_json["access_token"]
        self._config["refresh_token"] = token_json["refresh_token"]
        now = round(datetime.utcnow().timestamp())
        self._config["expires_in"] = int(token_json["expires_in"]) + now

        with open(self._config_file_path, "w") as outfile:
            json.dump(self._config, outfile, indent=4)

