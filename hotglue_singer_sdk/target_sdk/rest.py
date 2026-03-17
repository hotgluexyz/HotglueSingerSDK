"""Hotglue target sink class, which handles writing streams."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Callable, Dict, Optional, Tuple, Type, Union, TypeVar, Generator

import backoff
import requests
import json

from hotglue_singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from hotglue_singer_sdk.target_sdk.auth import Authenticator
from hotglue_singer_sdk.target_sdk.common import HGJSONEncoder

_T = TypeVar("_T")
_MaybeCallable = Union[_T, Callable[[], _T]]
class Rest:
    timeout: int = 300
    http_headers: Dict[str, Any] = {}
    params: Dict[str, Any] = {}
    authenticator: Optional[Authenticator] = None

    @property
    def default_headers(self):
        headers = self.http_headers

        if self.authenticator and isinstance(self.authenticator, Authenticator):
            headers.update(self.authenticator.auth_headers)

        return headers

    def request_decorator(self, func: Callable) -> Callable:
        """Instantiate a decorator for handling request failures.

        Uses a wait generator from `backoff_wait_generator`, exception types from
        `backoff_exceptions`, try limit from `backoff_max_tries`, and calls
        `backoff_handler` before retrying. Override any of these in a child class
        to customize backoff behaviour.
        """
        decorator: Callable = backoff.on_exception(
            self.backoff_wait_generator,
            self.backoff_exceptions(),
            max_tries=self.backoff_max_tries,
            on_backoff=self.backoff_handler,
        )(func)
        return decorator

    def backoff_wait_generator(self) -> Callable[..., Generator[int, Any, None]]:
        """The wait generator used by the backoff decorator on request failure.

        See for options:
        https://github.com/litl/backoff/blob/master/backoff/_wait_gen.py

        And see for examples: `Code Samples <../code_samples.html#custom-backoff>`_

        Returns:
            The wait generator
        """
        return backoff.expo(factor=2)  # type: ignore # ignore 'Returning Any'

    def backoff_exceptions(self) -> Tuple[Type[Exception], ...]:
        """Exception types that trigger a retry. Override to add or change."""
        return (
            RetriableAPIError, 
            requests.exceptions.ReadTimeout, 
            requests.exceptions.ConnectionError
        )

    def backoff_max_tries(self) -> _MaybeCallable[int] | None:
        """The number of attempts before giving up when retrying requests.

        Can be an integer, a zero-argument callable that returns an integer,
        or ``None`` to retry indefinitely.

        Returns:
            int | Callable[[], int] | None: Number of max retries, callable or
            ``None``.
        """
        return 5

    def backoff_handler(self, details: dict) -> None:
        """Called before each retry. Override to log or add behaviour."""
        pass

    def _request(
        self, http_method, endpoint, params={}, request_data=None, headers={}, verify=True
    ) -> requests.PreparedRequest:
        """Prepare a request object."""
        url = self.url(endpoint)
        headers.update(self.default_headers)
        headers.update({"Content-Type": "application/json"})
        params.update(self.params)
        data = (
            json.dumps(request_data, cls=HGJSONEncoder)
            if request_data
            else None
        )

        response = requests.request(
            method=http_method,
            url=url,
            params=params,
            headers=headers,
            data=data,
            verify=verify
        )
        self.validate_response(response)
        return response

    def request_api(self, http_method, endpoint=None, params={}, request_data=None, headers={}, verify=True):
        """Request records from REST endpoint(s), returning response records."""
        decorated_request = self.request_decorator(self._request)
        return decorated_request(
            http_method, endpoint, params=params, request_data=request_data, headers=headers, verify=verify
        )

    def validate_response(self, response: requests.Response) -> None:
        """Validate HTTP response."""
        if response.status_code in [429] or 500 <= response.status_code < 600:
            msg = self.response_error_message(response)
            raise RetriableAPIError(msg, response)
        elif 400 <= response.status_code < 500:
            try:
                msg = response.text
            except:
                msg = self.response_error_message(response)
            raise FatalAPIError(msg)

    def response_error_message(self, response: requests.Response) -> str:
        """Build error message for invalid http statuses."""
        if 400 <= response.status_code < 500:
            error_type = "Client"
        else:
            error_type = "Server"

        return (
            f"{response.status_code} {error_type} Error: "
            f"{response.reason} for path: {self.endpoint}"
        )

    @staticmethod
    def clean_dict_items(dict):
        return {k: v for k, v in dict.items() if v not in [None, ""]}

    def clean_payload(self, item):
        item = self.clean_dict_items(item)
        output = {}
        for k, v in item.items():
            if isinstance(v, datetime):
                dt_str = v.strftime("%Y-%m-%dT%H:%M:%S%z")
                if len(dt_str) > 20:
                    output[k] = f"{dt_str[:-2]}:{dt_str[-2:]}"
                else:
                    output[k] = dt_str
            elif isinstance(v, dict):
                output[k] = self.clean_payload(v)
            else:
                output[k] = v
        return output

