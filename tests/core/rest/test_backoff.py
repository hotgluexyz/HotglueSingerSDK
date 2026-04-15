import json
import errno

try:
    from contextlib import nullcontext
except ImportError:
    from contextlib2 import nullcontext

from enum import Enum

import pytest
import requests
import backoff

from hotglue_singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from hotglue_singer_sdk.helpers._network import giveup_oserror_not_transient_network
from hotglue_singer_sdk.streams.rest import (
    RESTStream,
)


class CustomResponseValidationStream(RESTStream):
    """Stream with non-conventional error response."""

    url_base = "https://badapi.test"
    name = "imabadapi"
    schema = {"type": "object", "properties": {}}
    path = "/dummy"

    class StatusMessage(str, Enum):
        """Possible status messages."""

        OK = "OK"
        ERROR = "ERROR"
        UNAVAILABLE = "UNAVAILABLE"

    def validate_response(self, response: requests.Response):
        super().validate_response(response)
        data = response.json()
        if data["status"] == self.StatusMessage.ERROR:
            raise FatalAPIError("Error message found :(")
        if data["status"] == self.StatusMessage.UNAVAILABLE:
            raise RetriableAPIError("API is unavailable")


@pytest.fixture
def basic_rest_stream(rest_tap):
    """Get a RESTful tap stream."""
    return rest_tap.streams["some_stream"]


@pytest.fixture
def custom_validation_stream(rest_tap):
    """Get a RESTful tap stream with custom response validation."""
    return CustomResponseValidationStream(rest_tap)


@pytest.mark.parametrize(
    "status_code,reason,expectation",
    [
        (
            400,
            "Bad request",
            pytest.raises(
                FatalAPIError,
                match=r"400 Client Error: Bad request for path: /dummy",
            ),
        ),
        (
            503,
            "Service Unavailable",
            pytest.raises(
                RetriableAPIError,
                match=r"503 Server Error: Service Unavailable for path: /dummy",
            ),
        ),
        (
            429,
            "Too Many Requests",
            pytest.raises(
                RetriableAPIError,
                match=r"429 Client Error: Too Many Requests for path: /dummy",
            ),
        ),
        (200, "OK", nullcontext()),
    ],
    ids=["client-error", "server-error", "rate-limited", "ok"],
)
def test_status_code_api(basic_rest_stream, status_code, reason, expectation):
    fake_response = requests.Response()
    fake_response.status_code = status_code
    fake_response.reason = reason

    with expectation:
        basic_rest_stream.validate_response(fake_response)


@pytest.mark.parametrize(
    "message,expectation",
    [
        (
            CustomResponseValidationStream.StatusMessage.ERROR,
            pytest.raises(FatalAPIError),
        ),
        (
            CustomResponseValidationStream.StatusMessage.UNAVAILABLE,
            pytest.raises(RetriableAPIError),
        ),
        (
            CustomResponseValidationStream.StatusMessage.OK,
            nullcontext(),
        ),
    ],
    ids=["fatal", "retriable", "ok"],
)
def test_status_message_api(custom_validation_stream, message, expectation):
    fake_response = requests.Response()
    fake_response.status_code = 200
    fake_response._content = json.dumps({"status": message}).encode()
    fake_response.url = custom_validation_stream.url_base

    with expectation:
        custom_validation_stream.validate_response(fake_response)


@pytest.mark.parametrize(
    "rate_limit_codes,response_status,expectation",
    [
        (
            CustomResponseValidationStream.extra_retry_statuses,
            429,
            pytest.raises(RetriableAPIError),
        ),
        (
            [429, 403],
            403,
            pytest.raises(RetriableAPIError),
        ),
        (
            [],
            429,
            pytest.raises(FatalAPIError),
        ),
    ],
    ids=[
        "default",
        "changed",
        "missing",
    ],
)
def test_rate_limiting_status_override(
    basic_rest_stream, rate_limit_codes, response_status, expectation
):
    fake_response = requests.Response()
    fake_response.status_code = response_status
    basic_rest_stream.extra_retry_statuses = rate_limit_codes

    with expectation:
        basic_rest_stream.validate_response(fake_response)


def test_giveup_oserror_not_transient_network_matrix():
    assert not giveup_oserror_not_transient_network(
        requests.exceptions.ConnectionError("wrapped network error")
    )
    assert not giveup_oserror_not_transient_network(
        OSError(errno.ENETUNREACH, "Network is unreachable")
    )
    assert giveup_oserror_not_transient_network(
        OSError(errno.EINVAL, "Non-transient error")
    )
    assert giveup_oserror_not_transient_network(OSError("Unknown errno"))


def test_request_decorator_retries_transient_bare_oserror(basic_rest_stream):
    basic_rest_stream.backoff_wait_generator = lambda: backoff.constant(interval=0)
    basic_rest_stream.backoff_max_tries = lambda: 3
    attempts = {"count": 0}

    def transient_then_success():
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise OSError(errno.ENETUNREACH, "Network is unreachable")
        return "ok"

    decorated = basic_rest_stream.request_decorator(transient_then_success)
    assert decorated() == "ok"
    assert attempts["count"] == 2


def test_request_decorator_gives_up_non_transient_oserror(basic_rest_stream):
    basic_rest_stream.backoff_wait_generator = lambda: backoff.constant(interval=0)
    basic_rest_stream.backoff_max_tries = lambda: 3
    attempts = {"count": 0}

    def non_transient_failure():
        attempts["count"] += 1
        raise OSError(errno.EINVAL, "Invalid argument")

    decorated = basic_rest_stream.request_decorator(non_transient_failure)
    with pytest.raises(OSError):
        decorated()
    assert attempts["count"] == 1

def test_request_backoff_exceptions_include_oserror(basic_rest_stream):
    exceptions = basic_rest_stream.backoff_exceptions()
    assert OSError in exceptions
    assert requests.exceptions.ConnectionError in exceptions
