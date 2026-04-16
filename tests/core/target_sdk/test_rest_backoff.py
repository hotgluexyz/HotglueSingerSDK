"""Tests for target REST backoff behavior."""

import errno

import backoff
import pytest
import requests

from hotglue_singer_sdk.helpers._network import giveup_oserror_not_transient_network
from hotglue_singer_sdk.target_sdk.rest import Rest


def test_target_giveup_oserror_not_transient_network_matrix():
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


def test_target_request_decorator_retries_transient_bare_oserror():
    rest = Rest()
    rest.backoff_wait_generator = lambda: backoff.constant(interval=0)
    rest.backoff_max_tries = lambda: 3
    attempts = {"count": 0}

    def transient_then_success():
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise OSError(errno.ENETUNREACH, "Network is unreachable")
        return "ok"

    decorated = rest.request_decorator(transient_then_success)
    assert decorated() == "ok"
    assert attempts["count"] == 2


def test_target_request_decorator_gives_up_non_transient_oserror():
    rest = Rest()
    rest.backoff_wait_generator = lambda: backoff.constant(interval=0)
    rest.backoff_max_tries = lambda: 3
    attempts = {"count": 0}

    def non_transient_failure():
        attempts["count"] += 1
        raise OSError(errno.EINVAL, "Invalid argument")

    decorated = rest.request_decorator(non_transient_failure)
    with pytest.raises(OSError):
        decorated()
    assert attempts["count"] == 1


def test_target_backoff_exceptions_include_oserror():
    rest = Rest()
    exceptions = rest.backoff_exceptions()
    assert OSError in exceptions
    assert requests.exceptions.ConnectionError in exceptions
