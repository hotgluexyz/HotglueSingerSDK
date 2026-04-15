"""Shared network/backoff helpers."""

import errno

import requests

_TRANSIENT_NETWORK_ERRNOS = frozenset(
    (
        errno.ENETUNREACH,
        errno.ENETDOWN,
        errno.ETIMEDOUT,
        errno.EHOSTUNREACH,
        errno.ECONNREFUSED,
        errno.ECONNRESET,
        errno.EPIPE,
        errno.ECONNABORTED,
    )
)


def giveup_oserror_not_transient_network(exc: Exception) -> bool:
    """Backoff giveup for non-transient OSErrors.

    Keep retry behavior for requests exceptions,
    even when they inherit from OSError.
    """
    if isinstance(exc, requests.exceptions.RequestException):
        return False
    if isinstance(exc, OSError):
        return exc.errno not in _TRANSIENT_NETWORK_ERRNOS
    return False
