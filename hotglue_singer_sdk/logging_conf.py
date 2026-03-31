"""Path to the SDK's bundled logging.conf for :envvar:`LOGGING_CONF_FILE`."""

import logging
import os
from pathlib import Path

# Log line templates (tests lock these strings so edits are intentional).
JOB_AWARE_SINGER_DATEFMT = "%Y-%m-%d %H:%M:%S"
JOB_AWARE_SINGER_FMT_WITH_TIME = "time=%(asctime)s - %(name)s - %(message)s"
JOB_AWARE_SINGER_FMT_NO_TIME = "%(name)s - %(message)s"

JOB_AWARE_VERBOSE_DATEFMT = "%Y-%m-%d %H:%M:%S"
JOB_AWARE_VERBOSE_FMT_WITH_TIME = (
    "%(asctime)s,%(msecs)03d - %(module)s.%(funcName)s - "
    "%(levelname)s - %(name)s - %(message)s"
)
JOB_AWARE_VERBOSE_FMT_NO_TIME = (
    "%(module)s.%(funcName)s - %(levelname)s - %(name)s - %(message)s"
)


class JobAwareSingerFormatter(logging.Formatter):
    """Line with ``time=`` locally; without when :envvar:`JOB_ID` is set (Hotglue job)."""

    def __init__(self, fmt=None, datefmt=None, style="%"):  # noqa: ARG002
        super().__init__()
        self._with_time = logging.Formatter(
            fmt=JOB_AWARE_SINGER_FMT_WITH_TIME,
            datefmt=JOB_AWARE_SINGER_DATEFMT,
        )
        self._no_time = logging.Formatter(fmt=JOB_AWARE_SINGER_FMT_NO_TIME)

    def format(self, record: logging.LogRecord) -> str:
        if os.environ.get("JOB_ID"):
            return self._no_time.format(record)
        return self._with_time.format(record)


class JobAwareVerboseFormatter(logging.Formatter):
    """Verbose layout; omits leading timestamp when :envvar:`JOB` is set."""

    def __init__(self, fmt=None, datefmt=None, style="%"):  # noqa: ARG002
        super().__init__()
        self._with_time = logging.Formatter(
            fmt=JOB_AWARE_VERBOSE_FMT_WITH_TIME,
            datefmt=JOB_AWARE_VERBOSE_DATEFMT,
        )
        self._no_time = logging.Formatter(fmt=JOB_AWARE_VERBOSE_FMT_NO_TIME)

    def format(self, record: logging.LogRecord) -> str:
        if os.environ.get("JOB_ID"):
            return self._no_time.format(record)
        return self._with_time.format(record)


def default_logging_conf_path() -> str:
    """Return the absolute path to ``default_logging.conf`` shipped with this package."""
    return str((Path(__file__).resolve().parent / "default_logging.conf"))


def ensure_default_logging_conf_env() -> None:
    """If ``LOGGING_CONF_FILE`` is unset, set it to :func:`default_logging_conf_path`.

    Singer's ``get_logger()`` (via ``singer.messages``) reads this env var on first use.
    Call this **before** any code imports ``singer`` — :mod:`hotglue_singer_sdk` does that
    at package import time. Override by setting ``LOGGING_CONF_FILE`` yourself before
    importing the SDK, or by importing ``singer`` before ``hotglue_singer_sdk`` (not recommended).
    """
    if os.environ.get("LOGGING_CONF_FILE", "").strip():
        return
    path = default_logging_conf_path()
    if os.path.isfile(path):
        os.environ["LOGGING_CONF_FILE"] = path
