"""Path to the SDK's bundled logging.conf for :envvar:`LOGGING_CONF_FILE`."""

import logging
import os
from pathlib import Path


class JobAwareSingerFormatter(logging.Formatter):
    """Pipelinewise-style key=value line; omits ``time=`` when :envvar:`JOB` is set."""

    def __init__(self, fmt=None, datefmt=None, style="%"):  # noqa: ARG002
        super().__init__()
        self._with_time = logging.Formatter(
            fmt="time=%(asctime)s - %(name)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        self._no_time = logging.Formatter(
            fmt="%(name)s - %(message)s",
        )

    def format(self, record: logging.LogRecord) -> str:
        if os.environ.get("JOB"):
            return self._no_time.format(record)
        return self._with_time.format(record)


class JobAwareVerboseFormatter(logging.Formatter):
    """Verbose layout; omits leading timestamp when :envvar:`JOB` is set."""

    def __init__(self, fmt=None, datefmt=None, style="%"):  # noqa: ARG002
        super().__init__()
        self._with_time = logging.Formatter(
            fmt=(
                "%(asctime)s,%(msecs)03d - %(module)s.%(funcName)s - "
                "%(levelname)s - %(name)s - %(message)s"
            ),
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        self._no_time = logging.Formatter(
            fmt="%(module)s.%(funcName)s - %(levelname)s - %(name)s - %(message)s",
        )

    def format(self, record: logging.LogRecord) -> str:
        if os.environ.get("JOB"):
            return self._no_time.format(record)
        return self._with_time.format(record)


def default_logging_conf_path() -> str:
    """Return the absolute path to ``default_logging.conf`` shipped with this package.

    Set before starting the tap/target process (Singer reads this on first ``get_logger()``)::

        export LOGGING_CONF_FILE="$(python -c 'from hotglue_singer_sdk.logging_conf import default_logging_conf_path; print(default_logging_conf_path())')"
    """
    return str((Path(__file__).resolve().parent / "default_logging.conf"))
