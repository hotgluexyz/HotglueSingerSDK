"""Bundled logging.conf path for LOGGING_CONF_FILE."""

import logging
import os
import subprocess
import sys
import time
from pathlib import Path

import freezegun
import pytest

from hotglue_singer_sdk.logging_conf import (
    JOB_AWARE_SINGER_DATEFMT,
    JOB_AWARE_SINGER_FMT_NO_TIME,
    JOB_AWARE_SINGER_FMT_WITH_TIME,
    JOB_AWARE_VERBOSE_DATEFMT,
    JOB_AWARE_VERBOSE_FMT_NO_TIME,
    JOB_AWARE_VERBOSE_FMT_WITH_TIME,
    JobAwareSingerFormatter,
    JobAwareVerboseFormatter,
    default_logging_conf_path,
    ensure_default_logging_conf_env,
)


def test_default_logging_conf_path_is_absolute_and_exists():
    path = default_logging_conf_path()
    assert os.path.isabs(path)
    assert Path(path).is_file()
    assert path.endswith("default_logging.conf")


def test_ensure_default_logging_conf_env_sets_when_unset(monkeypatch):
    monkeypatch.delenv("LOGGING_CONF_FILE", raising=False)
    ensure_default_logging_conf_env()
    assert os.environ.get("LOGGING_CONF_FILE") == default_logging_conf_path()


def test_ensure_default_logging_conf_env_respects_existing(monkeypatch):
    monkeypatch.setenv("LOGGING_CONF_FILE", "/tmp/custom.conf")
    ensure_default_logging_conf_env()
    assert os.environ.get("LOGGING_CONF_FILE") == "/tmp/custom.conf"


def test_sdk_import_sets_logging_conf_file_in_fresh_process():
    code = (
        "import os\n"
        "import hotglue_singer_sdk\n"
        "p = os.environ.get('LOGGING_CONF_FILE', '')\n"
        "assert p.endswith('default_logging.conf')\n"
        "assert os.path.isfile(p)\n"
    )
    proc = subprocess.run(
        [sys.executable, "-c", code],
        cwd=Path(__file__).resolve().parents[2],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, (proc.stdout, proc.stderr)


def test_job_aware_singer_format_templates_contract():
    """Changing these strings changes log output; update deliberately and fix tests."""
    assert JOB_AWARE_SINGER_DATEFMT == "%Y-%m-%d %H:%M:%S"
    assert JOB_AWARE_SINGER_FMT_WITH_TIME == "time=%(asctime)s - %(name)s - %(message)s"
    assert JOB_AWARE_SINGER_FMT_NO_TIME == "%(name)s - %(message)s"


def test_job_aware_verbose_format_templates_contract():
    assert JOB_AWARE_VERBOSE_DATEFMT == "%Y-%m-%d %H:%M:%S"
    assert JOB_AWARE_VERBOSE_FMT_WITH_TIME == (
        "%(asctime)s,%(msecs)03d - %(module)s.%(funcName)s - "
        "%(levelname)s - %(name)s - %(message)s"
    )
    assert JOB_AWARE_VERBOSE_FMT_NO_TIME == (
        "%(module)s.%(funcName)s - %(levelname)s - %(name)s - %(message)s"
    )


@pytest.fixture
def utc_tz(monkeypatch):
    """Make ``logging.Formatter`` use UTC for ``%(asctime)s`` (matches frozen wall clock).

    Freezegun pins ``time.time()``; the stdlib still formats ``asctime`` with
    ``localtime(record.created)``. Without ``TZ=UTC``, a dev machine in e.g. US/Eastern
    turns ``2026-03-31 12:00:00`` into ``07:00:00`` in the log line.
    """
    monkeypatch.setenv("TZ", "UTC")
    if hasattr(time, "tzset"):
        time.tzset()


def _sample_log_record(*, pathname: str, func: str = "my_func") -> logging.LogRecord:
    return logging.LogRecord(
        name="tap-example",
        level=logging.INFO,
        pathname=pathname,
        lineno=10,
        msg="sync done",
        args=(),
        exc_info=None,
        func=func,
    )


@pytest.mark.parametrize(
    "job_id_set, expected_line",
    [
        (
            False,
            "time=2026-03-31 12:00:00 - tap-example - sync done",
        ),
        (
            True,
            "tap-example - sync done",
        ),
    ],
)
def test_job_aware_singer_formatter_respects_job_id(
    monkeypatch, utc_tz, job_id_set, expected_line
):
    if job_id_set:
        monkeypatch.setenv("JOB_ID", "hg-job-1")
    else:
        monkeypatch.delenv("JOB_ID", raising=False)

    with freezegun.freeze_time("2026-03-31 12:00:00"):
        record = _sample_log_record(pathname=str(Path(__file__).resolve()))
        assert JobAwareSingerFormatter().format(record) == expected_line


@pytest.mark.parametrize(
    "job_set, expected_line",
    [
        (
            False,
            "2026-03-31 12:00:00,000 - test_logging_conf.my_func - INFO - tap-example - sync done",
        ),
        (
            True,
            "test_logging_conf.my_func - INFO - tap-example - sync done",
        ),
    ],
)
def test_job_aware_verbose_formatter_respects_job(
    monkeypatch, utc_tz, job_set, expected_line
):
    if job_set:
        monkeypatch.setenv("JOB", "1")
    else:
        monkeypatch.delenv("JOB", raising=False)

    with freezegun.freeze_time("2026-03-31 12:00:00"):
        record = _sample_log_record(pathname=str(Path(__file__).resolve()))
        assert JobAwareVerboseFormatter().format(record) == expected_line
