"""Regression: avoid circular imports between singer and hotglue_singer_sdk."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]


def _run_fresh_python(code: str) -> None:
    env = os.environ.copy()
    prev = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = str(REPO_ROOT) + (os.pathsep + prev if prev else "")
    subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        env=env,
        cwd=str(REPO_ROOT),
    )


def test_import_singer_then_hotglue_singer_sdk():
    """Full public package import after singer must succeed."""
    code = (
        "import singer\n"
        "import hotglue_singer_sdk\n"
        "assert hotglue_singer_sdk.Stream is not None\n"
    )
    _run_fresh_python(code)


@pytest.mark.parametrize(
    "trigger",
    [
        "import singer.utils",
        "import singer.catalog",
    ],
)
def test_singer_submodule_import_with_sdk_logging_conf_triggers_hotglue(trigger: str):
    """Mimics Lambda/tap: LOGGING_CONF_FILE loads SDK formatters while singer is still initializing.

    ``singer.catalog`` runs ``get_logger()`` at import time; with our default logging.conf,
    ``fileConfig`` imports ``hotglue_singer_sdk``, which must not use ``from singer import ...``
    against a partially initialized ``singer`` package.
    """
    conf = REPO_ROOT / "hotglue_singer_sdk" / "default_logging.conf"
    assert conf.is_file(), conf
    code = (
        "import os\n"
        "import sys\n"
        f"sys.path.insert(0, {str(REPO_ROOT)!r})\n"
        f"os.environ['LOGGING_CONF_FILE'] = {str(conf)!r}\n"
        f"{trigger}\n"
        "from hotglue_singer_sdk.streams.core import Stream\n"
        "assert Stream is not None\n"
    )
    _run_fresh_python(code)
