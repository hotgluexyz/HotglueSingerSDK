"""Tests for tap exception alerting."""

import json
import sys
import threading
from typing import List

from hotglue_etl_exceptions import InvalidCredentialsError
from hotglue_singer_sdk.helpers.capabilities import AlertingLevel
from hotglue_singer_sdk.streams import Stream
from hotglue_singer_sdk.tap_base import Tap, custom_hotglue_tap_exception_handling
from hotglue_singer_sdk.typing import PropertiesList


class ChildInvalidCredentialsError(InvalidCredentialsError):
    """Subclass of a mapped exception."""


class ExceptionMapTap(Tap):
    """Tap used to raise a mapped exception subclass."""

    name = "exception-map-tap"
    config_jsonschema = PropertiesList().to_dict()

    def discover_streams(self) -> List[Stream]:
        return []

    def raise_mapped_subclass(self) -> None:
        raise ChildInvalidCredentialsError("bad creds")


def test_subclass_exception_name_uses_mapped_parent(tmp_path, monkeypatch):
    """Report the mapped parent class name for exception subclasses."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        "hotglue_singer_sdk.tap_base.sys.__excepthook__",
        lambda *_: None,
    )

    tap = ExceptionMapTap()
    try:
        tap.raise_mapped_subclass()
    except ChildInvalidCredentialsError:
        exc_type, exc_value, exc_traceback = sys.exc_info()

    custom_hotglue_tap_exception_handling(exc_type, exc_value, exc_traceback)

    payload = json.loads(
        (tmp_path / f"hg-tap-exception-{threading.get_ident()}.json").read_text()
    )
    assert payload["exception_name"] == InvalidCredentialsError.__name__
    assert payload["alerting_level"] == AlertingLevel.NONE.value
