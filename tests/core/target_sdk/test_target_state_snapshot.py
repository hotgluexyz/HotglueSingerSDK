"""Tests for x-hotglue target state snapshot customData (HGI-10878)."""

from __future__ import annotations

from typing import List, Optional

import pytest

from hotglue_singer_sdk.target_sdk.client import HotglueBaseSink
from hotglue_singer_sdk.target_sdk.target_base import Target

from tests.core.target_sdk.test_sink_state import (
    CapturingSink,
    FakeTarget,
    PreprocessStripsFieldSink,
    UpsertErrorSink,
    _make_sink,
)


WIDGETS_SCHEMA = {
    "type": "object",
    "properties": {
        "name": {"type": "string"},
        "Notes": {"type": "string"},
        "externalId": {"type": "string"},
    },
}


class SnapshotCapturingSink(CapturingSink):
    """Capturing sink registered under the widgets stream name."""

    name = "widgets"


class SnapshotStripsFieldSink(PreprocessStripsFieldSink):
    """PreprocessStripsFieldSink registered under the widgets stream name."""

    name = "widgets"


class SnapshotTarget(Target):
    """Minimal target for SCHEMA to sink snapshot wiring tests."""

    name = "snapshot_target"
    EXTERNAL_ID_KEY = "externalId"
    default_sink_class = SnapshotCapturingSink

    def __init__(self) -> None:
        super().__init__(config={})
        self._state = {}
        self._latest_state = {}
        self.incremental_target_state_path = "/tmp/nonexistent_snapshot_target_state.json"


class SnapshotStripsFieldTarget(SnapshotTarget):
    """Snapshot target whose sink strips fields during preprocess."""

    default_sink_class = SnapshotStripsFieldSink


def _schema_message(
    stream: str = "widgets",
    *,
    x_hotglue: Optional[dict] = None,
    schema: Optional[dict] = None,
) -> dict:
    message = {
        "type": "SCHEMA",
        "stream": stream,
        "schema": schema or WIDGETS_SCHEMA,
        "key_properties": [],
    }
    if x_hotglue is not None:
        message["x-hotglue"] = x_hotglue
    return message


def _record_message(
    stream: str = "widgets",
    *,
    record: Optional[dict] = None,
) -> dict:
    return {
        "type": "RECORD",
        "stream": stream,
        "record": record
        or {
            "name": "a",
            "externalId": "e1",
            "Notes": "from schema wiring test",
        },
    }


@pytest.fixture(autouse=True)
def reset_hotglue_base_state():
    HotglueBaseSink.summary_init = False
    HotglueBaseSink.previous_state = None
    HotglueBaseSink.processed_hashes = []
    yield
    HotglueBaseSink.summary_init = False
    HotglueBaseSink.previous_state = None
    HotglueBaseSink.processed_hashes = []


def test_schema_message_configures_sink_for_custom_data():
    """SCHEMA x-hotglue is applied to the sink without manual configure calls."""
    target = SnapshotTarget()
    target._process_schema_message(
        _schema_message(
            x_hotglue={
                "target_state_fields": ["Notes"],
                "target_state_include_hash": True,
            }
        )
    )

    target._process_record_message(_record_message())

    sink = target.get_sink("widgets")
    state_entry = sink.latest_state["bookmarks"]["widgets"][0]
    assert state_entry["customData"]["Notes"] == "from schema wiring test"
    assert state_entry["customData"]["hash"] == state_entry["hash"]


def test_duplicate_schema_message_updates_snapshot_config():
    """Unchanged SCHEMA still re-applies x-hotglue snapshot settings."""
    target = SnapshotTarget()
    target._process_schema_message(
        _schema_message(x_hotglue={"target_state_fields": ["Notes"]})
    )
    sink = target.get_sink("widgets")

    assert sink._target_state_fields == ["Notes"]
    assert sink._target_state_include_hash is False

    target._process_schema_message(
        _schema_message(x_hotglue={"target_state_include_hash": True})
    )

    assert sink._target_state_fields == []
    assert sink._target_state_include_hash is True


def test_record_message_preserves_source_fields_after_preprocess():
    """Target path captures ETL fields before sink preprocess strips them."""
    target = SnapshotStripsFieldTarget()
    target._process_schema_message(
        _schema_message(x_hotglue={"target_state_fields": ["Notes"]})
    )
    target._process_record_message(
        _record_message(
            record={
                "name": "a",
                "externalId": "e1",
                "Notes": "kept from singer",
            }
        )
    )

    sink = target.get_sink("widgets")
    state_entry = sink.latest_state["bookmarks"]["widgets"][0]
    assert state_entry["customData"] == {"Notes": "kept from singer"}


@pytest.mark.parametrize(
    ("x_hotglue", "expected_fields", "expected_include_hash"),
    [
        (None, [], False),
        ("not-a-dict", [], False),
        ({"target_state_fields": []}, [], False),
        (
            {"target_state_fields": ["Notes", 123, None, ""]},
            ["Notes", ""],
            False,
        ),
        ({"target_state_include_hash": True}, [], True),
        ({"target_state_fields": 123}, [], False),
        ({"target_state_fields": "Notes"}, [], False),
        ({"target_state_include_hash": "false"}, [], False),
        ({"target_state_include_hash": False}, [], False),
    ],
)
def test_configure_target_state_snapshot_parsing(
    x_hotglue,
    expected_fields: List[str],
    expected_include_hash: bool,
):
    sink = _make_sink(FakeTarget())
    sink.configure_target_state_snapshot(x_hotglue)
    assert sink._target_state_fields == expected_fields
    assert sink._target_state_include_hash is expected_include_hash


def test_failed_record_skips_custom_data_enrichment():
    target = FakeTarget()
    sink = _make_sink(target, UpsertErrorSink)
    sink.configure_target_state_snapshot(
        {"target_state_fields": ["Notes"], "target_state_include_hash": True}
    )

    sink.process_record(
        {"name": "a", "externalId": "e1", "Notes": "should not appear"},
        context={},
    )

    state_entry = sink.latest_state["bookmarks"]["widgets"][0]
    assert state_entry["success"] is False
    assert "customData" not in state_entry


def test_duplicate_bookmark_skips_custom_data_enrichment():
    target = FakeTarget()
    sink = _make_sink(target)
    sink.configure_target_state_snapshot({"target_state_fields": ["Notes"]})
    sink.init_state()

    sink.update_state(
        {
            "success": True,
            "hash": "existing-hash",
            "id": "id-1",
            "externalId": "e1",
        },
        is_duplicate=True,
        source_record={"Notes": "should not appear"},
    )

    state_entry = sink.latest_state["bookmarks"]["widgets"][0]
    assert "customData" not in state_entry
