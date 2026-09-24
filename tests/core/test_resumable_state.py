"""Tests for resumable interim STATE on sorted leaf streams."""

from __future__ import annotations

import io
import json
from contextlib import redirect_stdout
from typing import Any, Dict, Iterable, List, Optional

from hotglue_singer_sdk import Stream, Tap
from hotglue_singer_sdk.helpers._state import (
    PROGRESS_MARKERS,
    SIGNPOST_MARKER,
    STARTING_MARKER,
)
from hotglue_singer_sdk.io_base import SingerMessageType
from hotglue_singer_sdk.typing import (
    DateTimeType,
    IntegerType,
    PropertiesList,
    Property,
    StringType,
)


class SortedLeafStream(Stream):
    name = "sorted_leaf"
    schema = PropertiesList(
        Property("id", IntegerType, required=True),
        Property("value", StringType, required=True),
        Property("updatedAt", DateTimeType, required=True),
    ).to_dict()
    replication_key = "updatedAt"
    STATE_MSG_FREQUENCY = 2

    @property
    def is_sorted(self) -> bool:
        return True

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        yield {"id": 1, "value": "a", "updatedAt": "2021-01-01T00:00:00Z"}
        yield {"id": 2, "value": "b", "updatedAt": "2021-01-02T00:00:00Z"}
        yield {"id": 3, "value": "c", "updatedAt": "2021-01-03T00:00:00Z"}
        yield {"id": 4, "value": "d", "updatedAt": "2021-01-04T00:00:00Z"}


class UnsortedLeafStream(SortedLeafStream):
    name = "unsorted_leaf"

    @property
    def is_sorted(self) -> bool:
        return False


class SortedParentStream(SortedLeafStream):
    name = "sorted_parent"

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        return {"pid": record["id"]}


class SortedChildStream(Stream):
    name = "sorted_child"
    schema = PropertiesList(
        Property("id", IntegerType, required=True),
        Property("pid", IntegerType, required=True),
    ).to_dict()
    parent_stream_type = SortedParentStream

    @property
    def is_sorted(self) -> bool:
        return True

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        yield {"id": 1, "pid": context["pid"]}


def _parse_messages(buf: io.StringIO) -> List[dict]:
    buf.seek(0)
    return [json.loads(line) for line in buf.read().splitlines() if line.strip()]


def _state_messages(messages: List[dict]) -> List[dict]:
    return [m for m in messages if m["type"] == SingerMessageType.STATE]


def test_sorted_leaf_interim_state_matches_records_and_keeps_markers():
    class SortedLeafTap(Tap):
        name = "sorted-leaf-tap"

        def discover_streams(self) -> List[Stream]:
            return [SortedLeafStream(self)]

    tap = SortedLeafTap(config={"start_date": "2020-01-01"}, parse_env_config=False)
    buf = io.StringIO()
    with redirect_stdout(buf):
        tap.sync_all()

    messages = _parse_messages(buf)
    states = _state_messages(messages)
    # After records 2 and 4, plus final STATE
    assert len(states) == 3

    first_interim = states[0]["value"]["bookmarks"]["sorted_leaf"]
    assert first_interim["replication_key"] == "updatedAt"
    assert first_interim["replication_key_value"] == "2021-01-02T00:00:00Z"
    assert PROGRESS_MARKERS not in first_interim
    assert STARTING_MARKER in first_interim
    assert SIGNPOST_MARKER in first_interim

    second_interim = states[1]["value"]["bookmarks"]["sorted_leaf"]
    assert second_interim["replication_key_value"] == "2021-01-04T00:00:00Z"
    assert PROGRESS_MARKERS not in second_interim
    assert STARTING_MARKER in second_interim
    assert SIGNPOST_MARKER in second_interim


def test_unsorted_leaf_interim_state_keeps_progress_markers():
    class UnsortedLeafTap(Tap):
        name = "unsorted-leaf-tap"

        def discover_streams(self) -> List[Stream]:
            return [UnsortedLeafStream(self)]

    tap = UnsortedLeafTap(config={"start_date": "2020-01-01"}, parse_env_config=False)
    buf = io.StringIO()
    with redirect_stdout(buf):
        tap.sync_all()

    states = _state_messages(_parse_messages(buf))
    assert len(states) >= 2
    interim = states[0]["value"]["bookmarks"]["unsorted_leaf"]
    assert PROGRESS_MARKERS in interim
    assert (
        interim[PROGRESS_MARKERS]["replication_key_value"] == "2021-01-02T00:00:00Z"
    )


def test_sorted_parent_with_children_keeps_interim_markers_in_emitted_state():
    class ParentChildTap(Tap):
        name = "parent-child-tap"

        def discover_streams(self) -> List[Stream]:
            return [SortedParentStream(self), SortedChildStream(self)]

    tap = ParentChildTap(config={"start_date": "2020-01-01"}, parse_env_config=False)
    parent = tap.streams["sorted_parent"]
    parent.STATE_MSG_FREQUENCY = 1
    assert parent.child_streams
    assert not parent._emits_resumable_interim_state()

    buf = io.StringIO()
    with redirect_stdout(buf):
        tap.sync_all()

    parent_bookmarks = [
        m["value"]["bookmarks"]["sorted_parent"]
        for m in _state_messages(_parse_messages(buf))
        if "sorted_parent" in m["value"].get("bookmarks", {})
    ]
    assert parent_bookmarks
    interim_with_markers = [
        bookmark
        for bookmark in parent_bookmarks
        if STARTING_MARKER in bookmark or SIGNPOST_MARKER in bookmark
    ]
    assert interim_with_markers
