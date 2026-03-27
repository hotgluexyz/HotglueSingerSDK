"""Tests for target_sdk sink state handling."""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

import pytest

from hotglue_singer_sdk.target_sdk.client import HotglueBaseSink, HotglueBatchSink, HotglueSink


class FakeTarget:
    name = "fake_target"
    EXTERNAL_ID_KEY = "externalId"

    def __init__(self, config: Optional[dict] = None, state: Optional[dict] = None, state_path: str | None = None) -> None:
        self._config = config or {}
        self._state = state or {}
        self.incremental_target_state_path = state_path or "/tmp/nonexistent_state.json"
        self._latest_state: dict = {}
        self.logger = logging.getLogger("test")

    @property
    def config(self) -> Dict[str, Any]:
        return self._config


class CapturingSink(HotglueSink):
    name = "widgets"
    endpoint = "/widgets"
    base_url = "https://example.com"
    allows_externalid: List[str] = []

    def __init__(self, *args, **kwargs) -> None:
        self.last_payload: Optional[dict] = None
        super().__init__(*args, **kwargs)

    @property
    def unified_schema(self):
        return None

    def preprocess_record(self, record: dict, context: dict) -> dict:
        return record

    def upsert_record(self, record: dict, context: dict):
        self.last_payload = dict(record)
        return "id-1", True, {}


class CapturingSinkExternalAllowed(CapturingSink):
    allows_externalid = ["widgets"]


class ErrorSink(HotglueSink):
    name = "widgets"
    endpoint = "/widgets"
    base_url = "https://example.com"

    @property
    def unified_schema(self):
        return None

    def preprocess_record(self, record: dict, context: dict) -> dict:
        raise ValueError("boom")

    def upsert_record(self, record: dict, context: dict):
        return "id-1", True, {}


class BatchStateSink(HotglueBatchSink):
    name = "widgets"
    endpoint = "/widgets"
    base_url = "https://example.com"

    @property
    def unified_schema(self):
        return None

    def make_batch_request(self, records: List[dict]):
        return {"ok": True}

    def handle_batch_response(self, response) -> dict:
        return {
            "state_updates": [
                {"success": True, "id": "b1", "hash": "h1"},
                {"success": False, "error": "bad"},
            ]
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


def _make_sink(target: FakeTarget, sink_cls=CapturingSink):
    schema = {"type": "object", "properties": {}}
    return sink_cls(target=target, stream_name="widgets", schema=schema, key_properties=[])


def test_hash_deterministic():
    target = FakeTarget()
    sink = _make_sink(target)

    record = {"name": "a", "externalId": "e1"}
    h1 = sink.build_record_hash(record)
    h2 = sink.build_record_hash(record)
    assert h1 == h2


def test_externalid_removed_from_payload_but_in_state():
    target = FakeTarget()
    sink = _make_sink(target, CapturingSink)

    # for standard externalId
    record = {"name": "a", "externalId": "e1"}
    sink.process_record(record, context={})

    assert sink.last_payload is not None
    assert "externalId" not in sink.last_payload

    state_entry = sink.latest_state["bookmarks"]["widgets"][0]
    assert state_entry["externalId"] == "e1"
    assert state_entry["hash"] == sink.build_record_hash({"name": "a", "externalId": "e1"})

    # for externalid in lowercase (backward compatibility)
    record = {"name": "a", "externalid": "e2"}
    sink.process_record(record, context={})

    assert sink.last_payload is not None
    assert "externalid" not in sink.last_payload

    state_entry = sink.latest_state["bookmarks"]["widgets"][1]
    assert state_entry["externalId"] == "e2"
    assert state_entry["hash"] == sink.build_record_hash({"name": "a", "externalId": "e2"})


def test_externalid_kept_in_payload_when_allowed():
    target = FakeTarget()
    sink = _make_sink(target, CapturingSinkExternalAllowed)

    record = {"name": "a", "externalId": "e1"}
    sink.process_record(record, context={})

    assert sink.last_payload is not None
    assert sink.last_payload["externalId"] == "e1"

    state_entry = sink.latest_state["bookmarks"]["widgets"][0]
    assert state_entry["externalId"] == "e1"

    # for externalid in lowercase (backward compatibility)
    record = {"name": "a", "externalid": "e2"}
    sink.process_record(record, context={})

    assert sink.last_payload is not None
    assert sink.last_payload["externalid"] == "e2"

    state_entry = sink.latest_state["bookmarks"]["widgets"][1]
    assert state_entry["externalId"] == "e2"


def test_error_writes_state_and_summary_fail():
    target = FakeTarget()
    sink = _make_sink(target, ErrorSink)

    sink.process_record({"name": "a"}, context={})

    state_entry = sink.latest_state["bookmarks"]["widgets"][0]
    assert state_entry["success"] is False
    assert "boom" in state_entry["error"]
    assert sink.latest_state["summary"]["widgets"]["fail"] == 1


def test_output_mapped_record_flag(monkeypatch):
    monkeypatch.setenv("OUTPUT_MAPPED_RECORD", "true")
    target = FakeTarget()
    sink = _make_sink(target, CapturingSink)

    record = {"name": "a", "externalId": "e1"}
    sink.process_record(record, context={})

    state_entry = sink.latest_state["bookmarks"]["widgets"][0]
    assert state_entry["mapped_record"]["name"] == "a"


def test_duplicate_hash_skips_processing():
    target = FakeTarget()
    sink = _make_sink(target, CapturingSink)

    record = {"name": "a", "externalId": "e1"}
    h = sink.build_record_hash(record)
    sink.processed_hashes.append(h)

    sink.process_record(record, context={})

    bookmarks = sink.latest_state["bookmarks"]["widgets"]
    assert len(bookmarks) == 0
    assert sink.latest_state["summary"]["widgets"]["success"] == 0


def test_get_previous_state_sanitizes_failures(tmp_path):
    previous = {
        "bookmarks": {
            "widgets": [
                {"hash": "h1", "success": True},
                {"hash": "h2", "success": False, "error": "bad"},
            ]
        },
        "summary": {"widgets": {"success": 1, "fail": 1, "existing": 0, "updated": 0}},
    }
    state_path = tmp_path / "state.json"
    state_path.write_text(json.dumps(previous))

    target = FakeTarget(state_path=str(state_path))
    sink = _make_sink(target, CapturingSink)

    sink.init_state()
    assert target._state == {}
    sanitized = sink.previous_state
    assert len(sanitized["bookmarks"]["widgets"]) == 1
    assert sanitized["summary"]["widgets"]["fail"] == 0
    assert target._latest_state == sanitized
    assert "h1" in sink.processed_hashes
    assert "h2" not in sink.processed_hashes


def test_batch_state_updates_propagate():
    target = FakeTarget()
    sink = _make_sink(target, BatchStateSink)

    sink.init_state()
    sink.process_batch({"records": [{"id": 1}, {"id": 2}]})

    bookmarks = sink.latest_state["bookmarks"]["widgets"]
    assert len(bookmarks) == 2
    assert sink.latest_state["summary"]["widgets"]["fail"] == 1
    assert sink.latest_state["summary"]["widgets"]["success"] == 1
