"""Tests for target_sdk sink state handling."""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

import pytest

from hotglue_etl_exceptions import InvalidPayloadError

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


class CustomDataSink(CapturingSink):
    def upsert_record(self, record: dict, context: dict):
        id, success, _ = super().upsert_record(record, context)
        return id, success, {
            "customData": {
                "Notes": "from-target",
                "validation": "hgi-10581",
            }
        }


class PreprocessStripsFieldSink(CapturingSink):
    def preprocess_record(self, record: dict, context: dict) -> dict:
        record = dict(record)
        record.pop("Notes", None)
        return record


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


class UpsertErrorSink(HotglueSink):
    name = "widgets"
    endpoint = "/widgets"
    base_url = "https://example.com"

    @property
    def unified_schema(self):
        return None

    def preprocess_record(self, record: dict, context: dict) -> dict:
        return record

    def upsert_record(self, record: dict, context: dict):
        raise ValueError("upsert failed")


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


class BatchPreprocessErrorSink(HotglueBatchSink):
    name = "widgets"
    endpoint = "/widgets"
    base_url = "https://example.com"

    @property
    def unified_schema(self):
        return None

    def process_batch_record(self, record: dict, index: int) -> dict:
        if record.get("fail"):
            raise InvalidPayloadError("missing sku")
        return record

    def make_batch_request(self, records: List[dict]):
        return {"records": records}

    def handle_batch_response(self, response) -> dict:
        return {
            "state_updates": [
                {"success": True, "id": record["id"]}
                for record in response["records"]
            ]
        }


class BatchRequestErrorSink(HotglueBatchSink):
    name = "widgets"
    endpoint = "/widgets"
    base_url = "https://example.com"

    @property
    def unified_schema(self):
        return None

    def make_batch_request(self, records: List[dict]):
        raise InvalidPayloadError("batch request failed")

    def handle_batch_response(self, response) -> dict:
        return {"state_updates": []}


class BatchMixedBatchSink(HotglueBatchSink):
    name = "widgets"
    endpoint = "/widgets"
    base_url = "https://example.com"

    @property
    def unified_schema(self):
        return None

    def process_batch_record(self, record: dict, index: int) -> dict:
        if record.get("fail"):
            raise InvalidPayloadError("invalid record")
        return record

    def make_batch_request(self, records: List[dict]):
        return {"records": records}

    def handle_batch_response(self, response) -> dict:
        return {
            "state_updates": [
                {"success": True, "id": record["id"]}
                for record in response["records"]
            ]
        }


class BatchNoneResponseSink(HotglueBatchSink):
    name = "widgets"
    endpoint = "/widgets"
    base_url = "https://example.com"

    @property
    def unified_schema(self):
        return None

    def make_batch_request(self, records: List[dict]):
        return None

    def handle_batch_response(self, response) -> dict:
        return {"state_updates": [{"success": True, "id": "handled-none"}]}


class BatchResponseErrorSink(HotglueBatchSink):
    name = "widgets"
    endpoint = "/widgets"
    base_url = "https://example.com"

    @property
    def unified_schema(self):
        return None

    def process_batch_record(self, record: dict, index: int) -> dict:
        if record.get("fail"):
            raise InvalidPayloadError("invalid record")
        return record

    def make_batch_request(self, records: List[dict]):
        return {"records": records}

    def handle_batch_response(self, response) -> dict:
        raise ValueError("handler failed")


class BatchTransformedRequestErrorSink(HotglueBatchSink):
    name = "widgets"
    endpoint = "/widgets"
    base_url = "https://example.com"

    @property
    def unified_schema(self):
        return None

    def process_batch_record(self, record: dict, index: int) -> dict:
        return {"api_id": record["id"]}

    def make_batch_request(self, records: List[dict]):
        raise InvalidPayloadError("batch request failed")

    def handle_batch_response(self, response) -> dict:
        return {"state_updates": []}


class BatchInPlaceMutationRequestErrorSink(HotglueBatchSink):
    name = "widgets"
    endpoint = "/widgets"
    base_url = "https://example.com"

    @property
    def unified_schema(self):
        return None

    def process_batch_record(self, record: dict, index: int) -> dict:
        record.pop("id", None)
        record.pop("externalId", None)
        record["api_id"] = "mutated"
        return record

    def make_batch_request(self, records: List[dict]):
        raise InvalidPayloadError("batch request failed")

    def handle_batch_response(self, response) -> dict:
        return {"state_updates": []}


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


def test_target_state_fields_in_custom_data():
    target = FakeTarget()
    sink = _make_sink(target)
    sink.configure_target_state_snapshot(
        {"target_state_fields": ["Notes"], "target_state_include_hash": False}
    )

    record = {"name": "a", "externalId": "e1", "Notes": "snapshot note"}
    sink.process_record(record, context={})

    state_entry = sink.latest_state["bookmarks"]["widgets"][0]
    assert state_entry["customData"] == {"Notes": "snapshot note"}


class PreprocessMutatesNestedSink(CapturingSink):
    def preprocess_record(self, record: dict, context: dict) -> dict:
        record["details"]["Notes"] = "mutated"
        return record


def test_target_state_fields_use_deepcopy_for_nested_values():
    target = FakeTarget()
    sink = _make_sink(target, PreprocessMutatesNestedSink)
    sink.configure_target_state_snapshot({"target_state_fields": ["details"]})

    record = {
        "name": "a",
        "externalId": "e1",
        "details": {"Notes": "original"},
    }
    sink.process_record(record, context={})

    state_entry = sink.latest_state["bookmarks"]["widgets"][0]
    assert state_entry["customData"] == {"details": {"Notes": "original"}}


def test_target_state_include_hash_with_empty_source_record():
    target = FakeTarget()
    sink = _make_sink(target)
    sink.configure_target_state_snapshot({"target_state_include_hash": True})
    sink.init_state()

    sink.update_state(
        {"success": True, "hash": "empty-record-hash", "id": "id-1", "externalId": "e1"},
        source_record={},
    )

    state_entry = sink.latest_state["bookmarks"]["widgets"][0]
    assert state_entry["customData"] == {"hash": "empty-record-hash"}


def test_target_state_fields_use_source_record_before_preprocess():
    target = FakeTarget()
    sink = _make_sink(target, PreprocessStripsFieldSink)
    sink.configure_target_state_snapshot({"target_state_fields": ["Notes"]})

    record = {"name": "a", "externalId": "e1", "Notes": "kept from singer"}
    sink.process_record(record, context={})

    state_entry = sink.latest_state["bookmarks"]["widgets"][0]
    assert state_entry["customData"] == {"Notes": "kept from singer"}


def test_target_state_include_hash_in_custom_data():
    target = FakeTarget()
    sink = _make_sink(target)
    sink.configure_target_state_snapshot({"target_state_include_hash": True})

    record = {"name": "a", "externalId": "e1"}
    sink.process_record(record, context={})

    state_entry = sink.latest_state["bookmarks"]["widgets"][0]
    assert state_entry["customData"]["hash"] == state_entry["hash"]


def test_target_custom_data_wins_merge():
    target = FakeTarget()
    sink = _make_sink(target, CustomDataSink)
    sink.configure_target_state_snapshot(
        {"target_state_fields": ["Notes"], "target_state_include_hash": True}
    )

    record = {"name": "a", "externalId": "e1", "Notes": "from-etl"}
    sink.process_record(record, context={})

    state_entry = sink.latest_state["bookmarks"]["widgets"][0]
    assert state_entry["customData"] == {
        "Notes": "from-target",
        "hash": state_entry["hash"],
        "validation": "hgi-10581",
    }


def test_no_custom_data_without_snapshot_config():
    target = FakeTarget()
    sink = _make_sink(target)

    record = {"name": "a", "externalId": "e1", "Notes": "ignored"}
    sink.process_record(record, context={})

    state_entry = sink.latest_state["bookmarks"]["widgets"][0]
    assert "customData" not in state_entry


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


def test_upsert_error_writes_id_and_external_id():
    target = FakeTarget()
    sink = _make_sink(target, UpsertErrorSink)

    record = {"id": "source-1", "name": "a", "externalId": "ext-1"}
    expected_hash = sink.build_record_hash(record)

    sink.process_record(record, context={})

    state_entry = sink.latest_state["bookmarks"]["widgets"][0]
    assert state_entry["success"] is False
    assert "upsert failed" in state_entry["error"]
    assert state_entry["id"] == "source-1"
    assert state_entry["externalId"] == "ext-1"
    assert state_entry["hash"] == expected_hash
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


def test_batch_preprocess_error_writes_state_and_continues():
    target = FakeTarget()
    sink = _make_sink(target, BatchPreprocessErrorSink)

    sink.process_batch(
        {
            "records": [
                {"id": "good-1"},
                {"id": "bad-1", "fail": True, "externalId": "ext-bad"},
            ]
        }
    )

    bookmarks = sink.latest_state["bookmarks"]["widgets"]
    assert len(bookmarks) == 2

    failed = next(entry for entry in bookmarks if entry.get("id") == "bad-1")
    assert failed["success"] is False
    assert "missing sku" in failed["error"]
    assert failed["hg_error_class"] == "InvalidPayloadError"
    assert failed["externalId"] == "ext-bad"

    succeeded = next(entry for entry in bookmarks if entry.get("id") == "good-1")
    assert succeeded["success"] is True
    assert sink.latest_state["summary"]["widgets"]["fail"] == 1
    assert sink.latest_state["summary"]["widgets"]["success"] == 1


def test_batch_request_error_writes_state_for_each_record():
    target = FakeTarget()
    sink = _make_sink(target, BatchRequestErrorSink)

    sink.process_batch(
        {
            "records": [
                {"id": "sku-1", "externalId": "ext-1"},
                {"id": "sku-2"},
            ]
        }
    )

    bookmarks = sink.latest_state["bookmarks"]["widgets"]
    assert len(bookmarks) == 2
    assert all(entry["success"] is False for entry in bookmarks)
    assert all("batch request failed" in entry["error"] for entry in bookmarks)
    assert all(entry["hg_error_class"] == "InvalidPayloadError" for entry in bookmarks)
    assert bookmarks[0]["id"] == "sku-1"
    assert bookmarks[0]["externalId"] == "ext-1"
    assert bookmarks[1]["id"] == "sku-2"
    assert sink.latest_state["summary"]["widgets"]["fail"] == 2


def test_batch_mixed_preprocess_error_still_processes_valid_records():
    target = FakeTarget()
    sink = _make_sink(target, BatchMixedBatchSink)

    sink.process_batch(
        {
            "records": [
                {"id": "bad", "fail": True},
                {"id": "good"},
            ]
        }
    )

    bookmarks = sink.latest_state["bookmarks"]["widgets"]
    assert len(bookmarks) == 2

    failed = next(entry for entry in bookmarks if entry.get("id") == "bad")
    assert failed["success"] is False
    assert failed["hg_error_class"] == "InvalidPayloadError"

    succeeded = next(entry for entry in bookmarks if entry.get("id") == "good")
    assert succeeded["success"] is True
    assert sink.latest_state["summary"]["widgets"]["fail"] == 1
    assert sink.latest_state["summary"]["widgets"]["success"] == 1


def test_batch_none_response_still_runs_handle_batch_response():
    target = FakeTarget()
    sink = _make_sink(target, BatchNoneResponseSink)

    sink.process_batch({"records": [{"id": "ignored"}]})

    bookmarks = sink.latest_state["bookmarks"]["widgets"]
    assert len(bookmarks) == 1
    assert bookmarks[0]["success"] is True
    assert bookmarks[0]["id"] == "handled-none"


def test_batch_preprocess_errors_persist_when_handle_batch_response_raises():
    target = FakeTarget()
    sink = _make_sink(target, BatchResponseErrorSink)

    with pytest.raises(ValueError, match="handler failed"):
        sink.process_batch(
            {
                "records": [
                    {"id": "good"},
                    {"id": "bad", "fail": True, "externalId": "ext-bad"},
                ]
            }
        )

    bookmarks = sink.latest_state["bookmarks"]["widgets"]
    assert len(bookmarks) == 1
    failed = bookmarks[0]
    assert failed["success"] is False
    assert failed["id"] == "bad"
    assert failed["externalId"] == "ext-bad"


def test_batch_request_error_uses_raw_record_identifiers():
    target = FakeTarget()
    sink = _make_sink(target, BatchTransformedRequestErrorSink)

    sink.process_batch({"records": [{"id": "source-1", "externalId": "ext-1"}]})

    bookmarks = sink.latest_state["bookmarks"]["widgets"]
    assert len(bookmarks) == 1
    failed = bookmarks[0]
    assert failed["success"] is False
    assert failed["id"] == "source-1"
    assert failed["externalId"] == "ext-1"


def test_batch_request_error_preserves_identifiers_after_inplace_preprocess():
    target = FakeTarget()
    sink = _make_sink(target, BatchInPlaceMutationRequestErrorSink)

    sink.process_batch({"records": [{"id": "source-1", "externalId": "ext-1"}]})

    bookmarks = sink.latest_state["bookmarks"]["widgets"]
    assert len(bookmarks) == 1
    failed = bookmarks[0]
    assert failed["success"] is False
    assert failed["id"] == "source-1"
    assert failed["externalId"] == "ext-1"
