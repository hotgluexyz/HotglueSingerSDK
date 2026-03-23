"""Tests for target_sdk state output building."""

from __future__ import annotations

from typing import List, Optional

from hotglue_singer_sdk.target_sdk.target_base import Target, update_state
from hotglue_singer_sdk.sinks import BatchSink


SIMPLE_SCHEMA = {"type": "object", "properties": {"id": {"type": "integer"}}}


class DummyBatchSink(BatchSink):
    """Batch sink with name bound to stream."""

    name: str

    def __init__(
        self,
        target: Target,
        stream_name: str,
        schema: dict,
        key_properties: Optional[List[str]],
    ) -> None:
        super().__init__(target, stream_name, schema, key_properties)
        self.name = stream_name

    def process_batch(self, context: dict) -> None:
        pass


class DummyTarget(Target):
    """Target capturing state output."""

    name = "dummy_target"
    default_sink_class = DummyBatchSink

    def __init__(self) -> None:
        super().__init__(config={})
        self.state_messages: List[dict] = []
        self.max_parallelism = 1

    def _write_state_message(self, state: dict) -> None:
        self.state_messages.append(state)


def _state_for(stream: str, success: int = 1, fail: int = 0) -> dict:
    return {
        "bookmarks": {stream: [{"id": 1, "success": True}]},
        "summary": {stream: {"success": success, "fail": fail, "existing": 0, "updated": 0}},
    }


def test_update_state_merges_bookmarks_and_summary():
    logger = DummyTarget.logger
    old_state = _state_for("stream1", success=1, fail=0)
    new_state = _state_for("stream1", success=2, fail=1)

    merged = update_state(old_state, new_state, logger)

    assert len(merged["bookmarks"]["stream1"]) == 2
    assert merged["summary"]["stream1"]["success"] == 3
    assert merged["summary"]["stream1"]["fail"] == 1


def test_drain_all_non_streaming_merges_batch_sinks(monkeypatch):
    monkeypatch.delenv("STREAMING_JOB", raising=False)
    target = DummyTarget()

    sink1 = target.add_sink("stream1", SIMPLE_SCHEMA)
    sink2 = target.add_sink("stream2", SIMPLE_SCHEMA)

    sink1.latest_state = _state_for("stream1", success=1)
    sink2.latest_state = _state_for("stream2", success=2)

    target.drain_all()

    assert target.state_messages
    state = target.state_messages[-1]
    assert state["summary"]["stream1"]["success"] == 1
    assert state["summary"]["stream2"]["success"] == 2
    assert "stream1" in state["bookmarks"]
    assert "stream2" in state["bookmarks"]


def test_drain_all_non_streaming_replaces_existing_stream_state(monkeypatch):
    monkeypatch.delenv("STREAMING_JOB", raising=False)
    target = DummyTarget()

    sink1 = target.add_sink("stream1", SIMPLE_SCHEMA)
    sink1.latest_state = _state_for("stream1", success=5)

    target._latest_state = {
        "bookmarks": {"stream1": [{"id": 999, "success": False}]},
        "summary": {"stream1": {"success": 0, "fail": 2, "existing": 0, "updated": 0}},
    }

    target.drain_all()

    state = target.state_messages[-1]
    assert state["summary"]["stream1"]["success"] == 5
    assert state["bookmarks"]["stream1"][0]["id"] == 1


def test_drain_all_streaming_preserves_tap_and_merges_target(monkeypatch):
    monkeypatch.setenv("STREAMING_JOB", "True")
    target = DummyTarget()

    target._latest_state["tap"] = {"pos": "123"}

    sink1 = target.add_sink("stream1", SIMPLE_SCHEMA)
    sink1.latest_state = _state_for("stream1", success=3)

    target.drain_all()

    state = target.state_messages[-1]
    assert state["tap"] == {"pos": "123"}
    assert state["target"]["summary"]["stream1"]["success"] == 3


def test_drain_all_no_batch_sinks_emits_latest_state(monkeypatch):
    monkeypatch.delenv("STREAMING_JOB", raising=False)
    target = DummyTarget()
    target._latest_state = {"bookmarks": {"s": []}, "summary": {"s": {"success": 0, "fail": 0, "existing": 0, "updated": 0}}}

    target.drain_all()

    state = target.state_messages[-1]
    assert state == target._latest_state
