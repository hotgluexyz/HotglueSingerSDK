"""Tests for TargetHotglue.get_record_id snapshot lookup flag."""

from __future__ import annotations

import pandas as pd

from hotglue_singer_sdk.target_sdk.target import TargetHotglue


class _FakeTarget:
    EXTERNAL_ID_KEY = "externalId"
    GLOBAL_PRIMARY_KEY = "id"
    _latest_state = {}

    def __init__(self, config: dict) -> None:
        self._config = config

    @property
    def config(self) -> dict:
        return self._config

    def read_snapshot(self, object_name: str) -> pd.DataFrame:
        return pd.DataFrame([{"InputId": "e1", "RemoteId": "remote-1"}])


def test_resolve_ids_from_snapshot_false_skips_id_injection():
    target = _FakeTarget({"resolve_ids_from_snapshot": False})
    record = {"externalId": "e1"}
    result = TargetHotglue.get_record_id(target, "widgets", record)
    assert "id" not in result


def test_resolve_ids_from_snapshot_default_resolves_id():
    target = _FakeTarget({})
    record = {"externalId": "e1"}
    result = TargetHotglue.get_record_id(target, "widgets", record)
    assert result["id"] == "remote-1"
