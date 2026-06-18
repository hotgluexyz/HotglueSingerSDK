"""Tests for estimated record totals with parent-child streams."""

import json
from pathlib import Path
from typing import Iterable, List, Optional

from hotglue_singer_sdk.helpers._catalog import (
    deselect_all_streams,
    set_catalog_stream_selected,
)
from hotglue_singer_sdk.helpers._singer import Catalog
from hotglue_singer_sdk.streams.core import Stream
from hotglue_singer_sdk.tap_base import Tap
from hotglue_singer_sdk.typing import IntegerType, PropertiesList, Property, StringType


class BaseEstimatedStream(Stream):
    schema = PropertiesList(
        Property("id", IntegerType, required=True),
        Property("value", StringType, required=True),
    ).to_dict()
    estimated_record_count = 7

    def __init__(self, tap: Tap):
        super().__init__(tap, schema=self.schema, name=self.name)

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        yield {"id": 1, "value": "test"}

    def get_estimated_record_count(self) -> Optional[int]:
        #the base class will probably have the get_estimated_record_count implemented;
        #it is the SDK job to figure out what stream to emit it or not
        return self.estimated_record_count


class ParentWithChildEstimatedStream(BaseEstimatedStream):
    name = "parentWithChild"
    estimated_record_count = 1

class ParentWithoutChildEstimatedStream(BaseEstimatedStream):
    name = "parentWithoutChild"
    estimated_record_count = 2

class ChildEstimatedStream(BaseEstimatedStream):
    name = "child"
    parent_stream_type = ParentWithChildEstimatedStream
    estimated_record_count = 10


class EstimatedRecordTotalsParentChildTap(Tap):
    name = "estimated-record-totals-parent-child-tap"

    def discover_streams(self) -> List[Stream]:
        return [
            ParentWithChildEstimatedStream(self),
            ParentWithoutChildEstimatedStream(self),
            ChildEstimatedStream(self),
        ]


def _catalog_with_selected_streams(*stream_names: str) -> Catalog:
    tap = EstimatedRecordTotalsParentChildTap(
        config={},
        parse_env_config=False,
    )
    catalog = Catalog.from_dict(tap.catalog_dict)
    deselect_all_streams(catalog)
    for stream_name in stream_names:
        set_catalog_stream_selected(catalog, stream_name, selected=True)
    return catalog


def _emit_estimated_record_totals(
    tmp_path: Path,
    *selected_streams: str,
) -> Optional[dict]:
    tap = EstimatedRecordTotalsParentChildTap(
        config={"hg_sync_output": str(tmp_path)},
        catalog=_catalog_with_selected_streams(*selected_streams),
        parse_env_config=False,
    )

    tap._emit_estimated_record_totals_snapshot()

    metrics_path = tmp_path / "estimated_job_metrics.json"
    if not metrics_path.exists():
        return None

    return json.loads(metrics_path.read_text())


def test_emit_estimated_record_totals_skips_child_when_only_child_selected(
    tmp_path: Path,
):
    assert _emit_estimated_record_totals(tmp_path, "child") is None


def test_emit_estimated_record_totals_emits_parent_when_parent_and_child_selected(
    tmp_path: Path,
):
    assert _emit_estimated_record_totals(tmp_path, "parentWithChild", "child") == {
        "estimatedRecordCount": {"parentWithChild": 1},
    }


def test_emit_estimated_record_totals_emits_parent_when_only_parent_selected(
    tmp_path: Path,
):
    assert _emit_estimated_record_totals(tmp_path, "parentWithChild") == {
        "estimatedRecordCount": {"parentWithChild": 1},
    }


def test_emit_estimated_record_totals_emits_parents_when_parents_and_child_selected(
    tmp_path: Path,
):
    assert _emit_estimated_record_totals(
        tmp_path, "parentWithChild", "parentWithoutChild", "child"
    ) == {
        "estimatedRecordCount": {"parentWithChild": 1, "parentWithoutChild": 2},
    }

def test_emit_estimated_record_totals_emits_parent_without_child_when_child_selected(
    tmp_path: Path,
):
    assert _emit_estimated_record_totals(tmp_path, "parentWithoutChild", "child") == {
        "estimatedRecordCount": {"parentWithoutChild": 2},
    }