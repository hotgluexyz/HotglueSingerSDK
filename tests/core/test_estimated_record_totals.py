"""Tests for estimated record totals snapshot."""

import json
from pathlib import Path
from typing import Iterable, List, Optional

from hotglue_singer_sdk.streams.core import Stream
from hotglue_singer_sdk.tap_base import Tap
from hotglue_singer_sdk.typing import IntegerType, PropertiesList, Property, StringType


class EstimatedCountStream(Stream):
    name = "estimated"
    schema = PropertiesList(
        Property("id", IntegerType, required=True),
        Property("value", StringType, required=True),
    ).to_dict()

    def __init__(self, tap: Tap, estimated_total: Optional[int] = None):
        super().__init__(tap, schema=self.schema, name=self.name)
        self._estimated_total = estimated_total

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        yield {"id": 1, "value": "test"}

    def get_estimated_record_count(self, context: Optional[dict] = None) -> Optional[int]:
        return self._estimated_total


class UnsupportedCountStream(EstimatedCountStream):
    name = "unsupported"


class EstimatedRecordTotalsTap(Tap):
    name = "estimated-record-totals-tap"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.load_streams()

    def discover_streams(self) -> List[Stream]:
        return [
            EstimatedCountStream(self, estimated_total=42),
            UnsupportedCountStream(self, estimated_total=None),
        ]


def test_emit_estimated_record_totals_snapshot_writes_metrics(tmp_path: Path):
    tap = EstimatedRecordTotalsTap(
        config={
            "hg_sync_output": str(tmp_path),
        },
        parse_env_config=False,
    )

    tap._emit_estimated_record_totals_snapshot()

    metrics_path = tmp_path / "estimated_job_metrics.json"
    assert metrics_path.exists()
    assert json.loads(metrics_path.read_text()) == {
        "estimatedRecordCount": {"estimated": 42},
    }


def test_write_estimated_total_metric_merges_existing_entries(tmp_path: Path):
    metrics_path = tmp_path / "estimated_job_metrics.json"
    metrics_path.write_text(
        json.dumps({"estimatedRecordCount": {"existing": 10}}),
    )

    tap = EstimatedRecordTotalsTap(
        config={"hg_sync_output": str(tmp_path)},
        parse_env_config=False,
    )

    tap._write_estimated_total_metric("new_stream", 99)

    assert json.loads(metrics_path.read_text()) == {
        "estimatedRecordCount": {"existing": 10, "new_stream": 99},
    }
