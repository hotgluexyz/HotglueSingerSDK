"""Sample target that writes streams to CSV files."""

import csv
import os
from typing import Dict, List, Optional

from hotglue_singer_sdk import typing as th
from hotglue_singer_sdk.sinks import RecordSink
from hotglue_singer_sdk.target_base import Target


class CSVSink(RecordSink):
    """Sink that writes records to a CSV file per stream."""

    def __init__(
        self,
        target: Target,
        stream_name: str,
        schema: Dict,
        key_properties: Optional[List[str]],
    ) -> None:
        super().__init__(target, stream_name, schema, key_properties)
        self._file = None
        self._writer = None
        self._headers_written = False
        self._header_list: Optional[List[str]] = None

    def _get_path(self) -> str:
        folder = self._config.get("target_folder", ".")
        os.makedirs(folder, exist_ok=True)
        return os.path.join(folder, f"{self.stream_name}.csv")

    def process_record(self, record: dict, context: dict) -> None:
        """Append record to the stream's CSV file."""
        if self._file is None:
            self._file = open(self._get_path(), "w", newline="", encoding="utf-8")
            self._writer = csv.writer(self._file)
        if self._header_list is None:
            self._header_list = self._get_headers() or list(record.keys())
        if not self._headers_written:
            self._writer.writerow(self._header_list)
            self._headers_written = True
        row = [record.get(h) for h in self._header_list]
        self._writer.writerow(row)

    def _get_headers(self) -> List[str]:
        if self.schema and self.schema.get("properties"):
            return list(self.schema["properties"].keys())
        return []

    def __del__(self) -> None:
        if getattr(self, "_file", None) is not None:
            try:
                self._file.close()
            except Exception:
                pass


class SampleTargetCSV(Target):
    """Sample target that writes each stream to a CSV file in target_folder."""

    name = "sample-target-csv"
    config_jsonschema = th.PropertiesList(
        th.Property("target_folder", th.StringType(), required=True),
    ).to_dict()
    default_sink_class = CSVSink
