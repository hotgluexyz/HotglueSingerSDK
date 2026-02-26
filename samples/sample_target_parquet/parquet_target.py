"""Sample target that writes streams to Parquet files."""

from typing import Dict, Optional

import pandas as pd

from hotglue_singer_sdk import typing as th
from hotglue_singer_sdk.sinks import BatchSink
from hotglue_singer_sdk.target_base import Target


class ParquetSink(BatchSink):
    """Sink that writes each batch to a Parquet file per stream."""

    def process_batch(self, context: dict) -> None:
        """Write batch records to Parquet file."""
        records = context.get("records", [])
        if not records:
            return
        filepath = self._config.get("filepath")
        if filepath:
            base = filepath.rsplit(".", 1)[0] if filepath.endswith(".parquet") else filepath
            path = f"{base}_{self.stream_name}.parquet"
        else:
            path = f"{self.stream_name}.parquet"
        df = pd.DataFrame(records)
        df.to_parquet(path, index=False)


class SampleTargetParquet(Target):
    """Sample target that writes each stream to a Parquet file."""

    name = "sample-target-parquet"
    config_jsonschema = th.PropertiesList(
        th.Property("filepath", th.StringType(), required=True),
    ).to_dict()
    default_sink_class = ParquetSink
