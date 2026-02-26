"""Sample target that writes to a SQLite database."""

from typing import Any, Dict

from hotglue_singer_sdk import typing as th
from hotglue_singer_sdk.sinks.sql import SQLSink
from hotglue_singer_sdk.target_base import SQLTarget
from samples.sample_tap_sqlite import SQLiteConnector


class SQLiteSink(SQLSink):
    """SQLite sink using path_to_db config."""

    connector_class = SQLiteConnector


class SQLiteTarget(SQLTarget):
    """Sample target that loads streams into SQLite."""

    name = "sample-target-sqlite"
    config_jsonschema = th.PropertiesList(
        th.Property("path_to_db", th.StringType(), required=True),
    ).to_dict()
    default_sink_class = SQLiteSink
