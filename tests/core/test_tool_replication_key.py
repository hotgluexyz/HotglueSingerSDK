"""Tests for MCP replication bookmark coercion."""

import pytest

from hotglue_singer_sdk.streams.core import Stream
from hotglue_singer_sdk.tap_base import Tap
from hotglue_singer_sdk.tools.errors import ToolExecutionError
from hotglue_singer_sdk.tools.listing import build_tool_input_schema
from hotglue_singer_sdk.tools.replication_key import (
    coerce_tool_replication_key_value,
    replication_key_value_kind,
)
from hotglue_singer_sdk.typing import DateTimeType, IntegerType, PropertiesList, Property

from tests.core.test_tool_listing import ParentStream


class EpochReplicationStream(Stream):
    """Stream with an integer epoch replication key."""

    name = "events"
    replication_key = "generated_at"
    schema = PropertiesList(
        Property("id", IntegerType, required=True),
        Property("generated_at", IntegerType),
    ).to_dict()

    def __init__(self, tap: Tap) -> None:
        super().__init__(tap, schema=self.schema, name=self.name)

    def get_records(self, context):
        yield {"id": 1, "generated_at": 1_699_920_969}


def test_replication_key_value_kind_integer() -> None:
    assert replication_key_value_kind(EpochReplicationStream) == "integer"
    assert replication_key_value_kind(ParentStream) == "datetime"


def test_coerce_integer_replication_from_string() -> None:
    assert coerce_tool_replication_key_value(EpochReplicationStream, "1699920969") == 1699920969
    assert coerce_tool_replication_key_value(EpochReplicationStream, 1699920969) == 1699920969


def test_coerce_integer_replication_rejects_bool() -> None:
    with pytest.raises(ToolExecutionError, match="boolean"):
        coerce_tool_replication_key_value(EpochReplicationStream, True)


def test_build_tool_input_schema_describes_integer_replication() -> None:
    schema = build_tool_input_schema(EpochReplicationStream)
    replication_schema = schema["properties"]["replication_key_value"]
    assert "Unix epoch" in replication_schema["description"]
    assert {"type": "integer"} in replication_schema["anyOf"]


def test_build_tool_input_schema_describes_datetime_replication() -> None:
    schema = build_tool_input_schema(ParentStream)
    replication_schema = schema["properties"]["replication_key_value"]
    assert "ISO-8601" in replication_schema["description"]
    assert replication_schema["anyOf"] == [{"type": "string", "format": "date-time"}]
