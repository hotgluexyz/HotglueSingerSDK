"""Tests for list-tools / MCP tool discovery."""

import json
from typing import Any, Dict, List, Optional, Type

import pytest

from hotglue_singer_sdk.helpers.capabilities import TapCapabilities
from hotglue_singer_sdk.streams.core import Stream
from hotglue_singer_sdk.tap_base import Tap
from hotglue_singer_sdk.tools.constants import (
    DEFAULT_TOOL_CALL_RECORD_LIMIT,
    MAX_TOOL_CALL_RECORD_LIMIT,
    build_filter_value_input_schema_property,
    get_limit_bounds,
)
from hotglue_singer_sdk.tools.listing import (
    build_tool_catalog_from_stream_types,
    build_tool_input_schema,
    build_tool_output_schema,
    format_connector_label,
    resolve_stream_replication_key,
    stream_to_tool_descriptor,
)
from hotglue_singer_sdk.typing import DateTimeType, IntegerType, PropertiesList, Property

CONFIG_START_DATE = "2021-01-01"
MCP_TOOL_KEYS = frozenset({"name", "description", "inputSchema", "outputSchema"})


class ParentStream(Stream):
    name = "bills"
    replication_key = "lastmodifieddate"
    schema = PropertiesList(
        Property("id", IntegerType, required=True),
        Property("lastmodifieddate", DateTimeType),
    ).to_dict()

    def __init__(self, tap: Tap) -> None:
        super().__init__(tap, schema=self.schema, name=self.name)

    def get_records(self, context: Optional[dict]):
        yield {"id": 1, "lastmodifieddate": "2021-01-01"}

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        return {"ids": [record["id"]]}


class ChildStream(Stream):
    name = "bill_lines"
    parent_stream_type = ParentStream
    schema = ParentStream.schema

    def __init__(self, tap: Tap) -> None:
        super().__init__(tap, schema=self.schema, name=self.name)

    def get_records(self, context: Optional[dict]):
        yield {"id": 1, "transaction": context["ids"][0]}


class FilteredStream(Stream):
    name = "vendor"
    replication_key = "lastmodifieddate"
    schema = ParentStream.schema

    def __init__(self, tap: Tap) -> None:
        super().__init__(tap, schema=self.schema, name=self.name)

    def get_records(self, context: Optional[dict]):
        yield {"id": 1}

    def get_available_filters_metadata(self) -> Dict[str, Any]:
        return {
            "supported_operators": ["AND"],
            "supports_nesting_clauses": True,
            "filters": {
                "vendor_id": {
                    "label": "Vendor ID",
                    "supported_operators": ["EQ", "IN"],
                    "target_field": "v.id",
                }
            },
        }


class LimitedStream(Stream):
    name = "limited"
    tool_call_default_record_limit = 5
    tool_call_max_record_limit = 25
    schema = ParentStream.schema

    def __init__(self, tap: Tap) -> None:
        super().__init__(tap, schema=self.schema, name=self.name)

    def get_records(self, context: Optional[dict]):
        yield {"id": 1}


class SubFilteredStream(FilteredStream):
    name = "sub_vendor"


class EmptyFiltersStream(Stream):
    name = "empty_filters"
    schema = ParentStream.schema

    def __init__(self, tap: Tap) -> None:
        super().__init__(tap, schema=self.schema, name=self.name)

    def get_records(self, context: Optional[dict]):
        yield {"id": 1}

    def get_available_filters_metadata(self) -> Dict[str, Any]:
        return {"filters": {}}


class PlainStream(Stream):
    name = "term"
    schema = ParentStream.schema

    def __init__(self, tap: Tap) -> None:
        super().__init__(tap, schema=self.schema, name=self.name)

    def get_records(self, context: Optional[dict]):
        yield {"id": 1}


class PropertyReplicationKeyStream(Stream):
    name = "property_key"
    schema = ParentStream.schema

    def __init__(self, tap: Tap) -> None:
        super().__init__(tap, schema=self.schema, name=self.name)

    def get_records(self, context: Optional[dict]):
        yield {"id": 1}

    @property
    def replication_key(self) -> str:
        return "from_property"


class ToolCallsTestTap(Tap):
    name = "test-tap-tools"

    settings_jsonschema = PropertiesList(Property("start_date", DateTimeType)).to_dict()

    def discover_streams(self) -> List[Stream]:
        return [
            ParentStream(self),
            ChildStream(self),
            FilteredStream(self),
            PlainStream(self),
        ]


@pytest.mark.parametrize(
    ("stream_cls", "tap_name", "expected"),
    [
        pytest.param(
            ParentStream,
            "tap-example",
            {
                "name": "bills",
                "description": "Query bills records from example",
                "properties": ["limit", "replication_key_value"],
                "required": None,
                "absent": ["filters", "context"],
                "replication_key_in_description": "lastmodifieddate",
            },
            id="parent",
        ),
        pytest.param(
            PlainStream,
            "tap-example",
            {
                "name": "term",
                "description": "Query term records from example",
                "properties": ["limit"],
                "required": None,
                "absent": ["filters", "context", "replication_key_value"],
            },
            id="plain",
        ),
        pytest.param(
            ChildStream,
            "tap-example",
            {
                "name": "bill_lines",
                "description": (
                    "Query bill_lines records from example "
                    "(child of bills stream). Pass context copied from a bills record's child_context."
                ),
                "properties": ["limit", "context"],
                "required": ["context"],
                "absent": ["filters", "replication_key_value"],
                "context_description_contains": "child_context",
            },
            id="child",
        ),
        pytest.param(
            FilteredStream,
            "tap-example",
            {
                "name": "vendor",
                "description": "Query vendor records from example",
                "properties": ["limit", "filters", "replication_key_value"],
                "required": None,
                "absent": ["context"],
                "filter_name": "vendor_id",
                "filter_operators": ["EQ", "IN"],
            },
            id="filtered",
        ),
    ],
)
def test_stream_to_tool_descriptor(
    stream_cls: Type[Stream],
    tap_name: str,
    expected: Dict[str, Any],
) -> None:
    descriptor = stream_to_tool_descriptor(stream_cls, tap_name)

    assert set(descriptor.keys()) == MCP_TOOL_KEYS
    assert descriptor["name"] == expected["name"]
    assert descriptor["description"] == expected["description"]
    assert descriptor["outputSchema"] == build_tool_output_schema()

    schema = descriptor["inputSchema"]
    properties = schema["properties"]
    for prop_name in expected["properties"]:
        assert prop_name in properties
    for prop_name in expected["absent"]:
        assert prop_name not in properties

    if expected["required"] is None:
        assert schema.get("required") is None
    else:
        assert schema["required"] == expected["required"]

    if "filter_name" in expected:
        filter_schema = properties["filters"]["properties"][expected["filter_name"]]
        assert filter_schema["properties"]["operator"]["enum"] == expected["filter_operators"]
        assert (
            filter_schema["properties"]["value"] == build_filter_value_input_schema_property()
        )

    if "replication_key_in_description" in expected:
        replication_key_description = properties["replication_key_value"]["description"]
        assert expected["replication_key_in_description"] in replication_key_description
        assert "property object" not in replication_key_description

    if "context_description_contains" in expected:
        assert expected["context_description_contains"] in properties["context"]["description"]


def test_build_tool_input_schema_skips_empty_filter_definitions() -> None:
    schema = build_tool_input_schema(EmptyFiltersStream)

    assert "filters" not in schema["properties"]
    assert "limit" in schema["properties"]


def test_filter_value_schema_declares_portable_validation_keywords() -> None:
    """Filter value schemas must include type constraints for MCP client portability."""
    value_schema = build_filter_value_input_schema_property()
    assert "anyOf" in value_schema
    declared_types = {option["type"] for option in value_schema["anyOf"]}
    assert declared_types == {"string", "number", "integer", "boolean", "array", "null"}

    filter_value = build_tool_input_schema(FilteredStream)["properties"]["filters"][
        "properties"
    ]["vendor_id"]["properties"]["value"]
    assert filter_value == value_schema


def test_resolve_stream_replication_key() -> None:
    assert resolve_stream_replication_key(ParentStream) == "lastmodifieddate"
    assert resolve_stream_replication_key(SubFilteredStream) == "lastmodifieddate"
    assert resolve_stream_replication_key(PlainStream) is None
    assert resolve_stream_replication_key(PropertyReplicationKeyStream) is None


def test_get_limit_bounds_uses_sdk_defaults() -> None:
    assert get_limit_bounds(PlainStream) == (
        DEFAULT_TOOL_CALL_RECORD_LIMIT,
        MAX_TOOL_CALL_RECORD_LIMIT,
    )


def test_get_limit_bounds_uses_stream_overrides() -> None:
    assert get_limit_bounds(LimitedStream) == (5, 25)

    limit_schema = build_tool_input_schema(LimitedStream)["properties"]["limit"]
    assert limit_schema["default"] == 5
    assert limit_schema["maximum"] == 25


@pytest.mark.parametrize(
    ("connector_name", "expected"),
    [
        ("tap-netsuite-rest", "netsuite-rest"),
        ("target-s3", "s3"),
        ("my-connector", "my-connector"),
    ],
)
def test_format_connector_label(connector_name: str, expected: str) -> None:
    assert format_connector_label(connector_name) == expected


def test_build_tool_catalog_from_stream_types() -> None:
    catalog = build_tool_catalog_from_stream_types(
        [ParentStream, ParentStream, ChildStream, FilteredStream, PlainStream],
        "tap-example",
    )

    assert [tool["name"] for tool in catalog] == [
        "bill_lines",
        "bills",
        "term",
        "vendor",
    ]
    assert all(set(tool.keys()) == MCP_TOOL_KEYS for tool in catalog)

    bills_tool = next(tool for tool in catalog if tool["name"] == "bills")
    assert bills_tool["description"] == "Query bills records from example"

    child_tool = next(tool for tool in catalog if tool["name"] == "bill_lines")
    assert child_tool["inputSchema"]["required"] == ["context"]


def test_list_available_tools_prints_json(capsys: pytest.CaptureFixture[str]) -> None:
    tap = ToolCallsTestTap(config={"start_date": CONFIG_START_DATE}, parse_env_config=False)
    tap.list_available_tools()

    tools = json.loads(capsys.readouterr().out)
    assert len(tools) == 4
    assert all(set(tool.keys()) == MCP_TOOL_KEYS for tool in tools)


def test_tap_capabilities_include_tool_calls() -> None:
    assert TapCapabilities.TOOL_CALLS in ToolCallsTestTap.capabilities
