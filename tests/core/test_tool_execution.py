"""Tests for execute-tool / MCP tool execution."""

import json
from typing import List, Optional

import pytest

from hotglue_singer_sdk.streams.core import Stream
from hotglue_singer_sdk.tap_base import CliTestOptionValue, Tap
from hotglue_singer_sdk.tools.execution import (
    ToolExecutionError,
    build_selected_filters_from_tool_args,
    build_tool_result,
    collect_tool_records,
    execute_stream_tool,
)
from hotglue_singer_sdk.typing import DateTimeType, IntegerType, PropertiesList, Property

from tests.core.test_tool_listing import ChildStream, FilteredStream, ParentStream, ToolCallsTestTap

CONFIG_START_DATE = "2021-01-01"


class ManyRecordsStream(Stream):
    name = "many"
    replication_key = "lastmodifieddate"
    schema = PropertiesList(
        Property("id", IntegerType, required=True),
        Property("lastmodifieddate", DateTimeType),
    ).to_dict()

    def __init__(self, tap: Tap) -> None:
        super().__init__(tap, schema=self.schema, name=self.name)

    def get_records(self, context: Optional[dict]):
        for index in range(1, 6):
            yield {"id": index, "lastmodifieddate": f"2021-01-0{index}"}

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        return {"ids": [record["id"]]}


class FilterTrackingStream(FilteredStream):
    _last_instance: Optional["FilterTrackingStream"] = None

    def __init__(self, tap: Tap) -> None:
        self.setup_selected_filters_called = False
        super().__init__(tap)
        FilterTrackingStream._last_instance = self

    def setup_selected_filters(self) -> None:
        self.setup_selected_filters_called = True


class TupleChildContextStream(Stream):
    name = "tuple_parent"
    schema = ParentStream.schema

    def __init__(self, tap: Tap) -> None:
        super().__init__(tap, schema=self.schema, name=self.name)

    def get_records(self, context: Optional[dict]):
        yield ({"id": 1, "lastmodifieddate": "2021-01-01"}, {"ids": ["from_tuple"]})

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        if context is not None:
            return context
        return {"ids": [record["id"]]}


class ExecutionTestTap(Tap):
    name = "test-tap-execution"

    settings_jsonschema = PropertiesList(Property("start_date", DateTimeType)).to_dict()

    def discover_streams(self) -> List[Stream]:
        return [
            ParentStream(self),
            ChildStream(self),
            ManyRecordsStream(self),
            FilterTrackingStream(self),
        ]


def test_build_selected_filters_from_tool_args() -> None:
    payload = build_selected_filters_from_tool_args(
        FilteredStream,
        {
            "vendor_id": {"operator": "EQ", "value": "123"},
            "status": {"operator": "IN", "value": ["Open"]},
        },
        filters_version="1.0.0",
    )

    assert payload["filters_version"] == "1.0.0"
    stream_filters = payload["streams"]["vendor"]
    assert stream_filters["clause_1"] == {
        "field": "v.id",
        "operator": "EQ",
        "value": "123",
    }
    assert stream_filters["operator_1"] == "AND"
    assert stream_filters["clause_2"] == {
        "field": "v.status",
        "operator": "IN",
        "value": ["Open"],
    }


def test_build_tool_result_includes_next_replication_key_when_truncated() -> None:
    result = build_tool_result(
        ManyRecordsStream,
        [{"id": 1, "lastmodifieddate": "2021-01-01"}],
        truncated=True,
    )

    assert result == {
        "records": [{"id": 1, "lastmodifieddate": "2021-01-01"}],
        "truncated": True,
        "next_replication_key_value": "2021-01-01",
    }


def test_collect_tool_records_attaches_child_context() -> None:
    tap = ToolCallsTestTap(config={"start_date": CONFIG_START_DATE}, parse_env_config=False)
    stream = ParentStream(tap)

    records, truncated = collect_tool_records(
        stream,
        None,
        limit=10,
        attach_child_context=True,
    )

    assert records == [{"id": 1, "lastmodifieddate": "2021-01-01", "child_context": {"ids": [1]}}]
    assert truncated is False


def test_collect_tool_records_preserves_tuple_child_context() -> None:
    tap = ExecutionTestTap(config={"start_date": CONFIG_START_DATE}, parse_env_config=False)
    stream = TupleChildContextStream(tap)

    records, truncated = collect_tool_records(
        stream,
        None,
        limit=10,
        attach_child_context=True,
    )

    assert records == [
        {
            "id": 1,
            "lastmodifieddate": "2021-01-01",
            "child_context": {"ids": ["from_tuple"]},
        }
    ]
    assert truncated is False


def test_run_cli_mode_rejects_tool_args_without_execute_tool() -> None:
    tap = ToolCallsTestTap(config={"start_date": CONFIG_START_DATE}, parse_env_config=False)

    with pytest.raises(SystemExit):
        Tap._run_cli_mode(
            tap,
            discover=False,
            catalog=None,
            state=None,
            test=CliTestOptionValue.Disabled.value,
            get_available_filters=False,
            list_tools=False,
            execute_tool=None,
            tool_args="args.json",
        )


def test_run_cli_mode_rejects_empty_execute_tool() -> None:
    tap = ToolCallsTestTap(config={"start_date": CONFIG_START_DATE}, parse_env_config=False)

    with pytest.raises(SystemExit):
        Tap._run_cli_mode(
            tap,
            discover=False,
            catalog=None,
            state=None,
            test=CliTestOptionValue.Disabled.value,
            get_available_filters=False,
            list_tools=False,
            execute_tool="",
            tool_args=None,
        )


def test_collect_tool_records_respects_limit() -> None:
    tap = ExecutionTestTap(config={"start_date": CONFIG_START_DATE}, parse_env_config=False)
    stream = ManyRecordsStream(tap)

    records, truncated = collect_tool_records(stream, None, limit=2, attach_child_context=False)

    assert len(records) == 2
    assert truncated is True


def test_collect_tool_records_not_truncated_at_exact_limit() -> None:
    tap = ExecutionTestTap(config={"start_date": CONFIG_START_DATE}, parse_env_config=False)
    stream = ManyRecordsStream(tap)

    records, truncated = collect_tool_records(stream, None, limit=5, attach_child_context=False)

    assert len(records) == 5
    assert truncated is False


def test_execute_stream_tool_rejects_non_object_arguments() -> None:
    tap = ToolCallsTestTap(config={"start_date": CONFIG_START_DATE}, parse_env_config=False)

    with pytest.raises(ToolExecutionError, match="Tool arguments must be a JSON object"):
        execute_stream_tool(tap, "bills", [])


def test_execute_stream_tool_parent_includes_child_context() -> None:
    tap = ToolCallsTestTap(config={"start_date": CONFIG_START_DATE}, parse_env_config=False)

    result = execute_stream_tool(tap, "bills", {})

    assert result["truncated"] is False
    assert result["records"] == [
        {
            "id": 1,
            "lastmodifieddate": "2021-01-01",
            "child_context": {"ids": [1]},
        }
    ]


def test_execute_stream_tool_child_requires_context() -> None:
    tap = ToolCallsTestTap(config={"start_date": CONFIG_START_DATE}, parse_env_config=False)

    with pytest.raises(ToolExecutionError, match="requires a 'context' object"):
        execute_stream_tool(tap, "bill_lines", {})


def test_execute_stream_tool_child_uses_context() -> None:
    tap = ToolCallsTestTap(config={"start_date": CONFIG_START_DATE}, parse_env_config=False)

    result = execute_stream_tool(tap, "bill_lines", {"context": {"ids": [1]}})

    assert result["records"] == [{"id": 1, "transaction": 1}]


def test_execute_stream_tool_applies_named_filters() -> None:
    tap = ExecutionTestTap(config={"start_date": CONFIG_START_DATE}, parse_env_config=False)

    execute_stream_tool(
        tap,
        "vendor",
        {"filters": {"vendor_id": {"operator": "EQ", "value": "123"}}},
    )

    assert tap._selected_filters is not None
    assert tap._selected_filters["streams"]["vendor"]["clause_1"]["field"] == "v.id"
    assert FilterTrackingStream._last_instance is not None
    assert FilterTrackingStream._last_instance.setup_selected_filters_called is True


def test_execute_stream_tool_seeds_replication_key_value() -> None:
    tap = ToolCallsTestTap(config={"start_date": CONFIG_START_DATE}, parse_env_config=False)

    execute_stream_tool(tap, "bills", {"replication_key_value": "2024-01-01T00:00:00Z"})

    stream = ParentStream(tap)
    assert stream.get_starting_replication_key_value(None) == "2024-01-01T00:00:00Z"


def test_execute_stream_tool_rejects_invalid_limit() -> None:
    tap = ExecutionTestTap(config={"start_date": CONFIG_START_DATE}, parse_env_config=False)

    with pytest.raises(ToolExecutionError, match="'limit' must be between"):
        execute_stream_tool(tap, "many", {"limit": 0})

    with pytest.raises(ToolExecutionError, match="'limit' must be between"):
        execute_stream_tool(tap, "many", {"limit": 999})


def test_execute_stream_tool_rejects_invalid_filter_operator() -> None:
    tap = ExecutionTestTap(config={"start_date": CONFIG_START_DATE}, parse_env_config=False)

    with pytest.raises(ToolExecutionError, match="is not supported"):
        execute_stream_tool(
            tap,
            "vendor",
            {"filters": {"vendor_id": {"operator": "GT", "value": "1"}}},
        )


def test_execute_stream_tool_rejects_missing_filter_value() -> None:
    tap = ExecutionTestTap(config={"start_date": CONFIG_START_DATE}, parse_env_config=False)

    with pytest.raises(ToolExecutionError, match="Filter 'vendor_id' is missing a value"):
        execute_stream_tool(
            tap,
            "vendor",
            {"filters": {"vendor_id": {"operator": "IN"}}},
        )


def test_execute_stream_tool_unknown_filter() -> None:
    tap = ExecutionTestTap(config={"start_date": CONFIG_START_DATE}, parse_env_config=False)

    with pytest.raises(ToolExecutionError, match="Unknown filter"):
        execute_stream_tool(
            tap,
            "vendor",
            {"filters": {"missing": {"operator": "EQ", "value": "1"}}},
        )


def test_execute_stream_tool_unknown_tool() -> None:
    tap = ToolCallsTestTap(config={"start_date": CONFIG_START_DATE}, parse_env_config=False)

    with pytest.raises(ToolExecutionError, match="Unknown tool"):
        execute_stream_tool(tap, "missing", {})


def test_execute_tool_prints_json(capsys: pytest.CaptureFixture[str]) -> None:
    tap = ToolCallsTestTap(config={"start_date": CONFIG_START_DATE}, parse_env_config=False)
    tap.execute_tool("bills", {})

    payload = json.loads(capsys.readouterr().out)
    assert payload["truncated"] is False
    assert payload["records"][0]["child_context"] == {"ids": [1]}


def test_execute_tool_without_arguments_uses_defaults(capsys: pytest.CaptureFixture[str]) -> None:
    tap = ToolCallsTestTap(config={"start_date": CONFIG_START_DATE}, parse_env_config=False)
    tap.execute_tool("bills")

    payload = json.loads(capsys.readouterr().out)
    assert payload["records"]
