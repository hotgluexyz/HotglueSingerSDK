"""MCP-style stream tool execution (execute-tool)."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Type, TYPE_CHECKING

from hotglue_singer_sdk.helpers._state import write_starting_replication_value
from hotglue_singer_sdk.streams.core import Stream
from hotglue_singer_sdk.tools.constants import get_limit_bounds, resolve_tool_call_limit
from hotglue_singer_sdk.tools.listing import (
    _StreamToolListingStub,
    _parent_stream_name,
    resolve_stream_replication_key,
)

if TYPE_CHECKING:
    from hotglue_singer_sdk.tap_base import Tap


class ToolExecutionError(Exception):
    """Raised when tool validation or execution fails."""


def _stream_class_for_tool_name(
    stream_types: List[Type[Stream]],
    tool_name: str,
) -> Type[Stream]:
    """Resolve a stream class from the tool name."""
    for stream_cls in stream_types:
        if stream_cls.name == tool_name:
            return stream_cls
    raise ToolExecutionError(f"Unknown tool: {tool_name}")


def _stream_class_has_children(
    stream_cls: Type[Stream],
    stream_types: List[Type[Stream]],
) -> bool:
    """Return whether any discovered stream lists this class as its parent."""
    return any(
        getattr(other_cls, "parent_stream_type", None) is stream_cls
        for other_cls in stream_types
    )


def build_selected_filters_from_tool_args(
    stream_name: str,
    filters: Dict[str, Any],
    *,
    filters_version: str,
) -> Dict[str, Any]:
    """Translate named tool filters into selected-filters.json shape."""
    stream_filters: Dict[str, Any] = {}
    for index, (filter_name, filter_def) in enumerate(filters.items(), start=1):
        if index > 1:
            stream_filters[f"operator_{index - 1}"] = "AND"
        if not isinstance(filter_def, dict):
            raise ToolExecutionError(
                f"Filter '{filter_name}' must be an object with operator and value."
            )
        operator = filter_def.get("operator")
        if not isinstance(operator, str) or not operator:
            raise ToolExecutionError(f"Filter '{filter_name}' is missing a valid operator.")
        clause: Dict[str, Any] = {
            "field": filter_name,
            "operator": operator,
        }
        if "value" in filter_def:
            clause["value"] = filter_def["value"]
        stream_filters[f"clause_{index}"] = clause

    return {
        "filters_version": filters_version,
        "streams": {stream_name: stream_filters},
    }


def _validate_filter_arguments(
    stream_cls: Type[Stream],
    filters: Dict[str, Any],
) -> None:
    """Validate named filter keys and operators against stream metadata."""
    listing = _StreamToolListingStub(stream_cls)
    metadata = listing.get_available_filters_metadata()
    if not isinstance(metadata, dict):
        raise ToolExecutionError(f"Stream '{stream_cls.name}' does not support filters.")

    available_filters = metadata.get("filters") or {}
    for filter_name, filter_def in filters.items():
        if filter_name not in available_filters:
            raise ToolExecutionError(f"Unknown filter '{filter_name}' for stream '{stream_cls.name}'.")
        if not isinstance(filter_def, dict):
            raise ToolExecutionError(
                f"Filter '{filter_name}' must be an object with operator and value."
            )
        operator = filter_def.get("operator")
        supported_operators = available_filters[filter_name].get("supported_operators") or []
        if supported_operators and operator not in supported_operators:
            raise ToolExecutionError(
                f"Filter '{filter_name}' operator '{operator}' is not supported. "
                f"Supported operators: {', '.join(supported_operators)}."
            )


def validate_tool_arguments(
    stream_cls: Type[Stream],
    arguments: Dict[str, Any],
) -> None:
    """Validate execute-tool arguments against the stream tool schema."""
    if not isinstance(arguments, dict):
        raise ToolExecutionError("Tool arguments must be a JSON object.")

    parent_name = _parent_stream_name(stream_cls)
    context = arguments.get("context")
    if parent_name:
        if not isinstance(context, dict):
            raise ToolExecutionError(
                f"Tool '{stream_cls.name}' requires a 'context' object copied from a "
                f"{parent_name} record's child_context."
            )
    elif context is not None and not isinstance(context, dict):
        raise ToolExecutionError("'context' must be an object when provided.")

    filters = arguments.get("filters")
    if filters is not None:
        if not isinstance(filters, dict):
            raise ToolExecutionError("'filters' must be an object keyed by filter name.")
        _validate_filter_arguments(stream_cls, filters)

    replication_key_value = arguments.get("replication_key_value")
    if replication_key_value is not None and not isinstance(replication_key_value, str):
        raise ToolExecutionError("'replication_key_value' must be a string when provided.")

    limit = arguments.get("limit")
    if limit is not None:
        if not isinstance(limit, int) or isinstance(limit, bool):
            raise ToolExecutionError("'limit' must be an integer when provided.")
        _, maximum = get_limit_bounds(stream_cls)
        if limit < 1 or limit > maximum:
            raise ToolExecutionError(f"'limit' must be between 1 and {maximum}.")

    unknown_keys = set(arguments) - {"context", "filters", "replication_key_value", "limit"}
    if unknown_keys:
        unknown = ", ".join(sorted(unknown_keys))
        raise ToolExecutionError(f"Unknown tool argument(s): {unknown}.")


def _seed_replication_key_value(
    stream: Stream,
    context: Optional[dict],
    replication_key_value: str,
) -> None:
    """Write the tool-provided replication bookmark into stream state."""
    state = stream.get_context_state(context)
    write_starting_replication_value(state, replication_key_value)


def collect_tool_records(
    stream: Stream,
    context: Optional[dict],
    *,
    limit: int,
    attach_child_context: bool,
) -> tuple[List[Dict[str, Any]], bool]:
    """Collect records from a stream up to the resolved limit."""
    records: List[Dict[str, Any]] = []
    truncated = False

    for record_result in stream.get_records(context):
        if isinstance(record_result, tuple):
            record = record_result[0]
        else:
            record = record_result

        output_record = dict(record)
        if attach_child_context:
            output_record["child_context"] = stream.get_child_context(record, None)

        records.append(output_record)
        if len(records) >= limit:
            truncated = True
            break

    return records, truncated


def build_tool_result(
    stream_cls: Type[Stream],
    records: List[Dict[str, Any]],
    *,
    truncated: bool,
) -> Dict[str, Any]:
    """Build the execute-tool JSON result wrapper."""
    result: Dict[str, Any] = {
        "records": records,
        "truncated": truncated,
    }

    if not truncated:
        return result

    replication_key = resolve_stream_replication_key(stream_cls)
    if replication_key and records:
        next_value = records[-1].get(replication_key)
        if next_value is not None:
            result["next_replication_key_value"] = str(next_value)

    return result


def execute_stream_tool(
    tap: "Tap",
    tool_name: str,
    arguments: Dict[str, Any],
) -> Dict[str, Any]:
    """Execute a stream tool and return the structured result payload."""
    stream_types = tap.discover_stream_types()
    stream_cls = _stream_class_for_tool_name(stream_types, tool_name)
    validate_tool_arguments(stream_cls, arguments)

    filters = arguments.get("filters")
    if filters:
        tap._selected_filters = build_selected_filters_from_tool_args(
            stream_cls.name,
            filters,
            filters_version=tap.available_filters_version,
        )

    stream = stream_cls(tap)
    context = arguments.get("context")
    limit = resolve_tool_call_limit(stream_cls, arguments.get("limit"))

    replication_key_value = arguments.get("replication_key_value")
    if replication_key_value:
        _seed_replication_key_value(stream, context, replication_key_value)

    attach_child_context = _stream_class_has_children(stream_cls, stream_types)
    records, truncated = collect_tool_records(
        stream,
        context,
        limit=limit,
        attach_child_context=attach_child_context,
    )
    return build_tool_result(stream_cls, records, truncated=truncated)
