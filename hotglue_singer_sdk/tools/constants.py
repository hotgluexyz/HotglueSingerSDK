"""Default limits for MCP-style stream tool execution."""

from __future__ import annotations

from typing import Any, Dict, Optional, Tuple, Type

from hotglue_singer_sdk.streams.core import Stream

DEFAULT_TOOL_CALL_RECORD_LIMIT = 50
MAX_TOOL_CALL_RECORD_LIMIT = 200

TOOL_CALL_DEFAULT_RECORD_LIMIT_ATTR = "tool_call_default_record_limit"
TOOL_CALL_MAX_RECORD_LIMIT_ATTR = "tool_call_max_record_limit"


def _resolve_stream_class_int_attr(stream_cls: Type[Stream], attr_name: str) -> Optional[int]:
    """Return a positive int class attribute from the stream MRO, if declared."""
    for cls in stream_cls.__mro__:
        if cls is Stream:
            break
        value = cls.__dict__.get(attr_name)
        if isinstance(value, int) and not isinstance(value, bool) and value > 0:
            return value
    return None


def get_limit_bounds(stream_cls: Type[Stream]) -> Tuple[int, int]:
    """Return default and maximum record limits for a stream tool."""
    default = _resolve_stream_class_int_attr(stream_cls, TOOL_CALL_DEFAULT_RECORD_LIMIT_ATTR)
    maximum = _resolve_stream_class_int_attr(stream_cls, TOOL_CALL_MAX_RECORD_LIMIT_ATTR)

    if default is None:
        default = DEFAULT_TOOL_CALL_RECORD_LIMIT
    if maximum is None:
        maximum = MAX_TOOL_CALL_RECORD_LIMIT

    default = min(default, maximum)
    return default, maximum


def resolve_tool_call_limit(stream_cls: Type[Stream], requested: Optional[int]) -> int:
    """Resolve the effective record limit for a tool call."""
    default, maximum = get_limit_bounds(stream_cls)
    if requested is None:
        return default
    return min(requested, maximum)


def build_limit_input_schema_property(stream_cls: Type[Stream]) -> Dict[str, Any]:
    """Build the JSON Schema fragment for the optional limit argument."""
    default, maximum = get_limit_bounds(stream_cls)
    return {
        "type": "integer",
        "minimum": 1,
        "default": default,
        "maximum": maximum,
        "description": "Maximum number of records to return.",
    }


def build_filter_value_input_schema_property() -> Dict[str, Any]:
    """Build the JSON Schema fragment for a named filter's value argument."""
    return {
        "description": "Filter value (type depends on field and operator).",
        "anyOf": [
            {"type": "string"},
            {"type": "number"},
            {"type": "integer"},
            {"type": "boolean"},
            {"type": "array"},
            {"type": "null"},
        ],
    }
