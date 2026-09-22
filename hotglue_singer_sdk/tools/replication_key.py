"""Replication bookmark typing and coercion for MCP stream tools."""

from __future__ import annotations

from typing import Any, Dict, Optional, Set, Type

from hotglue_singer_sdk.helpers._typing import is_datetime_type
from hotglue_singer_sdk.streams.core import Stream
from hotglue_singer_sdk.tools.errors import ToolExecutionError


def resolve_stream_replication_key(stream_cls: Type[Stream]) -> Optional[str]:
    """Return a stream class replication key when declared as a class attribute."""
    for cls in stream_cls.__mro__:
        if cls is Stream:
            break
        replication_key = cls.__dict__.get("replication_key")
        if isinstance(replication_key, str):
            return replication_key
    return None


def replication_key_property_schema(stream_cls: Type[Stream]) -> Optional[Dict[str, Any]]:
    """Return the JSON Schema fragment for the stream replication key field."""
    replication_key = resolve_stream_replication_key(stream_cls)
    if not replication_key:
        return None

    schema = getattr(stream_cls, "schema", None)
    if not isinstance(schema, dict):
        return None

    property_schema = schema.get("properties", {}).get(replication_key)
    if not isinstance(property_schema, dict):
        return None

    return property_schema


def _json_schema_types(property_schema: Dict[str, Any]) -> Set[str]:
    """Collect non-null JSON Schema type strings from a property definition."""
    if "anyOf" in property_schema:
        types: Set[str] = set()
        for option in property_schema["anyOf"]:
            if isinstance(option, dict):
                types |= _json_schema_types(option)
        return types

    schema_type = property_schema.get("type")
    if isinstance(schema_type, list):
        return {item for item in schema_type if isinstance(item, str) and item != "null"}
    if isinstance(schema_type, str) and schema_type != "null":
        return {schema_type}
    return set()


def replication_key_value_kind(stream_cls: Type[Stream]) -> str:
    """Classify how tool replication bookmarks should be interpreted for a stream."""
    property_schema = replication_key_property_schema(stream_cls)
    if not property_schema:
        return "unknown"

    if is_datetime_type(property_schema):
        return "datetime"

    types = _json_schema_types(property_schema)
    if "integer" in types:
        return "integer"
    if "number" in types:
        return "number"
    if "string" in types:
        return "string"
    return "unknown"


def build_replication_key_value_input_schema_property(stream_cls: Type[Stream]) -> Dict[str, Any]:
    """Build the inputSchema fragment for replication_key_value."""
    replication_key = resolve_stream_replication_key(stream_cls)
    assert replication_key is not None
    kind = replication_key_value_kind(stream_cls)
    base = f"Only return records where {replication_key} is at or after this value."
    if kind == "datetime":
        description = f"{base} ISO-8601 date-time string."
    elif kind == "integer":
        description = f"{base} Unix epoch seconds (integer or decimal string)."
    elif kind == "number":
        description = f"{base} Numeric value (number or decimal string)."
    else:
        description = base

    if kind == "datetime":
        return {
            "description": description,
            "anyOf": [
                {"type": "string", "format": "date-time"},
            ],
        }

    if kind == "integer":
        return {
            "description": description,
            "anyOf": [
                {"type": "integer"},
                {
                    "type": "string",
                    "description": "Decimal string (e.g. from next_replication_key_value).",
                },
            ],
        }

    if kind == "number":
        return {
            "description": description,
            "anyOf": [
                {"type": "number"},
                {
                    "type": "string",
                    "description": "Decimal string (e.g. from next_replication_key_value).",
                },
            ],
        }

    return {"type": "string", "description": description}


def _parse_integer_replication_value(raw: Any, stream_cls: Type[Stream]) -> int:
    """Parse a tool bookmark into an integer replication value."""
    if isinstance(raw, bool):
        raise ToolExecutionError("'replication_key_value' must not be a boolean.")
    if isinstance(raw, int):
        return raw
    if isinstance(raw, float):
        if not raw.is_integer():
            raise ToolExecutionError(
                f"'replication_key_value' must be an integer for stream '{stream_cls.name}'."
            )
        return int(raw)
    if isinstance(raw, str):
        stripped = raw.strip()
        try:
            return int(stripped, 10)
        except ValueError:
            raise ToolExecutionError(
                f"'replication_key_value' must be an integer for stream '{stream_cls.name}'."
            ) from None
    raise ToolExecutionError(
        "'replication_key_value' must be a string, integer, or number when provided."
    )


def _parse_number_replication_value(raw: Any, stream_cls: Type[Stream]) -> float:
    """Parse a tool bookmark into a numeric replication value."""
    if isinstance(raw, bool):
        raise ToolExecutionError("'replication_key_value' must not be a boolean.")
    if isinstance(raw, (int, float)):
        return float(raw)
    if isinstance(raw, str):
        try:
            return float(raw.strip())
        except ValueError:
            raise ToolExecutionError(
                f"'replication_key_value' must be a number for stream '{stream_cls.name}'."
            ) from None
    raise ToolExecutionError(
        "'replication_key_value' must be a string, integer, or number when provided."
    )


def _parse_string_replication_value(raw: Any) -> str:
    """Parse a tool bookmark into a string replication value."""
    if isinstance(raw, bool):
        raise ToolExecutionError("'replication_key_value' must not be a boolean.")
    if isinstance(raw, str):
        return raw
    if isinstance(raw, (int, float)):
        raise ToolExecutionError(
            "'replication_key_value' must be a string when the replication key is not numeric."
        )
    raise ToolExecutionError(
        "'replication_key_value' must be a string, integer, or number when provided."
    )


def coerce_tool_replication_key_value(stream_cls: Type[Stream], raw: Any) -> Any:
    """Coerce an MCP replication bookmark to the type the stream expects in state."""
    kind = replication_key_value_kind(stream_cls)
    if kind == "integer":
        return _parse_integer_replication_value(raw, stream_cls)
    if kind == "number":
        return _parse_number_replication_value(raw, stream_cls)
    if kind in {"datetime", "string"}:
        return _parse_string_replication_value(raw)
    if isinstance(raw, bool):
        raise ToolExecutionError("'replication_key_value' must not be a boolean.")
    if isinstance(raw, (int, float)):
        return raw
    if isinstance(raw, str):
        return raw
    raise ToolExecutionError(
        "'replication_key_value' must be a string, integer, or number when provided."
    )


def validate_replication_key_value_argument(stream_cls: Type[Stream], raw: Any) -> None:
    """Validate and dry-run coercion for replication_key_value."""
    coerce_tool_replication_key_value(stream_cls, raw)
