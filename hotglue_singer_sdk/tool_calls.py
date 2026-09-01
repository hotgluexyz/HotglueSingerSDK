"""Helpers for MCP-style tap tool discovery (list-tools)."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Type

from hotglue_singer_sdk.streams.core import Stream


class _StreamToolListingStub:
    """Stream class stand-in for tool listing without schema initialization."""

    def __init__(self, stream_cls: Type[Stream]) -> None:
        self.name = stream_cls.name
        self.parent_stream_type = stream_cls.parent_stream_type
        self.replication_key = resolve_stream_replication_key(stream_cls)
        self._stream_cls = stream_cls

    def get_available_filters_metadata(self) -> Optional[Dict[str, Any]]:
        if not _stream_overrides_filters_metadata(self._stream_cls):
            return None
        return self._stream_cls.get_available_filters_metadata(self)


def _stream_overrides_filters_metadata(stream_cls: Type[Stream]) -> bool:
    return stream_cls.get_available_filters_metadata is not Stream.get_available_filters_metadata


def resolve_stream_replication_key(stream_cls: Type[Stream]) -> Optional[str]:
    """Return a stream class replication key when declared as a class attribute."""
    for cls in stream_cls.__mro__:
        if cls is Stream:
            break
        replication_key = cls.__dict__.get("replication_key")
        if isinstance(replication_key, str):
            return replication_key
    return None


def format_connector_label(connector_name: str) -> str:
    """Normalize connector names for human-readable tool descriptions."""
    for prefix in ("tap-", "target-"):
        if connector_name.startswith(prefix):
            return connector_name[len(prefix) :]
    return connector_name


def _parent_stream_name(stream_cls: Type[Stream]) -> Optional[str]:
    parent_type = stream_cls.parent_stream_type
    if parent_type is None:
        return None
    return getattr(parent_type, "name", None)


def _tool_description(stream_cls: Type[Stream], tap_name: str) -> str:
    connector_label = format_connector_label(tap_name)
    parent_name = _parent_stream_name(stream_cls)
    if parent_name:
        return (
            f"Query {stream_cls.name} records from {connector_label} "
            f"(child of {parent_name} stream). "
            f"Pass context copied from a {parent_name} record's child_context."
        )
    return f"Query {stream_cls.name} records from {connector_label}"


def _build_named_filters_schema(filter_metadata: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    filters = filter_metadata.get("filters") or {}
    if not filters:
        return None

    filter_properties: Dict[str, Any] = {}
    for filter_name, filter_def in filters.items():
        if not isinstance(filter_def, dict):
            continue
        operators = filter_def.get("supported_operators") or []
        operator_schema: Dict[str, Any] = {"type": "string"}
        if operators:
            operator_schema["enum"] = operators
        filter_properties[filter_name] = {
            "type": "object",
            "description": filter_def.get("label", filter_name),
            "required": ["operator", "value"],
            "properties": {
                "operator": operator_schema,
                "value": {"description": "Filter value"},
            },
        }

    if not filter_properties:
        return None

    return {
        "type": "object",
        "description": "Optional filters keyed by filter name.",
        "properties": filter_properties,
    }


def build_tool_input_schema(stream_cls: Type[Stream]) -> Dict[str, Any]:
    """Build JSON Schema for a stream tool's input arguments."""
    listing = _StreamToolListingStub(stream_cls)
    properties: Dict[str, Any] = {}

    filter_metadata = listing.get_available_filters_metadata()
    if isinstance(filter_metadata, dict):
        filters_schema = _build_named_filters_schema(filter_metadata)
        if filters_schema:
            properties["filters"] = filters_schema

    if listing.replication_key:
        properties["replication_key_value"] = {
            "type": "string",
            "description": (
                f"Only return records where {listing.replication_key} is at or after this value."
            ),
        }

    parent_stream_name = _parent_stream_name(stream_cls)
    if parent_stream_name:
        properties["context"] = {
            "type": "object",
            "description": (
                f"Child stream partition from a {parent_stream_name} record. "
                "Copy the child_context value from a parent stream execute-tool result."
            ),
            "additionalProperties": True,
        }
        return {
            "type": "object",
            "properties": properties,
            "required": ["context"],
        }

    return {"type": "object", "properties": properties}


def stream_to_tool_descriptor(stream_cls: Type[Stream], tap_name: str) -> Dict[str, Any]:
    """Build an MCP-style tool descriptor for a stream."""
    return {
        "name": stream_cls.name,
        "description": _tool_description(stream_cls, tap_name),
        "inputSchema": build_tool_input_schema(stream_cls),
    }


def build_tool_catalog_from_stream_types(
    stream_types: List[Type[Stream]],
    tap_name: str,
) -> List[Dict[str, Any]]:
    """Build tool descriptors from stream classes without initializing stream schemas."""
    unique_types = list(dict.fromkeys(stream_types))
    return [
        stream_to_tool_descriptor(stream_cls, tap_name)
        for stream_cls in sorted(unique_types, key=lambda item: item.name)
    ]
