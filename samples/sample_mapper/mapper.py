"""Sample inline mapper that applies stream_maps config to Singer messages."""

import logging
from typing import Any, Dict, Iterable, List

import singer

from hotglue_singer_sdk.mapper import PluginMapper
from hotglue_singer_sdk.mapper_base import InlineMapper
from hotglue_singer_sdk.typing import PropertiesList


class StreamTransform(InlineMapper):
    """Inline mapper that filters and transforms streams using stream_maps config."""

    name = "stream-transform"
    config_jsonschema = PropertiesList().to_dict()

    def __init__(self, config: Dict[str, Any], validate_config: bool = True) -> None:
        super().__init__(config=config, validate_config=validate_config)
        self._plugin_mapper = PluginMapper(
            plugin_config=dict(config),
            logger=logging.getLogger(self.name),
        )

    def map_schema_message(self, message_dict: dict) -> Iterable[singer.Message]:
        """Register stream and emit SCHEMA only for streams in stream_maps."""
        stream_name = message_dict.get("stream", "")
        schema = message_dict.get("schema", {})
        key_properties = message_dict.get("key_properties") or []
        self._plugin_mapper.register_raw_stream_schema(
            stream_name, schema, key_properties
        )
        maps_list = self._plugin_mapper.stream_maps.get(stream_name) or []
        for stream_map in maps_list:
            if stream_map.get_filter_result({}):
                yield singer.SchemaMessage(
                    stream_map.stream_alias,
                    stream_map.transformed_schema,
                    stream_map.transformed_key_properties or [],
                    [],
                )

    def map_record_message(self, message_dict: dict) -> Iterable[singer.Message]:
        """Emit RECORD only for streams in stream_maps that pass filter and transform."""
        stream_name = message_dict.get("stream", "")
        record = message_dict.get("record", {})
        maps_list = self._plugin_mapper.stream_maps.get(stream_name) or []
        for stream_map in maps_list:
            if not stream_map.get_filter_result(record):
                continue
            transformed = stream_map.transform(record)
            if transformed is not None:
                yield singer.RecordMessage(
                    stream=stream_map.stream_alias,
                    record=transformed,
                )

    def map_state_message(self, message_dict: dict) -> Iterable[singer.Message]:
        """Pass through state messages."""
        yield singer.StateMessage(value=message_dict.get("value", {}))

    def map_activate_version_message(
        self, message_dict: dict
    ) -> Iterable[singer.Message]:
        """Pass through activate_version messages."""
        yield singer.ActivateVersionMessage(
            stream=message_dict["stream"],
            version=message_dict["version"],
        )
