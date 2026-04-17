"""Tests for Tap.get_available_filters and reference_data key collection."""

import json
from typing import Any, Dict, List, Optional

import pytest

from hotglue_singer_sdk.streams.core import Stream
from hotglue_singer_sdk.helpers._singer import Catalog
from hotglue_singer_sdk.tap_base import Tap
from hotglue_singer_sdk.typing import DateTimeType, IntegerType, PropertiesList, Property

CONFIG_START_DATE = "2021-01-01"


class FiltersStreamA(Stream):
    """Stream referencing reference_data.foo."""

    name = "stream_a"
    schema = PropertiesList(
        Property("id", IntegerType, required=True),
        Property("updatedAt", DateTimeType, required=True),
    ).to_dict()
    replication_key = "updatedAt"

    def __init__(self, tap: Tap) -> None:
        super().__init__(tap, schema=self.schema, name=self.name)

    def get_records(self, context: Optional[dict]):
        yield {"field_a": "value_a", "updatedAt": "2021-01-01"}

    def get_available_filters_metadata(self) -> Dict[str, Any]:
        return {
            "supported_operators": ["AND"],
            "supports_nesting_clauses": True,
            "filters": {
                "a_field_a": {
                    "label": "A",
                    "supported_operators": ["EQ"],
                    "target_field": "target_field_a",
                    "options": "reference_data.stream_a.field_a",
                }
            },
        }


class FiltersStreamB(Stream):
    """Second stream also using reference_data.foo (deduped fetch)."""

    name = "stream_b"
    schema = FiltersStreamA.schema
    replication_key = "updatedAt"

    def __init__(self, tap: Tap) -> None:
        super().__init__(tap, schema=self.schema, name=self.name)

    def get_records(self, context: Optional[dict]):
        yield {"field_b": "value_b", "updatedAt": "2021-01-01"}

    def get_available_filters_metadata(self) -> Dict[str, Any]:
        return {
            "supported_operators": ["AND", "OR"],
            "supports_nesting_clauses": True,
            "filters": {
                "a_field_a": {
                    "label": "A",
                    "supported_operators": ["EQ"],
                    "target_field": "target_field_a",
                    "options": "reference_data.stream_a.field_a",
                },
                "b_field_b": {
                    "label": "B",
                    "supported_operators": ["IN", "EQ"],
                    "target_field": "target_field_b",
                    "options": "reference_data.stream_b.field_b",
                }
            },
        }


class AvailableFiltersTestTap(Tap):
    """Tap exposing two streams with overlapping reference_data keys."""

    name = "test-tap-af"
    settings_jsonschema = PropertiesList(Property("start_date", DateTimeType)).to_dict()

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

    def discover_streams(self) -> List[Stream]:
        return [FiltersStreamA(self), FiltersStreamB(self)]

    @property
    def available_filters_version(self) -> str:
        return "1.1.0"


def test_extract_reference_data_fields_metadata() -> None:
    tap = AvailableFiltersTestTap(
        config={"start_date": CONFIG_START_DATE},
        parse_env_config=False,
    )
    payload = {
        "s1": {
            "filters": {
                "f1": {"options": "reference_data.vendors.vendor_id"},
                "f2": {"options": ["static", "list"]},
                "f3": {"options": "reference_data.accounts.name"},
                "f4": {"options": "reference_data.accounts.code"},
            }
        }
    }
    assert tap.extract_reference_data_fields_metadata(payload) == {"accounts": {"name", "code"}, "vendors": {"vendor_id"}}


def test_get_available_filters_requires_catalog() -> None:
    tap = AvailableFiltersTestTap(
        config={"start_date": CONFIG_START_DATE},
        parse_env_config=False,
    )
    with pytest.raises(Exception, match="Catalog is required to get available filters."):
        tap.get_available_filters()


def test_get_available_filters_dedupes_reference_keys_and_prints_json(
    capsys: pytest.CaptureFixture[str],
) -> None:
    tap = AvailableFiltersTestTap(
        config={"start_date": CONFIG_START_DATE},
        parse_env_config=False,
    )

    catalog = Catalog(
        (stream.tap_stream_id, stream._singer_catalog_entry)
        for stream in [FiltersStreamA(tap), FiltersStreamB(tap)]
    )

    tap.get_available_filters(catalog)
    out = capsys.readouterr().out
    data = json.loads(out)
    assert data == {
        "filters_version": "1.1.0",
        "reference_data": {
            "stream_a": [{"field_a": "value_a"}],
            "stream_b": [{"field_b": "value_b"}]
        },
        "streams": {
            "stream_a": {
                "supported_operators": ["AND"],
                "supports_nesting_clauses": True,
                "filters": {
                    "a_field_a": {
                        "label": "A",
                        "supported_operators": ["EQ"],
                        "target_field": "target_field_a",
                        "options": "reference_data.stream_a.field_a"
                    },
                },
            },
            "stream_b": {
                "supported_operators": ["AND", "OR"],
                "supports_nesting_clauses": True,
                "filters": {
                    "a_field_a": {
                        "label": "A",
                        "supported_operators": ["EQ"],
                        "target_field": "target_field_a",
                        "options": "reference_data.stream_a.field_a"
                    },
                    "b_field_b": {
                        "label": "B",
                        "supported_operators": ["IN", "EQ"],
                        "target_field": "target_field_b",
                        "options": "reference_data.stream_b.field_b"
                    }
                },
            },
        },
    }
