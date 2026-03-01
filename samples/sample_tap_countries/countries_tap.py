"""Sample tap that emits countries and continents streams."""

from typing import Any, Dict, Iterator, List, Optional

from hotglue_singer_sdk import typing as th
from hotglue_singer_sdk.streams import Stream
from hotglue_singer_sdk.tap_base import Tap


def _countries_data() -> List[Dict[str, Any]]:
    """Return 257 country records (code, name) for tests."""
    return [
        {"code": f"c{i:03d}", "name": f"Country {i}"}
        for i in range(1, 258)
    ]


def _continents_data() -> List[Dict[str, Any]]:
    """Return continent records."""
    return [
        {"code": "AF", "name": "Africa"},
        {"code": "AN", "name": "Antarctica"},
        {"code": "AS", "name": "Asia"},
        {"code": "EU", "name": "Europe"},
        {"code": "NA", "name": "North America"},
        {"code": "OC", "name": "Oceania"},
        {"code": "SA", "name": "South America"},
    ]


class CountriesStream(Stream):
    """Stream for countries. Primary key: code."""

    name = "countries"

    def __init__(self, tap: Tap, **kwargs: Any) -> None:
        schema = th.PropertiesList(
            th.Property("code", th.StringType()),
            th.Property("name", th.StringType()),
        ).to_dict()
        super().__init__(tap=tap, schema=schema, name=self.name, **kwargs)
        self._primary_keys = ["code"]

    def get_records(self, context: Optional[dict]) -> Iterator[Dict[str, Any]]:
        """Yield country records."""
        for record in _countries_data():
            yield record


class ContinentsStream(Stream):
    """Stream for continents."""

    name = "continents"

    def __init__(self, tap: Tap, **kwargs: Any) -> None:
        schema = th.PropertiesList(
            th.Property("code", th.StringType()),
            th.Property("name", th.StringType()),
        ).to_dict()
        super().__init__(tap=tap, schema=schema, name=self.name, **kwargs)
        self._primary_keys = ["code"]

    def get_records(self, context: Optional[dict]) -> Iterator[Dict[str, Any]]:
        """Yield continent records."""
        for record in _continents_data():
            yield record


class SampleTapCountries(Tap):
    """Sample tap with countries and continents streams."""

    name = "sample-tap-countries"
    config_jsonschema = th.PropertiesList().to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return countries and continents streams."""
        return [
            CountriesStream(self),
            ContinentsStream(self),
        ]
