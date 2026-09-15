"""Tests for supported_streams in tap --about metadata."""

from typing import List

from samples.sample_tap_countries.countries_tap import SampleTapCountries
from samples.sample_tap_sqlite import SQLiteTap
from hotglue_singer_sdk import typing as th
from hotglue_singer_sdk.streams import Stream
from hotglue_singer_sdk.tap_base import Tap


class DynamicCatalogTap(SampleTapCountries):
    """Static streams, but marked as dynamic so about omits them."""

    name = "dynamic-catalog-tap"
    dynamic_catalog = True


class RaisingDiscoverTap(Tap):
    """Tap whose discover_streams requires credentials / always fails."""

    name = "raising-discover-tap"
    config_jsonschema = th.PropertiesList().to_dict()

    def discover_streams(self) -> List[Stream]:
        raise RuntimeError("credentials required")


class EmptyDiscoverTap(Tap):
    """Tap that discovers no streams (e.g. Salesforce-style dynamic catalog)."""

    name = "empty-discover-tap"
    config_jsonschema = th.PropertiesList().to_dict()

    def discover_streams(self) -> List[Stream]:
        return []


def test_static_tap_about_includes_supported_streams():
    info = SampleTapCountries._get_about_info()
    assert info["supported_streams"] == ["continents", "countries"]


def test_dynamic_catalog_flag_omits_supported_streams():
    info = DynamicCatalogTap._get_about_info()
    assert "supported_streams" not in info


def test_raising_discover_omits_supported_streams():
    info = RaisingDiscoverTap._get_about_info()
    assert "supported_streams" not in info


def test_empty_discover_omits_supported_streams():
    info = EmptyDiscoverTap._get_about_info()
    assert "supported_streams" not in info


def test_sql_tap_omits_supported_streams():
    assert SQLiteTap.dynamic_catalog is True
    info = SQLiteTap._get_about_info()
    assert "supported_streams" not in info
