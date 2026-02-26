"""Stub Google Analytics tap for external tests. Requires real config to run."""

from typing import List

from hotglue_singer_sdk import typing as th
from hotglue_singer_sdk.streams import Stream
from hotglue_singer_sdk.tap_base import Tap


class SampleTapGoogleAnalytics(Tap):
    """Stub GA tap. Provide ga_config for real discovery/sync."""

    name = "sample-tap-google-analytics"
    config_jsonschema = th.PropertiesList().to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return minimal streams; requires valid config for full sync."""
        return []
