"""Stub GitLab tap for external tests. Requires real config to run discovery/sync."""

from typing import List

from hotglue_singer_sdk import typing as th
from hotglue_singer_sdk.streams import RESTStream
from hotglue_singer_sdk.tap_base import Tap


class SampleTapGitlab(Tap):
    """Stub GitLab tap. Provide gitlab_config with url/token for real discovery/sync."""

    name = "sample-tap-gitlab"
    config_jsonschema = th.PropertiesList(
        th.Property("start_date", th.DateTimeType()),
        th.Property("base_url", th.StringType()),
        th.Property("private_token", th.StringType()),
    ).to_dict()

    def discover_streams(self) -> List[RESTStream]:
        """Return minimal streams for discovery; requires valid config for full sync."""
        return []
