"""Shared fixtures for target_sdk tests."""

import pytest

from hotglue_singer_sdk.target_sdk.client import HotglueBaseSink


@pytest.fixture(autouse=True)
def reset_hotglue_base_state():
    """Reset HotglueBaseSink class-level state before and after each test."""
    HotglueBaseSink.summary_init = False
    HotglueBaseSink.previous_state = None
    HotglueBaseSink.processed_hashes = []
    yield
    HotglueBaseSink.summary_init = False
    HotglueBaseSink.previous_state = None
    HotglueBaseSink.processed_hashes = []
