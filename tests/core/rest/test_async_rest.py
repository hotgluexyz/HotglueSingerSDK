"""Tests for AsyncRESTStream helpers."""

import threading
import time
from unittest.mock import Mock
from unittest.mock import patch

import requests

from hotglue_singer_sdk.streams.async_rest import AsyncRESTStream
from hotglue_singer_sdk.typing import AsyncJobStatus


class _AsyncTestStream(AsyncRESTStream):
    """A simple async REST stream for testing."""

    url_base = "https://example.com"
    schema = {"type": "object", "properties": {}}
    path = "/dummy"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.created_contexts = []
        self.returned_results = {}

    def get_paging_windows(self, context):
        return [{"window": "A"}, {"window": "B"}]

    def create_async_job(self, context=None):
        self.created_contexts.append(context.copy())
        return context["window"]

    def get_async_job_status(self, job_id):
        return AsyncJobStatus.COMPLETED

    def get_async_job_results(self, job_id):
        return self.returned_results[job_id]

    def generate_records_from_job_response(self, job_response):
        yield from job_response["records"]


def test_make_request_uses_prepared_request_and_decorated_request(rest_tap):
    stream = _AsyncTestStream(rest_tap, name="async_stream")
    prepared_request = requests.PreparedRequest()
    response = requests.Response()
    response.status_code = 200
    context = {"partition": "A"}

    build_prepared_request = Mock(return_value=prepared_request)
    stream.build_prepared_request = build_prepared_request

    seen = {}

    def fake_request_decorator(func):
        assert func == stream._request

        def wrapped(request, ctx):
            seen["request"] = request
            seen["context"] = ctx
            return response

        return wrapped

    stream.request_decorator = fake_request_decorator

    result = stream.make_request(
        method="POST",
        url="https://example.com/resource",
        context=context,
        params={"offset": 10},
        headers={"X-Test": "1"},
        json={"key": "value"},
    )

    assert result is response
    assert seen["request"] is prepared_request
    assert seen["context"] == context
    build_prepared_request.assert_called_once_with(
        method="POST",
        url="https://example.com/resource",
        params={"offset": 10},
        headers={"X-Test": "1"},
        json={"key": "value"},
        data=None,
    )


def test_get_records_respects_paging_windows(rest_tap):
    stream = _AsyncTestStream(rest_tap, name="async_stream")
    stream.returned_results = {
        "A": {"records": [{"id": "a1"}]},
        "B": {"records": [{"id": "b1"}, {"id": "b2"}]},
    }

    with patch("hotglue_singer_sdk.streams.async_rest.time.sleep") as sleep_mock:
        records = list(stream.get_records({"base": "ctx"}))

    assert stream.created_contexts == [
        {"base": "ctx", "window": "A"},
        {"base": "ctx", "window": "B"},
    ]
    assert records == [{"id": "a1"}, {"id": "b1"}, {"id": "b2"}]
    sleep_mock.assert_not_called()


def test_get_records_runs_paging_windows_in_parallel(rest_tap):
    class _ParallelAsyncTestStream(_AsyncTestStream):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self._active_windows = 0
            self.max_concurrent_windows = 0
            self._lock = threading.Lock()

        def get_async_job_results(self, job_id):
            with self._lock:
                self._active_windows += 1
                self.max_concurrent_windows = max(
                    self.max_concurrent_windows, self._active_windows
                )
            # Hold execution briefly so overlap is observable.
            time.sleep(0.05)
            with self._lock:
                self._active_windows -= 1
            return self.returned_results[job_id]

    stream = _ParallelAsyncTestStream(rest_tap, name="parallel_async_stream")
    stream.returned_results = {
        "A": {"records": [{"id": "a1"}]},
        "B": {"records": [{"id": "b1"}]},
    }

    # Default limit is serial execution.
    records = list(stream.get_records({"base": "ctx"}))

    assert records == [{"id": "a1"}, {"id": "b1"}]
    assert stream.max_concurrent_windows == 1

    stream.max_concurrent_windows = 0
    stream.parallelization_limit = 2
    records = list(stream.get_records({"base": "ctx"}))

    assert records == [{"id": "a1"}, {"id": "b1"}]
    assert stream.max_concurrent_windows >= 2
