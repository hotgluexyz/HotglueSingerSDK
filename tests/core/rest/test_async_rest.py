"""Tests for AsyncRESTStream helpers."""

from unittest.mock import Mock

import requests

from hotglue_singer_sdk.streams.async_rest import AsyncRESTStream


class _AsyncTestStream(AsyncRESTStream):
    """A simple async REST stream for testing."""

    url_base = "https://example.com"
    schema = {"type": "object", "properties": {}}
    path = "/dummy"


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
