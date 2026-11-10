"""JSONPath helpers."""

from typing import Any, Generator, Union

import jsonpath_ng
import memoization

try:
    from jsonpath_ng.ext.parser import ExtendedJsonPathParser
except ImportError:  # jsonpath-ng < 1.8 used this spelling.
    from jsonpath_ng.ext.parser import ExtentedJsonPathParser as ExtendedJsonPathParser


class _SingletonSlice(jsonpath_ng.JSONPath):
    """Keep pre-1.11 array coercion for SDK record selectors."""

    def __init__(self, start=None, end=None, step=None):
        self.start = start
        self.end = end
        self.step = step

    def find(self, datum):
        datum = jsonpath_ng.DatumInContext.wrap(datum)
        if datum.value is None:
            return []
        if isinstance(datum.value, (dict, int, float, str, bool)):
            datum = jsonpath_ng.DatumInContext(
                [datum.value], path=datum.path, context=datum.context
            )
        return [
            jsonpath_ng.DatumInContext(
                datum.value[i], path=jsonpath_ng.Index(i), context=datum
            )
            for i in range(len(datum.value))[self.start : self.end : self.step]
        ]


class _ObjectWildcard(jsonpath_ng.JSONPath):
    """Keep dot wildcards selecting object members, not array elements."""

    def find(self, datum):
        datum = jsonpath_ng.DatumInContext.wrap(datum)
        if not isinstance(datum.value, dict):
            return []
        return [
            jsonpath_ng.DatumInContext(
                value, path=jsonpath_ng.Fields(key), context=datum
            )
            for key, value in datum.value.items()
        ]


class _RecordJsonPathParser(ExtendedJsonPathParser):
    """Preserve SDK selector semantics without patching jsonpath-ng globally."""

    def p_slice_any(self, p):
        "slice : '*'"
        p[0] = _SingletonSlice()

    def p_slice(self, p):
        """slice : maybe_int ':' maybe_int
        | maybe_int ':' maybe_int ':' maybe_int"""
        p[0] = _SingletonSlice(*p[1::2])

    def p_fields_or_any(self, p):
        """fields_or_any : fields
        | '*'
        | NUMBER"""
        if p[1] == "*":
            p[0] = _ObjectWildcard()
        else:
            super().p_fields_or_any(p)

    def p_jsonpath_fields(self, p):
        "jsonpath : fields_or_any"
        if isinstance(p[1], _ObjectWildcard):
            p[0] = p[1]
        else:
            super().p_jsonpath_fields(p)


def extract_jsonpath(
    expression: str, input: Union[dict, list]
) -> Generator[Any, None, None]:
    """Extract records from an input based on a JSONPath expression.

    Args:
        expression: JSONPath expression to match against the input.
        input: JSON object or array to extract records from.

    Yields:
        Records matched with JSONPath expression.

    Array wildcards and slices treat objects and non-null scalars as singleton
    arrays, retaining the SDK behavior from jsonpath-ng before version 1.11.
    Dot wildcards select object members only. Other operators use the installed
    jsonpath-ng semantics.
    """
    compiled_jsonpath = _compile_jsonpath(expression)

    match: jsonpath_ng.DatumInContext
    for match in compiled_jsonpath.find(input):
        yield match.value


@memoization.cached
def _compile_jsonpath(expression: str) -> jsonpath_ng.JSONPath:
    """Parse a JSONPath expression and cache the result.

    Args:
        expression: A string representing a JSONPath expression.

    Returns:
        A compiled JSONPath object.
    """
    return _RecordJsonPathParser().parse(expression)
