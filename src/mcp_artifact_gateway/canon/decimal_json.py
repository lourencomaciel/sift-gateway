"""JSON decoding helpers that avoid float drift."""

from __future__ import annotations

from decimal import Decimal
import json
from typing import Any


class NonFiniteNumberError(ValueError):
    """Raised when JSON payload contains non-finite number literals."""


def _reject_non_finite(token: str) -> Any:
    """Reject non-finite JSON number constants.

    Used as the ``parse_constant`` callback for
    ``json.loads`` to reject Infinity and NaN.

    Args:
        token: The non-finite constant string.

    Raises:
        NonFiniteNumberError: Always raised.
    """
    msg = f"non-finite JSON number is not allowed: {token}"
    raise NonFiniteNumberError(msg)


def loads_decimal(data: str | bytes | bytearray) -> Any:
    """Decode JSON using Decimal for all float literals.

    Parse the input with ``json.loads`` using
    ``parse_float=Decimal`` to avoid IEEE 754 drift, and
    reject non-finite constants (Infinity, NaN).

    Args:
        data: JSON text as str, bytes, or bytearray.

    Returns:
        Decoded Python value with Decimals instead of
        floats.

    Raises:
        NonFiniteNumberError: If the JSON contains
            non-finite number constants.
        json.JSONDecodeError: If the JSON is malformed.
    """
    if isinstance(data, (bytes, bytearray)):
        text = data.decode("utf-8")
    else:
        text = data
    return json.loads(
        text,
        parse_float=Decimal,
        parse_constant=_reject_non_finite,
    )


def ensure_no_floats(value: Any, path: str = "$") -> None:
    """Recursively verify no Python floats exist in a value tree.

    Walk dicts, lists, and tuples depth-first.  Raise on
    the first float encountered, reporting its JSONPath.

    Args:
        value: JSON-compatible Python value to validate.
        path: JSONPath prefix for error messages.

    Raises:
        TypeError: If a float value is found at any depth.
    """
    if isinstance(value, float):
        msg = f"float value not allowed at {path}"
        raise TypeError(msg)

    if isinstance(value, dict):
        for key, item in value.items():
            ensure_no_floats(item, f"{path}.{key}")
        return

    if isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            ensure_no_floats(item, f"{path}[{index}]")
