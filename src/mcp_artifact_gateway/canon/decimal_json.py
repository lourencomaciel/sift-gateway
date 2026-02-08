"""JSON decoding helpers that avoid float drift."""

from __future__ import annotations

import json
from decimal import Decimal
from typing import Any


class NonFiniteNumberError(ValueError):
    """Raised when JSON payload contains non-finite number literals."""


def _reject_non_finite(token: str) -> Any:
    msg = f"non-finite JSON number is not allowed: {token}"
    raise NonFiniteNumberError(msg)


def loads_decimal(data: str | bytes | bytearray) -> Any:
    """Decode JSON with Decimal for floats and strict non-finite rejection."""
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
    """Recursively ensure Python float is absent from a value tree."""
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

