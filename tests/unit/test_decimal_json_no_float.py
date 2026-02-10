from __future__ import annotations

from decimal import Decimal

from mcp_artifact_gateway.canon.decimal_json import (
    NonFiniteNumberError,
    ensure_no_floats,
    loads_decimal,
)


def test_loads_decimal_parses_float_as_decimal() -> None:
    payload = loads_decimal('{"v": 1.25}')
    assert isinstance(payload["v"], Decimal)
    assert payload["v"] == Decimal("1.25")


def test_loads_decimal_rejects_non_finite() -> None:
    try:
        loads_decimal('{"v": NaN}')
    except NonFiniteNumberError:
        pass
    else:
        raise AssertionError("expected NonFiniteNumberError")


def test_ensure_no_floats_detects_nested_float() -> None:
    try:
        ensure_no_floats({"a": [1, 1.5]})
    except TypeError as exc:
        assert "$.a[1]" in str(exc)
    else:
        raise AssertionError("expected TypeError")
