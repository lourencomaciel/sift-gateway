from decimal import Decimal

import pytest

from mcp_artifact_gateway.canon.rfc8785 import canonical_bytes, canonical_json_str


def test_canonical_key_ordering_and_basic_types() -> None:
    payload = {"b": 1, "a": 2, "c": True, "d": None}
    assert canonical_json_str(payload) == '{"a":2,"b":1,"c":true,"d":null}'


def test_string_escaping() -> None:
    payload = {"s": 'a"b\\c\n'}
    assert canonical_json_str(payload) == '{"s":"a\\"b\\\\c\\n"}'


def test_decimal_formatting_rules() -> None:
    payload = {
        "int_like": Decimal("1.2300"),
        "small": Decimal("0.000001"),
        "exp": Decimal("1e-7"),
        "zero": Decimal("-0"),
        "big": Decimal("1e3"),
    }
    # Note: order is lexicographic by key
    assert canonical_json_str(payload) == (
        '{"big":1000,"exp":1e-7,"int_like":1.23,"small":0.000001,"zero":0}'
    )


def test_float_rejected() -> None:
    with pytest.raises(TypeError):
        canonical_bytes({"x": 1.2})
