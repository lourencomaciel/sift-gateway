from __future__ import annotations

from decimal import Decimal

from sidepouch_mcp.canon.rfc8785 import canonical_text


def test_canonical_orders_keys_and_removes_whitespace() -> None:
    text = canonical_text({"b": 2, "a": 1})
    assert text == '{"a":1,"b":2}'


def test_canonical_uses_decimal_without_float() -> None:
    text = canonical_text({"v": Decimal("1.2300")})
    assert text == '{"v":1.23}'


def test_canonical_uses_scientific_for_tiny_numbers() -> None:
    text = canonical_text({"v": Decimal("0.0000001")})
    assert text == '{"v":1e-7}'


def test_canonical_uses_scientific_for_large_numbers_with_plus_sign() -> None:
    text = canonical_text({"v": Decimal("1e21")})
    assert text == '{"v":1e+21}'


def test_canonical_sorts_keys_by_utf16_code_units() -> None:
    key_bmp = "\ufffd"
    key_astral = "\U0001f600"
    text = canonical_text({key_bmp: 2, key_astral: 1})
    assert text == f'{{"{key_astral}":1,"{key_bmp}":2}}'


def test_canonical_rejects_python_float() -> None:
    try:
        canonical_text({"v": 1.2})
    except TypeError as exc:
        assert "float value not allowed" in str(exc)
    else:
        raise AssertionError("expected TypeError")
