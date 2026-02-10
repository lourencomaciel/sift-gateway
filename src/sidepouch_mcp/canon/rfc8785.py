"""Deterministic canonical JSON bytes.

This follows the project invariants for:
- deterministic object key ordering
- UTF-8 output
- Decimal-safe numeric rendering
- rejecting Python float inputs
"""

from __future__ import annotations

from decimal import Decimal
import json
from typing import Any

from sidepouch_mcp.canon.decimal_json import ensure_no_floats


def _utf16_sort_key(text: str) -> bytes:
    """Return UTF-16BE bytes for RFC 8785 key ordering.

    Args:
        text: String to encode for sorting.

    Returns:
        UTF-16BE encoded bytes of the string.
    """
    return text.encode("utf-16be", "surrogatepass")


def _decimal_to_plain(value: Decimal) -> str:
    """Convert a Decimal to its plain decimal string form.

    Normalize the value, strip trailing zeros from the
    fractional part, and strip leading zeros from the
    integer part.

    Args:
        value: Finite Decimal value to render.

    Returns:
        Plain decimal string (e.g. ``"123.45"``).
    """
    normalized = value.normalize()
    sign = "-" if normalized.is_signed() else ""
    digits = "".join(str(d) for d in normalized.as_tuple().digits)
    exponent = normalized.as_tuple().exponent

    if not digits:
        return "0"

    if exponent >= 0:
        plain = digits + ("0" * exponent)
    else:
        point = len(digits) + exponent
        if point > 0:
            plain = f"{digits[:point]}.{digits[point:]}"
        else:
            plain = f"0.{('0' * -point)}{digits}"

    if "." in plain:
        int_part, frac_part = plain.split(".", 1)
        int_part = int_part.lstrip("0") or "0"
        frac_part = frac_part.rstrip("0")
        plain = int_part if not frac_part else f"{int_part}.{frac_part}"
    else:
        plain = plain.lstrip("0") or "0"

    return f"{sign}{plain}"


def _decimal_to_canonical(value: Decimal) -> str:
    """Render a Decimal in JCS-canonical number format.

    Use exponential notation for very large (adjusted >= 21)
    or very small (adjusted <= -7) values, matching the JCS
    cutover thresholds. Otherwise use plain decimal form.

    Args:
        value: Finite Decimal value to render.

    Returns:
        Canonical number string per RFC 8785 / JCS rules.

    Raises:
        ValueError: If the value is non-finite (NaN, Inf).
    """
    if not value.is_finite():
        msg = "non-finite Decimal is not allowed in canonical JSON"
        raise ValueError(msg)
    if value.is_zero():
        return "0"

    plain = _decimal_to_plain(value)
    adjusted = value.normalize().adjusted()

    # Match JCS large/small number cutovers.
    if adjusted >= 21 or adjusted <= -7:
        sign = "-" if value.is_signed() else ""
        digits = "".join(
            str(d) for d in value.copy_abs().normalize().as_tuple().digits
        ).lstrip("0")
        if not digits:
            return "0"
        head = digits[0]
        tail = digits[1:].rstrip("0")
        mantissa = head if not tail else f"{head}.{tail}"
        exponent = f"+{adjusted}" if adjusted >= 0 else str(adjusted)
        return f"{sign}{mantissa}e{exponent}"

    return plain


def _serialize(value: Any) -> str:
    """Recursively serialize a value to canonical JSON text.

    Produce deterministic output following RFC 8785: sorted
    object keys (UTF-16BE order), no whitespace, Decimal-safe
    number rendering, and rejection of float and non-string
    keys.

    Args:
        value: JSON-compatible Python value (None, bool,
            str, int, Decimal, list, or dict).

    Returns:
        Canonical JSON text fragment.

    Raises:
        TypeError: If the value type is unsupported or
            object keys are not strings.
        ValueError: If a Decimal is non-finite.
    """
    if value is None:
        return "null"
    if value is True:
        return "true"
    if value is False:
        return "false"
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    if isinstance(value, int):
        return str(value)
    if isinstance(value, Decimal):
        return _decimal_to_canonical(value)
    if isinstance(value, list):
        return "[" + ",".join(_serialize(item) for item in value) + "]"
    if isinstance(value, dict):
        for key in value:
            if not isinstance(key, str):
                msg = "JSON object keys must be strings"
                raise TypeError(msg)
        items = []
        for key in sorted(value.keys(), key=_utf16_sort_key):
            items.append(f"{_serialize(key)}:{_serialize(value[key])}")
        return "{" + ",".join(items) + "}"

    msg = f"unsupported type for canonicalization: {type(value)!r}"
    raise TypeError(msg)


def canonical_text(value: Any) -> str:
    """Return RFC 8785 canonical JSON text.

    Validate that no Python floats are present, then
    serialize the value tree to deterministic JSON text.

    Args:
        value: JSON-compatible Python value to serialize.

    Returns:
        Canonical JSON string.

    Raises:
        TypeError: If the value contains Python floats or
            unsupported types.
    """
    ensure_no_floats(value)
    return _serialize(value)


def canonical_bytes(value: Any) -> bytes:
    """Return RFC 8785 canonical JSON as UTF-8 bytes.

    Args:
        value: JSON-compatible Python value to serialize.

    Returns:
        UTF-8 encoded canonical JSON bytes.

    Raises:
        TypeError: If the value contains Python floats or
            unsupported types.
    """
    return canonical_text(value).encode("utf-8")
