"""RFC 8785 (JCS - JSON Canonicalization Scheme) implementation.

This module provides deterministic JSON serialization per RFC 8785.
It is the foundation of the entire gateway's hash-based identity system:
canonical JSON is used for forwarded args, upstream tool schema, envelope,
cursor payload, and record hashing.

CRITICAL: Python ``float`` values are NEVER accepted. All fractional numbers
must be represented as ``decimal.Decimal`` to preserve exact precision.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any


# ---------------------------------------------------------------------------
# RFC 8785 string escaping
# ---------------------------------------------------------------------------
# Control characters (U+0000..U+001F) that have dedicated two-char escapes.
_TWO_CHAR_ESCAPES: dict[int, str] = {
    0x08: "\\b",
    0x09: "\\t",
    0x0A: "\\n",
    0x0C: "\\f",
    0x0D: "\\r",
    0x22: '\\"',
    0x5C: "\\\\",
}


def _escape_string(s: str) -> str:
    """Escape a string per RFC 8785 rules.

    Rules:
    - ``"`` and ``\\`` get two-char escapes.
    - Control chars with dedicated escapes use them (\\b, \\t, \\n, \\f, \\r).
    - Other control chars (U+0000..U+001F) use \\uXXXX lowercase hex.
    - All other characters pass through literally (RFC 8785 mandates UTF-8 for
      the output, not \\uXXXX escaping for non-ASCII).
    """
    parts: list[str] = []
    for ch in s:
        cp = ord(ch)
        if cp in _TWO_CHAR_ESCAPES:
            parts.append(_TWO_CHAR_ESCAPES[cp])
        elif cp < 0x20:
            parts.append(f"\\u{cp:04x}")
        else:
            parts.append(ch)
    return '"' + "".join(parts) + '"'


# ---------------------------------------------------------------------------
# RFC 8785 number formatting for decimal.Decimal
# ---------------------------------------------------------------------------

def _format_decimal(d: Decimal) -> str:
    """Format a ``decimal.Decimal`` per RFC 8785 / ES6 Number serialization.

    Rules:
    - NaN and Infinity are rejected (they are not valid JSON).
    - If the value is an integer (no fractional part), output as an integer
      string with no decimal point, no exponent. Negative zero becomes "0".
    - If the value has a fractional part, output in shortest representation:
      - No trailing zeros in the fraction.
      - Use exponential notation when the exponent would be < -6 or >= 21
        (matching ECMAScript Number::toString).
      - Exponent uses lowercase 'e', with explicit '+' or '-' sign.
    """
    if d.is_nan() or d.is_infinite():
        raise ValueError(f"Cannot serialize {d!r} to JSON: NaN and Infinity are not allowed")

    # Handle sign and negative zero.
    sign, digits, exponent = d.as_tuple()

    # Special case: the Decimal is numerically zero.
    if not any(digits):
        return "0"

    # Determine if value is an exact integer.
    # exponent >= 0 means definitely integer (e.g. 1E+2 = 100).
    # exponent < 0 means we need to check if trailing digits cancel it.
    try:
        int_val = int(d)
        if d == Decimal(int_val):
            # It IS an integer. Output as plain integer string.
            # This covers Decimal("1.0"), Decimal("100"), Decimal("1E+2"), etc.
            return str(int_val)
    except (InvalidOperation, OverflowError, ValueError):
        pass

    # The value has a genuine fractional part.  We need ES6-style shortest
    # representation.  Strategy: normalize to remove trailing zeros, then
    # decide plain vs exponential notation.
    d_normalized = d.normalize()
    sign_n, digits_n, exp_n = d_normalized.as_tuple()

    # Build the coefficient string from digits.
    coeff = "".join(str(dig) for dig in digits_n)

    # exp_n is the exponent such that the number = (-1)^sign * int(coeff) * 10^exp_n.
    # The number of digits is len(coeff).
    # The "scientific exponent" e such that the number = X.YYY * 10^e is:
    #   e = exp_n + len(coeff) - 1
    num_digits = len(coeff)
    sci_exp = exp_n + num_digits - 1  # type: ignore[operator]

    sign_str = "-" if sign_n else ""

    # ES6 Number::toString rules for when to use exponential notation:
    # Let n = number of digits, e = sci_exp (0-indexed).
    # Plain decimal is used when -6 <= adjusted position allows it.
    #
    # More precisely, per ECMA-262 7.1.12.1:
    # Let k = number of significant digits, let n = sci_exp + 1.
    # - If k <= n <= 21: integer-like (pad with zeros, no fraction)
    #   e.g. 1.5e20 -> "150000000000000000000" -- but this case means it IS
    #   an integer (already handled above). However, due to Decimal precision,
    #   some values may land here.
    # - If 0 < n <= 0 (impossible) or 0 < n < k: plain decimal "DDD.DDD"
    # - If -6 < n <= 0: "0.000DDD"
    # - Otherwise: exponential notation

    k = num_digits
    n = sci_exp + 1  # ES6 calls this 'n'

    if k <= n <= 21:
        # Integer-like (shouldn't normally reach here since we handled ints above,
        # but possible for e.g. Decimal("1.5E+1") = 15 -- which IS int).
        result = sign_str + coeff + "0" * (n - k)
        return result
    elif 0 < n < k:
        # Plain decimal with digits on both sides of the point.
        # e.g. coeff="12345", n=3 -> "123.45"
        result = sign_str + coeff[:n] + "." + coeff[n:]
        return result
    elif -6 < n <= 0:
        # Plain decimal with leading zeros after the point.
        # e.g. coeff="123", n=-2 -> "0.00123"
        result = sign_str + "0." + "0" * (-n) + coeff
        return result
    else:
        # Exponential notation.
        if k == 1:
            mantissa = coeff
        else:
            mantissa = coeff[0] + "." + coeff[1:]
        exp_sign = "+" if sci_exp >= 0 else "-"
        result = sign_str + mantissa + "e" + exp_sign + str(abs(sci_exp))
        return result


# ---------------------------------------------------------------------------
# RFC 8785 integer formatting
# ---------------------------------------------------------------------------

def _format_int(value: int) -> str:
    """Format a Python ``int`` per RFC 8785 rules.

    - No leading zeros (Python int already satisfies this).
    - Negative zero is impossible for Python int (int(-0) == 0).
    - Just use str().
    """
    return str(value)


# ---------------------------------------------------------------------------
# Core recursive serializer
# ---------------------------------------------------------------------------

def _serialize(obj: Any) -> str:
    """Recursively serialize *obj* to an RFC 8785 canonical JSON string.

    Raises ``TypeError`` for unsupported types, including ``float``.
    """
    if obj is None:
        return "null"

    # IMPORTANT: bool check MUST come before int check because
    # isinstance(True, int) is True in Python.
    if isinstance(obj, bool):
        return "true" if obj else "false"

    if isinstance(obj, int):
        return _format_int(obj)

    if isinstance(obj, float):
        raise TypeError(
            f"Python float is not allowed in canonical JSON serialization. "
            f"Got float value: {obj!r}. Use decimal.Decimal for fractional numbers."
        )

    if isinstance(obj, Decimal):
        return _format_decimal(obj)

    if isinstance(obj, str):
        return _escape_string(obj)

    if isinstance(obj, (list, tuple)):
        elements = ",".join(_serialize(item) for item in obj)
        return "[" + elements + "]"

    if isinstance(obj, dict):
        # RFC 8785: keys sorted by Unicode code point value.
        # Python's default string sort is by Unicode code point, which matches.
        sorted_keys = sorted(obj.keys())
        pairs: list[str] = []
        for key in sorted_keys:
            if not isinstance(key, str):
                raise TypeError(
                    f"RFC 8785 requires all object keys to be strings. "
                    f"Got key of type {type(key).__name__}: {key!r}"
                )
            pairs.append(_escape_string(key) + ":" + _serialize(obj[key]))
        return "{" + ",".join(pairs) + "}"

    raise TypeError(
        f"Type {type(obj).__name__} is not serializable to RFC 8785 canonical JSON. "
        f"Value: {obj!r}"
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def canonical_bytes(obj: Any) -> bytes:
    """Serialize a Python object to RFC 8785 canonical JSON bytes (UTF-8).

    The output is fully deterministic: the same logical input always produces
    identical bytes.

    Supported types:
    - ``dict`` -- keys sorted by Unicode code point
    - ``list``, ``tuple`` -- ordered sequences
    - ``str`` -- escaped per RFC 8785
    - ``int`` -- plain integer string
    - ``decimal.Decimal`` -- RFC 8785 number formatting
    - ``bool`` -- ``true`` / ``false``
    - ``None`` -- ``null``

    Raises:
        TypeError: If a ``float`` is encountered, or any other unsupported type.
        ValueError: If a ``Decimal`` is NaN or Infinity.
    """
    return _serialize(obj).encode("utf-8")


def canonical_json_str(obj: Any) -> str:
    """Serialize a Python object to an RFC 8785 canonical JSON string.

    This is equivalent to ``canonical_bytes(obj).decode('utf-8')`` but avoids
    the intermediate encode/decode round-trip.
    """
    return _serialize(obj)
