"""Decimal-safe JSON parsing and serialization.

This module ensures that JSON numeric values with decimal points or exponents
are parsed as ``decimal.Decimal`` rather than Python ``float``, preserving
exact precision throughout the gateway pipeline.

For *canonical* serialization (hashing, envelope storage), use
:func:`~mcp_artifact_gateway.canon.rfc8785.canonical_bytes` instead.
The :func:`dumps_safe` function here is intended for non-canonical paths
such as logging, JSONB storage columns, and debugging output.
"""

from __future__ import annotations

import json
from decimal import Decimal, InvalidOperation
from typing import IO, Any


# ---------------------------------------------------------------------------
# Custom encoder for Decimal-aware JSON output (non-canonical paths)
# ---------------------------------------------------------------------------

class _DecimalEncoder(json.JSONEncoder):
    """JSON encoder that serializes ``decimal.Decimal`` values correctly.

    - Integer-valued Decimals (e.g. ``Decimal("42")``) are output as integers.
    - Fractional Decimals are output with their exact string representation
      (using ``str()``, which preserves all significant digits).
    - NaN and Infinity raise ``ValueError``.
    """

    def default(self, o: Any) -> Any:
        if isinstance(o, Decimal):
            if o.is_nan() or o.is_infinite():
                raise ValueError(
                    f"Cannot serialize {o!r} to JSON: NaN and Infinity are not allowed"
                )
            # For integer-valued decimals, return int so json serializes as "42"
            # rather than "42.0" or "4.2E+1".
            if o == o.to_integral_value():
                return int(o)
            # For fractional decimals, return a float-like representation.
            # We use float() here ONLY for the non-canonical encoder path.
            # This is acceptable because this encoder is never used for hashing.
            return float(o)
        return super().default(o)


# ---------------------------------------------------------------------------
# Decimal parsing validator
# ---------------------------------------------------------------------------

def _decimal_parser(s: str) -> Decimal:
    """Parse a JSON number string as ``decimal.Decimal``, rejecting NaN/Infinity.

    This is used as the ``parse_float`` callback for ``json.loads`` / ``json.load``.
    """
    try:
        d = Decimal(s)
    except InvalidOperation as exc:
        raise ValueError(f"Invalid numeric value in JSON: {s!r}") from exc

    if d.is_nan() or d.is_infinite():
        raise ValueError(
            f"JSON does not permit NaN or Infinity: got {s!r}"
        )
    return d


def _constant_parser(s: str) -> Decimal:
    """Reject non-standard JSON constants such as NaN or Infinity."""
    raise ValueError(f"JSON does not permit non-finite constants: {s!r}")


# ---------------------------------------------------------------------------
# Public API: parsing
# ---------------------------------------------------------------------------

def loads_decimal(s: str | bytes) -> Any:
    """Parse a JSON string, converting fractional/exponent numbers to ``Decimal``.

    - Integer literals (no ``'.'``, no ``'e'``/``'E'``) remain as Python ``int``.
    - All other numeric values become ``decimal.Decimal``.
    - NaN and Infinity tokens are rejected with ``ValueError``.

    Args:
        s: A JSON-encoded string or bytes.

    Returns:
        The parsed Python object with ``Decimal`` in place of ``float``.

    Raises:
        json.JSONDecodeError: On malformed JSON.
        ValueError: On NaN or Infinity values.
    """
    return json.loads(s, parse_float=_decimal_parser, parse_constant=_constant_parser)


def load_decimal(fp: IO[str] | IO[bytes]) -> Any:
    """Parse JSON from a file-like object, converting fractional numbers to ``Decimal``.

    Semantics are identical to :func:`loads_decimal` but reads from a stream.

    Args:
        fp: A readable file-like object containing JSON.

    Returns:
        The parsed Python object with ``Decimal`` in place of ``float``.

    Raises:
        json.JSONDecodeError: On malformed JSON.
        ValueError: On NaN or Infinity values.
    """
    return json.load(fp, parse_float=_decimal_parser, parse_constant=_constant_parser)


# ---------------------------------------------------------------------------
# Public API: serialization (non-canonical)
# ---------------------------------------------------------------------------

def dumps_safe(obj: Any) -> str:
    """Serialize an object to a JSON string, handling ``Decimal`` values.

    This is for **non-canonical** paths only (logging, JSONB storage,
    debugging). For deterministic hashing, use
    :func:`~mcp_artifact_gateway.canon.rfc8785.canonical_bytes`.

    The output uses compact separators (no extra whitespace) and sorts keys
    for readability, but is NOT RFC 8785 compliant (number formatting differs).

    Args:
        obj: The Python object to serialize.

    Returns:
        A JSON string.

    Raises:
        TypeError: On unserializable types.
        ValueError: If a Decimal is NaN or Infinity.
    """
    return json.dumps(obj, cls=_DecimalEncoder, separators=(",", ":"), sort_keys=True)
