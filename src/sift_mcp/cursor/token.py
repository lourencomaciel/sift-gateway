"""Encode and decode unsigned cursor tokens.

Produce compact ``cur1.<payload_b64u>`` tokens using
base64url over RFC 8785 canonical JSON payloads, with
TTL enforcement on decode.  Key exports are
``encode_cursor`` and ``decode_cursor``.
"""

from __future__ import annotations

import base64
import datetime as dt
import json
from typing import Any

from sift_mcp.canon.rfc8785 import canonical_bytes

_PREFIX = "cur1."


class CursorTokenError(ValueError):
    """Raised when a cursor token is malformed or invalid.

    Covers format errors, base64 decoding failures, and
    missing required fields.
    """


class CursorExpiredError(CursorTokenError):
    """Raised when a cursor token's expiry timestamp has passed."""


def _b64u_encode(data: bytes) -> str:
    """Encode bytes as unpadded URL-safe base64.

    Args:
        data: Raw bytes to encode.

    Returns:
        An ASCII string with no trailing ``=`` padding.
    """
    return (
        base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")
    )


def _b64u_decode(data: str) -> bytes:
    """Decode an unpadded URL-safe base64 string to bytes.

    Args:
        data: Base64-encoded ASCII string, possibly unpadded.

    Returns:
        The decoded raw bytes.
    """
    padding = "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode(
        (data + padding).encode("ascii")
    )


def _parse_utc(timestamp: str) -> dt.datetime:
    """Parse an ISO-8601 timestamp string as UTC datetime.

    Args:
        timestamp: ISO-8601 formatted timestamp string.

    Returns:
        A timezone-aware UTC datetime.
    """
    if timestamp.endswith("Z"):
        timestamp = timestamp[:-1] + "+00:00"
    return dt.datetime.fromisoformat(timestamp).astimezone(
        dt.UTC
    )


def _to_aware_utc(value: dt.datetime) -> dt.datetime:
    """Ensure a datetime is timezone-aware in UTC.

    Naive datetimes are assumed to be UTC.

    Args:
        value: Datetime, possibly naive.

    Returns:
        A timezone-aware UTC datetime.
    """
    if value.tzinfo is None:
        return value.replace(tzinfo=dt.UTC)
    return value.astimezone(dt.UTC)


def encode_cursor(payload: dict[str, Any]) -> str:
    """Base64url-encode a cursor payload.

    The payload must already contain ``issued_at`` and
    ``expires_at`` timestamps (set by ``build_cursor_payload``).

    Args:
        payload: Cursor payload dict with timestamps.

    Returns:
        Token string in ``cur1.<payload_b64u>`` format.
    """
    return _PREFIX + _b64u_encode(canonical_bytes(payload))


def decode_cursor(
    token: str,
    *,
    now: dt.datetime | None = None,
) -> dict[str, Any]:
    """Decode and validate a cursor token.

    Parse the base64url payload, verify it is a JSON object,
    and enforce the ``expires_at`` TTL.

    Args:
        token: Opaque cursor token (``cur1.<b64>``).
        now: Optional current time override for testing.

    Returns:
        The decoded cursor payload dict.

    Raises:
        CursorTokenError: If the token format, base64
            encoding, or payload structure is invalid.
        CursorExpiredError: If the token has expired.
    """
    if not token.startswith(_PREFIX):
        msg = "invalid cursor token format"
        raise CursorTokenError(msg)
    try:
        payload_bytes = _b64u_decode(token[len(_PREFIX) :])
    except Exception as exc:
        msg = "invalid base64 cursor token"
        raise CursorTokenError(msg) from exc
    try:
        payload = json.loads(payload_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        msg = "invalid cursor payload"
        raise CursorTokenError(msg) from exc
    if not isinstance(payload, dict):
        msg = "cursor payload must be a JSON object"
        raise CursorTokenError(msg)

    expires_at = payload.get("expires_at")
    if not isinstance(expires_at, str):
        msg = "cursor missing expires_at"
        raise CursorTokenError(msg)
    current = (
        _to_aware_utc(now)
        if now is not None
        else dt.datetime.now(dt.UTC)
    )
    try:
        expires_dt = _parse_utc(expires_at)
    except ValueError as exc:
        msg = "invalid expires_at timestamp"
        raise CursorTokenError(msg) from exc
    if expires_dt <= current:
        msg = "cursor expired"
        raise CursorExpiredError(msg)

    return payload
