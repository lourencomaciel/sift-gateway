"""Sign and verify HMAC-based opaque cursor tokens.

Produce compact ``cur.<version>.<payload>.<sig>`` tokens
using HMAC-SHA256 over RFC 8785 canonical payloads, and
verify them with expiry enforcement and secret version
rotation.  Key exports are ``sign_cursor_payload`` and
``verify_cursor_token``.
"""

from __future__ import annotations

import base64
import binascii
import datetime as dt
from hashlib import sha256
import hmac
import json
from typing import Any

from mcp_artifact_gateway.canon.rfc8785 import canonical_bytes
from mcp_artifact_gateway.constants import CURSOR_VERSION
from mcp_artifact_gateway.cursor.secrets import CursorSecrets


class CursorTokenError(ValueError):
    """Raised when a cursor token is malformed or invalid.

    Covers format errors, unknown secret versions,
    base64 decoding failures, and signature mismatches.
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
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def _b64u_decode(data: str) -> bytes:
    """Decode an unpadded URL-safe base64 string to bytes.

    Args:
        data: Base64-encoded ASCII string, possibly unpadded.

    Returns:
        The decoded raw bytes.
    """
    padding = "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode((data + padding).encode("ascii"))


def _parse_utc(timestamp: str) -> dt.datetime:
    """Parse an ISO-8601 timestamp string as UTC datetime.

    Handles both ``Z`` suffix and ``+00:00`` offset formats.

    Args:
        timestamp: ISO-8601 formatted timestamp string.

    Returns:
        A timezone-aware UTC datetime.
    """
    if timestamp.endswith("Z"):
        timestamp = timestamp[:-1] + "+00:00"
    return dt.datetime.fromisoformat(timestamp).astimezone(dt.timezone.utc)


def _to_aware_utc(value: dt.datetime) -> dt.datetime:
    """Ensure a datetime is timezone-aware in UTC.

    Naive datetimes are assumed to be UTC.

    Args:
        value: Datetime, possibly naive.

    Returns:
        A timezone-aware UTC datetime.
    """
    if value.tzinfo is None:
        return value.replace(tzinfo=dt.timezone.utc)
    return value.astimezone(dt.timezone.utc)


def sign_cursor_payload(payload: dict[str, Any], secrets: CursorSecrets) -> str:
    """Sign a canonical payload and return an opaque cursor token.

    Produce a ``cur.<version>.<payload>.<sig>`` token using
    HMAC-SHA256 over the RFC 8785 canonical encoding of the
    payload dict.

    Args:
        payload: Cursor payload dict to sign.
        secrets: Versioned HMAC signing secrets.

    Returns:
        An opaque cursor token string.
    """
    payload_bytes = canonical_bytes(payload)
    secret_version = secrets.signing_version
    secret = secrets.current_secret().encode("utf-8")
    signature = hmac.new(secret, payload_bytes, sha256).digest()
    encoded_payload = _b64u_encode(payload_bytes)
    encoded_sig = _b64u_encode(signature)
    return f"cur.{secret_version}.{encoded_payload}.{encoded_sig}"


def verify_cursor_token(
    token: str,
    secrets: CursorSecrets,
    *,
    now: dt.datetime | None = None,
) -> dict[str, Any]:
    """Verify a cursor token and return the decoded payload.

    Validate the token format, resolve the secret version,
    verify the HMAC-SHA256 signature, check cursor_version
    compatibility, and enforce the expiry timestamp.

    Args:
        token: Opaque cursor token (``cur.<ver>.<b64>.<sig>``).
        secrets: Versioned HMAC secrets for verification.
        now: Optional current time override for testing.

    Returns:
        The decoded and verified cursor payload dict.

    Raises:
        CursorTokenError: If the token is malformed, the
            secret version is unknown, base64 decoding fails,
            or the signature does not match.
        CursorExpiredError: If the token has expired.
    """
    parts = token.split(".")
    if len(parts) != 4 or parts[0] != "cur":
        msg = "invalid cursor token format"
        raise CursorTokenError(msg)

    secret_version, payload_b64, signature_b64 = parts[1], parts[2], parts[3]
    try:
        secret = secrets.secret_for(secret_version).encode("utf-8")
    except KeyError as exc:
        raise CursorTokenError(str(exc)) from exc

    try:
        payload_bytes = _b64u_decode(payload_b64)
        signature = _b64u_decode(signature_b64)
    except (binascii.Error, ValueError) as exc:
        msg = "invalid base64 cursor token"
        raise CursorTokenError(msg) from exc
    expected = hmac.new(secret, payload_bytes, sha256).digest()
    if not hmac.compare_digest(signature, expected):
        msg = "cursor signature mismatch"
        raise CursorTokenError(msg)

    try:
        payload = json.loads(payload_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        msg = "invalid cursor payload"
        raise CursorTokenError(msg) from exc
    if not isinstance(payload, dict):
        msg = "cursor payload must be a JSON object"
        raise CursorTokenError(msg)
    if payload.get("cursor_version") != CURSOR_VERSION:
        msg = "cursor_version mismatch"
        raise CursorTokenError(msg)

    expires_at = payload.get("expires_at")
    if not isinstance(expires_at, str):
        msg = "cursor missing expires_at"
        raise CursorTokenError(msg)
    current = (
        _to_aware_utc(now)
        if now is not None
        else dt.datetime.now(dt.timezone.utc)
    )
    try:
        expires_at_dt = _parse_utc(expires_at)
    except ValueError as exc:
        msg = "invalid expires_at timestamp"
        raise CursorTokenError(msg) from exc
    if expires_at_dt <= current:
        msg = "cursor expired"
        raise CursorExpiredError(msg)

    return payload
