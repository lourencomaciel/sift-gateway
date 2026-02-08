"""Cursor token signing and verification."""

from __future__ import annotations

import base64
import binascii
import datetime as dt
import hmac
import json
from hashlib import sha256
from typing import Any

from mcp_artifact_gateway.canon.rfc8785 import canonical_bytes
from mcp_artifact_gateway.constants import CURSOR_VERSION
from mcp_artifact_gateway.cursor.secrets import CursorSecrets


class CursorTokenError(ValueError):
    """Raised when cursor token is malformed or invalid."""


class CursorExpiredError(CursorTokenError):
    """Raised when cursor token expiry is in the past."""


def _b64u_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def _b64u_decode(data: str) -> bytes:
    padding = "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode((data + padding).encode("ascii"))


def _parse_utc(timestamp: str) -> dt.datetime:
    if timestamp.endswith("Z"):
        timestamp = timestamp[:-1] + "+00:00"
    return dt.datetime.fromisoformat(timestamp).astimezone(dt.timezone.utc)


def _to_aware_utc(value: dt.datetime) -> dt.datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=dt.timezone.utc)
    return value.astimezone(dt.timezone.utc)


def sign_cursor_payload(payload: dict[str, Any], secrets: CursorSecrets) -> str:
    """Sign canonical payload and return opaque cursor token."""
    payload_bytes = canonical_bytes(payload)
    secret_version = secrets.signing_version
    secret = secrets.current_secret().encode("utf-8")
    signature = hmac.new(secret, payload_bytes, sha256).digest()
    return f"cur.{secret_version}.{_b64u_encode(payload_bytes)}.{_b64u_encode(signature)}"


def verify_cursor_token(
    token: str,
    secrets: CursorSecrets,
    *,
    now: dt.datetime | None = None,
) -> dict[str, Any]:
    """Verify token signature/version/expiry and return decoded payload."""
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
    current = _to_aware_utc(now) if now is not None else dt.datetime.now(dt.timezone.utc)
    try:
        expires_at_dt = _parse_utc(expires_at)
    except ValueError as exc:
        msg = "invalid expires_at timestamp"
        raise CursorTokenError(msg) from exc
    if expires_at_dt <= current:
        msg = "cursor expired"
        raise CursorExpiredError(msg)

    return payload
