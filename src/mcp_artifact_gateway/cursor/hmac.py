"""HMAC-based cursor signing and verification per Addendum D.1 and D.2.

Cursor token format::

    base64url(payload_bytes) + "." + base64url(sig_bytes)

Where:
- base64url is URL-safe base64 **without** padding (``=`` stripped).
- payload_bytes = RFC 8785 canonical JSON of the cursor payload, UTF-8.
- sig_bytes = HMAC-SHA256(secret_key_bytes, payload_bytes).

Verification is constant-time via :func:`hmac.compare_digest`.
"""

from __future__ import annotations

import base64
import hmac as hmac_mod
import hashlib
import json
from datetime import datetime, timezone

from mcp_artifact_gateway.canon.rfc8785 import canonical_bytes
from mcp_artifact_gateway.cursor.secrets import SecretStore


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class CursorError(Exception):
    """Base exception for cursor-related errors."""

    code: str = "CURSOR_ERROR"

    def __init__(self, message: str, code: str | None = None) -> None:
        super().__init__(message)
        if code is not None:
            self.code = code


class CursorInvalidError(CursorError):
    """The cursor token is malformed, has a bad signature, or references an
    unknown secret version."""

    code: str = "CURSOR_INVALID"

    def __init__(self, message: str) -> None:
        super().__init__(message, code="CURSOR_INVALID")


class CursorExpiredError(CursorError):
    """The cursor's ``expires_at`` timestamp is in the past."""

    code: str = "CURSOR_EXPIRED"

    def __init__(self, message: str) -> None:
        super().__init__(message, code="CURSOR_EXPIRED")


class CursorStaleError(CursorError):
    """The cursor's binding fields do not match the current server state
    (e.g. artifact generation changed, map budget changed)."""

    code: str = "CURSOR_STALE"

    def __init__(self, message: str) -> None:
        super().__init__(message, code="CURSOR_STALE")


# ---------------------------------------------------------------------------
# Base64url helpers (unpadded, per RFC 4648 §5)
# ---------------------------------------------------------------------------

def _b64url_encode(data: bytes) -> str:
    """Encode bytes to unpadded base64url string."""
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64url_decode(s: str) -> bytes:
    """Decode an unpadded base64url string to bytes."""
    # Restore padding: base64 input length must be a multiple of 4.
    padding = 4 - (len(s) % 4)
    if padding != 4:
        s += "=" * padding
    return base64.urlsafe_b64decode(s)


# ---------------------------------------------------------------------------
# Signing
# ---------------------------------------------------------------------------

def sign_cursor(payload_obj: dict, secret_key: bytes) -> str:
    """Sign a cursor payload and return the cursor token string.

    Parameters:
        payload_obj: The cursor payload dictionary.  It will be serialized to
            RFC 8785 canonical JSON.
        secret_key: The raw HMAC-SHA256 key bytes.

    Returns:
        A cursor token in the format ``base64url(payload) . base64url(sig)``.
    """
    payload_bytes = canonical_bytes(payload_obj)
    sig_bytes = hmac_mod.new(secret_key, payload_bytes, hashlib.sha256).digest()
    return _b64url_encode(payload_bytes) + "." + _b64url_encode(sig_bytes)


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------

def verify_cursor(cursor_token: str, secret_store: SecretStore) -> dict:
    """Verify a cursor token and return the parsed payload.

    Steps:
    1. Split on ``"."`` -- must have exactly 2 parts.
    2. Decode base64url payload bytes.
    3. Parse payload as JSON.
    4. Check ``expires_at >= now()``.
    5. Look up ``cursor_secret_version`` in the secret store.
    6. Recompute HMAC-SHA256 and compare in constant time.

    Parameters:
        cursor_token: The cursor token string to verify.
        secret_store: The :class:`SecretStore` providing signing secrets.

    Returns:
        The parsed cursor payload dictionary.

    Raises:
        CursorInvalidError: If the token is malformed, the signature is
            invalid, or the secret version is unknown.
        CursorExpiredError: If the cursor has expired.
    """
    # 1. Split on "."
    parts = cursor_token.split(".")
    if len(parts) != 2:
        raise CursorInvalidError(
            "Cursor token must contain exactly one '.' separator"
        )

    encoded_payload, encoded_sig = parts

    # 2. Decode base64url payload
    try:
        payload_bytes = _b64url_decode(encoded_payload)
    except Exception as exc:
        raise CursorInvalidError(
            f"Failed to decode cursor payload: {exc}"
        ) from exc

    # 3. Parse payload as JSON
    try:
        payload: dict = json.loads(payload_bytes)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise CursorInvalidError(
            f"Cursor payload is not valid JSON: {exc}"
        ) from exc

    if not isinstance(payload, dict):
        raise CursorInvalidError("Cursor payload must be a JSON object")

    # 4. Check expiration
    expires_at_str = payload.get("expires_at")
    if expires_at_str is None:
        raise CursorInvalidError("Cursor payload missing 'expires_at' field")

    if isinstance(expires_at_str, str) and expires_at_str.endswith("Z"):
        # Support RFC 3339 "Z" suffix by translating to an explicit UTC offset.
        expires_at_str = expires_at_str[:-1] + "+00:00"

    try:
        expires_at = datetime.fromisoformat(expires_at_str)
    except (ValueError, TypeError) as exc:
        raise CursorInvalidError(
            f"Invalid 'expires_at' timestamp: {expires_at_str}"
        ) from exc

    # Ensure timezone-aware comparison.
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)

    if expires_at < datetime.now(timezone.utc):
        raise CursorExpiredError(
            f"Cursor expired at {expires_at_str}"
        )

    # 5. Look up the secret version
    secret_version = payload.get("cursor_secret_version")
    if secret_version is None:
        raise CursorInvalidError(
            "Cursor payload missing 'cursor_secret_version' field"
        )

    secret_entry = secret_store.get_secret(secret_version)
    if secret_entry is None:
        raise CursorInvalidError(
            f"Unknown cursor secret version: {secret_version!r}"
        )

    # 6. Verify HMAC signature (constant-time comparison)
    expected_sig = hmac_mod.new(
        secret_entry.key_bytes, payload_bytes, hashlib.sha256
    ).digest()

    try:
        actual_sig = _b64url_decode(encoded_sig)
    except Exception as exc:
        raise CursorInvalidError(
            f"Failed to decode cursor signature: {exc}"
        ) from exc

    if not hmac_mod.compare_digest(expected_sig, actual_sig):
        raise CursorInvalidError("Cursor signature verification failed")

    return payload
