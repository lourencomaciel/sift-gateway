from __future__ import annotations

import base64
import datetime as dt
from hashlib import sha256
import hmac

from sidepouch_mcp.constants import CURSOR_VERSION
from sidepouch_mcp.cursor.hmac import (
    CursorExpiredError,
    CursorTokenError,
    sign_cursor_payload,
    verify_cursor_token,
)
from sidepouch_mcp.cursor.secrets import CursorSecrets


def _secrets() -> CursorSecrets:
    return CursorSecrets(active={"v1": "secret-abc"}, signing_version="v1")


def test_cursor_hmac_sign_and_verify() -> None:
    payload = {
        "cursor_version": CURSOR_VERSION,
        "tool": "artifact.search",
        "expires_at": "2099-01-01T00:00:00Z",
    }
    token = sign_cursor_payload(payload, _secrets())
    verified = verify_cursor_token(token, _secrets())
    assert verified["tool"] == "artifact.search"


def test_cursor_hmac_rejects_tamper() -> None:
    payload = {
        "cursor_version": CURSOR_VERSION,
        "expires_at": "2099-01-01T00:00:00Z",
    }
    token = sign_cursor_payload(payload, _secrets()) + "x"
    try:
        verify_cursor_token(token, _secrets())
    except CursorTokenError:
        pass
    else:
        raise AssertionError("expected CursorTokenError")


def test_cursor_hmac_detects_expired() -> None:
    payload = {
        "cursor_version": CURSOR_VERSION,
        "expires_at": "2020-01-01T00:00:00Z",
    }
    token = sign_cursor_payload(payload, _secrets())
    try:
        verify_cursor_token(
            token,
            _secrets(),
            now=dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc),
        )
    except CursorExpiredError:
        pass
    else:
        raise AssertionError("expected CursorExpiredError")


def test_cursor_hmac_requires_expires_at() -> None:
    payload = {"cursor_version": CURSOR_VERSION}
    token = sign_cursor_payload(payload, _secrets())
    try:
        verify_cursor_token(token, _secrets())
    except CursorTokenError as exc:
        assert "missing expires_at" in str(exc)
    else:
        raise AssertionError("expected CursorTokenError")


def test_cursor_hmac_rejects_invalid_base64() -> None:
    token = "cur.v1.a.a"
    try:
        verify_cursor_token(token, _secrets())
    except CursorTokenError as exc:
        assert "invalid base64" in str(exc)
    else:
        raise AssertionError("expected CursorTokenError")


def test_cursor_hmac_rejects_non_object_payload() -> None:
    payload_bytes = b'["not-an-object"]'
    secret = _secrets().current_secret().encode("utf-8")
    signature = hmac.new(secret, payload_bytes, sha256).digest()
    payload_b64 = (
        base64.urlsafe_b64encode(payload_bytes).decode("ascii").rstrip("=")
    )
    signature_b64 = (
        base64.urlsafe_b64encode(signature).decode("ascii").rstrip("=")
    )
    token = f"cur.v1.{payload_b64}.{signature_b64}"

    try:
        verify_cursor_token(token, _secrets())
    except CursorTokenError as exc:
        assert "JSON object" in str(exc)
    else:
        raise AssertionError("expected CursorTokenError")


def test_cursor_hmac_accepts_naive_now_for_comparison() -> None:
    payload = {
        "cursor_version": CURSOR_VERSION,
        "expires_at": "2099-01-01T00:00:00Z",
    }
    token = sign_cursor_payload(payload, _secrets())
    verified = verify_cursor_token(
        token, _secrets(), now=dt.datetime(2026, 1, 1)
    )
    assert verified["expires_at"] == "2099-01-01T00:00:00Z"
