from __future__ import annotations

import datetime as dt

import pytest

from sift_mcp.cursor.payload import build_cursor_payload
from sift_mcp.cursor.token import (
    CursorExpiredError,
    CursorTokenError,
    decode_cursor,
    encode_cursor,
)


def _future_payload() -> dict[str, object]:
    """Build a payload that expires in the future."""
    return build_cursor_payload(
        tool="artifact",
        artifact_id="art_1",
        position_state={"offset": 0},
        ttl_minutes=60,
    )


def test_encode_decode_roundtrip() -> None:
    payload = _future_payload()
    token = encode_cursor(payload)
    assert token.startswith("cur1.")
    decoded = decode_cursor(token)
    assert decoded["tool"] == "artifact"
    assert decoded["artifact_id"] == "art_1"
    assert decoded["position_state"] == {"offset": 0}


def test_decode_rejects_invalid_prefix() -> None:
    with pytest.raises(CursorTokenError, match="invalid cursor token"):
        decode_cursor("bad.token")


def test_decode_rejects_old_format() -> None:
    with pytest.raises(CursorTokenError, match="invalid cursor token"):
        decode_cursor("cur.v1.payload.signature")


def test_decode_rejects_invalid_base64() -> None:
    with pytest.raises(CursorTokenError, match="invalid base64"):
        decode_cursor("cur1.!!!not-valid-b64!!!")


def test_decode_rejects_non_object() -> None:
    import base64

    b64 = base64.urlsafe_b64encode(b"[1,2,3]").decode().rstrip("=")
    with pytest.raises(
        CursorTokenError, match="must be a JSON object"
    ):
        decode_cursor(f"cur1.{b64}")


def test_decode_rejects_missing_expires_at() -> None:
    import base64
    import json

    b64 = (
        base64.urlsafe_b64encode(
            json.dumps({"tool": "x"}).encode()
        )
        .decode()
        .rstrip("=")
    )
    with pytest.raises(CursorTokenError, match="missing expires_at"):
        decode_cursor(f"cur1.{b64}")


def test_decode_detects_expired() -> None:
    payload = build_cursor_payload(
        tool="artifact",
        artifact_id="art_1",
        position_state={"offset": 0},
        ttl_minutes=1,
        now=dt.datetime(2020, 1, 1, tzinfo=dt.UTC),
    )
    token = encode_cursor(payload)
    with pytest.raises(CursorExpiredError, match="expired"):
        decode_cursor(token)


def test_decode_accepts_naive_now() -> None:
    payload = _future_payload()
    token = encode_cursor(payload)
    decoded = decode_cursor(
        token, now=dt.datetime.now(dt.UTC).replace(tzinfo=None)
    )
    assert decoded["tool"] == "artifact"
