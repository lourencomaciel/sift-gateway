from datetime import datetime, timedelta, timezone

import pytest

from mcp_artifact_gateway.cursor.hmac import (
    CursorExpiredError,
    CursorInvalidError,
    sign_cursor,
    verify_cursor,
)
from mcp_artifact_gateway.cursor.secrets import SecretStore, generate_secrets_file


def _secret_store(tmp_path) -> SecretStore:
    path = tmp_path / "secrets.json"
    generate_secrets_file(path, num_secrets=1)
    store = SecretStore(path)
    store.load()
    return store


def test_sign_and_verify_roundtrip(tmp_path) -> None:
    store = _secret_store(tmp_path)
    payload = {
        "cursor_version": "cursor_v1",
        "cursor_secret_version": store.signing_secret().version,
        "expires_at": (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat(),
    }
    token = sign_cursor(payload, store.signing_secret().key_bytes)
    decoded = verify_cursor(token, store)
    assert decoded["cursor_version"] == "cursor_v1"


def test_invalid_format(tmp_path) -> None:
    store = _secret_store(tmp_path)
    with pytest.raises(CursorInvalidError):
        verify_cursor("not-a-token", store)


def test_invalid_signature(tmp_path) -> None:
    store = _secret_store(tmp_path)
    payload = {
        "cursor_version": "cursor_v1",
        "cursor_secret_version": store.signing_secret().version,
        "expires_at": (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat(),
    }
    token = sign_cursor(payload, store.signing_secret().key_bytes)
    bad = token[:-1] + ("A" if token[-1] != "A" else "B")
    with pytest.raises(CursorInvalidError):
        verify_cursor(bad, store)


def test_expired_cursor(tmp_path) -> None:
    store = _secret_store(tmp_path)
    payload = {
        "cursor_version": "cursor_v1",
        "cursor_secret_version": store.signing_secret().version,
        "expires_at": (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat(),
    }
    token = sign_cursor(payload, store.signing_secret().key_bytes)
    with pytest.raises(CursorExpiredError):
        verify_cursor(token, store)


def test_z_suffix_timestamp(tmp_path) -> None:
    store = _secret_store(tmp_path)
    expires_at = (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat()
    payload = {
        "cursor_version": "cursor_v1",
        "cursor_secret_version": store.signing_secret().version,
        "expires_at": expires_at.replace("+00:00", "Z"),
    }
    token = sign_cursor(payload, store.signing_secret().key_bytes)
    decoded = verify_cursor(token, store)
    assert decoded["expires_at"].endswith("Z")
