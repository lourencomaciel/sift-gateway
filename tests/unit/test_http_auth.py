from __future__ import annotations

import asyncio

import pytest

from sidepouch_mcp.mcp.http_auth import (
    bearer_auth_middleware,
    is_local_host,
    validate_http_bind,
)


def test_is_local_host_localhost() -> None:
    assert is_local_host("localhost") is True


def test_is_local_host_127() -> None:
    assert is_local_host("127.0.0.1") is True


def test_is_local_host_ipv6_loopback() -> None:
    assert is_local_host("::1") is True


def test_is_local_host_external() -> None:
    assert is_local_host("0.0.0.0") is False


def test_is_local_host_custom() -> None:
    assert is_local_host("192.168.1.1") is False


def test_validate_local_no_token_ok() -> None:
    validate_http_bind("localhost", None)


def test_validate_local_with_token_ok() -> None:
    validate_http_bind("localhost", "secret-token")


def test_validate_nonlocal_with_token_ok() -> None:
    validate_http_bind("0.0.0.0", "secret-token")


def test_validate_nonlocal_without_token_raises() -> None:
    with pytest.raises(SystemExit, match="Security error"):
        validate_http_bind("0.0.0.0", None)


def test_validate_nonlocal_empty_token_raises() -> None:
    with pytest.raises(SystemExit, match="Security error"):
        validate_http_bind("0.0.0.0", "")


# ---- bearer_auth_middleware ----


def _run(coro):
    return asyncio.run(coro)


def _make_scope(scope_type, auth_header=None):
    headers = []
    if auth_header is not None:
        headers.append((b"authorization", auth_header.encode("utf-8")))
    return {"type": scope_type, "headers": headers}


class _Recorder:
    def __init__(self):
        self.calls = []
        self.app_called = False

    async def app(self, scope, receive, send):
        self.app_called = True

    async def send(self, message):
        self.calls.append(message)


def test_middleware_valid_token_passes_through() -> None:
    rec = _Recorder()
    mw = bearer_auth_middleware(rec.app, "secret")
    scope = _make_scope("http", "Bearer secret")
    _run(mw(scope, None, rec.send))
    assert rec.app_called is True
    assert rec.calls == []


def test_middleware_invalid_token_returns_401() -> None:
    rec = _Recorder()
    mw = bearer_auth_middleware(rec.app, "secret")
    scope = _make_scope("http", "Bearer wrong")
    _run(mw(scope, None, rec.send))
    assert rec.app_called is False
    assert rec.calls[0]["status"] == 401
    assert rec.calls[1]["body"] == b"Unauthorized"


def test_middleware_missing_header_returns_401() -> None:
    rec = _Recorder()
    mw = bearer_auth_middleware(rec.app, "secret")
    scope = _make_scope("http")
    _run(mw(scope, None, rec.send))
    assert rec.app_called is False
    assert rec.calls[0]["status"] == 401


def test_middleware_websocket_invalid_closes_4401() -> None:
    rec = _Recorder()
    mw = bearer_auth_middleware(rec.app, "secret")
    scope = _make_scope("websocket", "Bearer wrong")
    _run(mw(scope, None, rec.send))
    assert rec.app_called is False
    assert rec.calls[0] == {"type": "websocket.close", "code": 4401}


def test_middleware_non_http_scope_passes_through() -> None:
    rec = _Recorder()
    mw = bearer_auth_middleware(rec.app, "secret")
    scope = {"type": "lifespan"}
    _run(mw(scope, None, rec.send))
    assert rec.app_called is True
    assert rec.calls == []
