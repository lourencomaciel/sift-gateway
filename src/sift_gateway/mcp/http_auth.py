"""HTTP authentication for URL transport mode.

Validates that non-local HTTP binds have proper authentication
configured and provides ASGI middleware that enforces bearer
token checks on every incoming HTTP request.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
import hmac
from typing import Any

_LOCAL_HOSTS = frozenset({"127.0.0.1", "localhost", "::1"})

Scope = dict[str, Any]
Receive = Callable[[], Awaitable[dict[str, Any]]]
Send = Callable[[dict[str, Any]], Awaitable[None]]
AsgiApp = Callable[[Scope, Receive, Send], Awaitable[None]]


def is_local_host(host: str) -> bool:
    """Check whether the given host refers to the loopback interface.

    Args:
        host: Hostname or IP address to check.

    Returns:
        ``True`` if *host* resolves to a loopback address,
        ``False`` otherwise.  Note that ``0.0.0.0`` is **not**
        considered local because it binds all interfaces and is
        externally accessible.
    """
    return host in _LOCAL_HOSTS


def validate_http_bind(
    host: str,
    auth_token: str | None,
) -> None:
    """Ensure non-local HTTP binds have authentication configured.

    Local binds (loopback addresses) are allowed without a token.
    Non-local binds require an explicit bearer token so that the
    server is not accidentally exposed without authentication.

    Args:
        host: The host address the server will bind to.
        auth_token: Bearer token for authenticating HTTP
            requests, or ``None`` if not configured.

    Raises:
        SystemExit: If *host* is non-local and *auth_token* is
            ``None`` or empty.
    """
    if is_local_host(host):
        return
    if not auth_token:
        raise SystemExit(
            f"Security error: Non-local HTTP bind ({host}) "
            "requires --auth-token or "
            "SIFT_GATEWAY_AUTH_TOKEN environment variable."
        )


def bearer_auth_middleware(
    app: AsgiApp,
    auth_token: str,
) -> AsgiApp:
    """Wrap an ASGI app with bearer token authentication.

    Every incoming HTTP request must include an
    ``Authorization: Bearer <token>`` header matching the
    configured token. Requests without a valid token receive
    a ``401 Unauthorized`` response.

    Args:
        app: The ASGI application to wrap.
        auth_token: Expected bearer token value.

    Returns:
        An ASGI callable that checks the token before
        delegating to *app*.
    """
    expected = f"Bearer {auth_token}"

    async def _middleware(
        scope: Scope,
        receive: Receive,
        send: Send,
    ) -> None:
        if scope["type"] in ("http", "websocket"):
            headers = dict(scope.get("headers", []))
            auth_header = headers.get(b"authorization", b"").decode(
                "utf-8", errors="replace"
            )
            if not hmac.compare_digest(auth_header, expected):
                if scope["type"] == "http":
                    await send(
                        {
                            "type": "http.response.start",
                            "status": 401,
                            "headers": [
                                [
                                    b"content-type",
                                    b"text/plain",
                                ],
                            ],
                        }
                    )
                    await send(
                        {
                            "type": "http.response.body",
                            "body": b"Unauthorized",
                        }
                    )
                    return
                # For websocket, close the connection
                await send(
                    {
                        "type": "websocket.close",
                        "code": 4401,
                    }
                )
                return

        await app(scope, receive, send)

    return _middleware
