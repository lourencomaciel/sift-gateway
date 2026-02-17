"""Provide stdio transport compatibility for framed and newline JSON.

The upstream ``mcp.server.stdio`` transport in current dependencies
parses newline-delimited JSON only. Some MCP clients use framed stdio
messages with ``Content-Length`` headers. This module implements a
compatibility transport that auto-detects input mode and mirrors that
mode for output so both client families can interoperate.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from functools import partial
import os
import sys
from typing import Any

import anyio
from anyio.streams.memory import (
    MemoryObjectReceiveStream,
    MemoryObjectSendStream,
)
from mcp.server.lowlevel.server import NotificationOptions
from mcp.shared.message import SessionMessage
import mcp.types as types


@dataclass
class _ModeState:
    """Track the negotiated stdio wire mode for this session."""

    value: str | None = None  # ``"framed"`` | ``"line"``


@dataclass(frozen=True)
class _ParseResult:
    """Represent a parsed message (or parse error) from a byte buffer."""

    payload: bytes
    consumed: int
    error: str | None = None


def _detect_mode(buffer: bytes) -> str | None:
    """Detect framed-vs-line mode from current input buffer prefix."""
    if not buffer:
        return None
    trimmed = buffer.lstrip(b" \t\r\n")
    if not trimmed:
        return None
    if trimmed[:1] in {b"{", b"["}:
        return "line"
    first_line_end = trimmed.find(b"\n")
    if first_line_end >= 0:
        first_line = trimmed[:first_line_end].rstrip(b"\r")
    else:
        first_line = trimmed
    if _looks_like_header_line(first_line):
        return "framed"
    return None


def _looks_like_header_line(line: bytes) -> bool:
    """Return whether *line* looks like an RFC-7230 header field."""
    key, sep, _value = line.partition(b":")
    if not sep:
        return False
    key = key.strip()
    if not key:
        return False

    header_token_chars = b"!#$%&'*+-.^_`|~"
    return all(
        (
            48 <= byte <= 57
            or 65 <= byte <= 90
            or 97 <= byte <= 122
            or byte in header_token_chars
        )
        for byte in key
    )


def _parse_line_message(buffer: bytes) -> _ParseResult | None:
    """Parse one newline-delimited JSON message from ``buffer``."""
    line_end = buffer.find(b"\n")
    if line_end < 0:
        return None
    raw_line = buffer[:line_end].rstrip(b"\r")
    return _ParseResult(payload=raw_line, consumed=line_end + 1)


def _header_end(buffer: bytes) -> tuple[int, int] | None:
    """Return ``(offset, delimiter_len)`` for framed header termination."""
    offset = buffer.find(b"\r\n\r\n")
    if offset >= 0:
        return offset, 4
    offset = buffer.find(b"\n\n")
    if offset >= 0:
        return offset, 2
    return None


def _parse_framed_message(buffer: bytes) -> _ParseResult | None:
    """Parse one ``Content-Length`` framed JSON message from ``buffer``."""
    header_pos = _header_end(buffer)
    if header_pos is None:
        return None
    header_end, delim_len = header_pos
    consumed_headers = header_end + delim_len
    header_blob = buffer[:header_end]

    content_length: int | None = None
    for raw_line in header_blob.replace(b"\r", b"").split(b"\n"):
        line = raw_line.strip()
        if not line:
            continue
        key, sep, value = line.partition(b":")
        if not sep:
            continue
        if key.strip().lower() != b"content-length":
            continue
        try:
            content_length = int(value.strip().decode("ascii"))
        except (UnicodeDecodeError, ValueError):
            return _ParseResult(
                payload=b"",
                consumed=consumed_headers,
                error="invalid Content-Length header",
            )
        break

    if content_length is None:
        return _ParseResult(
            payload=b"",
            consumed=consumed_headers,
            error="missing Content-Length header",
        )
    if content_length < 0:
        return _ParseResult(
            payload=b"",
            consumed=consumed_headers,
            error="negative Content-Length header",
        )

    body_end = consumed_headers + content_length
    if len(buffer) < body_end:
        return None

    return _ParseResult(
        payload=buffer[consumed_headers:body_end],
        consumed=body_end,
    )


def _parse_next_message(
    buffer: bytes,
    mode_state: _ModeState,
) -> _ParseResult | None:
    """Parse exactly one message using auto-detected transport mode."""
    mode = mode_state.value or _detect_mode(buffer)
    if mode is None:
        return None
    mode_state.value = mode
    if mode == "framed":
        return _parse_framed_message(buffer)
    return _parse_line_message(buffer)


def _encode_output(payload: bytes, mode: str | None) -> bytes:
    """Encode outbound JSON payload using negotiated transport mode."""
    if mode == "framed":
        header = f"Content-Length: {len(payload)}\r\n\r\n".encode("ascii")
        return header + payload
    return payload + b"\n"


def _read_stdin_chunk() -> bytes:
    """Read one chunk from process stdin as raw bytes."""
    return os.read(sys.stdin.fileno(), 4096)


def _write_stdout_chunk(payload: bytes) -> None:
    """Write full payload to process stdout as raw bytes."""
    offset = 0
    fd = sys.stdout.fileno()
    while offset < len(payload):
        written = os.write(fd, payload[offset:])
        if written <= 0:  # pragma: no cover
            msg = "stdout write failed"
            raise OSError(msg)
        offset += written


@asynccontextmanager
async def stdio_server_compat() -> AsyncIterator[
    tuple[
        MemoryObjectReceiveStream[SessionMessage | Exception],
        MemoryObjectSendStream[SessionMessage],
    ]
]:
    """Yield MCP stdio streams supporting framed and line JSON protocols."""
    read_stream: MemoryObjectReceiveStream[SessionMessage | Exception]
    read_stream_writer: MemoryObjectSendStream[SessionMessage | Exception]
    write_stream: MemoryObjectSendStream[SessionMessage]
    write_stream_reader: MemoryObjectReceiveStream[SessionMessage]

    read_stream_writer, read_stream = anyio.create_memory_object_stream(0)
    write_stream, write_stream_reader = anyio.create_memory_object_stream(0)
    mode_state = _ModeState()

    async def stdin_reader() -> None:
        buffer = b""
        try:
            async with read_stream_writer:
                while True:
                    chunk = await anyio.to_thread.run_sync(_read_stdin_chunk)
                    if not chunk:
                        break
                    buffer += chunk

                    while True:
                        parsed = _parse_next_message(buffer, mode_state)
                        if parsed is None:
                            break
                        buffer = buffer[parsed.consumed :]
                        if parsed.error is not None:
                            await read_stream_writer.send(
                                ValueError(parsed.error)
                            )
                            continue
                        if not parsed.payload.strip():
                            continue
                        try:
                            message = types.JSONRPCMessage.model_validate_json(
                                parsed.payload
                            )
                        except Exception as exc:  # pragma: no cover
                            await read_stream_writer.send(exc)
                            continue
                        await read_stream_writer.send(SessionMessage(message))

                # EOF fallback for line-mode clients that omit trailing newline.
                if mode_state.value == "line" and buffer.strip():
                    try:
                        message = types.JSONRPCMessage.model_validate_json(
                            buffer
                        )
                    except Exception as exc:  # pragma: no cover
                        await read_stream_writer.send(exc)
                    else:
                        await read_stream_writer.send(SessionMessage(message))
        except anyio.ClosedResourceError:  # pragma: no cover
            await anyio.lowlevel.checkpoint()

    async def stdout_writer() -> None:
        try:
            async with write_stream_reader:
                async for session_message in write_stream_reader:
                    payload = session_message.message.model_dump_json(
                        by_alias=True,
                        exclude_none=True,
                    ).encode("utf-8")
                    await anyio.to_thread.run_sync(
                        _write_stdout_chunk,
                        _encode_output(payload, mode_state.value),
                    )
        except anyio.ClosedResourceError:  # pragma: no cover
            await anyio.lowlevel.checkpoint()

    async with anyio.create_task_group() as task_group:
        task_group.start_soon(stdin_reader)
        task_group.start_soon(stdout_writer)
        yield read_stream, write_stream


async def run_fastmcp_stdio_async_compat(
    app: Any,
    *,
    show_banner: bool = True,
    log_level: str | None = None,
) -> None:
    """Run a FastMCP app over compatibility stdio transport."""
    from fastmcp.server.server import logger
    from fastmcp.utilities.cli import log_server_banner
    from fastmcp.utilities.logging import temporary_log_level

    if show_banner:
        log_server_banner(server=app)

    with temporary_log_level(log_level):
        async with app._lifespan_manager():
            async with stdio_server_compat() as (
                read_stream,
                write_stream,
            ):
                logger.info(
                    "Starting MCP server %r with transport %r",
                    app.name,
                    "stdio",
                )
                await app._mcp_server.run(
                    read_stream,
                    write_stream,
                    app._mcp_server.create_initialization_options(
                        notification_options=NotificationOptions(
                            tools_changed=True
                        ),
                    ),
                )


def run_fastmcp_stdio_compat(
    app: Any,
    *,
    show_banner: bool = True,
    log_level: str | None = None,
) -> None:
    """Run a FastMCP app over compatibility stdio transport (sync API)."""
    # Test doubles may expose only ``run()`` and not FastMCP internals.
    if not hasattr(app, "_lifespan_manager") or not hasattr(app, "_mcp_server"):
        app.run(show_banner=show_banner)
        return
    anyio.run(
        partial(
            run_fastmcp_stdio_async_compat,
            app,
            show_banner=show_banner,
            log_level=log_level,
        )
    )
