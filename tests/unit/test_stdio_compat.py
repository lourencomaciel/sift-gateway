"""Regression tests for stdio protocol compatibility modes."""

from __future__ import annotations

import contextlib
import json
import os
from pathlib import Path
import select
import subprocess
import sys
import time
from typing import Any


class _ServerSession:
    """Manage a sidepouch stdio subprocess for protocol tests."""

    def __init__(self, tmp_path: Path) -> None:
        env = dict(os.environ)
        src_dir = Path(__file__).resolve().parents[2] / "src"
        existing = env.get("PYTHONPATH")
        env["PYTHONPATH"] = (
            f"{src_dir}:{existing}" if existing else str(src_dir)
        )
        cmd = [
            sys.executable,
            "-c",
            "from sidepouch_mcp.main import cli; cli()",
            "--data-dir",
            str(tmp_path),
        ]
        self._process = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
        )
        self._stdout_buffer = b""

    def close(self) -> None:
        """Terminate subprocess and wait for exit."""
        self._process.terminate()
        with contextlib.suppress(subprocess.TimeoutExpired):
            self._process.wait(timeout=1.0)
        if self._process.poll() is None:
            self._process.kill()
            self._process.wait(timeout=1.0)

    def send_line(self, payload: dict[str, Any]) -> None:
        """Send newline-delimited JSON payload."""
        assert self._process.stdin is not None
        body = json.dumps(payload).encode("utf-8") + b"\n"
        self._process.stdin.write(body)
        self._process.stdin.flush()

    def send_framed(self, payload: dict[str, Any]) -> None:
        """Send Content-Length framed JSON payload."""
        assert self._process.stdin is not None
        body = json.dumps(payload).encode("utf-8")
        frame = f"Content-Length: {len(body)}\r\n\r\n".encode("ascii") + body
        self._process.stdin.write(frame)
        self._process.stdin.flush()

    def _read_chunk(self, timeout: float) -> bytes:
        assert self._process.stdout is not None
        ready, _, _ = select.select([self._process.stdout], [], [], timeout)
        if not ready:
            return b""
        return os.read(self._process.stdout.fileno(), 65536)

    def read_line_message(self, timeout: float = 5.0) -> dict[str, Any]:
        """Read one newline-delimited JSON message."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            line_end = self._stdout_buffer.find(b"\n")
            if line_end >= 0:
                raw = self._stdout_buffer[:line_end].rstrip(b"\r")
                self._stdout_buffer = self._stdout_buffer[line_end + 1 :]
                if not raw:
                    continue
                return json.loads(raw.decode("utf-8"))
            chunk = self._read_chunk(0.1)
            if chunk:
                self._stdout_buffer += chunk
                continue
            if self._process.poll() is not None:
                break
        raise AssertionError(f"timed out waiting for line message: {self._stderr()}")

    def read_framed_message(self, timeout: float = 5.0) -> dict[str, Any]:
        """Read one Content-Length framed JSON message."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            header_sep = self._stdout_buffer.find(b"\r\n\r\n")
            if header_sep >= 0:
                header = self._stdout_buffer[:header_sep]
                content_length = self._content_length(header)
                if content_length is None:
                    raise AssertionError(
                        f"missing Content-Length header: {header!r}"
                    )
                body_start = header_sep + 4
                body_end = body_start + content_length
                if len(self._stdout_buffer) < body_end:
                    chunk = self._read_chunk(0.1)
                    if chunk:
                        self._stdout_buffer += chunk
                        continue
                    if self._process.poll() is not None:
                        break
                    continue
                body = self._stdout_buffer[body_start:body_end]
                self._stdout_buffer = self._stdout_buffer[body_end:]
                return json.loads(body.decode("utf-8"))

            chunk = self._read_chunk(0.1)
            if chunk:
                self._stdout_buffer += chunk
                continue
            if self._process.poll() is not None:
                break
        raise AssertionError(
            f"timed out waiting for framed message: {self._stderr()}"
        )

    @staticmethod
    def _content_length(header_blob: bytes) -> int | None:
        for raw_line in header_blob.replace(b"\r", b"").split(b"\n"):
            line = raw_line.strip()
            if not line:
                continue
            key, sep, value = line.partition(b":")
            if not sep:
                continue
            if key.strip().lower() != b"content-length":
                continue
            return int(value.strip().decode("ascii"))
        return None

    def _stderr(self) -> str:
        assert self._process.stderr is not None
        with contextlib.suppress(Exception):
            ready, _, _ = select.select([self._process.stderr], [], [], 0)
            if ready:
                chunk = os.read(self._process.stderr.fileno(), 65536)
                if chunk:
                    return chunk.decode("utf-8", errors="replace")
        return "<no-stderr>"


def _initialize_payload(message_id: int = 1) -> dict[str, Any]:
    return {
        "jsonrpc": "2.0",
        "id": message_id,
        "method": "initialize",
        "params": {
            "protocolVersion": "2025-06-18",
            "capabilities": {},
            "clientInfo": {"name": "pytest", "version": "0.1"},
        },
    }


def _initialized_notification() -> dict[str, Any]:
    return {
        "jsonrpc": "2.0",
        "method": "notifications/initialized",
        "params": {},
    }


def _tools_list_payload(message_id: int = 2) -> dict[str, Any]:
    return {
        "jsonrpc": "2.0",
        "id": message_id,
        "method": "tools/list",
        "params": {},
    }


def _resources_list_payload(message_id: int = 3) -> dict[str, Any]:
    return {
        "jsonrpc": "2.0",
        "id": message_id,
        "method": "resources/list",
        "params": {},
    }


def test_stdio_supports_content_length_framing(tmp_path: Path) -> None:
    session = _ServerSession(tmp_path)
    try:
        session.send_framed(_initialize_payload())
        init_response = session.read_framed_message()
        assert init_response["id"] == 1
        assert "result" in init_response

        session.send_framed(_initialized_notification())
        session.send_framed(_tools_list_payload())
        tools_response = session.read_framed_message()
        assert tools_response["id"] == 2
        tools = tools_response["result"]["tools"]
        names = [tool["name"] for tool in tools]
        assert "gateway_status" in names

        session.send_framed(_resources_list_payload())
        resources_response = session.read_framed_message()
        assert resources_response["id"] == 3
        assert isinstance(resources_response["result"]["resources"], list)
    finally:
        session.close()


def test_stdio_keeps_newline_json_compatibility(tmp_path: Path) -> None:
    session = _ServerSession(tmp_path)
    try:
        session.send_line(_initialize_payload())
        init_response = session.read_line_message()
        assert init_response["id"] == 1
        assert "result" in init_response

        session.send_line(_initialized_notification())
        session.send_line(_tools_list_payload())
        tools_response = session.read_line_message()
        assert tools_response["id"] == 2
        names = [tool["name"] for tool in tools_response["result"]["tools"]]
        assert "gateway_status" in names

        session.send_line(_resources_list_payload())
        resources_response = session.read_line_message()
        assert resources_response["id"] == 3
        assert isinstance(resources_response["result"]["resources"], list)
    finally:
        session.close()
