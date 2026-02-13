from __future__ import annotations

import http.client
from pathlib import Path
import socket
import subprocess
import sys
import time


def _pick_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _http_status(
    port: int,
    path: str,
    token: str | None = None,
) -> int:
    headers: dict[str, str] = {}
    if token is not None:
        headers["Authorization"] = f"Bearer {token}"

    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=1.0)
    try:
        conn.request("GET", path, headers=headers)
        response = conn.getresponse()
        status = response.status
        response.close()
        return status
    finally:
        conn.close()


def _wait_until_listening(
    proc: subprocess.Popen[str],
    *,
    port: int,
    path: str,
    timeout_seconds: float = 10.0,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            stdout, stderr = proc.communicate(timeout=1.0)
            msg = (
                "sift-mcp exited before startup "
                f"(code={proc.returncode})\n"
                f"stdout:\n{stdout}\n"
                f"stderr:\n{stderr}"
            )
            raise AssertionError(msg)
        try:
            _http_status(port, path)
            return
        except OSError:
            time.sleep(0.05)

    raise AssertionError(
        f"Timed out waiting for sift-mcp to listen on 127.0.0.1:{port}"
    )


def test_http_auth_token_enforced_end_to_end(tmp_path: Path) -> None:
    token = "test-token-123"
    port = _pick_free_port()
    path = "/mcp"
    data_dir = tmp_path / "data"

    cmd = [
        sys.executable,
        "-c",
        "from sift_mcp.main import cli; cli()",
        "--data-dir",
        str(data_dir),
        "--transport",
        "sse",
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
        "--path",
        path,
        "--auth-token",
        token,
    ]

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        _wait_until_listening(proc, port=port, path=path)

        assert _http_status(port, path) == 401
        assert _http_status(port, path, token="wrong-token") == 401
        assert _http_status(port, path, token=token) != 401
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5.0)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5.0)
