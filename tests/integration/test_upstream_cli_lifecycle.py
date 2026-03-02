"""End-to-end integration test for the upstream admin CLI lifecycle.

Exercises the full CRUD lifecycle by invoking the real CLI subprocess
against a temporary data directory and asserting outcomes via stdout.
"""

from __future__ import annotations

import http.client
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time


def _clean_env() -> dict[str, str]:
    """Return a copy of ``os.environ`` without ``SIFT_GATEWAY_*`` vars."""
    return {
        k: v
        for k, v in os.environ.items()
        if not k.startswith("SIFT_GATEWAY_")
    }


def _run_cli(
    data_dir: Path,
    *args: str,
) -> subprocess.CompletedProcess[str]:
    """Invoke the gateway CLI in a subprocess.

    Args:
        data_dir: Temporary data directory for state isolation.
        *args: CLI arguments after ``--data-dir``.

    Returns:
        Completed process with captured stdout/stderr.
    """
    cmd = [
        sys.executable,
        "-c",
        "from sift_gateway.main import cli; cli()",
        "--data-dir",
        str(data_dir),
        *args,
    ]
    return subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=30,
        env=_clean_env(),
    )


def _pick_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _http_status(
    *,
    port: int,
    path: str,
) -> int:
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=1.0)
    try:
        conn.request("GET", path)
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
                "oauth upstream exited before startup "
                f"(code={proc.returncode})\n"
                f"stdout:\n{stdout}\n"
                f"stderr:\n{stderr}"
            )
            raise AssertionError(msg)
        try:
            _http_status(port=port, path=path)
            return
        except OSError:
            time.sleep(0.05)
    raise AssertionError(
        f"Timed out waiting for oauth upstream on 127.0.0.1:{port}"
    )


def _run_oauth_upstream_server(
    *,
    port: int,
    path: str,
) -> subprocess.Popen[str]:
    script = f"""
from fastmcp import FastMCP
from fastmcp.server.auth.providers.in_memory import InMemoryOAuthProvider
from mcp.server.auth.settings import ClientRegistrationOptions

app = FastMCP(
    "oauth-upstream",
    auth=InMemoryOAuthProvider(
        base_url="http://127.0.0.1:{port}{path}",
        client_registration_options=ClientRegistrationOptions(enabled=True),
    ),
)

@app.tool
def ping() -> str:
    return "pong"

app.run(
    transport="streamable-http",
    host="127.0.0.1",
    port={port},
    path={path!r},
    show_banner=False,
)
"""
    cmd = [sys.executable, "-c", script]
    return subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=_clean_env(),
    )


def test_upstream_lifecycle(tmp_path: Path) -> None:
    """Full CRUD lifecycle: add, list, inspect, disable, enable, auth, remove."""
    data_dir = tmp_path / "gateway"

    # 1. list on empty state
    proc = _run_cli(data_dir, "upstream", "list")
    assert proc.returncode == 0
    assert "No upstreams configured." in proc.stdout

    # 2. snippet-based add (stdio)
    snippet = json.dumps(
        {"my-mcp": {"command": "echo", "args": ["hi"]}}
    )
    proc = _run_cli(data_dir, "upstream", "add", snippet)
    assert proc.returncode == 0, proc.stderr

    # 3. flag-based add (http)
    proc = _run_cli(
        data_dir,
        "upstream",
        "add",
        "--name",
        "http-api",
        "--transport",
        "http",
        "--url",
        "http://localhost:9999/mcp",
        "--header",
        "X-Key=val",
    )
    assert proc.returncode == 0, proc.stderr

    # 4. list --json shows both upstreams
    proc = _run_cli(data_dir, "upstream", "list", "--json")
    assert proc.returncode == 0, proc.stderr
    rows = json.loads(proc.stdout)
    names = {row["name"] for row in rows}
    assert names == {"my-mcp", "http-api"}

    stdio_row = next(r for r in rows if r["name"] == "my-mcp")
    assert stdio_row["transport"] == "stdio"
    assert stdio_row["enabled"] is True
    assert stdio_row["command"] == "echo"

    http_row = next(r for r in rows if r["name"] == "http-api")
    assert http_row["transport"] == "http"
    assert http_row["enabled"] is True
    assert http_row["url"] == "http://localhost:9999/mcp"

    # 5. inspect --json for stdio upstream
    proc = _run_cli(
        data_dir, "upstream", "inspect", "--server", "my-mcp", "--json"
    )
    assert proc.returncode == 0, proc.stderr
    detail = json.loads(proc.stdout)
    assert detail["name"] == "my-mcp"
    assert detail["transport"] == "stdio"
    assert detail["enabled"] is True
    assert detail["command"] == "echo"
    assert detail["args"] == ["hi"]

    # 6. disable
    proc = _run_cli(
        data_dir, "upstream", "disable", "--server", "my-mcp"
    )
    assert proc.returncode == 0, proc.stderr

    # 7. list --json shows disabled
    proc = _run_cli(data_dir, "upstream", "list", "--json")
    assert proc.returncode == 0
    rows = json.loads(proc.stdout)
    disabled_row = next(r for r in rows if r["name"] == "my-mcp")
    assert disabled_row["enabled"] is False

    # 8. enable
    proc = _run_cli(
        data_dir, "upstream", "enable", "--server", "my-mcp"
    )
    assert proc.returncode == 0, proc.stderr

    # verify re-enabled
    proc = _run_cli(data_dir, "upstream", "list", "--json")
    assert proc.returncode == 0, proc.stderr
    rows = json.loads(proc.stdout)
    enabled_row = next(r for r in rows if r["name"] == "my-mcp")
    assert enabled_row["enabled"] is True

    # 9. auth set for http upstream
    proc = _run_cli(
        data_dir,
        "upstream",
        "auth",
        "set",
        "--server",
        "http-api",
        "--header",
        "Authorization=Bearer tok",
    )
    assert proc.returncode == 0, proc.stderr

    # 10. inspect http upstream — verify secret metadata
    proc = _run_cli(
        data_dir,
        "upstream",
        "inspect",
        "--server",
        "http-api",
        "--json",
    )
    assert proc.returncode == 0, proc.stderr
    detail = json.loads(proc.stdout)
    assert detail["name"] == "http-api"
    assert detail["transport"] == "http"
    secret = detail.get("secret")
    assert secret is not None
    assert isinstance(secret.get("ref"), str)
    header_keys = secret.get("header_keys", [])
    assert "Authorization" in header_keys
    assert "X-Key" in header_keys, "auth set should merge with existing headers"

    # 11. remove --dry-run has no side effects
    proc = _run_cli(
        data_dir,
        "upstream",
        "remove",
        "--server",
        "my-mcp",
        "--dry-run",
    )
    assert proc.returncode == 0, proc.stderr

    proc = _run_cli(data_dir, "upstream", "list", "--json")
    rows = json.loads(proc.stdout)
    assert len(rows) == 2, "dry-run should not remove anything"

    # 12. actual remove
    proc = _run_cli(
        data_dir, "upstream", "remove", "--server", "my-mcp"
    )
    assert proc.returncode == 0, proc.stderr

    # 13. list confirms only 1 upstream remains
    proc = _run_cli(data_dir, "upstream", "list", "--json")
    assert proc.returncode == 0
    rows = json.loads(proc.stdout)
    assert len(rows) == 1
    assert rows[0]["name"] == "http-api"


def test_upstream_login_headless_end_to_end(tmp_path: Path) -> None:
    """Headless OAuth login flow: add HTTP upstream, login, probe successfully."""
    data_dir = tmp_path / "gateway"
    port = _pick_free_port()
    path = "/mcp"

    upstream_proc = _run_oauth_upstream_server(port=port, path=path)
    try:
        _wait_until_listening(upstream_proc, port=port, path=path)
        upstream_url = f"http://127.0.0.1:{port}{path}"

        proc = _run_cli(
            data_dir,
            "upstream",
            "add",
            "--name",
            "oauth-api",
            "--transport",
            "http",
            "--url",
            upstream_url,
        )
        assert proc.returncode == 0, proc.stderr

        proc = _run_cli(
            data_dir,
            "upstream",
            "login",
            "--server",
            "oauth-api",
            "--headless",
            "--json",
        )
        assert proc.returncode == 0, proc.stderr
        payload = json.loads(proc.stdout)
        assert payload["server"] == "oauth-api"
        assert payload["login"] == "oauth"
        assert "Authorization" in payload["updated_header_keys"]

        proc = _run_cli(
            data_dir,
            "upstream",
            "test",
            "--server",
            "oauth-api",
        )
        assert proc.returncode == 0, proc.stderr
        assert "ok oauth-api" in proc.stdout
    finally:
        upstream_proc.terminate()
        try:
            upstream_proc.wait(timeout=5.0)
        except subprocess.TimeoutExpired:
            upstream_proc.kill()
            upstream_proc.wait(timeout=5.0)


def test_upstream_login_runtime_ignores_stale_auth_header(
    tmp_path: Path,
) -> None:
    """Runtime should use persisted OAuth cache over static auth header."""
    data_dir = tmp_path / "gateway"
    port = _pick_free_port()
    path = "/mcp"

    upstream_proc = _run_oauth_upstream_server(port=port, path=path)
    try:
        _wait_until_listening(upstream_proc, port=port, path=path)
        upstream_url = f"http://127.0.0.1:{port}{path}"

        proc = _run_cli(
            data_dir,
            "upstream",
            "add",
            "--name",
            "oauth-api",
            "--transport",
            "http",
            "--url",
            upstream_url,
        )
        assert proc.returncode == 0, proc.stderr

        proc = _run_cli(
            data_dir,
            "upstream",
            "login",
            "--server",
            "oauth-api",
            "--headless",
        )
        assert proc.returncode == 0, proc.stderr

        secret_path = (
            data_dir / "state" / "upstream_secrets" / "oauth-api.json"
        )
        payload = json.loads(secret_path.read_text(encoding="utf-8"))
        headers = payload.get("headers")
        assert isinstance(headers, dict)
        headers["Authorization"] = "Bearer definitely-invalid-static-token"
        payload["headers"] = headers
        secret_path.write_text(json.dumps(payload), encoding="utf-8")

        proc = _run_cli(
            data_dir,
            "upstream",
            "test",
            "--server",
            "oauth-api",
        )
        assert proc.returncode == 0, proc.stderr
        assert "ok oauth-api" in proc.stdout
    finally:
        upstream_proc.terminate()
        try:
            upstream_proc.wait(timeout=5.0)
        except subprocess.TimeoutExpired:
            upstream_proc.kill()
            upstream_proc.wait(timeout=5.0)
