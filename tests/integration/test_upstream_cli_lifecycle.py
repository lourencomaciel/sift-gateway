"""End-to-end integration test for the upstream admin CLI lifecycle.

Exercises the full CRUD lifecycle by invoking the real CLI subprocess
against a temporary data directory and asserting outcomes via stdout.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys


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
