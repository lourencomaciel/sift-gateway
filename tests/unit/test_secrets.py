from __future__ import annotations

import os
from pathlib import Path

from mcp_artifact_gateway.cursor.secrets import load_or_create_cursor_secrets


def test_load_or_create_cursor_secrets_roundtrip(tmp_path: Path) -> None:
    path = tmp_path / "state" / "secrets.json"

    first = load_or_create_cursor_secrets(path)
    second = load_or_create_cursor_secrets(path)

    assert first.signing_version == "v1"
    assert first.active == second.active
    if os.name == "posix":
        assert (path.stat().st_mode & 0o777) == 0o600
