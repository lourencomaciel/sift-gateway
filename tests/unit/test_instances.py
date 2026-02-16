from __future__ import annotations

import json
from pathlib import Path
import re

import pytest

import sift_mcp.config.instances as instances_mod
from sift_mcp.config.instances import (
    _utc_now_iso,
    default_instance_data_dir,
    get_instance_data_dir,
    instance_id_for_source,
    resolve_instance_data_dir,
    upsert_instance,
)


def _touch_config(data_dir: Path) -> None:
    state_dir = data_dir / "state"
    state_dir.mkdir(parents=True, exist_ok=True)
    (state_dir / "config.json").write_text(
        json.dumps({"mcpServers": {}}),
        encoding="utf-8",
    )


def test_instance_id_for_source_is_readable(tmp_path: Path) -> None:
    source = tmp_path / "project-name" / ".mcp.json"
    instance_id = instance_id_for_source(source)

    assert instance_id.startswith("claude-code-project-name-")


def test_upsert_and_get_instance_data_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(
        "SIFT_MCP_INSTANCES_DIR",
        str(tmp_path / "instances-root"),
    )
    source = tmp_path / "repo" / "config.json"
    data_dir = tmp_path / "data" / "tenant-a"
    _touch_config(data_dir)

    entry = upsert_instance(source_path=source, data_dir=data_dir)

    resolved = get_instance_data_dir(entry["id"])
    assert resolved == data_dir.resolve()


def test_resolve_instance_data_dir_defaults_without_registry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(
        "SIFT_MCP_INSTANCES_DIR",
        str(tmp_path / "instances-root"),
    )
    source = tmp_path / "repo" / "config.json"

    resolved = resolve_instance_data_dir(source)

    assert resolved == default_instance_data_dir(source).resolve()


def test_resolve_instance_data_dir_require_existing_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(
        "SIFT_MCP_INSTANCES_DIR",
        str(tmp_path / "instances-root"),
    )
    source = tmp_path / "repo" / "config.json"

    with pytest.raises(ValueError, match="No initialized Sift instance found"):
        resolve_instance_data_dir(source, require_existing=True)


def test_resolve_instance_data_dir_uses_existing_default_config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(
        "SIFT_MCP_INSTANCES_DIR",
        str(tmp_path / "instances-root"),
    )
    source = tmp_path / "repo" / "config.json"
    default_data_dir = default_instance_data_dir(source)
    _touch_config(default_data_dir)

    resolved = resolve_instance_data_dir(source, require_existing=True)

    assert resolved == default_data_dir.resolve()


def test_utc_now_iso_includes_microseconds() -> None:
    stamp = _utc_now_iso()
    assert re.fullmatch(
        r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z",
        stamp,
    )


def test_save_registry_preserves_existing_file_on_replace_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(
        "SIFT_MCP_INSTANCES_DIR",
        str(tmp_path / "instances-root"),
    )
    path = instances_mod.registry_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    original = {
        "version": 1,
        "instances": [{"id": "old", "source_path": "/a", "data_dir": "/b"}],
    }
    path.write_text(json.dumps(original), encoding="utf-8")

    def _raise_replace(_src: str, _dst: str) -> None:
        raise OSError("replace failed")

    monkeypatch.setattr(instances_mod.os, "replace", _raise_replace)

    with pytest.raises(OSError, match="replace failed"):
        instances_mod.save_registry(
            {
                "version": 1,
                "instances": [
                    {"id": "new", "source_path": "/x", "data_dir": "/y"}
                ],
            }
        )

    after = json.loads(path.read_text(encoding="utf-8"))
    assert after == original
