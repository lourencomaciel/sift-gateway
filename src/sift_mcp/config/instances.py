"""Manage per-source Sift instances and registry metadata."""

from __future__ import annotations

import contextlib
from datetime import UTC, datetime
import hashlib
import json
import os
from pathlib import Path
from typing import Any

from sift_mcp.constants import CONFIG_FILENAME, STATE_SUBDIR


def instances_root() -> Path:
    """Return the root directory for managed Sift instances."""
    override = os.environ.get("SIFT_MCP_INSTANCES_DIR")
    if override:
        return Path(override).expanduser().resolve()
    return (Path.home() / ".sift-mcp" / "instances").resolve()


def registry_path() -> Path:
    """Return the registry file path."""
    return instances_root() / "registry.json"


def instance_id_for_source(source_path: Path) -> str:
    """Build a stable, readable instance id for a source config path."""
    source = source_path.expanduser().resolve()
    client = _detect_client_hint(source)
    label = _derive_label(source)
    digest = hashlib.sha1(str(source).encode("utf-8")).hexdigest()[:8]
    return f"{client}-{label}-{digest}"


def default_instance_data_dir(source_path: Path) -> Path:
    """Return the default data dir for a source config path."""
    return instances_root() / instance_id_for_source(source_path)


def resolve_instance_data_dir(
    source_path: Path,
    *,
    require_existing: bool = False,
) -> Path:
    """Resolve data dir for source path from registry or deterministic default."""
    source = source_path.expanduser().resolve()
    entry = find_instance_by_source(source)
    if entry is not None:
        raw = entry.get("data_dir")
        if isinstance(raw, str):
            configured = Path(raw).expanduser().resolve()
            if not require_existing or _has_instance_config(configured):
                return configured

    data_dir = default_instance_data_dir(source)
    if _has_instance_config(data_dir):
        return data_dir

    if require_existing:
        msg = (
            "No initialized Sift instance found for source "
            f"{source}. Run 'sift-mcp init --from {source}' first, "
            "or pass --data-dir explicitly."
        )
        raise ValueError(msg)

    return data_dir


def get_instance_data_dir(instance_id: str) -> Path:
    """Resolve data dir for an instance id from registry."""
    entry = find_instance_by_id(instance_id)
    if entry is None:
        msg = f"unknown instance id: {instance_id}"
        raise ValueError(msg)
    raw = entry.get("data_dir")
    if not isinstance(raw, str):
        msg = f"instance '{instance_id}' has invalid data_dir in registry"
        raise ValueError(msg)
    data_dir = Path(raw).expanduser().resolve()
    if not _has_instance_config(data_dir):
        msg = (
            f"instance '{instance_id}' has no initialized config at "
            f"{data_dir / STATE_SUBDIR / CONFIG_FILENAME}"
        )
        raise ValueError(msg)
    return data_dir


def load_registry() -> dict[str, Any]:
    """Load instance registry from disk (or empty registry)."""
    path = registry_path()
    if not path.exists():
        return {"version": 1, "instances": []}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {"version": 1, "instances": []}
    if not isinstance(raw, dict):
        return {"version": 1, "instances": []}
    instances = raw.get("instances")
    if not isinstance(instances, list):
        raw["instances"] = []
    raw.setdefault("version", 1)
    return raw


def save_registry(registry: dict[str, Any]) -> None:
    """Persist registry to disk."""
    import tempfile

    root = instances_root()
    root.mkdir(parents=True, exist_ok=True)
    path = registry_path()
    content = json.dumps(registry, indent=2, ensure_ascii=False) + "\n"
    fd, tmp = tempfile.mkstemp(dir=str(root), suffix=".tmp")
    try:
        os.write(fd, content.encode("utf-8"))
        os.close(fd)
        fd = -1
        os.replace(tmp, str(path))
    except BaseException:
        if fd >= 0:
            os.close(fd)
        with contextlib.suppress(OSError):
            os.unlink(tmp)
        raise


def upsert_instance(
    *,
    source_path: Path,
    data_dir: Path,
) -> dict[str, Any]:
    """Create or update registry entry for a source/data_dir pair."""
    source = source_path.expanduser().resolve()
    data = data_dir.expanduser().resolve()
    instance_id = instance_id_for_source(source)
    now = _utc_now_iso()
    client = _detect_client_hint(source)
    label = _derive_label(source)

    registry = load_registry()
    entries = registry.get("instances")
    if not isinstance(entries, list):
        entries = []
        registry["instances"] = entries

    updated: dict[str, Any] | None = None
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        if entry.get("id") == instance_id:
            entry["source_path"] = str(source)
            entry["data_dir"] = str(data)
            entry["client"] = client
            entry["label"] = label
            entry["last_used_at"] = now
            entry.setdefault("created_at", now)
            updated = entry
            break

    if updated is None:
        updated = {
            "id": instance_id,
            "source_path": str(source),
            "data_dir": str(data),
            "client": client,
            "label": label,
            "created_at": now,
            "last_used_at": now,
        }
        entries.append(updated)

    save_registry(registry)
    return dict(updated)


def touch_instance_by_id(instance_id: str) -> None:
    """Update last_used_at for a registry instance id."""
    registry = load_registry()
    entries = registry.get("instances")
    if not isinstance(entries, list):
        return
    now = _utc_now_iso()
    changed = False
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        if entry.get("id") == instance_id:
            entry["last_used_at"] = now
            changed = True
            break
    if changed:
        save_registry(registry)


def find_instance_by_source(source_path: Path) -> dict[str, Any] | None:
    """Find registry entry by source path."""
    source = str(source_path.expanduser().resolve())
    registry = load_registry()
    entries = registry.get("instances")
    if not isinstance(entries, list):
        return None
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        if entry.get("source_path") == source:
            return dict(entry)
    return None


def find_instance_by_id(instance_id: str) -> dict[str, Any] | None:
    """Find registry entry by instance id."""
    registry = load_registry()
    entries = registry.get("instances")
    if not isinstance(entries, list):
        return None
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        if entry.get("id") == instance_id:
            return dict(entry)
    return None


def list_instances() -> list[dict[str, Any]]:
    """List registry entries sorted by last-used timestamp descending."""
    registry = load_registry()
    entries = registry.get("instances")
    if not isinstance(entries, list):
        return []
    result = [dict(e) for e in entries if isinstance(e, dict)]
    result.sort(
        key=lambda e: str(e.get("last_used_at", "")),
        reverse=True,
    )
    return result


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _detect_client_hint(source: Path) -> str:
    parts = [p.lower() for p in source.parts]
    name = source.name.lower()

    if name == "claude_desktop_config.json":
        return "claude"
    if name in {".mcp.json", ".claude.json"} or ".claude" in parts:
        return "claude-code"
    if "cursor" in parts:
        return "cursor"
    if "windsurf" in parts or "codeium" in parts:
        return "windsurf"
    if "zed" in parts:
        return "zed"
    if ".vscode" in parts or ("code" in parts and "user" in parts):
        return "vscode"
    return "mcp"


def _derive_label(source: Path) -> str:
    name = source.name
    parent = source.parent

    if name == ".mcp.json":
        raw = parent.name
    elif (
        (
            name in {"settings.local.json", "settings.json"}
            and parent.name == ".claude"
        )
        or (name == "mcp.json" and parent.name == ".vscode")
        or (name == "settings.json" and parent.name == ".zed")
    ):
        raw = parent.parent.name
    elif name == "claude_desktop_config.json":
        raw = "desktop"
    else:
        raw = source.stem
        if raw in {"config", "settings", "mcp"}:
            raw = parent.name

    slug = _slugify(raw)
    return slug or "default"


def _slugify(value: str) -> str:
    out: list[str] = []
    prev_dash = False
    for char in value.lower():
        if char.isalnum():
            out.append(char)
            prev_dash = False
            continue
        if not prev_dash:
            out.append("-")
            prev_dash = True
    slug = "".join(out).strip("-")
    if len(slug) > 32:
        slug = slug[:32].rstrip("-")
    return slug


def _has_instance_config(data_dir: Path) -> bool:
    config_path = data_dir / STATE_SUBDIR / CONFIG_FILENAME
    return config_path.is_file()
